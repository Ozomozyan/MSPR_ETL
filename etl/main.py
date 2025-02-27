#!/usr/bin/env python3

"""
ETL pipeline script for:
1. Resetting tables if both are empty or whenever needed (TRUNCATE + RESTART IDENTITY).
2. Filling `infos_especes` table if empty (or merging fallback columns if partial).
3. Processing images in Google Cloud Storage under "Mammifères/<SpeciesName>/..."
   -> duplication/corruption checks, resizing, and storing/updating references in `footprint_images`.
4. Running data quality tests on both tables (exhaustiveness, pertinence, accuracy) including
   missing cells/rows checks and file extension checks for images.
5. Logging the data quality results (including vectors and error descriptions) into data_quality_log table,
   also logging if any ETL step fails.
"""

import os
import hashlib
import pandas as pd
import numpy as np
import tempfile
import re
import datetime

from PIL import Image
from google.cloud import storage
from supabase import create_client, Client

# -------------------------------------------------------------------------
# 1. CONNECT TO SUPABASE
# -------------------------------------------------------------------------
def get_supabase_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
    return create_client(url, key)

# -------------------------------------------------------------------------
# 2. CONNECT TO GOOGLE CLOUD STORAGE
# -------------------------------------------------------------------------
def get_gcs_client() -> storage.Client:
    # Make sure GOOGLE_APPLICATION_CREDENTIALS is set to the path of your service account JSON
    return storage.Client()

# -------------------------------------------------------------------------
# 3. CHECK IF WE NEED TO RESET TABLES (truncate + restart identity)
# -------------------------------------------------------------------------
def maybe_reset_tables(supabase: Client):
    """
    Calls reset_my_tables() if and only if both infos_especes AND footprint_images
    are completely empty. If either has data, we skip the reset so we won't overwrite
    partially-filled data.
    """
    r_especes = supabase.table("infos_especes").select("id").limit(1).execute()
    r_images = supabase.table("footprint_images").select("id").limit(1).execute()

    count_especes = len(r_especes.data)
    count_images = len(r_images.data)

    if count_especes == 0 and count_images == 0:
        print("Both infos_especes and footprint_images are empty. Resetting tables...")
        supabase.rpc("reset_my_tables").execute()  # Must truncate BOTH tables in your DB function
        print("Tables truncated and sequences reset to 1.")
    else:
        print(
            "Tables are not both empty. Skipping reset:\n"
            f"  infos_especes has {count_especes} row(s).\n"
            f"  footprint_images has {count_images} row(s)."
        )

# -------------------------------------------------------------------------
# 4. CHECK AND FILL infos_especes TABLE (merge fallback columns, partial fill)
# -------------------------------------------------------------------------
def ensure_infos_especes_filled(
    supabase: Client,
    bucket_name: str,
    xlsx_blob_name: str = "infos_especes.xlsx",
) -> None:
    """
    Checks if infos_especes table is empty in Supabase.
    If empty, fetches the main infos_especes.xlsx from GCS.
    Then uses fallback column files (espece.xlsx, description.xlsx, etc.)
    to fill missing cells in the main DataFrame.
    Finally, inserts/upserts the resulting data into the infos_especes table.
    """
    response = supabase.table("infos_especes").select("*").execute()
    count_infos = len(response.data)

    if count_infos > 0:
        print(f"infos_especes table already has {count_infos} row(s). We'll try to fill missing cells row by row.")
    else:
        print("infos_especes table is empty. We'll insert from the GCS xlsx.")

    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)
    blob_main = bucket.blob(xlsx_blob_name)

    if not blob_main.exists():
        raise FileNotFoundError(f"Could not find {xlsx_blob_name} in bucket {bucket_name}.")

    with tempfile.TemporaryDirectory() as tmpdir:
        local_main_path = os.path.join(tmpdir, xlsx_blob_name)
        blob_main.download_to_filename(local_main_path)
        print(f"Downloaded main file: {xlsx_blob_name}")

        df_main = pd.read_excel(local_main_path)

        # fallback files
        fallback_files = [
            ("espece.xlsx", "Espèce"),
            ("description.xlsx", "Description"),
            ("nom_latin.xlsx", "Nom latin"),
            ("famille.xlsx", "Famille"),
            ("taille.xlsx", "Taille"),
            ("region.xlsx", "Région"),
            ("habitat.xlsx", "Habitat"),
            ("fun_fact.xlsx", "Fun fact"),
        ]

        # Merge missing cells from fallback DataFrames
        for fallback_blob_name, col_name in fallback_files:
            fallback_blob = bucket.blob(fallback_blob_name)
            if fallback_blob.exists():
                local_fallback_path = os.path.join(tmpdir, fallback_blob_name)
                fallback_blob.download_to_filename(local_fallback_path)
                print(f"Downloaded fallback file: {fallback_blob_name}")

                df_fallback = pd.read_excel(local_fallback_path)
                if col_name not in df_fallback.columns:
                    print(f"Warning: {fallback_blob_name} lacks column '{col_name}'. Skipping.")
                    continue

                min_rows = min(len(df_main), len(df_fallback))
                for i in range(min_rows):
                    if pd.isnull(df_main.loc[i, col_name]):
                        df_main.loc[i, col_name] = df_fallback.loc[i, col_name]
            else:
                print(f"Fallback file {fallback_blob_name} not found. Skipping fallback for {col_name}.")

        # Convert inf -> nan, then nan -> None
        df_main.replace([np.inf, -np.inf], np.nan, inplace=True)
        df_main = df_main.where(df_main.notnull(), None)

        # Rename columns for Supabase naming (e.g. spaces -> underscores)
        rename_map = {}
        for col in df_main.columns:
            clean_col = re.sub(r"\s+", "_", col.strip())
            rename_map[col] = clean_col
        df_main.rename(columns=rename_map, inplace=True)

        records = df_main.to_dict(orient="records")

        if count_infos == 0:
            # If table is empty, insert all rows
            print("Inserting all rows into infos_especes because table was empty.")
            chunk_size = 500
            for i in range(0, len(records), chunk_size):
                batch = records[i : i + chunk_size]
                supabase.table("infos_especes").insert(batch).execute()
        else:
            # If table is partially filled, do a naive row-by-row "upsert"
            # We'll assume "Espèce" is a unique identifier. Adjust as needed.
            existing_species_query = supabase.table("infos_especes").select("Espèce").execute()
            existing_species_set = set(row.get("Espèce") for row in existing_species_query.data if row.get("Espèce"))

            for rec in records:
                espece_name = rec.get("Espèce")
                # if not already in DB, insert
                if espece_name not in existing_species_set:
                    supabase.table("infos_especes").insert(rec).execute()
                else:
                    # if it's there, we do partial update on any columns that are None in DB
                    # we'll fetch the current record, compare columns, fill missing
                    current_data = (
                        supabase.table("infos_especes")
                        .select("*")
                        .eq("Espèce", espece_name)
                        .limit(1)
                        .execute()
                        .data
                    )
                    if not current_data:
                        # edge case: mismatch or concurrency
                        continue
                    current_record = current_data[0]
                    update_dict = {}
                    # For each column, if DB is None or empty, fill from rec
                    for k, v in rec.items():
                        if current_record.get(k) in [None, ""] and v not in [None, ""]:
                            update_dict[k] = v
                    if update_dict:
                        supabase.table("infos_especes").update(update_dict).eq("Espèce", espece_name).execute()

# -------------------------------------------------------------------------
# 5. PROCESS IMAGES WITH PARTIAL "UPSERT" LOGIC
# -------------------------------------------------------------------------
def process_images(
    supabase: Client,
    bucket_name: str,
    raw_folder_prefix: str = "Mammifères/",
    processed_folder_prefix: str = "processed_data/"
):
    """
    Download images from GCS, remove duplicates/corrupted, resize, re-upload to processed_data.
    Then upsert references in footprint_images table with the correct foreign key.
    - We now check if (species_id, image_name) exists; if so, we only fill missing columns.
    """
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)

    # 1. Build a species map from infos_especes
    species_map = fetch_species_map(supabase)

    # 2. List all blobs in raw_folder_prefix
    raw_blobs = bucket.list_blobs(prefix=raw_folder_prefix)
    image_candidates = []
    for blob in raw_blobs:
        # Skip directories
        if blob.name.endswith("/"):
            continue
        path_parts = blob.name.split('/')
        if len(path_parts) < 3:
            print(f"Skipping {blob.name}, no subfolder structure.")
            continue
        species_name = path_parts[1]
        image_filename = path_parts[-1]
        image_candidates.append((blob.name, species_name, image_filename))

    local_temp_raw = tempfile.mkdtemp(prefix="raw_images_")
    local_temp_processed = tempfile.mkdtemp(prefix="processed_images_")

    seen_hashes = set()
    images_to_upload = []

    for blob_name, species_name, image_filename in image_candidates:
        if species_name not in species_map:
            print(f"Species '{species_name}' not found in infos_especes. Skipping {blob_name}.")
            continue

        local_download_path = os.path.join(local_temp_raw, image_filename)
        blob = bucket.blob(blob_name)

        # Attempt download
        try:
            blob.download_to_filename(local_download_path)
        except Exception as e:
            print(f"Failed downloading {blob_name}: {e}")
            continue

        # Check corruption
        try:
            with Image.open(local_download_path) as img:
                img.verify()
        except Exception as e:
            print(f"Corrupt image detected. Skipping {blob_name}. Error: {e}")
            continue

        # Check duplicates via MD5
        filehash = hashlib.md5(open(local_download_path, 'rb').read()).hexdigest()
        if filehash in seen_hashes:
            print(f"Duplicate image detected. Skipping {blob_name}.")
            continue
        else:
            seen_hashes.add(filehash)

        # File extension check (jpg, jpeg, png, etc.)
        valid_extensions = (".jpg", ".jpeg", ".png")
        if not image_filename.lower().endswith(valid_extensions):
            print(f"Image {image_filename} does not have a valid extension. Skipping.")
            continue

        # Resize or other processing
        processed_filename = image_filename
        local_processed_path = os.path.join(local_temp_processed, processed_filename)
        try:
            with Image.open(local_download_path) as img:
                # Example: we do a basic 128x128 resize
                img_resized = img.resize((128, 128))
                img_resized.save(local_processed_path)
        except Exception as e:
            print(f"Error resizing image {blob_name}: {e}")
            continue

        images_to_upload.append((local_processed_path, species_name, processed_filename))

    # 3. Upload processed images & upsert references in footprint_images
    for local_path, species_name, processed_filename in images_to_upload:
        new_blob_path = f"{processed_folder_prefix}{species_name}/{processed_filename}"
        new_blob = bucket.blob(new_blob_path)

        try:
            new_blob.upload_from_filename(local_path)
        except Exception as e:
            print(f"Error uploading {local_path} -> {new_blob_path}: {e}")
            continue

        # Record creation or update
        record = {
            "species_id": species_map[species_name],
            "image_name": processed_filename,
            "image_url": new_blob.public_url,
            # Could also add "image_type": "jpg" or something if you have a separate column
        }

        # We'll check if this record (species_id, image_name) already exists
        # If yes, update missing fields. If no, insert.
        existing = (
            supabase.table("footprint_images")
            .select("*")
            .eq("species_id", record["species_id"])
            .eq("image_name", record["image_name"])
            .execute()
            .data
        )
        if existing:
            # update missing columns
            current_row = existing[0]
            update_dict = {}
            for k, v in record.items():
                if current_row.get(k) in [None, ""] and v not in [None, ""]:
                    update_dict[k] = v
            if update_dict:
                supabase.table("footprint_images") \
                    .update(update_dict) \
                    .eq("species_id", record["species_id"]) \
                    .eq("image_name", record["image_name"]) \
                    .execute()
        else:
            # Insert if not found
            supabase.table("footprint_images").insert(record).execute()

    print("Image processing and insertion/updating complete.")

# -------------------------------------------------------------------------
# 6. FETCH SPECIES MAP (species_name -> id)
# -------------------------------------------------------------------------
def fetch_species_map(supabase: Client) -> dict:
    """
    Returns a dictionary mapping the infos_especes.Espèce to its id.
    """
    data = supabase.table("infos_especes").select("id, Espèce").execute()
    species_map = {}
    for row in data.data:
        species_name = row.get("Espèce")
        species_id = row.get("id")
        species_map[species_name] = species_id
    return species_map

# -------------------------------------------------------------------------
# 7. DATA QUALITY CONTROL & LOGGING
# -------------------------------------------------------------------------
def run_data_quality_checks_and_log(supabase: Client):
    """
    Runs data quality checks (exhaustiveness, pertinence, accuracy) for both
    'infos_especes' and 'footprint_images' and logs results in a data_quality_log table.
    
    We'll also check for missing columns/cells vs. the source data if we can reference them.
    We'll log failures from the ETL process as well if desired (e.g., partial approach).
    """
    run_timestamp = datetime.datetime.utcnow().isoformat()

    # Example references
    expected_species_count = 13
    allowed_species_categories = [
        "Castor",
        "Chat",
        "Chien",
        "Coyote",
        "Ecureuil",
        "Lapin",
        "Loup",
        "Lynx",
        "Ours",
        "Puma",
        "Rat",
        "Raton laveur",
        "Renard",
    ]
    # Check image files for valid extension. We'll do that in process_images, but you could do it here as well.
    allowed_extensions = [".jpg", ".jpeg", ".png"]

    # We run checks on both tables
    tables_to_check = ["infos_especes", "footprint_images"]
    for tbl in tables_to_check:
        results_vector, errors = run_quality_tests_for_table(
            supabase, tbl,
            expected_species_count,
            allowed_species_categories,
            allowed_extensions
        )

        test_vector_str = str(results_vector)
        error_desc = "; ".join(errors) if errors else "No issues detected"

        # Insert a log record
        log_record = {
            "execution_time": run_timestamp,
            "table_name": tbl,
            "test_results": test_vector_str,
            "error_description": error_desc
        }
        supabase.table("data_quality_log").insert(log_record).execute()
        print(f"Data Quality for {tbl}: {test_vector_str}, Errors: {error_desc}")

def run_quality_tests_for_table(
    supabase: Client,
    table_name: str,
    expected_species_count: int,
    allowed_species_categories: list,
    allowed_extensions: list
) -> (list, list):
    """
    For a given table name, run 3 tests:
      1) Exhaustiveness
      2) Pertinence
      3) Accuracy
    We also add checks for missing cells if possible.
    """
    # 0=fail, 1=pass, 2=not applicable
    results = [2, 2, 2]
    errors = []

    if table_name == "infos_especes":
        # 1) Exhaustiveness: row count >= expected
        resp = supabase.table("infos_especes").select("id", count="exact").execute()
        actual_count = resp.count or len(resp.data)
        if actual_count < expected_species_count:
            results[0] = 0
            errors.append(f"Exhaustiveness: Only {actual_count} rows, expected >= {expected_species_count}.")
        else:
            results[0] = 1

        # 2) Pertinence: check if "Categorie" or "Famille" is in allowed list
        # This requires you to have a real column for categories. We'll assume "Categorie" here.
        data = supabase.table("infos_especes").select("*").execute().data
        if data and "Categorie" in data[0].keys():
            invalid_cat_count = 0
            for row in data:
                cat_val = row.get("Categorie")
                if cat_val and cat_val not in allowed_species_categories:
                    invalid_cat_count += 1
            if invalid_cat_count > 0:
                results[1] = 0
                errors.append(f"Pertinence: {invalid_cat_count} rows have invalid 'Categorie'.")
            else:
                results[1] = 1
        else:
            # if no "Categorie" column, not applicable
            results[1] = 2

        # 3) Accuracy: check for duplicates in "Espèce" or missing cells
        # also check if any "Espèce" is None
        species_counts = {}
        null_species_count = 0
        for row in data:
            espece = row.get("Espèce")
            if not espece:
                null_species_count += 1
                continue
            species_counts[espece] = species_counts.get(espece, 0) + 1
        duplicates = [k for k, v in species_counts.items() if v > 1]
        if duplicates:
            errors.append(f"Accuracy: Duplicate Espèce found: {duplicates}")
        if null_species_count > 0:
            errors.append(f"Accuracy: Found {null_species_count} rows with null Espèce.")
        if duplicates or null_species_count:
            results[2] = 0
        else:
            results[2] = 1

    elif table_name == "footprint_images":
        # 1) Exhaustiveness: species with images vs. total species in infos_especes
        data_imgs = supabase.table("footprint_images").select("*").execute().data
        data_species = supabase.table("infos_especes").select("id").execute().data
        species_with_images = set(row["species_id"] for row in data_imgs if row.get("species_id"))
        all_species_ids = set(row["id"] for row in data_species if row.get("id"))

        if len(species_with_images) < len(all_species_ids):
            results[0] = 0
            errors.append(
                f"Exhaustiveness: {len(species_with_images)}/{len(all_species_ids)} species have images."
            )
        else:
            results[0] = 1

        # 2) Pertinence: check if the image file extension is allowed (if we store file ext)
        # In the current schema, we only have "image_name". We can check the extension from that.
        invalid_ext_count = 0
        for row in data_imgs:
            img_name = row.get("image_name", "").lower()
            if not any(img_name.endswith(ext) for ext in allowed_extensions):
                invalid_ext_count += 1
        if invalid_ext_count > 0:
            results[1] = 0
            errors.append(f"Pertinence: {invalid_ext_count} images have invalid file extension.")
        else:
            results[1] = 1

        # 3) Accuracy: check referential integrity, also check for missing "image_name"
        missing_refs = []
        null_name_count = 0
        for row in data_imgs:
            sp_id = row.get("species_id")
            if sp_id and sp_id not in all_species_ids:
                missing_refs.append(sp_id)
            if not row.get("image_name"):
                null_name_count += 1

        if missing_refs:
            errors.append(f"Accuracy: {len(missing_refs)} footprint_images rows with invalid species_id: {missing_refs}")
        if null_name_count > 0:
            errors.append(f"Accuracy: {null_name_count} rows with empty 'image_name'.")

        if missing_refs or null_name_count:
            results[2] = 0
        else:
            results[2] = 1

    else:
        # If table_name is something else
        results = [2, 2, 2]

    return results, errors

# -------------------------------------------------------------------------
# 8. MAIN
# -------------------------------------------------------------------------
def main():
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "bucket-mspr_epsi-vine-449913-f6")
    if not BUCKET_NAME:
        raise ValueError("Please set GCS_BUCKET_NAME in environment.")

    supabase = get_supabase_client()

    # Step A: Possibly reset tables (only if both are empty)
    maybe_reset_tables(supabase)

    # Step B: Fill / upsert in infos_especes
    ensure_infos_especes_filled(supabase, bucket_name=BUCKET_NAME, xlsx_blob_name="infos_especes.xlsx")

    # Step C: Process images with partial upsert
    process_images(supabase, bucket_name=BUCKET_NAME, raw_folder_prefix="Mammifères/", processed_folder_prefix="processed_data/")

    # Step D: Run data quality checks and log
    run_data_quality_checks_and_log(supabase)

if __name__ == "__main__":
    main()
