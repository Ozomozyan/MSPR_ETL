#!/usr/bin/env python3

"""
ETL pipeline script for:
1. Resetting tables if both are empty or whenever needed (TRUNCATE + RESTART IDENTITY).
2. Filling `infos_especes` table if empty (or merging fallback columns if partial).
3. Processing images in Google Cloud Storage under "Mammifères/<SpeciesName>/..."
   -> duplication/corruption checks, resizing, and storing references in `footprint_images`.
4. Running data quality tests on both tables (exhaustiveness, pertinence, accuracy).
5. Logging the data quality results (including vectors and error descriptions) into data_quality_log table.
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
    # Check if infos_especes is empty
    r_especes = supabase.table("infos_especes").select("id").limit(1).execute()
    # Check if footprint_images is empty
    r_images = supabase.table("footprint_images").select("id").limit(1).execute()

    count_especes = len(r_especes.data)
    count_images = len(r_images.data)

    if count_especes == 0 and count_images == 0:
        print("Both infos_especes and footprint_images are empty. Resetting tables...")
        # This calls our DB RPC that truncates BOTH infos_especes and footprint_images
        supabase.rpc("reset_my_tables").execute()
        print("Tables truncated and sequences reset to 1.")
    else:
        print(
            "Tables are not both empty. Skipping reset:\n"
            f"  infos_especes has {count_especes} row(s).\n"
            f"  footprint_images has {count_images} row(s)."
        )

# -------------------------------------------------------------------------
# 4. CHECK AND FILL infos_especes TABLE (merge fallback columns)
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
    Finally, inserts the resulting data into the infos_especes table.
    """
    # 1) Check how many rows are in infos_especes
    response = supabase.table("infos_especes").select("*").execute()
    count_infos = len(response.data)

    if count_infos > 0:
        print(f"infos_especes table already has {count_infos} row(s). We can fill missing cells if needed.")
    else:
        print("infos_especes table is empty. Proceeding to fill from GCS xlsx.")

    # Download the main file from GCS
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)
    blob_main = bucket.blob(xlsx_blob_name)

    if not blob_main.exists():
        raise FileNotFoundError(f"Could not find {xlsx_blob_name} in bucket {bucket_name}.")

    with tempfile.TemporaryDirectory() as tmpdir:
        local_main_path = os.path.join(tmpdir, xlsx_blob_name)
        blob_main.download_to_filename(local_main_path)
        print(f"Downloaded main file: {xlsx_blob_name}")

        # Read main DataFrame
        df_main = pd.read_excel(local_main_path)

        # Fallback column files
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

        # Rename columns if needed (for Supabase naming)
        rename_map = {}
        for col in df_main.columns:
            clean_col = re.sub(r"\s+", "_", col.strip())
            rename_map[col] = clean_col
        df_main.rename(columns=rename_map, inplace=True)

        # Insert logic
        records = df_main.to_dict(orient="records")

        if count_infos == 0:
            # Insert all rows if table is empty
            print("Inserting all rows into infos_especes because table was empty.")
            chunk_size = 500
            for i in range(0, len(records), chunk_size):
                batch = records[i:i+chunk_size]
                supabase.table("infos_especes").insert(batch).execute()
        else:
            # The table has partial data. We can do partial "upsert" if needed, or skip entirely.
            existing_species = set()
            existing_query = supabase.table("infos_especes").select("Espèce").execute()
            for row in existing_query.data:
                existing_species.add(row.get("Espèce"))

            new_records = []
            for rec in records:
                espece_name = rec.get("Espèce")
                if espece_name not in existing_species:
                    new_records.append(rec)

            if new_records:
                print(f"Inserting {len(new_records)} new record(s) for species not in DB.")
                chunk_size = 500
                for i in range(0, len(new_records), chunk_size):
                    batch = new_records[i:i+chunk_size]
                    supabase.table("infos_especes").insert(batch).execute()
            else:
                print("No new species to add. Possibly fill missing columns via an UPDATE approach if needed.")

# -------------------------------------------------------------------------
# 5. PROCESS IMAGES (CHECK DUPLICATES, CORRUPTION, ETC.)
# -------------------------------------------------------------------------
def process_images(
    supabase: Client,
    bucket_name: str,
    raw_folder_prefix: str = "Mammifères/",
    processed_folder_prefix: str = "processed_data/"
):
    """
    Download images from GCS, remove duplicates/corrupted, resize, re-upload to processed_data.
    Then insert references in footprint_images table with the correct foreign key.
    """
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)

    # 5.1 Get existing species from infos_especes to map species name -> ID
    species_map = fetch_species_map(supabase)

    # 5.2 List all blobs in raw_folder_prefix
    raw_blobs = bucket.list_blobs(prefix=raw_folder_prefix)
    image_candidates = []
    for blob in raw_blobs:
        # Skip directories (in GCS, these are just objects ending with "/")
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
        blob.download_to_filename(local_download_path)

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

        # Resize or other processing
        processed_filename = image_filename
        local_processed_path = os.path.join(local_temp_processed, processed_filename)
        try:
            with Image.open(local_download_path) as img:
                img_resized = img.resize((128, 128))
                img_resized.save(local_processed_path)
        except Exception as e:
            print(f"Error resizing image {blob_name}: {e}")
            continue

        images_to_upload.append((local_processed_path, species_name, processed_filename))

    # 5.3 Upload processed images and insert references
    for local_path, species_name, processed_filename in images_to_upload:
        new_blob_path = f"{processed_folder_prefix}{species_name}/{processed_filename}"
        new_blob = bucket.blob(new_blob_path)
        new_blob.upload_from_filename(local_path)
        # optionally make_public if needed
        # new_blob.make_public()

        record = {
            "species_id": species_map[species_name],
            "image_name": processed_filename,
            "image_url": new_blob.public_url,  # or handle if private
        }
        supabase.table("footprint_images").insert(record).execute()

    print("Image processing and insertion complete.")

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
    """
    # Step 1: Setup or assume data_quality_log table exists in Supabase
    # (In practice, you'd create it via migrations. We'll assume it's there.)
    # The table might have columns: execution_time, table_name, test_results (text), error_description (text)
    # For example:
    # CREATE TABLE IF NOT EXISTS data_quality_log (
    #    id SERIAL PRIMARY KEY,
    #    execution_time TIMESTAMP NOT NULL,
    #    table_name TEXT NOT NULL,
    #    test_results TEXT NOT NULL,
    #    error_description TEXT
    # );

    run_timestamp = datetime.datetime.utcnow().isoformat()

    # We define some placeholders for expected data or constraints
    expected_species_count = 13  # Example: If we expect ~100 species
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
    "Renard"]
    allowed_image_types = []  # If you have a column image_type, you'd define it. For now we'll skip.

    # We run checks on both tables
    tables_to_check = ["infos_especes", "footprint_images"]
    for tbl in tables_to_check:
        results_vector, errors = run_quality_tests_for_table(
            supabase, tbl,
            expected_species_count,
            allowed_species_categories,
            allowed_image_types
        )

        # Convert results to string like "[1, 0, 2]"
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
    allowed_image_types: list
) -> (list, list):
    """
    For a given table name, run our 3 tests:
      1) Exhaustiveness
      2) Pertinence
      3) Accuracy

    Return (results_vector, errors_list):
      - results_vector is something like [1, 0, 2]
      - errors_list is a list of strings describing failures
    """
    # We'll store 3 test results (0/1/2)
    # 0 = fail, 1 = pass, 2 = not applicable
    results = [2, 2, 2]  # default them to 2, we'll override as needed
    errors = []

    # Common approach is to query the data from Supabase
    # Then do the checks in Python. We'll do minimal examples below.

    if table_name == "infos_especes":
        # Exhaustiveness test: compare row count to expected
        r = supabase.table("infos_especes").select("id", count="exact").execute()
        actual_count = r.count or len(r.data)  # r.count is available if we used count='exact'
        if actual_count < expected_species_count:
            results[0] = 0
            errors.append(f"Exhaustiveness: Only {actual_count} rows, expected at least {expected_species_count}.")
        else:
            results[0] = 1

        # Pertinence test: Suppose there's a column "Categorie" in the table
        # We can check if any row has a category not in allowed_species_categories
        # Because we only have "Espèce", "Description", etc. let's assume "Categorie" or "Famille"
        # We'll just do a naive approach:
        data = supabase.table("infos_especes").select("*").execute().data
        invalid_cat = 0
        for row in data:
            # let's pretend there's a column "Categorie"
            cat_val = row.get("Categorie", None)
            if cat_val and cat_val not in allowed_species_categories:
                invalid_cat += 1
        if invalid_cat > 0:
            results[1] = 0
            errors.append(f"Pertinence: {invalid_cat} rows have invalid 'Categorie' values.")
        else:
            results[1] = 1

        # Accuracy test: check for duplicates in "Espèce"
        species_counts = {}
        for row in data:
            sp = row.get("Espèce", "").strip() if row.get("Espèce") else ""
            if not sp:
                continue
            species_counts[sp] = species_counts.get(sp, 0) + 1
        duplicates = [k for k,v in species_counts.items() if v > 1]
        if duplicates:
            results[2] = 0
            errors.append(f"Accuracy: Found duplicate species: {duplicates}")
        else:
            results[2] = 1

    elif table_name == "footprint_images":
        # For footprint_images, we'll do different checks

        # Exhaustiveness: ensure that the number of unique species with images
        # matches the number of species in infos_especes. (Just as an example.)
        # We can do a left join, but let's do it in a simpler approach:
        r_images = supabase.table("footprint_images").select("species_id").execute().data
        species_with_images = set(row["species_id"] for row in r_images if row.get("species_id") is not None)

        r_species = supabase.table("infos_especes").select("id").execute().data
        species_ids = set(row["id"] for row in r_species if row.get("id") is not None)

        if len(species_with_images) < len(species_ids):
            results[0] = 0
            errors.append(
                f"Exhaustiveness: Only {len(species_with_images)} species have images "
                f"out of {len(species_ids)} total species."
            )
        else:
            results[0] = 1

        # Pertinence test: Suppose footprint_images has a column "image_type"
        # We'll check if it's in allowed_image_types. If not defined, we skip.
        # We'll keep it not applicable if there's no 'image_type' column.
        columns = supabase.table("footprint_images").select("*").limit(1).execute().data
        if columns and "image_type" in columns[0].keys():
            results[1] = 1  # assume pass
            # check values
            data_img = supabase.table("footprint_images").select("image_type").execute().data
            invalid_type_count = 0
            for row in data_img:
                itype = row.get("image_type", None)
                if itype and itype not in allowed_image_types:
                    invalid_type_count += 1
            if invalid_type_count > 0:
                results[1] = 0
                errors.append(f"Pertinence: {invalid_type_count} images with invalid 'image_type'.")
        else:
            # If there's no image_type column at all, we mark it not applicable
            results[1] = 2

        # Accuracy: check referential integrity. Each species_id must exist in infos_especes
        missing_refs = []
        for sid in species_with_images:
            if sid not in species_ids:
                missing_refs.append(sid)
        if missing_refs:
            results[2] = 0
            errors.append(f"Accuracy: The following species_ids in images do not exist in infos_especes: {missing_refs}")
        else:
            results[2] = 1

    else:
        # If table_name is something else, no checks
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

    # If both tables are empty, reset them (TRUNCATE + RESTART IDENTITY).
    maybe_reset_tables(supabase)

    # Fill or partially fill the infos_especes table
    ensure_infos_especes_filled(
        supabase=supabase,
        bucket_name=BUCKET_NAME,
        xlsx_blob_name="infos_especes.xlsx"
    )

    # Process images
    process_images(
        supabase=supabase,
        bucket_name=BUCKET_NAME,
        raw_folder_prefix="Mammifères/",
        processed_folder_prefix="processed_data/"
    )

    # Finally, run data quality checks and log results
    run_data_quality_checks_and_log(supabase)

if __name__ == "__main__":
    main()
