#!/usr/bin/env python3

"""
ETL pipeline script for:
 1. Selectively resetting tables that are empty:
    - If infos_especes is empty, reset it
    - If footprint_images is empty, reset it
    - If both are empty, reset both
 2. Filling/updating infos_especes (merging fallback columns)
 3. Processing images in Google Cloud Storage
    -> duplication/corruption checks, resizing, upserting references in footprint_images
    -> single-pass approach: for each image, we always store the final correct image_name and image_url
       so there's no second pass that can accidentally duplicate or partially fill
 4. Running data quality tests (exhaustiveness, pertinence, accuracy, missing cells, file extension checks)
 5. Logging data quality results in data_quality_log
"""

import os
import hashlib
import pandas as pd
import numpy as np
import tempfile
import re
import datetime
import urllib.parse

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
# 2. CONNECT TO GCS
# -------------------------------------------------------------------------
def get_gcs_client() -> storage.Client:
    return storage.Client()

# -------------------------------------------------------------------------
# 3. SELECTIVELY RESET TABLES
# -------------------------------------------------------------------------
def maybe_reset_tables(supabase: Client):
    """
    Checks row counts in both tables:
      - If both are empty => reset both
      - Else if only one is empty => reset just that one
      - If neither is empty => skip
    
    We assume you have:
      - supabase.rpc("reset_my_tables")     -> truncates both
      - supabase.rpc("reset_one_table", ...) -> truncates just one table

    Adjust as needed for your actual DB RPC structure.
    """

    r_especes = supabase.table("infos_especes").select("id").limit(1).execute()
    r_images = supabase.table("footprint_images").select("id").limit(1).execute()

    count_especes = len(r_especes.data)
    count_images = len(r_images.data)

    if count_especes == 0 and count_images == 0:
        print("Both infos_especes and footprint_images are empty. Resetting BOTH tables...")
        supabase.rpc("reset_my_tables").execute()  # Or call 2 different RPCs if needed
    elif count_especes == 0:
        print("infos_especes is empty; resetting just infos_especes.")
        supabase.rpc("reset_one_table", {"table_name": "infos_especes"}).execute()
    elif count_images == 0:
        print("footprint_images is empty; resetting just footprint_images.")
        supabase.rpc("reset_one_table", {"table_name": "footprint_images"}).execute()
    else:
        print(
            f"Skipping reset: infos_especes has {count_especes} row(s), "
            f"footprint_images has {count_images} row(s)."
        )

# -------------------------------------------------------------------------
# 4. FILL/UPDATE infos_especes
# -------------------------------------------------------------------------
def ensure_infos_especes_filled(supabase: Client, bucket_name: str, xlsx_blob_name: str):
    """
    Merges fallback columns, inserts new rows, updates empty cells
    in existing rows. If the table is empty, we insert all data;
    otherwise we partially fill missing columns for existing records.
    """

    response = supabase.table("infos_especes").select("*").execute()
    count_infos = len(response.data)

    if count_infos > 0:
        print(f"infos_especes already has {count_infos} row(s). We'll do partial fill as needed.")
    else:
        print("infos_especes is empty. Will insert data from GCS xlsx...")

    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)
    blob_main = bucket.blob(xlsx_blob_name)
    if not blob_main.exists():
        raise FileNotFoundError(f"Could not find {xlsx_blob_name} in {bucket_name}.")

    with tempfile.TemporaryDirectory() as tmpdir:
        local_main_path = os.path.join(tmpdir, xlsx_blob_name)
        blob_main.download_to_filename(local_main_path)
        print(f"Downloaded main Excel: {xlsx_blob_name}")

        df_main = pd.read_excel(local_main_path)

        # Fallback merges...
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
        for fallback_blob_name, col_name in fallback_files:
            fb_blob = bucket.blob(fallback_blob_name)
            if fb_blob.exists():
                fb_local_path = os.path.join(tmpdir, fallback_blob_name)
                fb_blob.download_to_filename(fb_local_path)
                fb_df = pd.read_excel(fb_local_path)
                if col_name in fb_df.columns:
                    min_len = min(len(df_main), len(fb_df))
                    for i in range(min_len):
                        if pd.isnull(df_main.loc[i, col_name]):
                            df_main.loc[i, col_name] = fb_df.loc[i, col_name]

        # Clean up infinite, None
        df_main.replace([np.inf, -np.inf], np.nan, inplace=True)
        df_main = df_main.where(df_main.notnull(), None)

        # Rename columns (spaces -> underscores)
        rename_map = {}
        for c in df_main.columns:
            rename_map[c] = re.sub(r"\s+", "_", c.strip())
        df_main.rename(columns=rename_map, inplace=True)

        records = df_main.to_dict(orient="records")

        if count_infos == 0:
            # Insert all
            chunk_size = 500
            for i in range(0, len(records), chunk_size):
                batch = records[i : i + chunk_size]
                supabase.table("infos_especes").insert(batch).execute()
        else:
            # Upsert logic:
            existing_species = set()
            existing_data = supabase.table("infos_especes").select("Espèce").execute().data
            for row in existing_data:
                if row.get("Espèce"):
                    existing_species.add(row["Espèce"])
            for rec in records:
                espece = rec.get("Espèce")
                if not espece:
                    continue
                if espece not in existing_species:
                    supabase.table("infos_especes").insert(rec).execute()
                else:
                    # partial update if needed
                    existing_row_q = supabase.table("infos_especes").select("*").eq("Espèce", espece).limit(1).execute()
                    if existing_row_q.data:
                        existing_row = existing_row_q.data[0]
                        to_update = {}
                        for k, v in rec.items():
                            if existing_row.get(k) in [None, ""] and v not in [None, ""]:
                                to_update[k] = v
                        if to_update:
                            supabase.table("infos_especes").update(to_update).eq("Espèce", espece).execute()

# -------------------------------------------------------------------------
# 5. PROCESS IMAGES -- SINGLE-PASS UPSERT
# -------------------------------------------------------------------------
def process_images(
    supabase: Client,
    bucket_name: str,
    raw_folder_prefix: str = "Mammifères/",
    processed_folder_prefix: str = "processed_data/"
):
    """
    SINGLE-PASS logic:
      - Download/verify/resize images from GCS
      - For each image, we already know the correct "image_name" from GCS
      - We upload to processed_data, get the final "image_url"
      - We do a single upsert by (species_id, image_name):
         * If row exists, fill missing columns
         * If not, create a new row
      - We do not do a second pass to 'fill missing fields' later;
        the row is correct in a single pass.
    """
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)

    # Map from species_name -> species_id
    species_map = fetch_species_map(supabase)

    # 1) Identify all raw images in GCS
    raw_blobs = bucket.list_blobs(prefix=raw_folder_prefix)
    image_candidates = []
    for blob in raw_blobs:
        if blob.name.endswith("/"):
            continue
        parts = blob.name.split("/")
        if len(parts) < 3:
            print(f"Skipping {blob.name}; no subfolder structure.")
            continue
        species_name = parts[1]  # e.g., "Castor"
        image_filename = parts[-1]
        image_candidates.append((blob.name, species_name, image_filename))

    # Temp local folders
    tmp_raw = tempfile.mkdtemp(prefix="raw_imgs_")
    tmp_proc = tempfile.mkdtemp(prefix="proc_imgs_")

    seen_hashes = set()

    # 2) For each image in GCS
    for blob_name, species_name, image_filename in image_candidates:
        # If species doesn't exist, skip
        if species_name not in species_map:
            print(f"Species '{species_name}' not found in DB. Skipping {blob_name}.")
            continue

        local_raw_path = os.path.join(tmp_raw, image_filename)
        blob = bucket.blob(blob_name)

        # Download
        try:
            blob.download_to_filename(local_raw_path)
        except Exception as e:
            print(f"Error downloading {blob_name}: {e}")
            continue

        # Check corruption
        try:
            with Image.open(local_raw_path) as im:
                im.verify()
        except Exception as e:
            print(f"Corrupt image {blob_name}: {e}")
            continue

        # Check duplicates via MD5
        filehash = hashlib.md5(open(local_raw_path, "rb").read()).hexdigest()
        if filehash in seen_hashes:
            print(f"Duplicate image {blob_name}. Skipping.")
            continue
        seen_hashes.add(filehash)

        # Extension check
        valid_exts = (".jpg", ".jpeg", ".png")
        if not image_filename.lower().endswith(valid_exts):
            print(f"Invalid extension for {image_filename}, skipping.")
            continue

        # Resize
        local_proc_path = os.path.join(tmp_proc, image_filename)
        try:
            with Image.open(local_raw_path) as im:
                im_resized = im.resize((128, 128))
                im_resized.save(local_proc_path)
        except Exception as e:
            print(f"Error resizing {blob_name}: {e}")
            continue

        # 3) Upload to processed_data
        new_blob_path = f"{processed_folder_prefix}{species_name}/{image_filename}"
        new_blob = bucket.blob(new_blob_path)
        try:
            new_blob.upload_from_filename(local_proc_path)
        except Exception as e:
            print(f"Error uploading processed {local_proc_path}: {e}")
            continue

        # GCS might encode special chars, but the actual object name is still `image_filename`.
        # The public URL might contain %28, %29, etc., so if you want a "clean" image_name
        # in DB, we just store `image_filename` as is, and keep the URL with % encoding.

        final_image_url = new_blob.public_url

        # 4) SINGLE-PASS: We do a single upsert for footprint_images
        record = {
            "species_id": species_map[species_name],
            "image_name": image_filename,    # the original name from GCS
            "image_url": final_image_url,    # the final link
        }

        # Attempt partial upsert by (species_id, image_name)
        existing_q = (
            supabase.table("footprint_images")
            .select("*")
            .eq("species_id", record["species_id"])
            .eq("image_name", record["image_name"])
            .execute()
        )
        existing_data = existing_q.data
        if existing_data:
            row_in_db = existing_data[0]
            updates = {}
            # Fill missing fields
            for k, v in record.items():
                if row_in_db.get(k) in [None, ""] and v not in [None, ""]:
                    updates[k] = v
            if updates:
                supabase.table("footprint_images") \
                    .update(updates) \
                    .eq("id", row_in_db["id"]) \
                    .execute()
        else:
            supabase.table("footprint_images").insert(record).execute()

    print("Single-pass image processing & upserting complete.")

# -------------------------------------------------------------------------
# 6. FETCH SPECIES MAP
# -------------------------------------------------------------------------
def fetch_species_map(supabase: Client) -> dict:
    """
    Return a dictionary {species_name: species_id}.
    We assume 'Espèce' is unique in the DB.
    """
    data = supabase.table("infos_especes").select("id, Espèce").execute()
    sp_map = {}
    for row in data.data:
        sp_map[row["Espèce"]] = row["id"]
    return sp_map

# -------------------------------------------------------------------------
# 7. DATA QUALITY CONTROL & LOGGING
# -------------------------------------------------------------------------
def run_data_quality_checks_and_log(supabase: Client):
    """
    Run typical tests: Exhaustiveness, Pertinence, Accuracy,
    and log results in data_quality_log.
    """
    run_timestamp = datetime.datetime.utcnow().isoformat()

    # Example references
    expected_species_count = 13
    allowed_species_categories = [
        "Castor","Chat","Chien","Coyote","Ecureuil","Lapin","Loup",
        "Lynx","Ours","Puma","Rat","Raton laveur","Renard"
    ]
    allowed_extensions = [".jpg", ".jpeg", ".png"]

    tables_to_check = ["infos_especes", "footprint_images"]
    for tbl in tables_to_check:
        results_vector, errors = run_quality_tests_for_table(
            supabase, tbl, expected_species_count, allowed_species_categories, allowed_extensions
        )

        test_vector_str = str(results_vector)
        error_desc = "; ".join(errors) if errors else "No issues detected"

        # Insert log
        supabase.table("data_quality_log").insert({
            "execution_time": run_timestamp,
            "table_name": tbl,
            "test_results": test_vector_str,
            "error_description": error_desc
        }).execute()

        print(f"Data Quality for {tbl}: {test_vector_str}, Errors: {error_desc}")

def run_quality_tests_for_table(
    supabase: Client,
    table_name: str,
    expected_species_count: int,
    allowed_species_categories: list,
    allowed_extensions: list
) -> (list, list):
    """
    3 tests: [Exhaustiveness, Pertinence, Accuracy]
    plus checks for missing cells/columns.
    0=fail, 1=pass, 2=n/a
    """
    results = [2, 2, 2]
    errors = []

    if table_name == "infos_especes":
        # 1) Exhaustiveness
        resp = supabase.table("infos_especes").select("id", count="exact").execute()
        actual_count = resp.count or len(resp.data)
        if actual_count < expected_species_count:
            results[0] = 0
            errors.append(f"Exhaustiveness: Only {actual_count} rows, expected >= {expected_species_count}.")
        else:
            results[0] = 1

        # 2) Pertinence: check if "Categorie" is valid
        rows = supabase.table("infos_especes").select("*").execute().data
        if rows and "Categorie" in rows[0]:
            invalid_cat = 0
            for row in rows:
                cat_val = row.get("Categorie")
                if cat_val and cat_val not in allowed_species_categories:
                    invalid_cat += 1
            if invalid_cat > 0:
                results[1] = 0
                errors.append(f"Pertinence: {invalid_cat} row(s) have invalid 'Categorie'.")
            else:
                results[1] = 1
        else:
            results[1] = 2

        # 3) Accuracy: check duplicates in "Espèce" or missing "Espèce"
        species_counts = {}
        null_espece = 0
        for row in rows:
            sp = row.get("Espèce")
            if not sp:
                null_espece += 1
                continue
            species_counts[sp] = species_counts.get(sp, 0) + 1
        duplicates = [k for k,v in species_counts.items() if v > 1]
        if duplicates:
            errors.append(f"Accuracy: Duplicate Espèce => {duplicates}")
        if null_espece > 0:
            errors.append(f"Accuracy: {null_espece} row(s) missing Espèce.")
        if duplicates or null_espece:
            results[2] = 0
        else:
            results[2] = 1

    elif table_name == "footprint_images":
        # 1) Exhaustiveness: how many species have images?
        data_imgs = supabase.table("footprint_images").select("*").execute().data
        data_species = supabase.table("infos_especes").select("id").execute().data
        sp_with_imgs = set(r["species_id"] for r in data_imgs if r.get("species_id") is not None)
        sp_all = set(r["id"] for r in data_species if r.get("id") is not None)
        if len(sp_with_imgs) < len(sp_all):
            results[0] = 0
            errors.append(f"Exhaustiveness: {len(sp_with_imgs)}/{len(sp_all)} species have images.")
        else:
            results[0] = 1

        # 2) Pertinence: check file extension
        invalid_ext = 0
        for row in data_imgs:
            iname = row.get("image_name", "").lower()
            if iname and not any(iname.endswith(ext) for ext in allowed_extensions):
                invalid_ext += 1
        if invalid_ext > 0:
            results[1] = 0
            errors.append(f"Pertinence: {invalid_ext} images have invalid file extension.")
        else:
            results[1] = 1

        # 3) Accuracy: referential integrity & missing fields
        missing_sp = []
        missing_name = 0
        missing_url = 0
        for row in data_imgs:
            sp_id = row.get("species_id")
            if sp_id and sp_id not in sp_all:
                missing_sp.append(sp_id)
            if not row.get("image_name"):
                missing_name += 1
            if not row.get("image_url"):
                missing_url += 1
        if missing_sp:
            errors.append(f"Accuracy: {len(missing_sp)} images have invalid species_id => {missing_sp}")
        if missing_name > 0:
            errors.append(f"Accuracy: {missing_name} row(s) missing image_name.")
        if missing_url > 0:
            errors.append(f"Accuracy: {missing_url} row(s) missing image_url.")
        if missing_sp or missing_name or missing_url:
            results[2] = 0
        else:
            results[2] = 1

    else:
        # Some other table
        results = [2,2,2]

    return results, errors

# -------------------------------------------------------------------------
# 8. MAIN
# -------------------------------------------------------------------------
def main():
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "bucket-mspr_epsi-vine-449913-f6")
    if not BUCKET_NAME:
        raise ValueError("Please set GCS_BUCKET_NAME in environment.")

    supabase = get_supabase_client()

    # A) selectively reset tables
    maybe_reset_tables(supabase)

    # B) fill or partially fill infos_especes
    ensure_infos_especes_filled(supabase, BUCKET_NAME, "infos_especes.xlsx")

    # C) process images with a single-pass upsert (no second pass needed)
    process_images(supabase, BUCKET_NAME, "Mammifères/", "processed_data/")

    # D) run data quality checks
    run_data_quality_checks_and_log(supabase)

if __name__ == "__main__":
    main()
