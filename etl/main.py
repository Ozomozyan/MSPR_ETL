#!/usr/bin/env python3

"""
ETL pipeline script for:
1. Possibly resetting tables (truncate + restart identity) if both are empty.
2. Filling infos_especes table with data from XLSX (and fallback column files).
3. Processing images from GCS under "Mammifères/<SpeciesName>/...", performing:
   - duplication/corruption checks,
   - resizing,
   - inserting references into footprint_images.

We then run data quality checks (exhaustiveness, pertinence, accuracy) on both
infos_especes and footprint_images, and log the results in data_quality_logs.
"""

import os
import hashlib
import pandas as pd
import numpy as np
import tempfile
import re
from datetime import datetime

from PIL import Image
from google.cloud import storage
from supabase import create_client, Client

# ------------------------------------------------------------------------------
# 1. CONNECT TO SUPABASE
# ------------------------------------------------------------------------------
def get_supabase_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
    return create_client(url, key)

# ------------------------------------------------------------------------------
# 2. CONNECT TO GOOGLE CLOUD STORAGE
# ------------------------------------------------------------------------------
def get_gcs_client() -> storage.Client:
    # Make sure GOOGLE_APPLICATION_CREDENTIALS is set to the path of your service account JSON
    return storage.Client()

# ------------------------------------------------------------------------------
# 3. MAYBE RESET TABLES (truncate + restart identity if BOTH are empty)
# ------------------------------------------------------------------------------
def maybe_reset_tables(supabase: Client):
    """
    Calls reset_my_tables() if and only if both infos_especes AND footprint_images
    are completely empty. If either has data, skip reset so we won't overwrite partial data.
    """
    # Check if infos_especes is empty
    r_especes = supabase.table("infos_especes").select("id").limit(1).execute()
    # Check if footprint_images is empty
    r_images = supabase.table("footprint_images").select("id").limit(1).execute()

    count_especes = len(r_especes.data)
    count_images = len(r_images.data)

    if count_especes == 0 and count_images == 0:
        print("Both infos_especes and footprint_images are empty. Resetting tables...")
        supabase.rpc("reset_my_tables").execute()
        print("Tables truncated and sequences reset to 1.")
    else:
        print(
            "Tables are not both empty. Skipping reset:\n"
            f"  infos_especes has {count_especes} row(s).\n"
            f"  footprint_images has {count_images} row(s)."
        )

# ------------------------------------------------------------------------------
# 4. FILL infos_especes (with fallback columns)
# ------------------------------------------------------------------------------
def ensure_infos_especes_filled(
    supabase: Client,
    bucket_name: str,
    xlsx_blob_name: str = "infos_especes.xlsx",
) -> None:
    """
    Checks if infos_especes table is empty. If empty, fetches the main infos_especes.xlsx.
    Then uses fallback column files to fill missing cells. Finally, inserts data.
    If table is partially filled, only inserts new species that aren't in DB yet.
    """
    # 1) Check how many rows are in infos_especes
    response = supabase.table("infos_especes").select("*").execute()
    count_infos = len(response.data)

    if count_infos > 0:
        print(f"infos_especes table already has {count_infos} row(s). We can fill missing cells if needed.")
    else:
        print("infos_especes table is empty. Proceeding to fill from GCS xlsx.")

    # 2) Download main file from GCS
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

        # Merge fallback column files
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

        # Convert inf->nan, nan->None
        df_main.replace([np.inf, -np.inf], np.nan, inplace=True)
        df_main = df_main.where(df_main.notnull(), None)

        # Rename columns if needed
        rename_map = {}
        for col in df_main.columns:
            clean_col = re.sub(r"\\s+", "_", col.strip())
            rename_map[col] = clean_col
        df_main.rename(columns=rename_map, inplace=True)

        records = df_main.to_dict(orient="records")

        if count_infos == 0:
            # Table empty => insert all
            print("Inserting all rows into infos_especes because table was empty.")
            chunk_size = 500
            for i in range(0, len(records), chunk_size):
                batch = records[i:i+chunk_size]
                supabase.table("infos_especes").insert(batch).execute()
        else:
            # Table partially filled => upsert new species only
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

# ------------------------------------------------------------------------------
# 5. PROCESS IMAGES
# ------------------------------------------------------------------------------
def process_images(
    supabase: Client,
    bucket_name: str,
    raw_folder_prefix: str = "Mammifères/",
    processed_folder_prefix: str = "processed_data/"
):
    """
    Download images from GCS, remove duplicates/corrupted, resize,
    re-upload to processed_data, and insert references in footprint_images.
    """
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)

    species_map = fetch_species_map(supabase)

    raw_blobs = bucket.list_blobs(prefix=raw_folder_prefix)
    image_candidates = []
    for blob in raw_blobs:
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

        # Corruption check
        try:
            with Image.open(local_download_path) as img:
                img.verify()
        except Exception as e:
            print(f"Corrupt image detected. Skipping {blob_name}. Error: {e}")
            continue

        # Duplicate check
        filehash = hashlib.md5(open(local_download_path, 'rb').read()).hexdigest()
        if filehash in seen_hashes:
            print(f"Duplicate image detected. Skipping {blob_name}.")
            continue
        else:
            seen_hashes.add(filehash)

        # Resize
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

    # Upload & insert references
    for local_path, species_name, processed_filename in images_to_upload:
        new_blob_path = f"{processed_folder_prefix}{species_name}/{processed_filename}"
        new_blob = bucket.blob(new_blob_path)
        new_blob.upload_from_filename(local_path)
        # new_blob.make_public()  # if needed

        record = {
            "species_id": species_map[species_name],
            "image_name": processed_filename,
            "image_url": new_blob.public_url,
        }
        supabase.table("footprint_images").insert(record).execute()

    print("Image processing and insertion complete.")

# ------------------------------------------------------------------------------
# 6. FETCH SPECIES MAP
# ------------------------------------------------------------------------------
def fetch_species_map(supabase: Client) -> dict:
    """
    Map infos_especes.Espèce -> id
    """
    data = supabase.table("infos_especes").select("id, Espèce").execute()
    mapping = {}
    for row in data.data:
        species_name = row.get("Espèce")
        species_id = row.get("id")
        mapping[species_name] = species_id
    return mapping

# ------------------------------------------------------------------------------
# 7. DATA QUALITY CHECK FUNCTIONS - NEW CODE FOR DATA QUALITY
# ------------------------------------------------------------------------------
def record_data_quality_result(supabase: Client, table_name: str, test_vector, details: str = ""):
    """
    Insert a record into data_quality_logs (JSON test_vector).
    Example: test_vector = [1,0,1] => [Exhaustiveness, Pertinence, Accuracy]
    """
    payload = {
        "table_name": table_name,
        "test_vector": test_vector,  # supabase-py will convert list -> JSON
        "details": details,
        "run_timestamp": datetime.utcnow().isoformat()
    }
    supabase.table("data_quality_logs").insert(payload).execute()

def perform_data_quality_checks_for_infos_especes(supabase: Client):
    """
    Return (test_vector, details) for [Exhaustiveness, Pertinence, Accuracy].
    0=fail,1=pass,2=not applicable
    """
    # 3 tests: exhaustiveness, pertinence, accuracy
    exhaustiveness = 1
    pertinence = 1
    accuracy = 1
    details_list = []

    # We expect at least 13 species
    result = supabase.table("infos_especes").select("id, Espèce, Nom_latin").execute()
    data = result.data
    if len(data) < 13:
        exhaustiveness = 0
        details_list.append(f"Exhaustiveness: found {len(data)} rows, expected at least 13.")

    # Pertinence: check if Espèce is not empty
    for row in data:
        espece = row.get("Espèce", "")
        if not espece or espece.strip() == "":
            pertinence = 0
            details_list.append(f"Pertinence: row id={row['id']} has empty 'Espèce'.")
            break

    # Accuracy: check if Nom_latin is not empty
    for row in data:
        nom_latin = row.get("Nom_latin", "")
        if not nom_latin or nom_latin.strip() == "":
            accuracy = 0
            details_list.append(f"Accuracy: row id={row['id']} has empty 'Nom_latin'.")
            break

    return [exhaustiveness, pertinence, accuracy], "\n".join(details_list)

def perform_data_quality_checks_for_footprint_images(supabase: Client):
    """
    Return (test_vector, details) for [Exhaustiveness, Pertinence, Accuracy].
    """
    exhaustiveness = 1
    pertinence = 1
    accuracy = 1
    details_list = []

    # Query footprint_images
    images_result = supabase.table("footprint_images").select("id, species_id, image_url").execute()
    if len(images_result.data) == 0:
        exhaustiveness = 0
        details_list.append("Exhaustiveness: 0 rows in footprint_images.")

    # Pertinence: check if each species_id exists in infos_especes
    valid_ids_data = supabase.table("infos_especes").select("id").execute()
    valid_ids = {row["id"] for row in valid_ids_data.data}
    for row in images_result.data:
        if row["species_id"] not in valid_ids:
            pertinence = 0
            details_list.append(f"Pertinence: row {row['id']} references invalid species_id {row['species_id']}.")
            break

    # Accuracy: check if image_url starts with http
    for row in images_result.data:
        url = row.get("image_url", "")
        if not url or not url.startswith("http"):
            accuracy = 0
            details_list.append(f"Accuracy: row {row['id']} has invalid url '{url}'.")
            break

    return [exhaustiveness, pertinence, accuracy], "\n".join(details_list)

# ------------------------------------------------------------------------------
# 8. MAIN
# ------------------------------------------------------------------------------
def main():
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "bucket-mspr_epsi-vine-449913-f6")
    if not BUCKET_NAME:
        raise ValueError("Please set GCS_BUCKET_NAME in environment.")

    supabase = get_supabase_client()

    # Maybe reset if both tables empty
    maybe_reset_tables(supabase)

    # Fill or partially fill infos_especes
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

    # --------------------------------------------------------------------------
    # 9. DATA QUALITY CHECKS - NEW CODE FOR DATA QUALITY
    # --------------------------------------------------------------------------
    dq_infos_vector, dq_infos_details = perform_data_quality_checks_for_infos_especes(supabase)
    record_data_quality_result(
        supabase=supabase,
        table_name="infos_especes",
        test_vector=dq_infos_vector,
        details=dq_infos_details
    )
    print("Data quality for infos_especes =>", dq_infos_vector, dq_infos_details)

    dq_foot_vector, dq_foot_details = perform_data_quality_checks_for_footprint_images(supabase)
    record_data_quality_result(
        supabase=supabase,
        table_name="footprint_images",
        test_vector=dq_foot_vector,
        details=dq_foot_details
    )
    print("Data quality for footprint_images =>", dq_foot_vector, dq_foot_details)

if __name__ == "__main__":
    main()
