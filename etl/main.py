#!/usr/bin/env python3

"""
This script demonstrates an end-to-end ETL pipeline that:

1. Checks if the infos_especes table in Supabase is empty:
   - If empty, it fetches an infos_especes.xlsx file from Google Cloud Storage,
     converts it to CSV (if needed), and inserts data (Espece, Description, etc.)
     into Supabase.

2. Processes raw images from a GCS directory "Mammifères/<SpeciesName>/...":
   - Checks for duplicates (via file hash)
   - Checks for corrupt images (via PIL verify())
   - Resizes images (or any other augmentation steps)
   - Uploads processed images to GCS "processed_data/<SpeciesName>/..."
   - Inserts references (image_name, image_url, species_id) into the footprint_images table
     in Supabase, linking them with the infos_especes.id.

Environment Variables:
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY
- GOOGLE_APPLICATION_CREDENTIALS (file path to your service account JSON)

Additional assumptions:
- The table "infos_especes" has an auto-increment primary key (id) and columns like:
    id (pk), Espece (string), Description (string), etc.
- The table "footprint_images" has columns:
    id (pk), species_id (fk to infos_especes.id), image_name (string), image_url (string)
- This script relies on the supabase-py client, google-cloud-storage, pandas (for xlsx->csv), Pillow, etc.
"""

import os
import hashlib
import pandas as pd
import numpy as np
import tempfile
import re

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
# 3. CHECK AND FILL infos_especes TABLE
# ------------------------------------------------------------------------------
def ensure_infos_especes_filled(
    supabase: Client,
    bucket_name: str,
    xlsx_blob_name: str = "infos_especes.xlsx",
) -> None:
    """
    Checks if infos_especes table is empty in Supabase.
    If empty, fetches the xlsx from GCS, optionally converts to CSV, then inserts data.
    """
    # 3.1 Check if infos_especes is empty
    response = supabase.table("infos_especes").select("id").execute()
    if len(response.data) > 0:
        print("infos_especes table is not empty. Skipping fill.")
        return

    print("infos_especes table is empty. Proceeding to fill from GCS xlsx.")

    # 3.2 Download xlsx from GCS
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(xlsx_blob_name)
    if not blob.exists():
        raise FileNotFoundError(f"Could not find {xlsx_blob_name} in bucket {bucket_name}.")

    with tempfile.TemporaryDirectory() as tmpdir:
        local_xlsx_path = os.path.join(tmpdir, xlsx_blob_name)
        blob.download_to_filename(local_xlsx_path)
        print("Downloaded infos_especes.xlsx")

        # 3.3 Read xlsx into pandas
        df = pd.read_excel(local_xlsx_path)

        # Handle infinite values by converting them to NaN.
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # Instead of fillna(value=None), we'll convert NaN -> None using 'where' so that JSON is valid.
        # This approach ensures that nulls become Python None, which supabase-py can serialize as JSON null.
        df = df.where(df.notnull(), None)

        # Let's rename columns if needed (replace spaces with underscores).
        rename_map = {}
        for col in df.columns:
            clean_col = re.sub(r"\\s+", "_", col.strip())  # replace whitespace with underscores
            rename_map[col] = clean_col
        df.rename(columns=rename_map, inplace=True)

        # Convert to list of dicts
        records = df.to_dict(orient="records")

        # Insert data in batches
        chunk_size = 500
        for i in range(0, len(records), chunk_size):
            batch = records[i:i+chunk_size]
            supabase.table("infos_especes").insert(batch).execute()

        print("Inserted records into infos_especes.")

# ------------------------------------------------------------------------------
# 4. PROCESS IMAGES (CHECK DUPLICATES, CORRUPTION, ETC.)
# ------------------------------------------------------------------------------
def process_images(
    supabase: Client,
    bucket_name: str,
    raw_folder_prefix: str = "Mammifères/",
    processed_folder_prefix: str = "processed_data/"
):
    """
    Download images from GCS, remove duplicates/corrupted, resize, re-upload to processed_data.
    Then insert references in footprint_images table with the correct foreign key.
    """
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)

    # 4.1 Get existing species from infos_especes to map species name -> ID
    species_map = fetch_species_map(supabase)

    # 4.2 List all blobs in raw_folder_prefix (e.g. Mammifères/<SpeciesName>/...)
    raw_blobs = bucket.list_blobs(prefix=raw_folder_prefix)
    image_candidates = []
    for blob in raw_blobs:
        # Skip directories
        if blob.name.endswith("/"):
            continue
        # example: Mammifères/Hippopotame/img123.jpg
        path_parts = blob.name.split('/')
        if len(path_parts) < 3:
            # e.g. Mammifères/ + filename => 2 parts, no species subfolder
            print(f"Skipping {blob.name}, no subfolder structure.")
            continue
        species_name = path_parts[1]
        image_filename = path_parts[-1]
        image_candidates.append((blob.name, species_name, image_filename))

    # 4.3 Create local temp folders
    local_temp_raw = tempfile.mkdtemp(prefix="raw_images_")
    local_temp_processed = tempfile.mkdtemp(prefix="processed_images_")

    # 4.4 Hash set to detect duplicates
    seen_hashes = set()

    # Keep track of valid images to re-upload
    images_to_upload = []  # list of (local_path, species_name, new_filename)

    for blob_name, species_name, image_filename in image_candidates:
        # If there's no matching species, skip
        if species_name not in species_map:
            print(f"Species '{species_name}' not found in infos_especes. Skipping {blob_name}.")
            continue

        # Download the image locally
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

        # Check duplicates via md5
        filehash = hashlib.md5(open(local_download_path, 'rb').read()).hexdigest()
        if filehash in seen_hashes:
            print(f"Duplicate image detected. Skipping {blob_name}.")
            continue
        else:
            seen_hashes.add(filehash)

        # If valid and unique, do processing (resize, etc.)
        processed_filename = image_filename
        local_processed_path = os.path.join(local_temp_processed, processed_filename)
        try:
            with Image.open(local_download_path) as img:
                # Example resize to 128x128
                img_resized = img.resize((128, 128))
                img_resized.save(local_processed_path)
        except Exception as e:
            print(f"Error resizing image {blob_name}: {e}")
            continue

        images_to_upload.append((local_processed_path, species_name, processed_filename))

    # 4.5 Upload processed images and insert references into footprint_images
    for local_path, species_name, processed_filename in images_to_upload:
        new_blob_path = f"{processed_folder_prefix}{species_name}/{processed_filename}"
        new_blob = bucket.blob(new_blob_path)
        new_blob.upload_from_filename(local_path)
        # Optionally make public
        new_blob.make_public()

        record = {
            "species_id": species_map[species_name],
            "image_name": processed_filename,
            "image_url": new_blob.public_url,
        }
        supabase.table("footprint_images").insert(record).execute()

    print("Image processing and insertion complete.")

# ------------------------------------------------------------------------------
# 5. FETCH SPECIES MAP (species_name -> id)
# ------------------------------------------------------------------------------
def fetch_species_map(supabase: Client) -> dict:
    """
    Returns a dictionary mapping the infos_especes.Espece to its id.
    For example: { 'Hippopotame': 10, 'Chien': 11 }
    """
    data = supabase.table("infos_especes").select("id, Espèce").execute()
    species_map = {}
    for row in data.data:
        species_name = row.get("Espèce")
        species_id = row.get("id")
        species_map[species_name] = species_id
    return species_map

# ------------------------------------------------------------------------------
# 6. MAIN
# ------------------------------------------------------------------------------
def main():
    # Read from env
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "bucket-mspr_epsi-vine-449913-f6")
    if not BUCKET_NAME:
        raise ValueError("Please set GCS_BUCKET_NAME in environment.")

    # 6.1 Connect to Supabase
    supabase = get_supabase_client()

    # 6.2 Ensure infos_especes is filled
    ensure_infos_especes_filled(
        supabase=supabase,
        bucket_name=BUCKET_NAME,
        xlsx_blob_name="infos_especes.xlsx"
    )

    # 6.3 Process images (duplicates, corruption, etc.) and fill footprint_images
    process_images(
        supabase=supabase,
        bucket_name=BUCKET_NAME,
        raw_folder_prefix="Mammifères/",
        processed_folder_prefix="processed_data/"
    )

if __name__ == "__main__":
    main()
