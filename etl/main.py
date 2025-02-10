#!/usr/bin/env python3

import os
import hashlib
import pandas as pd
import numpy as np
import tempfile
import re

from PIL import Image
from google.cloud import storage
from supabase import create_client, Client

def get_supabase_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
    return create_client(url, key)

def get_gcs_client() -> storage.Client:
    return storage.Client()

def ensure_infos_especes_filled(
    supabase: Client,
    bucket_name: str,
    xlsx_blob_name: str = "infos_especes.xlsx",
) -> None:
    # (You can leave this logic as is or skip it—no changes needed for your test.)
    response = supabase.table("infos_especes").select("id").execute()
    if len(response.data) > 0:
        print("infos_especes table is not empty. Skipping fill.")
        return
    print("infos_especes table is empty. Proceeding to fill from GCS xlsx.")
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(xlsx_blob_name)
    if not blob.exists():
        raise FileNotFoundError(f"Could not find {xlsx_blob_name} in bucket {bucket_name}.")
    with tempfile.TemporaryDirectory() as tmpdir:
        local_xlsx_path = os.path.join(tmpdir, xlsx_blob_name)
        blob.download_to_filename(local_xlsx_path)
        print("Downloaded infos_especes.xlsx")
        df = pd.read_excel(local_xlsx_path)
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.where(df.notnull(), None)
        rename_map = {}
        for col in df.columns:
            clean_col = re.sub(r"\\s+", "_", col.strip())
            rename_map[col] = clean_col
        df.rename(columns=rename_map, inplace=True)
        records = df.to_dict(orient="records")
        chunk_size = 500
        for i in range(0, len(records), chunk_size):
            batch = records[i : i + chunk_size]
            supabase.table("infos_especes").insert(batch).execute()
        print("Inserted records into infos_especes.")

def process_images_no_db_check(
    bucket_name: str,
    raw_folder_prefix: str = "Mammifères/",
    processed_folder_prefix: str = "processed_data/"
):
    """
    A simplified version that:
      - Lists everything in raw_folder_prefix
      - Ignores supabase species checks
      - Resizes & reuploads images
      - DOES NOT insert into footprint_images
    """
    print(f"DEBUG: Starting simplified image processing. raw='{raw_folder_prefix}', processed='{processed_folder_prefix}'")
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)

    raw_blobs = bucket.list_blobs(prefix=raw_folder_prefix)
    image_candidates = []

    print("DEBUG: Enumerating objects in bucket with prefix:", raw_folder_prefix)
    for blob in raw_blobs:
        print(f"DEBUG: Found object in GCS -> {blob.name}")

        # Skip directories (any name ending with '/')
        if blob.name.endswith("/"):
            print(f"DEBUG: Skipping '{blob.name}' - directory placeholder.")
            continue

        path_parts = blob.name.split('/')
        if len(path_parts) < 3:
            print(f"DEBUG: Skipping {blob.name}, no subfolder structure.")
            continue

        # We'll just accept the species_name even if not in DB
        species_name = path_parts[1]
        image_filename = path_parts[-1]
        print(f"DEBUG: -> species='{species_name}', filename='{image_filename}'")

        image_candidates.append((blob.name, species_name, image_filename))

    local_temp_raw = tempfile.mkdtemp(prefix="raw_images_")
    local_temp_processed = tempfile.mkdtemp(prefix="processed_images_")
    print(f"DEBUG: Temp folders: raw='{local_temp_raw}', processed='{local_temp_processed}'")

    seen_hashes = set()
    images_to_upload = []

    # Download, check corruption, check duplicates, resize
    for blob_name, species_name, image_filename in image_candidates:
        print(f"\nDEBUG: Processing '{blob_name}' -> species='{species_name}', file='{image_filename}'")
        local_download_path = os.path.join(local_temp_raw, image_filename)

        # Download
        blob = bucket.blob(blob_name)
        blob.download_to_filename(local_download_path)
        print(f"DEBUG: Downloaded to {local_download_path}")

        # Check corruption
        try:
            with Image.open(local_download_path) as img:
                img.verify()
            print("DEBUG: Verified OK.")
        except Exception as e:
            print(f"WARNING: Corrupt image. Skipping {blob_name}. Error: {e}")
            continue

        # Check duplicates
        filehash = hashlib.md5(open(local_download_path, 'rb').read()).hexdigest()
        if filehash in seen_hashes:
            print(f"WARNING: Duplicate image. Skipping {blob_name}.")
            continue
        seen_hashes.add(filehash)
        print(f"DEBUG: Unique hash={filehash}")

        # Resize
        processed_filename = image_filename
        local_processed_path = os.path.join(local_temp_processed, processed_filename)
        try:
            with Image.open(local_download_path) as img:
                img_resized = img.resize((128, 128))
                img_resized.save(local_processed_path)
            print(f"DEBUG: Resized -> {local_processed_path}")
        except Exception as e:
            print(f"WARNING: Error resizing image {blob_name}: {e}")
            continue

        images_to_upload.append((local_processed_path, species_name, processed_filename))

    # Upload to 'processed_data/<species>/filename'
    print("\nDEBUG: Uploading processed images (no DB insert) ...")
    for local_path, species_name, processed_filename in images_to_upload:
        new_blob_path = f"{processed_folder_prefix}{species_name}/{processed_filename}"
        print(f"DEBUG: Uploading {local_path} to {new_blob_path}")
        new_blob = bucket.blob(new_blob_path)
        new_blob.upload_from_filename(local_path)
        new_blob.make_public()
        print(f"DEBUG: Done. Public URL: {new_blob.public_url}")

    print("\nSimplified image processing complete.")

def main():
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "bucket-mspr_epsi-vine-449913-f6")
    if not BUCKET_NAME:
        raise ValueError("Please set GCS_BUCKET_NAME in environment.")

    supabase = get_supabase_client()

    # 1. Optionally fill infos_especes (can keep or skip)
    ensure_infos_especes_filled(
        supabase=supabase,
        bucket_name=BUCKET_NAME,
        xlsx_blob_name="infos_especes.xlsx"
    )

    # 2. Use the no-DB-check version to confirm we see images
    process_images_no_db_check(
        bucket_name=BUCKET_NAME,
        raw_folder_prefix="Mammifères/",
        processed_folder_prefix="processed_data/"
    )

if __name__ == "__main__":
    main()
