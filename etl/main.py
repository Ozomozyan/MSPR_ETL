#!/usr/bin/env python3

"""
ETL pipeline script for:
1. Filling `infos_especes` table if empty (or merging fallback columns if partial).
2. Processing images in Google Cloud Storage under "Mammifères/<SpeciesName>/..."
   -> duplication/corruption checks, resizing, and storing references in `footprint_images`.

Only resets tables (truncate + restart identity) if *both* tables are completely empty.
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

        # Rename columns if needed
        rename_map = {}
        for col in df_main.columns:
            clean_col = re.sub(r"\\s+", "_", col.strip())  # e.g. "Espèce" -> "Espèce" or "Nom latin" -> "Nom_latin"
            rename_map[col] = clean_col
        df_main.rename(columns=rename_map, inplace=True)

        # If the table is empty, we do a normal insert of all rows
        # If the table is partially filled, you might want to update only missing rows.
        # For simplicity, let's assume you want to insert everything new.
        # (You could add logic to upsert by species name if you want partial fill.)

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
            # Here, we'll show an example of a naive approach:
            # Insert any row that doesn't match existing "Espèce" name in the table
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
# 7. MAIN
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

if __name__ == "__main__":
    main()
