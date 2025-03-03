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
    -> if image_name or image_url is missing, fill it from the other field when possible
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
    """
    Returns a Google Cloud Storage client using the default credentials
    set by the GOOGLE_APPLICATION_CREDENTIALS environment variable.
    """
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

    We assume the existence of:
      1) A supabase RPC 'reset_my_tables' that truncates both at once
      2) A supabase RPC 'reset_one_table(table_name)' that truncates one table
         (with RESTART IDENTITY CASCADE, etc.)
    Adapt as needed for your environment.
    """
    r_especes = supabase.table("infos_especes").select("id").limit(1).execute()
    r_images = supabase.table("footprint_images").select("id").limit(1).execute()

    count_especes = len(r_especes.data)
    count_images = len(r_images.data)

    if count_especes == 0 and count_images == 0:
        print("Both infos_especes and footprint_images are empty. Resetting BOTH tables...")
        supabase.rpc("reset_my_tables").execute()  # single RPC to reset both
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
    1) If infos_especes is empty, insert all rows from the main Excel.
    2) If not empty, do partial merges:
       - Insert new species (Espèce) if not found
       - For existing species, fill missing columns from the DataFrame
    3) Also merges fallback columns from other Excel files.
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
        raise FileNotFoundError(f"Could not find {xlsx_blob_name} in bucket '{bucket_name}'.")

    with tempfile.TemporaryDirectory() as tmpdir:
        local_main_path = os.path.join(tmpdir, xlsx_blob_name)
        blob_main.download_to_filename(local_main_path)
        print(f"Downloaded main Excel: {xlsx_blob_name}")

        df_main = pd.read_excel(local_main_path)

        # Merge fallback files
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

        # Convert inf -> nan, then nan -> None
        df_main.replace([np.inf, -np.inf], np.nan, inplace=True)
        df_main = df_main.where(df_main.notnull(), None)

        # Rename columns (spaces -> underscores)
        rename_map = {}
        for c in df_main.columns:
            rename_map[c] = re.sub(r"\s+", "_", c.strip())
        df_main.rename(columns=rename_map, inplace=True)

        records = df_main.to_dict(orient="records")

        # If table empty => insert everything
        if count_infos == 0:
            chunk_size = 500
            for i in range(0, len(records), chunk_size):
                batch = records[i : i + chunk_size]
                supabase.table("infos_especes").insert(batch).execute()
        else:
            # partial upsert
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
                    # partial update
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
# 5. PROCESS IMAGES (RESIZE, DUPLICATE CHECK, PARTIAL UPSERT) & FILL MISSING FIELDS
# -------------------------------------------------------------------------
def process_images(
    supabase: Client,
    bucket_name: str,
    raw_folder_prefix: str = "Mammifères/",
    processed_folder_prefix: str = "processed_data/"
):
    """
    1) Lists images in GCS under raw_folder_prefix (Mammifères/<SpeciesName>/<file>).
    2) Downloads each image, checks corruption, duplicates, extension.
    3) Resizes => uploads to processed_folder_prefix => upsert in 'footprint_images'.
    4) Then calls fill_missing_image_fields(...) to attempt to fill empty image_name or image_url if possible.
    """
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)

    # species_map helps map species_name => species_id in DB
    species_map = fetch_species_map(supabase)

    # List candidate images
    raw_blobs = bucket.list_blobs(prefix=raw_folder_prefix)
    image_candidates = []
    for blob in raw_blobs:
        # skip "directories"
        if blob.name.endswith("/"):
            continue
        parts = blob.name.split("/")
        if len(parts) < 3:
            print(f"Skipping {blob.name}; no subfolder structure.")
            continue
        species_name = parts[1]
        image_filename = parts[-1]
        image_candidates.append((blob.name, species_name, image_filename))

    tmp_raw = tempfile.mkdtemp(prefix="raw_imgs_")
    tmp_proc = tempfile.mkdtemp(prefix="proc_imgs_")

    seen_hashes = set()
    images_to_upload = []

    for blob_name, species_name, image_filename in image_candidates:
        # skip if species not found
        if species_name not in species_map:
            print(f"Species '{species_name}' not found in DB. Skipping {blob_name}.")
            continue

        local_raw_path = os.path.join(tmp_raw, image_filename)
        blob = bucket.blob(blob_name)

        try:
            blob.download_to_filename(local_raw_path)
        except Exception as e:
            print(f"Error downloading {blob_name}: {e}")
            continue

        # check corruption
        try:
            with Image.open(local_raw_path) as im:
                im.verify()
        except Exception as e:
            print(f"Corrupt image {blob_name}: {e}")
            continue

        # duplicate check
        filehash = hashlib.md5(open(local_raw_path, "rb").read()).hexdigest()
        if filehash in seen_hashes:
            print(f"Duplicate image {blob_name}. Skipping.")
            continue
        seen_hashes.add(filehash)

        # extension check
        valid_exts = (".jpg", ".jpeg", ".png")
        if not image_filename.lower().endswith(valid_exts):
            print(f"Invalid extension for {image_filename}, skipping.")
            continue

        # resize
        local_proc_path = os.path.join(tmp_proc, image_filename)
        try:
            with Image.open(local_raw_path) as im:
                im_resized = im.resize((128, 128))
                im_resized.save(local_proc_path)
        except Exception as e:
            print(f"Error resizing {blob_name}: {e}")
            continue

        images_to_upload.append((local_proc_path, species_name, image_filename))

    # Upload processed => upsert
    for local_path, species_name, filename in images_to_upload:
        new_blob_path = f"{processed_folder_prefix}{species_name}/{filename}"
        new_blob = bucket.blob(new_blob_path)
        try:
            new_blob.upload_from_filename(local_path)
        except Exception as e:
            print(f"Error uploading processed {local_path}: {e}")
            continue

        # build record
        record = {
            "species_id": species_map[species_name],
            "image_name": filename,
            "image_url": new_blob.public_url,
        }

        # partial upsert
        existing_q = (
            supabase.table("footprint_images")
            .select("*")
            .eq("species_id", record["species_id"])
            .eq("image_name", filename)
            .execute()
        )
        existing_data = existing_q.data
        if existing_data:
            row_in_db = existing_data[0]
            updates = {}
            for k, v in record.items():
                if row_in_db.get(k) in [None, ""] and v not in [None, ""]:
                    updates[k] = v
            if updates:
                supabase.table("footprint_images") \
                        .update(updates) \
                        .eq("species_id", record["species_id"]) \
                        .eq("image_name", filename) \
                        .execute()
        else:
            supabase.table("footprint_images").insert(record).execute()

    # after standard upserts, fill missing fields
    fill_missing_image_fields(supabase, bucket_name, processed_folder_prefix)
    print("Image processing complete.")

def fill_missing_image_fields(supabase: Client, bucket_name: str, processed_folder_prefix: str):
    """
    If a row in footprint_images is missing image_name but has image_url, or vice versa,
    we try to fill. E.g. we decode from the URL or build from the name.
    """
    rows = supabase.table("footprint_images").select("*").execute().data
    updated_count = 0

    for r in rows:
        species_id = r.get("species_id")
        name_in_db = r.get("image_name")
        url_in_db = r.get("image_url")
        row_id = r.get("id")

        update_dict = {}

        # CASE 1: missing image_name, but we have image_url
        if not name_in_db and url_in_db:
            encoded_filename = url_in_db.split("/")[-1]  # e.g. "Beaver-Tracks-%287%29.jpg"
            decoded_filename = urllib.parse.unquote(encoded_filename)  # => "Beaver-Tracks-(7).jpg"
            if decoded_filename:
                update_dict["image_name"] = decoded_filename

        # CASE 2: missing image_url, but we have image_name
        if not url_in_db and name_in_db and species_id:
            # get species name
            sp_row = (
                supabase.table("infos_especes")
                .select("Espèce")
                .eq("id", species_id)
                .execute()
                .data
            )
            if sp_row:
                sp_name = sp_row[0].get("Espèce")
                # build a plausible GCS public URL
                # might be: "https://storage.googleapis.com/<bucket>/processed_data/<sp_name>/<filename>"
                new_url = (
                    f"https://storage.googleapis.com/{bucket_name}/"
                    f"{processed_folder_prefix}{sp_name}/{name_in_db}"
                )
                update_dict["image_url"] = new_url

        if update_dict:
            supabase.table("footprint_images") \
                    .update(update_dict) \
                    .eq("id", row_id) \
                    .execute()
            updated_count += 1

    if updated_count > 0:
        print(f"Filled missing fields (image_name/image_url) for {updated_count} footprint_images rows.")

# -------------------------------------------------------------------------
# 6. FETCH SPECIES MAP
# -------------------------------------------------------------------------
def fetch_species_map(supabase: Client) -> dict:
    """
    Returns a dict {species_name -> id} from infos_especes.
    """
    data = supabase.table("infos_especes").select("id, Espèce").execute()
    sp_map = {}
    for row in data.data:
        sp_name = row["Espèce"]
        sp_id = row["id"]
        sp_map[sp_name] = sp_id
    return sp_map

# -------------------------------------------------------------------------
# 7. DATA QUALITY CONTROL & LOGGING
# -------------------------------------------------------------------------
def run_data_quality_checks_and_log(supabase: Client):
    """
    Run your data quality tests for:
     1) infos_especes
     2) footprint_images
    Then store results in data_quality_log table.
    """
    run_timestamp = datetime.datetime.utcnow().isoformat()

    # Example references
    expected_species_count = 13
    allowed_species_categories = [
        "Castor","Chat","Chien","Coyote","Ecureuil","Lapin","Loup",
        "Lynx","Ours","Puma","Rat","Raton laveur","Renard"
    ]
    allowed_extensions = [".jpg", ".jpeg", ".png"]

    # We'll run checks on these two tables
    tables_to_check = ["infos_especes", "footprint_images"]
    for tbl in tables_to_check:
        results_vector, errors = run_quality_tests_for_table(
            supabase, tbl, expected_species_count, allowed_species_categories, allowed_extensions
        )

        test_vector_str = str(results_vector)
        error_desc = "; ".join(errors) if errors else "No issues detected"

        # Insert log record
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
    We do 3 tests for each table: Exhaustiveness, Pertinence, Accuracy
    plus some checks for missing cells.

    results = [exhaustiveness, pertinence, accuracy]
    0=fail, 1=pass, 2=n/a
    """
    results = [2, 2, 2]
    errors = []

    if table_name == "infos_especes":
        # (1) Exhaustiveness
        r = supabase.table("infos_especes").select("id", count="exact").execute()
        actual_count = r.count or len(r.data)
        if actual_count < expected_species_count:
            results[0] = 0
            errors.append(f"Exhaustiveness: Only {actual_count} rows, expected >= {expected_species_count}.")
        else:
            results[0] = 1

        # (2) Pertinence: check if there's a "Categorie" column
        rows = supabase.table("infos_especes").select("*").execute().data
        if rows and "Categorie" in rows[0]:
            invalid_cat = 0
            for row in rows:
                cval = row.get("Categorie")
                if cval and cval not in allowed_species_categories:
                    invalid_cat += 1
            if invalid_cat > 0:
                results[1] = 0
                errors.append(f"Pertinence: {invalid_cat} row(s) have invalid 'Categorie'.")
            else:
                results[1] = 1
        else:
            results[1] = 2

        # (3) Accuracy: duplicates or missing Espèce
        species_counts = {}
        null_espece = 0
        for row in rows:
            sp = row.get("Espèce")
            if not sp:
                null_espece += 1
                continue
            species_counts[sp] = species_counts.get(sp, 0) + 1
        duplicates = [k for k, v in species_counts.items() if v > 1]
        if duplicates:
            errors.append(f"Accuracy: Duplicate Espèce => {duplicates}")
        if null_espece > 0:
            errors.append(f"Accuracy: {null_espece} row(s) missing Espèce.")
        if duplicates or null_espece:
            results[2] = 0
        else:
            results[2] = 1

    elif table_name == "footprint_images":
        # (1) Exhaustiveness
        data_imgs = supabase.table("footprint_images").select("*").execute().data
        data_species = supabase.table("infos_especes").select("id").execute().data
        sp_with_imgs = set(r["species_id"] for r in data_imgs if r.get("species_id") is not None)
        sp_all = set(r["id"] for r in data_species if r.get("id") is not None)
        if len(sp_with_imgs) < len(sp_all):
            results[0] = 0
            errors.append(f"Exhaustiveness: Only {len(sp_with_imgs)}/{len(sp_all)} species have images.")
        else:
            results[0] = 1

        # (2) Pertinence: check file extension from image_name
        invalid_ext = 0
        for row in data_imgs:
            iname = row.get("image_name", "").lower()
            if iname and not any(iname.endswith(ext) for ext in allowed_extensions):
                invalid_ext += 1
        if invalid_ext > 0:
            results[1] = 0
            errors.append(f"Pertinence: {invalid_ext} images have an invalid extension.")
        else:
            results[1] = 1

        # (3) Accuracy: referential integrity & missing fields
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
            errors.append(f"Accuracy: {len(missing_sp)} images reference invalid species_id => {missing_sp}")
        if missing_name > 0:
            errors.append(f"Accuracy: {missing_name} row(s) missing image_name.")
        if missing_url > 0:
            errors.append(f"Accuracy: {missing_url} row(s) missing image_url.")
        if missing_sp or missing_name or missing_url:
            results[2] = 0
        else:
            results[2] = 1

    else:
        # Some other table (not relevant here)
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

    # Step A) selectively reset tables
    maybe_reset_tables(supabase)

    # Step B) fill or partially fill infos_especes
    ensure_infos_especes_filled(supabase, BUCKET_NAME, "infos_especes.xlsx")

    # Step C) process images (duplicate check, resizing, partial upsert), then fill missing name/url
    process_images(supabase, BUCKET_NAME, "Mammifères/", "processed_data/")

    # Step D) run data quality checks
    run_data_quality_checks_and_log(supabase)

if __name__ == "__main__":
    main()
