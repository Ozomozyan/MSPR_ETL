#!/usr/bin/env python3

"""
ETL pipeline script with:
1. Optional reset of both infos_especes & footprint_images if they are empty.
2. Filling infos_especes from XLSX + fallback files.
3. Processing images from GCS (checking corruption/duplicates).
4. Updating/inserting rows in footprint_images so that if image_name or image_url is missing,
   it corrects that row rather than creating duplicates.
5. Data quality checks logged to data_quality_logs.
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
    return storage.Client()  # Requires GOOGLE_APPLICATION_CREDENTIALS in environment

# ------------------------------------------------------------------------------
# 3. MAYBE RESET TABLES IF BOTH EMPTY
# ------------------------------------------------------------------------------
def maybe_reset_tables(supabase: Client):
    """
    Calls reset_my_tables() if both infos_especes AND footprint_images are empty.
    Otherwise, skip the reset.
    Ensure your 'reset_my_tables' SQL truncates both tables:
        TRUNCATE TABLE infos_especes RESTART IDENTITY CASCADE;
        TRUNCATE TABLE footprint_images RESTART IDENTITY CASCADE;
    """
    r_especes = supabase.table("infos_especes").select("id").limit(1).execute()
    r_images = supabase.table("footprint_images").select("id").limit(1).execute()
    count_especes = len(r_especes.data)
    count_images = len(r_images.data)

    if count_especes == 0 and count_images == 0:
        print("Both infos_especes & footprint_images empty -> resetting tables via 'reset_my_tables' RPC.")
        supabase.rpc("reset_my_tables").execute()
        print("Tables truncated & sequences reset.")
    else:
        print(
            f"Skipping reset: infos_especes has {count_especes} row(s), "
            f"footprint_images has {count_images} row(s)."
        )


# ------------------------------------------------------------------------------
# 4. FILL infos_especes (with fallback column files)
# ------------------------------------------------------------------------------
def ensure_infos_especes_filled(
    supabase: Client,
    bucket_name: str,
    xlsx_blob_name: str = "infos_especes.xlsx",
):
    """
    If infos_especes is empty, read XLSX & fallback files & insert.
    If partially filled, only insert new species that aren't in DB yet.
    """
    resp = supabase.table("infos_especes").select("*").execute()
    count_infos = len(resp.data)

    if count_infos == 0:
        print("infos_especes table empty. Will fill from GCS xlsx.")
    else:
        print(f"infos_especes has {count_infos} row(s). Possibly partial fill...")

    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)

    blob_main = bucket.blob(xlsx_blob_name)
    if not blob_main.exists():
        raise FileNotFoundError(f"Cannot find {xlsx_blob_name} in bucket {bucket_name}.")

    with tempfile.TemporaryDirectory() as tmpdir:
        local_main_path = os.path.join(tmpdir, xlsx_blob_name)
        blob_main.download_to_filename(local_main_path)
        print("Downloaded main infos_especes.xlsx")

        df_main = pd.read_excel(local_main_path)

        # Fallback column files
        fallback_files = [
            ("espece.xlsx", "Espèce"),
            ("description.xlsx", "Description"),
            ("nom_latin.xlsx", "Nom_latin"),
            ("famille.xlsx", "Famille"),
            ("taille.xlsx", "Taille"),
            ("region.xlsx", "Région"),
            ("habitat.xlsx", "Habitat"),
            ("fun_fact.xlsx", "Fun_fact"),
        ]

        # Fill missing cells from fallback
        for fb_name, col_name in fallback_files:
            fb_blob = bucket.blob(fb_name)
            if fb_blob.exists():
                local_fb_path = os.path.join(tmpdir, fb_name)
                fb_blob.download_to_filename(local_fb_path)
                print(f"Downloaded fallback: {fb_name}")

                df_fb = pd.read_excel(local_fb_path)
                if col_name not in df_fb.columns:
                    print(f"Warning: {fb_name} lacks column '{col_name}'. Skipping.")
                    continue

                rows_to_check = min(len(df_main), len(df_fb))
                for i in range(rows_to_check):
                    if pd.isnull(df_main.loc[i, col_name]):
                        df_main.loc[i, col_name] = df_fb.loc[i, col_name]
            else:
                print(f"No fallback file {fb_name} found; skipping {col_name} fallback.")

        # Replace inf w/ nan, then nan -> None
        df_main.replace([np.inf, -np.inf], np.nan, inplace=True)
        df_main = df_main.where(df_main.notnull(), None)

        # Rename columns (replace spaces w/ underscores)
        rename_map = {}
        for col in df_main.columns:
            clean_col = re.sub(r"\\s+", "_", col.strip())
            rename_map[col] = clean_col
        df_main.rename(columns=rename_map, inplace=True)

        records = df_main.to_dict(orient="records")

        if count_infos == 0:
            # Insert all
            print("Inserting all rows (table was empty).")
            chunk_size = 500
            for i in range(0, len(records), chunk_size):
                supabase.table("infos_especes").insert(records[i:i+chunk_size]).execute()
        else:
            # Insert only species not in DB
            existing_rows = supabase.table("infos_especes").select("Espèce").execute()
            existing_species = {r["Espèce"] for r in existing_rows.data if r.get("Espèce")}
            new_records = []
            for rec in records:
                sp_name = rec.get("Espèce")
                if sp_name not in existing_species:
                    new_records.append(rec)
            if new_records:
                print(f"Inserting {len(new_records)} new row(s).")
                chunk_size = 500
                for i in range(0, len(new_records), chunk_size):
                    supabase.table("infos_especes").insert(new_records[i:i+chunk_size]).execute()
            else:
                print("No new species to add.")


# ------------------------------------------------------------------------------
# 5. FETCH SPECIES MAP
# ------------------------------------------------------------------------------
def fetch_species_map(supabase: Client) -> dict:
    """
    Return { species_name -> id } from infos_especes
    """
    data = supabase.table("infos_especes").select("id, Espèce").execute()
    mapping = {}
    for row in data.data:
        mapping[row["Espèce"]] = row["id"]
    return mapping

# ------------------------------------------------------------------------------
# 6. FIND FOOTPRINT ROW - NEW (HANDLES PARTIAL MISSING FIELDS)
# ------------------------------------------------------------------------------
def find_footprint_row(supabase: Client, species_id: int, image_name: str, image_url: str):
    """
    Search footprint_images for a row that either matches (species_id, image_name)
    OR (species_id, image_url). If found, return that row. Otherwise None.
    """
    # 1) Try matching by (species_id, image_name)
    q = (
        supabase.table("footprint_images")
        .select("*")
        .eq("species_id", species_id)
        .eq("image_name", image_name)
        .maybe_single()
        .execute()
    )
    if q.data:
        return q.data

    # 2) If not found, try matching by (species_id, image_url)
    q = (
        supabase.table("footprint_images")
        .select("*")
        .eq("species_id", species_id)
        .eq("image_url", image_url)
        .maybe_single()
        .execute()
    )
    if q.data:
        return q.data

    return None

# ------------------------------------------------------------------------------
# 7. PROCESS IMAGES (UPDATE/INSERT MISSING FIELDS)
# ------------------------------------------------------------------------------
def process_images(
    supabase: Client,
    bucket_name: str,
    raw_prefix: str = "Mammifères/",
    processed_prefix: str = "processed_data/"
):
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)

    species_map = fetch_species_map(supabase)

    raw_blobs = bucket.list_blobs(prefix=raw_prefix)
    image_candidates = []
    for blob in raw_blobs:
        if blob.name.endswith("/"):
            continue
        parts = blob.name.split("/")
        if len(parts) < 3:
            print(f"Skipping {blob.name}, no subfolder structure.")
            continue
        species_name = parts[1]
        img_filename = parts[-1]
        image_candidates.append((blob.name, species_name, img_filename))

    local_temp_raw = tempfile.mkdtemp(prefix="raw_images_")
    local_temp_processed = tempfile.mkdtemp(prefix="processed_images_")

    seen_hashes = set()
    to_upload = []

    for blob_name, species_name, filename in image_candidates:
        if species_name not in species_map:
            print(f"Species '{species_name}' not in infos_especes. Skipping {blob_name}.")
            continue

        local_raw_path = os.path.join(local_temp_raw, filename)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(local_raw_path)

        # corruption check
        try:
            with Image.open(local_raw_path) as img:
                img.verify()
        except Exception as e:
            print(f"Corrupt: {blob_name}, skipping. Error={e}")
            continue

        # same-run duplicate check by hash
        filehash = hashlib.md5(open(local_raw_path, "rb").read()).hexdigest()
        if filehash in seen_hashes:
            print(f"Duplicate in this run => skip {blob_name}")
            continue
        seen_hashes.add(filehash)

        # resize
        local_processed_path = os.path.join(local_temp_processed, filename)
        try:
            with Image.open(local_raw_path) as img:
                img_resized = img.resize((128, 128))
                img_resized.save(local_processed_path)
        except Exception as e:
            print(f"Error resizing {blob_name}: {e}")
            continue

        to_upload.append((local_processed_path, species_name, filename))

    # Upload & update/insert
    for local_path, species_name, filename in to_upload:
        new_blob_path = f"{processed_prefix}{species_name}/{filename}"
        new_blob = bucket.blob(new_blob_path)
        new_blob.upload_from_filename(local_path)

        # Build our desired record
        sp_id = species_map[species_name]
        rec = {
            "species_id": sp_id,
            "image_name": filename,
            "image_url": new_blob.public_url,  # or private
        }

        # 1) Try finding existing row by (species_id, image_name) or (species_id, image_url)
        row = find_footprint_row(supabase, sp_id, filename, new_blob.public_url)
        if row:
            # row found => update whichever field is missing
            updates = {}
            if not row.get("image_name"):  # or row["image_name"] is None
                updates["image_name"] = rec["image_name"]
            if not row.get("image_url"):
                updates["image_url"] = rec["image_url"]

            if updates:
                supabase.table("footprint_images").update(updates).eq("id", row["id"]).execute()
                print(f"Updated row ID={row['id']} with {updates}.")
            else:
                print(f"Row ID={row['id']} already has name & link; no update needed.")
        else:
            # no row => insert
            supabase.table("footprint_images").insert(rec).execute()
            print(f"Inserted new footprint row: {rec}")

    print("Image processing & insertion complete.")

# ------------------------------------------------------------------------------
# 8. DATA QUALITY CHECKS
# ------------------------------------------------------------------------------
def record_data_quality_result(supabase: Client, table_name: str, test_vector, details: str = ""):
    payload = {
        "table_name": table_name,
        "test_vector": test_vector,
        "details": details,
        "run_timestamp": datetime.utcnow().isoformat()
    }
    supabase.table("data_quality_logs").insert(payload).execute()

def perform_data_quality_checks_for_infos_especes(supabase: Client):
    # 3 checks => [exhaustiveness, pertinence, accuracy]
    exhaustiveness = 1
    pertinence = 1
    accuracy = 1
    details_list = []

    infos = supabase.table("infos_especes").select("id, Espèce, Nom_latin").execute()
    rows = infos.data
    if len(rows) < 13:
        exhaustiveness = 0
        details_list.append(f"Exhaustiveness: found {len(rows)}, expected >=13.")

    for r in rows:
        espece = r.get("Espèce", "")
        if not espece.strip():
            pertinence = 0
            details_list.append(f"Pertinence: id={r['id']} has empty Espèce.")
            break

    for r in rows:
        nom = r.get("Nom_latin", "")
        if not nom.strip():
            accuracy = 0
            details_list.append(f"Accuracy: id={r['id']} has empty Nom_latin.")
            break

    return [exhaustiveness, pertinence, accuracy], "\n".join(details_list)

def perform_data_quality_checks_for_footprint_images(supabase: Client):
    exhaustiveness = 1
    pertinence = 1
    accuracy = 1
    details_list = []

    imgs = supabase.table("footprint_images").select("id, species_id, image_url").execute()
    if len(imgs.data) == 0:
        exhaustiveness = 0
        details_list.append("Exhaustiveness: 0 rows in footprint_images.")

    valid_ids = set(r["id"] for r in supabase.table("infos_especes").select("id").execute().data)
    for r in imgs.data:
        if r["species_id"] not in valid_ids:
            pertinence = 0
            details_list.append(f"Pertinence: row {r['id']} references invalid species_id {r['species_id']}.")
            break

    for r in imgs.data:
        url = r.get("image_url", "")
        if not url.startswith("http"):
            accuracy = 0
            details_list.append(f"Accuracy: row {r['id']} has invalid url={url}")
            break

    return [exhaustiveness, pertinence, accuracy], "\n".join(details_list)

# ------------------------------------------------------------------------------
# 9. MAIN
# ------------------------------------------------------------------------------
def main():
    from datetime import datetime

    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "bucket-mspr_epsi-vine-449913-f6")
    if not BUCKET_NAME:
        raise ValueError("Please set GCS_BUCKET_NAME in environment.")

    supabase = get_supabase_client()

    # Possibly reset if both empty
    maybe_reset_tables(supabase)

    # Fill infos_especes
    ensure_infos_especes_filled(supabase, BUCKET_NAME, "infos_especes.xlsx")

    # Process images
    process_images(supabase, BUCKET_NAME, "Mammifères/", "processed_data/")

    # Data quality checks
    dq_vec_infos, dq_details_infos = perform_data_quality_checks_for_infos_especes(supabase)
    record_data_quality_result(supabase, "infos_especes", dq_vec_infos, dq_details_infos)
    print("infos_especes data quality =>", dq_vec_infos, dq_details_infos)

    dq_vec_images, dq_details_images = perform_data_quality_checks_for_footprint_images(supabase)
    record_data_quality_result(supabase, "footprint_images", dq_vec_images, dq_details_images)
    print("footprint_images data quality =>", dq_vec_images, dq_details_images)

if __name__ == "__main__":
    main()
