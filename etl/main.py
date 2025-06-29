#!/usr/bin/env python3

"""
ETL pipeline script for:
 1. Selectively resetting tables that are empty:
    - If infos_especes is empty, reset it
    - If footprint_images is empty, reset it
    - If both are empty, reset both
 2. Filling/updating infos_especes (merging fallback columns)
    -> now also handles missing `Espèce` rows in a final pass
 3. Processing images in Google Cloud Storage
    -> duplication/corruption checks, resizing, upserting references in footprint_images
    -> if image_name or image_url is missing, fill it from the other field when possible
 4. Cleanup any duplicate rows in footprint_images (same species_id, image_name)
 5. Running data quality tests (exhaustiveness, pertinence, accuracy, missing cells, file extension checks)
 6. Logging data quality results in data_quality_log
"""

import os
import hashlib
import pandas as pd
import numpy as np
import tempfile
import re
import datetime
import urllib.parse
from collections import defaultdict

from PIL import Image
from google.cloud import storage
from supabase import create_client, Client

import albumentations as A

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
      - If neither is empty => skip.
    """
    r_especes = supabase.table("infos_especes").select("id").limit(1).execute()
    r_images = supabase.table("footprint_images").select("id").limit(1).execute()

    count_especes = len(r_especes.data)
    count_images = len(r_images.data)

    if count_especes == 0 and count_images == 0:
        print("Both infos_especes and footprint_images are empty. Resetting BOTH tables...")
        supabase.rpc("reset_my_tables").execute()
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

# ─────── Data-set balancing targets ───────
TARGET_IMAGES_PER_SPECIES = int(os.getenv("TARGET_IMAGES_PER_SPECIES", 500))
AUG_FOLDER_PREFIX = "augmented_data/"
AUG_PER_ORIGINAL_MAX  = 3          # safety: don’t explode storage

# ─────── Albumentations transforms ───────
AUGMENT_PIPE = A.Compose([
    A.RandomRotate90(),
    A.HorizontalFlip(),
    A.VerticalFlip(),
    A.RandomBrightnessContrast(p=0.2),
    A.GaussianBlur(p=0.2),
])


def augment_and_balance(original_paths: list[str],    # CHANGED
                        species_name: str,
                        current_count: int,
                        gcs_bucket):
    deficit = TARGET_IMAGES_PER_SPECIES - current_count
    if deficit <= 0 or not original_paths:
        return 0, []

    n_augm = min(deficit, len(original_paths) * AUG_PER_ORIGINAL_MAX)
    created_meta = []

    for i in range(n_augm):
        src = original_paths[i % len(original_paths)]   
        img = np.array(Image.open(src).convert("RGB"))
        aug_img = AUGMENT_PIPE(image=img)["image"]
        aug_name = f"aug_{i}_{os.path.basename(src)}"
        local_aug_path = os.path.join(os.path.dirname(src), aug_name)
        Image.fromarray(aug_img).save(local_aug_path)

        gcs_path = f"{AUG_FOLDER_PREFIX}{species_name}/{aug_name}"
        gcs_bucket.blob(gcs_path).upload_from_filename(local_aug_path)
        
        created_meta.append(
            (species_name, aug_name,
             f"https://storage.googleapis.com/{gcs_bucket.name}/{gcs_path}")
        )

    return n_augm, created_meta


def write_dataset_manifest(supabase, species_counts: dict):
    now = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    commit = os.getenv("GIT_COMMIT", "unknown")
    script_hash = hashlib.sha256(open(__file__, "rb").read()).hexdigest()
    manifest = {
        "version_id": f"{now}-{commit[:7]}",
        "created_at": now,
        "git_commit": commit,
        "total_images": sum(species_counts.values()),
        "images_per_species": species_counts,
        "etl_script_sha256": script_hash,
    }
    supabase.table("dataset_versions").insert(manifest).execute()
    print(f"Dataset version recorded: {manifest['version_id']}")


# -------------------------------------------------------------------------
# 4. FILL/UPDATE INFOS_ESPECES
#    (Reads main xlsx + fallback, merges them, does partial upserts, then
#     attempts to fill missing `Espèce` from the same data.)
# -------------------------------------------------------------------------
def ensure_infos_especes_filled(supabase: Client, bucket_name: str, xlsx_blob_name: str):
    """
    1) Read main infos_especes.xlsx plus fallback files into df_main.
    2) Insert or partially update the DB from df_main (including rows missing Espèce).
    3) Then do a final pass to fill any DB rows that still have Espèce missing
       by matching them with df_main on some columns (if possible).
    """
    response = supabase.table("infos_especes").select("*").execute()
    count_infos = len(response.data)

    if count_infos > 0:
        print(f"infos_especes already has {count_infos} row(s). We'll do partial fill if needed.")
    else:
        print("infos_especes is empty. Will insert data from GCS xlsx...")

    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)
    blob_main = bucket.blob(xlsx_blob_name)
    if not blob_main.exists():
        raise FileNotFoundError(f"Could not find {xlsx_blob_name} in {bucket_name}.")

    # -- Download & read main Excel
    with tempfile.TemporaryDirectory() as tmpdir:
        local_main_path = os.path.join(tmpdir, xlsx_blob_name)
        blob_main.download_to_filename(local_main_path)
        print(f"Downloaded main Excel: {xlsx_blob_name}")

        df_main = pd.read_excel(local_main_path)

        # -- Merge fallback files (description.xlsx, etc.)
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

        # Rename columns (spaces -> underscores) e.g. "Nom latin" -> "Nom_latin"
        rename_map = {}
        for c in df_main.columns:
            rename_map[c] = re.sub(r"\s+", "_", c.strip())
        df_main.rename(columns=rename_map, inplace=True)

        # -- Insert or update in DB
        records = df_main.to_dict(orient="records")
        if count_infos == 0:
            # Table is empty => insert all rows (even if Espèce is missing).
            print("Inserting all rows into infos_especes (table empty initially).")
            chunk_size = 500
            for i in range(0, len(records), chunk_size):
                batch = records[i : i + chunk_size]
                supabase.table("infos_especes").insert(batch).execute()
        else:
            # Table has partial data => do row-by-row merges
            for rec in records:
                espece_in_excel = rec.get("Espèce")  # might be None
                # We'll match on 'nom_latin' or some other columns if "Espèce" is None or blank
                # If Espèce is present, we can do an upsert. If not, we still want to
                # insert/update because it might fill other columns.

                if espece_in_excel:
                    # attempt normal upsert by Espèce
                    existing_q = supabase.table("infos_especes").select("*") \
                                .eq("Espèce", espece_in_excel).limit(1).execute()
                    if not existing_q.data:
                        # Insert if not found
                        supabase.table("infos_especes").insert(rec).execute()
                    else:
                        # partial update
                        existing_row = existing_q.data[0]
                        to_update = {}
                        for k, v in rec.items():
                            if existing_row.get(k) in [None, ""] and v not in [None, ""]:
                                to_update[k] = v
                        if to_update:
                            supabase.table("infos_especes").update(to_update).eq("Espèce", espece_in_excel).execute()
                else:
                    # If Excel row doesn't have Espèce, we can attempt a fallback match on "nom_latin", etc.
                    # or we can insert a row with Espèce=None. We'll do a naive approach:
                    # We'll look for a row with the same "Nom_latin" if it's not empty:
                    nom_latin = rec.get("Nom_latin")
                    if nom_latin:
                        # Try matching by nom_latin
                        existing_q = supabase.table("infos_especes").select("*") \
                                    .eq("Nom_latin", nom_latin).limit(1).execute()
                        if not existing_q.data:
                            # Insert as new row
                            supabase.table("infos_especes").insert(rec).execute()
                        else:
                            # partial update
                            existing_row = existing_q.data[0]
                            to_update = {}
                            for k, v in rec.items():
                                if existing_row.get(k) in [None, ""] and v not in [None, ""]:
                                    to_update[k] = v
                            if to_update:
                                supabase.table("infos_especes").update(to_update).eq("id", existing_row["id"]).execute()
                    else:
                        # If no "nom_latin" to match, we just insert a new row with partial data
                        supabase.table("infos_especes").insert(rec).execute()

    # Finally, fill any DB rows that STILL have Espèce missing, from the same df_main if possible
    fill_missing_espece_in_db_from_df(supabase, df_main)

# -------------------------------------------------------------------------
# 4B. FILL MISSING ESPÈCE IN DB FROM THE SAME DF
# -------------------------------------------------------------------------
def fill_missing_espece_in_db_from_df(supabase: Client, df_main: pd.DataFrame):
    """
    If there's any row in infos_especes that still has Espèce = None or "",
    we try to find a matching row in df_main that has Espèce.
    We match on some set of columns (like 'Nom_latin', 'Famille', etc.)
    so we can fill the missing Espèce.

    Adjust 'match_columns' to your real data (must match your renamed columns).
    """
    # Suppose we rely on "Nom_latin" to identify the species if Espèce is missing
    match_columns = ["Nom_latin", "Famille", "Région"]  # adjust as needed

    # Convert df_main into a dict keyed by the match columns
    # e.g. {("Felis catus","Felidae","Monde"): "Chat"}
    # We'll store the entire row so we can get "Espèce"
    lookup_map = {}

    for _, row in df_main.iterrows():
        # build a tuple from the match_columns
        key_tuple = tuple(str(row.get(col) or "").strip().lower() for col in match_columns)
        # store "Espèce" from df_main if it exists
        espece_val = row.get("Espèce")
        if espece_val:
            lookup_map[key_tuple] = espece_val

    # Now, find rows in DB that are missing Espèce
    db_rows = supabase.table("infos_especes").select("*").execute().data
    updated_count = 0

    for row in db_rows:
        espece_db = row.get("Espèce")
        if espece_db in [None, ""]:
            # attempt to match
            key_tuple = tuple(str(row.get(col) or "").strip().lower() for col in match_columns)
            espece_candidate = lookup_map.get(key_tuple)
            if espece_candidate:
                # fill it
                supabase.table("infos_especes").update({"Espèce": espece_candidate}).eq("id", row["id"]).execute()
                updated_count += 1

    if updated_count > 0:
        print(f"Filled missing 'Espèce' for {updated_count} DB row(s) using df_main references.")


# -------------------------------------------------------------------------
# 5. PROCESS IMAGES & FILL MISSING FIELDS
# -------------------------------------------------------------------------
def process_images(
    supabase: Client,
    bucket_name: str,
    raw_folder_prefix: str = "Mammifères/",
    processed_folder_prefix: str = "processed_data/"
):
    species_originals = defaultdict(list)  
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)

    # We assume now that 'infos_especes' is complete enough that foreign keys are valid
    species_map = fetch_species_map(supabase)
    
    fill_missing_image_fields(supabase, bucket_name, processed_folder_prefix)

    raw_blobs = bucket.list_blobs(prefix=raw_folder_prefix)
    image_candidates = []
    for blob in raw_blobs:
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

        # Check corruption
        try:
            with Image.open(local_raw_path) as im:
                im.verify()
        except Exception as e:
            print(f"Corrupt image {blob_name}: {e}")
            continue

        # Duplicate check
        filehash = hashlib.md5(open(local_raw_path, 'rb').read()).hexdigest()
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
                species_originals[species_name].append(local_proc_path)
        except Exception as e:
            print(f"Error resizing {blob_name}: {e}")
            continue

        images_to_upload.append((local_proc_path, species_name, image_filename))

    # Upload processed & upsert in DB
    for local_path, species_name, filename in images_to_upload:
        new_blob_path = f"{processed_folder_prefix}{species_name}/{filename}"
        new_blob = bucket.blob(new_blob_path)
        try:
            new_blob.upload_from_filename(local_path)
        except Exception as e:
            print(f"Error uploading processed {local_path}: {e}")
            continue

        record = {
            "species_id": species_map[species_name],
            "image_name": filename,
            "image_url": new_blob.public_url,
        }

        # Attempt partial upsert for footprint_images
        existing_q = supabase.table("footprint_images") \
            .select("*") \
            .eq("species_id", record["species_id"]) \
            .eq("image_name", filename) \
            .execute()
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
            
    # ─── Data augmentation & class balancing ───
    # At this point all the originals are in GCS and in the DB.
    # Now ensure each species hits TARGET_IMAGES_PER_SPECIES by augmenting.
    added_total = 0
    # you’ll need to know how many originals you just uploaded per species:
    # build that map from images_to_upload
    species_counts = defaultdict(int)
    for _, sp_name, _ in images_to_upload:
        species_counts[sp_name] += 1

    # call your helper for each species
    aug_meta_total = [] 
    for sp, count in species_counts.items():
        originals = species_originals.get(sp, [])                     # NEW
        added, meta = augment_and_balance(originals, sp, count, bucket)  # CHANGED
        species_counts[sp] += added
        added_total += added
        aug_meta_total.extend(meta)
    print(f"Data augmentation: generated {added_total} extra images.")
    
    # ─── NEW / FIXED upsert augments into footprint_images ───
    for sp_name, filename, url in aug_meta_total:
        record = {
            "species_id": species_map[sp_name],
            "image_name": filename,
            "image_url": url,
            "is_augmented": True,
        }

        # 1️⃣ First, try to FIX an older row that has the same URL but a NULL image_name
        supabase.table("footprint_images") \
            .update({"image_name": filename, "is_augmented": True}) \
            .eq("species_id", record["species_id"]) \
            .eq("image_url", url) \
            .or_("image_name.is.null,image_name.eq.")\
            .execute()

        # 2️⃣ Then do the normal upsert (won't duplicate now)
        supabase.table("footprint_images") \
            .upsert(
                record,
                on_conflict="species_id,image_name",
                returning="minimal"
            ) \
            .execute()

    # If you want those augmented images to show up in footprint_images,
    # you can either upsert them here just like the originals,
    # or let your next pipeline (training) discover them by reading GCS.


    # Fill missing image_name/image_url if possible
    fill_missing_image_fields(supabase, bucket_name, processed_folder_prefix)

    # After filling, remove duplicates
    cleanup_duplicates_in_footprint_images(supabase)

    print("Image processing complete.")
    return species_counts 

def fill_missing_image_fields(supabase: Client, bucket_name: str, processed_folder_prefix: str):
    """
    If a row in footprint_images is missing image_name but has image_url, or vice versa,
    parse from the other field.
    """
    rows = supabase.table("footprint_images").select("*").execute().data
    updated_count = 0

    for r in rows:
        species_id = r.get("species_id")
        name_in_db = r.get("image_name")
        url_in_db = r.get("image_url")

        update_dict = {}

        # CASE 1: Missing image_name, but we have image_url
        if not name_in_db and url_in_db:
            encoded_filename = url_in_db.split("/")[-1]
            decoded_filename = urllib.parse.unquote(encoded_filename)
            if decoded_filename:
                update_dict["image_name"] = decoded_filename

        # CASE 2: Missing image_url, but we have image_name
        if not url_in_db and name_in_db and species_id:
            sp_row = (
                supabase.table("infos_especes")
                .select("Espèce")
                .eq("id", species_id)
                .execute()
                .data
            )
            if sp_row:
                sp_name = sp_row[0].get("Espèce")
                new_url = f"https://storage.googleapis.com/{bucket_name}/{processed_folder_prefix}{sp_name}/{name_in_db}"
                update_dict["image_url"] = new_url

        if update_dict:
            supabase.table("footprint_images").update(update_dict).eq("id", r["id"]).execute()
            updated_count += 1

    if updated_count > 0:
        print(f"Filled missing fields (image_name/image_url) for {updated_count} footprint_images rows.")

def cleanup_duplicates_in_footprint_images(supabase: Client):
    """
    Finds rows in `footprint_images` that share the same (species_id, image_name)
    and removes all but one (keeping the row with the smallest id).
    """
    all_rows = supabase.table("footprint_images").select("*").execute().data
    if not all_rows:
        return

    duplicates_map = defaultdict(list)
    for row in all_rows:
        key = (row.get("species_id"), row.get("image_name"))
        duplicates_map[key].append(row)

    for (sp_id, img_name), group_rows in duplicates_map.items():
        if len(group_rows) > 1:
            group_rows.sort(key=lambda r: r["id"])  # ascending by ID
            row_to_keep = group_rows[0]
            rows_to_delete = group_rows[1:]
            print(f"Found {len(group_rows)} duplicates for (species_id={sp_id}, image_name='{img_name}'). "
                  f"Keeping row ID={row_to_keep['id']} and removing the others.")
            
            for dup in rows_to_delete:
                supabase.table("footprint_images").delete().eq("id", dup["id"]).execute()
                
    # ─── extra dedup by URL (handles early rows with NULL image_name) ───
    url_map = defaultdict(list)
    for row in all_rows:
        key = (row.get("species_id"), row.get("image_url"))
        url_map[key].append(row)

    for (sp_id, url), duprows in url_map.items():
        if url and len(duprows) > 1:
            duprows.sort(key=lambda r: r["id"])
            for d in duprows[1:]:
                supabase.table("footprint_images").delete().eq("id", d["id"]).execute()
                
                
def cleanup_duplicates_in_infos_especes(supabase: Client):
    """
    Finds rows in `infos_especes` that share the same `Espèce`
    and removes all but one. We keep the row with the smallest `id`.
    """
    all_rows = supabase.table("infos_especes").select("*").execute().data
    if not all_rows:
        return

    # Group rows by their 'Espèce' value
    duplicates_map = defaultdict(list)
    for row in all_rows:
        espece_val = row.get("Espèce", None)
        duplicates_map[espece_val].append(row)

    # For each group with more than 1 row => keep the smallest ID, remove the rest
    for espece_val, group_rows in duplicates_map.items():
        if len(group_rows) > 1:
            # Sort ascending by "id"
            group_rows.sort(key=lambda r: r["id"])
            row_to_keep = group_rows[0]
            rows_to_delete = group_rows[1:]

            print(
                f"[infos_especes] Found {len(group_rows)} duplicates for Espèce='{espece_val}'. "
                f"Keeping row ID={row_to_keep['id']} and removing the others."
            )

            for dup in rows_to_delete:
                supabase.table("infos_especes").delete().eq("id", dup["id"]).execute()

# -------------------------------------------------------------------------
# 6. FETCH SPECIES MAP
# -------------------------------------------------------------------------
def fetch_species_map(supabase: Client) -> dict:
    """
    Create a dict mapping Espèce -> id from infos_especes.
    (We assume Espèce is unique enough to serve as the key.)
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
    allowed_extensions: list,
    bucket_name: str = None,
    raw_folder_prefix: str = "Mammifères/"
) -> (list, list):
    """
    Returns (results_vector, errors_list) where:
      results_vector = [exhaustiveness, pertinence, accuracy]
         each = 0 (fail), 1 (pass), 2 (N/A)
    """
    results = [2, 2, 2]  # [Exhaustiveness, Pertinence, Accuracy]
    errors = []

    # For cross-checking GCS files vs DB
    gcs_client = get_gcs_client() if bucket_name else None
    bucket = gcs_client.bucket(bucket_name) if gcs_client else None

    if table_name == "infos_especes":
        # ------------------------------------------------------------------------------
        # 1) Exhaustiveness
        # ------------------------------------------------------------------------------
        res = supabase.table("infos_especes").select("id", count="exact").execute()
        actual_count = res.count or len(res.data)

        if actual_count < expected_species_count:
            results[0] = 0  # fail
            errors.append(
                f"Exhaustiveness: Only {actual_count} rows in infos_especes, expected >= {expected_species_count}."
            )
        else:
            results[0] = 1  # pass

        # ------------------------------------------------------------------------------
        # 2) Pertinence (category checks, etc.)
        # ------------------------------------------------------------------------------
        rows = supabase.table("infos_especes").select("*").execute().data
        if rows and "Categorie" in rows[0]:
            invalid_count = 0
            for row in rows:
                cval = row.get("Categorie")
                if cval and cval not in allowed_species_categories:
                    invalid_count += 1
            if invalid_count > 0:
                results[1] = 0
                errors.append(f"Pertinence: {invalid_count} row(s) have invalid 'Categorie'.")
            else:
                results[1] = 1
        else:
            # maybe 'Categorie' doesn't exist => no check
            results[1] = 2

        # ------------------------------------------------------------------------------
        # 3) Accuracy (duplicates, missing Espèce, etc.)
        # ------------------------------------------------------------------------------
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
        # ------------------------------------------------------------------------------
        # 1) Exhaustiveness
        # ------------------------------------------------------------------------------
        data_imgs = supabase.table("footprint_images").select("id, species_id, image_name, image_url").execute().data
        
        # compare number of DB rows to the actual files in GCS (raw_folder_prefix)
        # This is an example logic: how many "Mammifères/.../*.jpg" are in GCS vs how many are in DB?
        if bucket:
            gcs_blobs = list(bucket.list_blobs(prefix=raw_folder_prefix))
            # Filter to actual files
            gcs_files = [
                b.name for b in gcs_blobs 
                if not b.name.endswith("/") and (b.name.lower().endswith(".jpg") 
                                                 or b.name.lower().endswith(".jpeg") 
                                                 or b.name.lower().endswith(".png"))
            ]
            gcs_count = len(gcs_files)
            db_count = len(data_imgs)

            # Decide how you define "exhaustive": do you want them to match exactly?
            # or do you just want DB_count >= some threshold?
            if db_count < gcs_count:
                results[0] = 0
                errors.append(f"Exhaustiveness: DB has {db_count} images but GCS has {gcs_count} images.")
            else:
                results[0] = 1
        else:
            # No bucket provided => skip
            results[0] = 2

        # ------------------------------------------------------------------------------
        # 2) Pertinence (file extension checks, correct species folder, etc.)
        # ------------------------------------------------------------------------------
        invalid_ext = 0
        for row in data_imgs:
            iname = (row.get("image_name") or "").lower()
            if iname and not any(iname.endswith(ext) for ext in allowed_extensions):
                invalid_ext += 1

        if invalid_ext > 0:
            results[1] = 0
            errors.append(f"Pertinence: {invalid_ext} images have an invalid extension.")
        else:
            results[1] = 1

        # Optionally, check if the species folder in GCS matches the species name in DB:
        #    1) fetch species from infos_especes
        #    2) ensure that the "folder" portion of the path matches the "Espèce"
        #    (This is an example idea; adapt as needed.)

        # ------------------------------------------------------------------------------
        # 3) Accuracy (referential integrity, duplicates, missing fields, etc.)
        # ------------------------------------------------------------------------------
        data_species = supabase.table("infos_especes").select("id").execute().data
        sp_all = {r["id"] for r in data_species if r.get("id")}

        # Check invalid references, missing image_name/url
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

        # Also check duplicates by (species_id, image_name)
        duplicates_map = defaultdict(int)
        for r in data_imgs:
            key = (r.get("species_id"), r.get("image_name"))
            duplicates_map[key] += 1
        dups = [k for k, count in duplicates_map.items() if count > 1]
        if dups:
            errors.append(f"Accuracy: Duplicate (species_id, image_name) => {dups}")

        # If any errors in Accuracy => fail
        if missing_sp or missing_name or missing_url or dups:
            results[2] = 0
        else:
            results[2] = 1

    else:
        # For other tables not in [infos_especes, footprint_images], skip checks
        results = [2,2,2]

    return results, errors

# -------------------------------------------------------------------------
# 8. MAIN
# -------------------------------------------------------------------------
def main():
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "bucket-mspr_epsi-vine-449913-f6")
    supabase = get_supabase_client()

    # For overall logging
    overall_success = True
    overall_errors = []

    try:
        maybe_reset_tables(supabase)
    except Exception as e:
        overall_success = False
        overall_errors.append(f"Error in maybe_reset_tables: {e}")

    try:
        ensure_infos_especes_filled(supabase, BUCKET_NAME, "infos_especes.xlsx")
        cleanup_duplicates_in_infos_especes(supabase)
    except Exception as e:
        overall_success = False
        overall_errors.append(f"Error in infos_especes step: {e}")

    try:
        species_counts = process_images(
            supabase, BUCKET_NAME, "Mammifères/", "processed_data/"
        )
    except Exception as e:
        overall_success = False
        overall_errors.append(f"Error in process_images: {e}")

    # Run data quality last
    try:
        run_data_quality_checks_and_log(supabase)
    except Exception as e:
        overall_success = False
        overall_errors.append(f"Error in data quality checks: {e}")

    # Finally, log the overall success/failure if you want a single line in data_quality_log:
    run_timestamp = datetime.datetime.utcnow().isoformat()
    supabase.table("data_quality_log").insert({
        "execution_time": run_timestamp,
        "table_name": "ETL_process",  # or "all_tables"
        "test_results": "[1]" if overall_success else "[0]",
        "error_description": ("No errors" if overall_success else "; ".join(overall_errors))
    }).execute()
    
    try:
        write_dataset_manifest(supabase, species_counts)
    except Exception as e:
        print(f"Could not write dataset manifest: {e}")

if __name__ == "__main__":
    main()