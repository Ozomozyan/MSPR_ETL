#!/usr/bin/env python3

import os
import tempfile
from google.cloud import storage
from PIL import Image

def get_gcs_client():
    """Return a Google Cloud Storage client using default credentials."""
    # Ensure GOOGLE_APPLICATION_CREDENTIALS is set
    return storage.Client()

def main():
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "bucket-mspr_epsi-vine-449913-f6")
    print(f"Using bucket: {BUCKET_NAME}")
    
    client = get_gcs_client()
    bucket = client.bucket(BUCKET_NAME)

    print("\n--- PHASE 1: List all possible 'directories' in top level ---")
    # We'll list blobs with a delimiter to see 'directory-like' prefixes
    top_level_blobs = client.list_blobs(bucket_or_name=BUCKET_NAME, delimiter='/')
    prefixes = top_level_blobs.prefixes
    if not prefixes:
        print("No subdirectories found at top level.")
    else:
        for prefix in prefixes:
            print(f"Found directory-like prefix: {prefix}")

    print("\n--- PHASE 2: For each top-level prefix, list its subcontents ---")
    if not prefixes:
        print("No directories to explore further.")
    else:
        for prefix in prefixes:
            print(f"\nExploring prefix: {prefix}")
            sub_blob_iterator = client.list_blobs(BUCKET_NAME, prefix=prefix, delimiter='/')
            sub_prefixes = sub_blob_iterator.prefixes
            if sub_prefixes:
                print(f"  Subdirectories of {prefix}:")
                for sp in sub_prefixes:
                    print(f"    {sp}")
            else:
                print(f"  No nested subdirectories under {prefix}")

            found_any_file = False
            for blob in sub_blob_iterator:
                found_any_file = True
                print(f"  File: {blob.name} (size={blob.size} bytes)")
            if not found_any_file:
                print(f"  No direct files in {prefix}")

    # Specifically look for 'Mammifères/' prefix:
    print("\n--- PHASE 3: List everything under 'Mammifères/' ---")
    mammals_prefix = "Mammifères/"
    mammif_blobs = client.list_blobs(BUCKET_NAME, prefix=mammals_prefix)
    
    found_files = []
    for blob in mammif_blobs:
        print(f"Found object: {blob.name}")
        if not blob.name.endswith("/"):
            found_files.append(blob)

    print("\n--- PHASE 4: Download & test process each image file ---")
    if not found_files:
        print("No files found under Mammifères/. Exiting early.")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        for blob in found_files:
            local_path = os.path.join(temp_dir, os.path.basename(blob.name))
            print(f"Downloading {blob.name} to {local_path}")
            blob.download_to_filename(local_path)

            # Attempt to open & resize the image
            try:
                with Image.open(local_path) as img:
                    print(f"  {blob.name} opened successfully, size={img.size}")
                    resized = img.resize((128, 128))
                    resized_path = os.path.join(temp_dir, "resized_" + os.path.basename(blob.name))
                    resized.save(resized_path)
                    print(f"  Resized and saved to {resized_path}")
            except Exception as e:
                print(f"  ERROR: Could not process {blob.name}: {e}")

    print("\nAll done. Check logs above to see which directories/files were found and processed.")

if __name__ == '__main__':
    main()
