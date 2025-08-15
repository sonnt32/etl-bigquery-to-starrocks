# load_all.py
import os
import pandas as pd
import gcsfs

# ==== Configuration ====
GOOGLE_CREDENTIALS_PATH = "C:/Users/PC/Downloads/brain-puzzle-tricky-test.json"
BUCKET_NAME = "brain-puzzle-tricky-test-export-demo"
TRANSFORM_PREFIX = "transform"  # destination folder on GCS

# ==== Connect to GCS ====
gcs = gcsfs.GCSFileSystem(token=GOOGLE_CREDENTIALS_PATH)

# ==== Get all transformed parquet files in local directory ====
local_files = [f for f in os.listdir(".") if f.startswith("df_transformed_") and f.endswith(".parquet")]

if not local_files:
    raise FileNotFoundError("❌ No df_transformed_*.parquet files found in the current directory.")

print(f"📂 Found {len(local_files)} transformed files to upload.")

# ==== Upload each file to GCS ====
for local_file in local_files:
    table_name = local_file.replace("df_transformed_", "").replace(".parquet", "")
    output_uri = f"gs://{BUCKET_NAME}/{TRANSFORM_PREFIX}/{table_name}.parquet"

    print(f"🚀 Uploading {local_file} → {output_uri}")
    df_transformed = pd.read_parquet(local_file)
    with gcs.open(output_uri, 'wb') as f:
        df_transformed.to_parquet(f, engine='pyarrow', index=False)

    print(f"✅ Successfully wrote transformed data to: {output_uri}")

print("🎉 Finished uploading all transformed files.")
