# load_all.py
import os
import pandas as pd
import gcsfs

# ==== Cấu hình ====
GOOGLE_CREDENTIALS_PATH = "C:/Users/PC/Downloads/brain-puzzle-tricky-test.json"
BUCKET_NAME = "brain-puzzle-tricky-test-export-demo"
TRANSFORM_PREFIX = "transform"  # thư mục đích trên GCS

# ==== Kết nối GCS ====
gcs = gcsfs.GCSFileSystem(token=GOOGLE_CREDENTIALS_PATH)

# ==== Lấy tất cả file transform parquet trong thư mục local ====
local_files = [f for f in os.listdir(".") if f.startswith("df_transformed_") and f.endswith(".parquet")]

if not local_files:
    raise FileNotFoundError("❌ Không tìm thấy file df_transformed_*.parquet trong thư mục hiện tại.")

print(f"📂 Tìm thấy {len(local_files)} file transform để upload.")

# ==== Upload từng file lên GCS ====
for local_file in local_files:
    table_name = local_file.replace("df_transformed_", "").replace(".parquet", "")
    output_uri = f"gs://{BUCKET_NAME}/{TRANSFORM_PREFIX}/{table_name}.parquet"

    print(f"🚀 Đang upload {local_file} → {output_uri}")
    df_transformed = pd.read_parquet(local_file)
    with gcs.open(output_uri, 'wb') as f:
        df_transformed.to_parquet(f, engine='pyarrow', index=False)

    print(f"✅ Đã ghi dữ liệu transform lên: {output_uri}")

print("🎉 Hoàn tất upload toàn bộ file transform.")
