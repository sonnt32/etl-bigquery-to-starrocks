import os
import re
import pandas as pd
import pyarrow.parquet as pq
import gcsfs
import numpy as np
import json

# ===== 1. Configuration =====
GOOGLE_CREDENTIALS_PATH = "C:/Users/PC/Downloads/brain-puzzle-tricky-test.json"
BUCKET_NAME = "brain-puzzle-tricky-test-export-demo"
EXPORT_PREFIX = "export/"

# ===== 2. Connect to GCS =====
fs = gcsfs.GCSFileSystem(token=GOOGLE_CREDENTIALS_PATH)

# ===== 3. Get list of folders in export/ =====
folders = [f for f in fs.ls(f"{BUCKET_NAME}/{EXPORT_PREFIX}") if not f.endswith(".parquet")]

if not folders:
    raise ValueError(f"❌ No folders found in: {EXPORT_PREFIX}")

print(f"📂 Found {len(folders)} data folders.")

# ===== 4. Functions for nested field processing =====
NESTED_PARAMS_COLS = ["event_params", "user_properties"]
DICT_COLS = ["geo", "device", "app_info", "traffic_source"]
TIMESTAMP_COLS = ["event_timestamp", "user_first_touch_timestamp"]
TRIM_EVENT_NAME = True

def convert_nested_params_numpy(params):
    if params is None or (isinstance(params, float) and pd.isna(params)):
        return None
    if isinstance(params, np.ndarray):
        params_list = params.tolist()
    elif isinstance(params, list):
        params_list = params
    else:
        return None

    params_dict = {}
    for param in params_list:
        key = param.get("key")
        value = None
        v = param.get("value", {})
        if v.get("string_value") is not None:
            value = v["string_value"]
        elif v.get("int_value") is not None:
            value = v["int_value"]
        elif v.get("float_value") is not None:
            value = v["float_value"]
        elif v.get("double_value") is not None:
            value = v["double_value"]

        if key is not None and value is not None:
            params_dict[key] = value

    return json.dumps(params_dict) if params_dict else None

def normalize_json_string(s):
    try:
        return json.dumps(json.loads(s))
    except Exception:
        return s

def transform_data_with_pandas(df: pd.DataFrame) -> pd.DataFrame:
    # Nested params → JSON
    for col_name in NESTED_PARAMS_COLS:
        if col_name in df.columns:
            df[col_name] = df[col_name].apply(convert_nested_params_numpy)

    # Dictionary columns → JSON
    for col_name in DICT_COLS:
        if col_name in df.columns:
            df[col_name] = df[col_name].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
            df[col_name] = df[col_name].apply(normalize_json_string)

    # Convert timestamp (microseconds) → datetime
    for col_name in TIMESTAMP_COLS:
        if col_name in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col_name]):
            df[col_name] = pd.to_datetime(df[col_name] / 1_000_000, unit="s", errors="coerce")

    # Trim event_name
    if TRIM_EVENT_NAME and "event_name" in df.columns:
        df["event_name"] = df["event_name"].astype(str).str.strip()

    # Calculate retention_day
    if "event_timestamp" in df.columns and "user_first_touch_timestamp" in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df["event_timestamp"]) and pd.api.types.is_datetime64_any_dtype(df["user_first_touch_timestamp"]):
            df["retention_day"] = (
                df["event_timestamp"].dt.normalize() - df["user_first_touch_timestamp"].dt.normalize()
            ).dt.days

    return df

# ===== 5. Process each folder =====
for folder in folders:
    table_name = folder.split("/")[-1]  # Table name (e.g., events_intraday_20250803)
    print(f"\n📥 Processing table: {table_name}")

    # Get list of parquet files
    files = fs.ls(folder)
    parquet_files = [f"gs://{file}" for file in files if file.endswith(".parquet")]
    if not parquet_files:
        print(f"⚠️ No .parquet files found in {table_name}, skipping.")
        continue

    # Read data
    df = pd.concat([pq.ParquetDataset(pf, filesystem=fs).read().to_pandas() for pf in parquet_files], ignore_index=True)
    print(f"   ✅ Loaded {len(df)} rows.")

    # Transform
    df_transformed = transform_data_with_pandas(df)
    df_transformed = df_transformed[['event_date','event_timestamp','event_name','user_pseudo_id','event_params','user_properties','geo','device','app_info','platform','traffic_source','event_value_in_usd','retention_day']]

    # Save locally (or could write back to GCS)
    output_file = f"df_transformed_{table_name}.parquet"
    df_transformed.to_parquet(output_file, index=False)
    print(f"   💾 Saved file: {output_file}")

print("\n🎉 Finished transforming all tables.")
