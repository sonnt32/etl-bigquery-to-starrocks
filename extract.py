from google.cloud import bigquery, storage
from google.oauth2 import service_account

# 🔑 Configure credentials
GOOGLE_CREDENTIALS_PATH = "C:/Users/PC/Downloads/brain-puzzle-tricky-test.json"
credentials = service_account.Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH)

project_id = credentials.project_id
bq_client = bigquery.Client(credentials=credentials, project=project_id)
storage_client = storage.Client(credentials=credentials, project=project_id)

# 🔍 Find dataset starting with "analytics_"
datasets = list(bq_client.list_datasets(project=project_id))
analytics_datasets = [d.dataset_id for d in datasets if d.dataset_id.startswith("analytics_")]

if not analytics_datasets:
    raise Exception("No dataset found starting with 'analytics_'")

dataset_id = analytics_datasets[0]
print(f"📦 Using dataset: {dataset_id}")

# 📍 Get dataset location
dataset_ref = bq_client.get_dataset(f"{project_id}.{dataset_id}")
dataset_location = dataset_ref.location
print(f"📍 Dataset location: {dataset_location}")

# 📦 Check export bucket
bucket_name = f"{project_id}-export-demo"
try:
    storage_client.get_bucket(bucket_name)
    print(f"✅ Bucket '{bucket_name}' already exists.")
except Exception:
    print(f"⚠️ Bucket '{bucket_name}' does not exist. Creating a new one...")
    storage_client.create_bucket(bucket_name, location=dataset_location)
    print(f"🎉 Bucket '{bucket_name}' has been successfully created.")

# 📋 List tables in the dataset
tables = list(bq_client.list_tables(dataset_id))
if not tables:
    raise Exception(f"Dataset {dataset_id} has no tables.")

# ⚙️ Configure export
job_config = bigquery.ExtractJobConfig()
job_config.destination_format = bigquery.DestinationFormat.PARQUET

# 🚀 Export each table
for table in tables:
    table_id = table.table_id
    table_ref = bigquery.TableReference.from_string(f"{project_id}.{dataset_id}.{table_id}")
    destination_uri = f"gs://{bucket_name}/export/{table_id}/*.parquet"

    print(f"🚀 Exporting table {table_id} → {destination_uri}")
    extract_job = bq_client.extract_table(table_ref, destination_uri, job_config=job_config)
    extract_job.result()
    print(f"✅ Export completed: {table_id}")

print("🎉 All tables exported successfully.")
