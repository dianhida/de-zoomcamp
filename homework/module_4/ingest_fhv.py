import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import gzip
import shutil

# =========================
# Config
# =========================

BUCKET_NAME = "zoomcamp-kestra-dian-bucket-us"
CREDENTIALS_FILE = "keys/my-creds.json"  # sesuaikan
DOWNLOAD_DIR = "./downloads"
CHUNK_SIZE = 8 * 1024 * 1024
MAX_WORKERS = 4
MAX_RETRIES = 3

# URLs CSV.GZ dari FHV GitHub release
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"
FHV_FILES = [
    "fhv_tripdata_2021-01.csv.gz",
    "fhv_tripdata_2021-02.csv.gz",
    "fhv_tripdata_2021-03.csv.gz",
    "fhv_tripdata_2021-04.csv.gz",
    "fhv_tripdata_2021-05.csv.gz",
    "fhv_tripdata_2021-06.csv.gz",
    "fhv_tripdata_2021-07.csv.gz",
    "fhv_tripdata_2021-08.csv.gz",
    "fhv_tripdata_2021-09.csv.gz",
    "fhv_tripdata_2021-10.csv.gz",
    "fhv_tripdata_2021-11.csv.gz",
    "fhv_tripdata_2021-12.csv.gz",
]

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# =========================
# Initialize GCS client
# =========================

client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)


# =========================
# Helper Functions
# =========================

def create_bucket(bucket_name):
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' exists. Proceeding...")
    except NotFound:
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(f"Bucket '{bucket_name}' exists but is inaccessible. Abort.")
        sys.exit(1)


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(MAX_RETRIES):
        try:
            print(f"Uploading {file_path} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            if verify_gcs_upload(blob_name):
                print(f"Uploaded and verified: gs://{BUCKET_NAME}/{blob_name}")
                return
        except Exception as e:
            print(f"Upload failed: {e}")
        time.sleep(5)

    print(f"Giving up on {file_path} after {MAX_RETRIES} attempts.")


def fix_schema_and_parquet(csv_path):
    """
    1. Unzip .gz
    2. Load CSV
    3. Convert numeric columns to float
    4. Save to parquet
    """
    print(f"Processing {csv_path}...")

    # Unzip to temporary CSV
    tmp_csv = csv_path.replace(".gz", "")
    with gzip.open(csv_path, "rb") as f_in:
        with open(tmp_csv, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    df = pd.read_csv(tmp_csv, encoding="latin1")

    # Fix float columns (common in FHV)
    float_columns = [
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "airport_fee",
    ]
    for col in float_columns:
        if col in df.columns:
            df[col] = df[col].astype("float64")

    int_columns = {
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "SR_Flag": "Int64",
    }
        
    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")


    # Save to parquet
    parquet_path = tmp_csv.replace(".csv", ".parquet")
    table = pa.Table.from_pandas(df)
    print(table.schema)
    pq.write_table(table, parquet_path)
    print(f"Saved parquet: {parquet_path}")

    # Clean up temporary CSV
    os.remove(tmp_csv)

    return parquet_path


def download_and_process(file_name):
    url = f"{BASE_URL}/{file_name}"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")

        parquet_file = fix_schema_and_parquet(file_path)

        # Upload to GCS
        upload_to_gcs(parquet_file)
        return parquet_file
    except Exception as e:
        print(f"Failed to process {file_name}: {e}")
        return None


# =========================
# Main
# =========================

if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(download_and_process, FHV_FILES))

    print("All files downloaded, processed, and uploaded.")
