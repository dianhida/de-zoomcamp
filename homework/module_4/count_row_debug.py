import os
import pyarrow.fs as fs
import pyarrow.parquet as pq
import re

# set credential
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/slig/workspace/local/zoomcamp_cohort/homework/module_4/keys/my-creds.json"

# connect GCS
gcs = fs.GcsFileSystem()

# selector → HARUS ada bucket name
selector = fs.FileSelector("zoomcamp-kestra-dian-bucket-us", recursive=True)
all_files = gcs.get_file_info(selector)

# filter parquet
pattern = re.compile(r"fhv_tripdata_\d{4}-\d{2}\.parquet")
parquet_files = [f.path for f in all_files if pattern.search(f.path)]

print(f"Found {len(parquet_files)} parquet files.")

# hitung total rows
total_rows = 0
for f in parquet_files:
    table = pq.read_table(f, filesystem=gcs, columns=[])
    total_rows += table.num_rows

print("Total rows across all files:", total_rows)
