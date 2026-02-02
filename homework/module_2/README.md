# Homework Solution

## Number 1: Tracking the file
1. Re-use the 08_gcp_taxi flow and disable the purge_files task.
2. Check docker volume storage: - kestra_data:/app/storage
3. Track any .csv (2020-12...) files using execution id from Kestra UI.
4. Found the file size is 129M for compressed file: ls -lh \
/app/storage/main/zoomcamp/08-gcp-taxi/executions/51NIQcNLTDwU4Sn9UFm1aU/tasks/extract/yellow_tripdata_2020-12.csv
5. However, using stat command, the actual file size is 134481400 bytes.
Answer:
134481400 or 134.5 MB


## Number 2: Rendered filename
Answer: 
green_tripdata_2020-04.csv


## Number 3: Use scheduled backfill to check the CSV rows in GCS :
Copy and run existing flow: 09_gcp_taxi_scheduled.yaml
After the bucket is filled with 1 year of data (12 files), check the total number of rows in GCS using GC console and run:
gsutil cat gs://zoomcamp-kestra-dian-bucket/green_tripdata_2020-*.csv | wc -l
Found 1,734,063 records, however, only by excluding 12 headers from 12 files we get the exact records.
Answer: 1,734,051

## Number 4: Similar step with Number 3
gsutil cat gs://zoomcamp-kestra-dian-bucket/yellow_tripdata_2021-*.csv | wc -l
Found 1,925,153 records minus 1 header.
Answer: 1,925,152

## Number 5:
From kestra documentation:
https://kestra.io/docs/workflow-components/triggers/schedule-trigger

triggers:
  - id: daily
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "@daily"
    timezone: America/New_York

Note: attempt to run the flow with the foreach loop but failed upon reading the extracted file inside the loop.