# Homework Solution - Module 2
## Overview
This document contains solutions for the homework exercises in Module 2 of the Data Engineering Zoomcamp course.
Many methods are being used here for the similar case.

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


## Number 3: Use scheduled backfill to check the CSV rows in GCS - Method: Query
1. Copy and run existing flow: 09_gcp_taxi_scheduled.yaml
2. Use BigQuery Query editor:
```sql
SELECT SUM(cnt) AS total_trips FROM (
  SELECT COUNT(*) AS cnt FROM `speedy-method-485303-p9.zoomcamp_kestra_dian_dataset.yellow_tripdata_2020_01_ext`
  UNION ALL
  SELECT COUNT(*) FROM `speedy-method-485303-p9.zoomcamp_kestra_dian_dataset.yellow_tripdata_2020_02_ext`
  UNION ALL
  SELECT COUNT(*) FROM `speedy-method-485303-p9.zoomcamp_kestra_dian_dataset.yellow_tripdata_2020_03_ext`
  UNION ALL
  SELECT COUNT(*) FROM `speedy-method-485303-p9.zoomcamp_kestra_dian_dataset.yellow_tripdata_2020_04_ext`
  UNION ALL
  SELECT COUNT(*) FROM `speedy-method-485303-p9.zoomcamp_kestra_dian_dataset.yellow_tripdata_2020_05_ext`
  UNION ALL
  SELECT COUNT(*) FROM `speedy-method-485303-p9.zoomcamp_kestra_dian_dataset.yellow_tripdata_2020_06_ext`
  UNION ALL
  SELECT COUNT(*) FROM `speedy-method-485303-p9.zoomcamp_kestra_dian_dataset.yellow_tripdata_2020_07_ext`
  UNION ALL
  SELECT COUNT(*) FROM `speedy-method-485303-p9.zoomcamp_kestra_dian_dataset.yellow_tripdata_2020_08_ext`
  UNION ALL
  SELECT COUNT(*) FROM `speedy-method-485303-p9.zoomcamp_kestra_dian_dataset.yellow_tripdata_2020_09_ext`
  UNION ALL
  SELECT COUNT(*) FROM `speedy-method-485303-p9.zoomcamp_kestra_dian_dataset.yellow_tripdata_2020_10_ext`
  UNION ALL
  SELECT COUNT(*) FROM `speedy-method-485303-p9.zoomcamp_kestra_dian_dataset.yellow_tripdata_2020_11_ext`
  UNION ALL
  SELECT COUNT(*) FROM `speedy-method-485303-p9.zoomcamp_kestra_dian_dataset.yellow_tripdata_2020_12_ext`
)
```
Answer: 24,648,499

## Number 4: Use scheduled backfill to check the CSV rows in GCS - Method: gsutil
Similar to point 3, after the bucket and dataset are filled with 1 year of data (12 files), check the total number of rows in GCS using GC console and run:

gsutil cat gs://zoomcamp-kestra-dian-bucket/green_tripdata_2020-*.csv | wc -l
Found 1,734,063 records, however, only by excluding 12 headers from 12 files we get the exact records.
Answer: 1,734,051

## Number 5: Similar step with Number 3 - Method: gsutil
gsutil cat gs://zoomcamp-kestra-dian-bucket/yellow_tripdata_2021-*.csv | wc -l
Found 1,925,153 records minus 1 header.
Answer: 1,925,152

## Number 6:
From kestra documentation:
https://kestra.io/docs/workflow-components/triggers/schedule-trigger

triggers:
  - id: daily
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "@daily"
    timezone: America/New_York

Note: attempt to run the flow with the foreach loop but failed upon reading the extracted file inside the loop.