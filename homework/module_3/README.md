# Homework Solution - Module 3
## Overview
This document contains solutions for the homework exercises in Module 3 of the Data Engineering Zoomcamp course.

## Preparation
Refers to file bigquery_hw.sql

## Number 1: Row Count
After the Regular table has been created, count the table.
```sql
select count(*) from `zoomcamp_3_dataset.non_partition_yellow`;
```
Answer:
20,332,093

## Number 2: Query resource
Step: Highlight each query and check for bigquery processing resource information.
1. Use this query for external table:
```sql
select distinct(PULocationID) 
from `zoomcamp_3_dataset.external_yellow`
```
Result: 0 MB. It's because bigquery can not attempt to predict the query process for external table (explained in the video).

2. Use this query for regular table:
```sql
  select distinct(PULocationID) 
  from `zoomcamp_3_dataset.non_partition_yellow`
```
Result:
155.21 MB

Answer: 
0 MB and 155.21 MB

## Number 3: Understanding BigQuery
```sql
SELECT PULocationID 
FROM `zoomcamp_3_dataset.non_partition_yellow`;

SELECT PULocationID, DOLocationID 
FROM `zoomcamp_3_dataset.non_partition_yellow`;
```
Use above query to check the highlighted resource process.
The first query: 155.12 MB
The second query: 310.24 MB

Answer:
Bigquery is a columnar database. That's why the the query process multiply when more than 1 field are selected. Also another reason to not use * query to avoid huge resource for querying the data.

## Number 4: Count for Record with 0 fare_amlunt
```sql
SELECT count(*)
FROM `zoomcamp_3_dataset.non_partition_yellow`
WHERE fare_amount = 0
;
```
Answer: 8,333

## Number 5: Partitioning and Clustering
```sql
-- Partition Only Table
CREATE OR REPLACE TABLE zoomcamp_3_dataset.partition_yellow
PARTITION BY DATE(tpep_pickup_datetime) AS
SELECT * FROM zoomcamp_3_dataset.external_yellow;
```
```sql
-- Partition and Cluster
CREATE OR REPLACE TABLE zoomcamp_3_dataset.partition_cluster_yellow
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM zoomcamp_3_dataset.external_yellow;
```
Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID
Reason: at most cases, always partition first for the most used field for searching the data, in this case the date. Additionally, use the cluster if another field is frequently  used for query.

## Number 6: Comparing Partition-Cluster and Regular
```sql
SELECT DISTINCT VendorID
FROM `zoomcamp_3_dataset.partition_cluster_yellow`
WHERE tpep_dropoff_datetime >= TIMESTAMP('2024-03-01')
  AND tpep_dropoff_datetime < TIMESTAMP('2024-03-16');
```
Estimation:
Answer:
Using non-partition: 310.24 MB
Using partition: to be exact, it's 28.83 MB, but the answer only provide 26.84 MB

## Number 7: External Table
Answer: GCP
External table neither created nor stored at BigQuery, but BigQuery only read the csv or parquet in the GCP site.

## Number 8: Is Clustering always Needed?
Answer: No.
If the frequently used query is solely on the first field that has been partitioned (e.g., date) field, no need to create any clustering for other field(s) for no reason. 

## Number 9: Count(*)
Answer: 0 MB.
Analysis: Probably because BQ already has storage info which covers the total rows of a table. Therefore, this query will always fetch that metadata instead of scanning all over the table.