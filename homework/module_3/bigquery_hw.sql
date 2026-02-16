SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


CREATE OR REPLACE EXTERNAL TABLE
`speedy-method-485303-p9.zoomcamp_3_dataset.external_yellow`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp-kestra-dian-bucket/yellow_tripdata_2024-*.parquet']
);


CREATE OR REPLACE TABLE zoomcamp_3_dataset.non_partition_yellow AS
SELECT * FROM zoomcamp_3_dataset.external_yellow;


select count(PULocationID) from `zoomcamp_3_dataset.non_partition_yellow`;

select distinct(PULocationID) 
from `zoomcamp_3_dataset.external_yellow`

select distinct(PULocationID) 
from `zoomcamp_3_dataset.non_partition_yellow`;



SELECT PULocationID 
FROM `zoomcamp_3_dataset.non_partition_yellow`;

SELECT PULocationID, DOLocationID 
FROM `zoomcamp_3_dataset.non_partition_yellow`;


SELECT count(*)
FROM `zoomcamp_3_dataset.non_partition_yellow`
WHERE fare_amount = 0
;

CREATE OR REPLACE TABLE zoomcamp_3_dataset.partition_cluster_yellow
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM zoomcamp_3_dataset.external_yellow;


SELECT DISTINCT VendorID
FROM `zoomcamp_3_dataset.partition_cluster_yellow`
WHERE tpep_dropoff_datetime >= TIMESTAMP('2024-03-01')
  AND tpep_dropoff_datetime < TIMESTAMP('2024-03-16');

SELECT DISTINCT VendorID
FROM `zoomcamp_3_dataset.non_partition_yellow`
WHERE tpep_dropoff_datetime >= TIMESTAMP('2024-03-01')
  AND tpep_dropoff_datetime < TIMESTAMP('2024-03-16');
