# Homework Solution - Module 4

# IMPORTANT NOTE!
I will have different calculation when it came for the records count due to the different source usage.
It seems the tutorial is using the dataTalk official data while I used the kestra one, the parquet source:
https://d37ci6vzurychx.cloudfront.net/trip-data


## Overview
This document contains solutions for the homework exercises in Module 3 of the Data Engineering Zoomcamp course.

## Preparation
1. Load 2019-2020 yellowtrip & greentrip data to bigQuery. I'm using the ingest.py with Parquet type. [Schema: zoomcamp_4_dataset]
2. Build the DBT using dbt core. [Schema: dbt_dev & dbt_prod]

## Number 1: DBT Run Select
dbt run --select int_trips_unioned
logs:
06:17:52  1 of 1 START sql table model dbt_dev.int_trips_unioned ......................... [RUN]
06:17:57  1 of 1 OK created sql table model dbt_dev.int_trips_unioned .................... [CREATE TABLE (8.4m rows, 14.3 GiB processed) in 5.36s] 
Answer:
int_trips_unioned only

## Number 2: DBT fct_trips
Because there is no value of 6 in the source tables, we try to modify the accepted values to [1,2,3,4], therefore when we run:
dbt test --select fct_trips
logs:
06:33:32  Completed with 1 error, 0 partial successes, and 0 warnings:
06:33:32  Failure in test accepted_values_fct_trips_payment_type__1__2__3__4 (models/marts/schema.yml)

Answer: dbt will fail the test, returning a non-zero exit code

## Number 3: Counting revenue
```sql
SELECT count(*) 
FROM `zoomcamp_4_dataset.fct_monthly_zone_revenue`;
```
The actual result is 12,514 records. There is no this option for the answers. Possibly because the source issue. My source is:
https://d37ci6vzurychx.cloudfront.net/trip-data

Answer:
Nearest: 12,988

## Number 4: Highest monthly revenue
```sql

  SELECT
      pickup_zone,
      SUM(revenue_monthly_total_amount) AS total_revenue
  FROM
      `speedy-method-485303-p9.dbt_prod.fct_monthly_zone_revenue`
  WHERE
      service_type = 'Green'
      AND EXTRACT(YEAR FROM revenue_month) = 2020
  GROUP BY
      pickup_zone
  ORDER BY
      total_revenue DESC
  LIMIT 1;
;
```
Answer: East Harlem North |	2,037,905.57	

## Number 5: Counting
```sql
SELECT
    SUM(total_monthly_trips) AS total_trips
FROM
    `dbt_prod.fct_monthly_zone_revenue`
WHERE
    service_type = 'Green'
    AND EXTRACT(YEAR FROM revenue_month) = 2019
    AND EXTRACT(MONTH FROM revenue_month) = 10
;
```
Actual result: 472,427. Again, it seems I'm using a different source (refers to question 3)
Answer: nearest | 500,234


## Number 6: FHV Data
```sql
with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        dispatching_base_num,
        pickup_datetime,
        dropOff_datetime as dropoff_datetime,
        PUlocationID as pulocationid,
        DOlocationID as dolocationid
    from source
    -- Filter out records with not null dispatch base num (data quality requirement)
    where dispatching_base_num is not null
)

select *
from renamed
```
Then count in bigQuery
```sql
select count(*) from `dbt_prod.stg_fhv_tripdata`;
```
Answer: 43,244,693
