# Homework Solution

## Number 1: Checked pip using `pip --version` at docker bash it
Result: 25.3

## Number 2: Refers to docker-compose.yaml file
Result: 
- db:5432 => service name
- postgres:5432 => container name

## Number 3: Run the following command in the terminal:
`uv run python ingest_data.py   --pg-user=postgres   --pg-pass=postgres   --pg-host=localhost   --pg-port=5433   --pg-db=ny_taxi   --year=2025   --month=11   --chunksize=100000`
After database is loaded:

```sql
SELECT COUNT(*) AS short_trips
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime <  '2025-12-01'
  AND trip_distance <= 1;
```

Result: 8007

## Number 4: 
```sql
SELECT lpep_pickup_datetime::DATE as pickup_date, trip_distance AS max_trip
FROM green_taxi_trips
WHERE trip_distance < 100
ORDER BY trip_distance DESC;
```
Result: 2025-11-14

## Number 5:
```sql
SELECT tl."Zone", count(*) AS trip_count FROM taxi_location tl
JOIN green_taxi_trips gtt ON gtt."PULocationID" = tl."LocationID"
WHERE gtt.lpep_pickup_datetime::DATE = '2025-11-18'
GROUP BY tl."Zone"
ORDER BY trip_count DESC;
```
Result: East Harlem North

*Note: if total_amount means exactly the field of total_amount, then the result would be different.
```sql
SELECT tl.*, gtt.total_amount FROM taxi_location tl
JOIN green_taxi_trips gtt ON gtt."PULocationID" = tl."LocationID"
WHERE gtt.lpep_pickup_datetime::DATE = '2025-11-18'
ORDER BY gtt.total_amount DESC	
```
Alt. result : Flushing, which is not available at the answer options.

## Number 6:
```sql
SELECT tl_dropoff."Zone" as dropoff_zone, gtt.tip_amount
FROM green_taxi_trips gtt
JOIN taxi_location tl_pickup ON gtt."PULocationID" = tl_pickup."LocationID"
JOIN taxi_location tl_dropoff ON gtt."DOLocationID" = tl_dropoff."LocationID"
WHERE tl_pickup."Zone" = 'East Harlem North'
ORDER BY tip_amount DESC;
```
Result: Yorkville West

## Number 7:
Refers to terraform files and after watching the video, the answer is :
terraform init
terraform apply -auto-approve
terraform destroy