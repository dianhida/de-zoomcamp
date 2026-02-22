/* @bruin

# Reports layer: aggregate staging trips for dashboards and analytics.
# - Aggregates by date, taxi_type, payment_type_name
# - Uses time_interval with trip_date for incremental consistency with staging
# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: trip_date
    type: date
    description: "Date of trip (from pickup_datetime)"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment type label"
    primary_key: true
    checks:
      - name: not_null
  - name: trip_count
    type: bigint
    description: "Number of trips"
    checks:
      - name: not_null
      - name: non_negative
  - name: total_fare_amount
    type: float
    description: "Sum of fare amounts in USD"
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: "Sum of total amounts in USD"
    checks:
      - name: non_negative
  - name: total_passengers
    type: float
    description: "Sum of passenger counts"
    checks:
      - name: non_negative
  - name: total_distance
    type: float
    description: "Sum of trip distances in miles"
    checks:
      - name: non_negative

@bruin */

SELECT
  DATE(pickup_datetime) AS trip_date,
  taxi_type,
  COALESCE(payment_type_name, 'unknown') AS payment_type_name,
  COUNT(*) AS trip_count,
  SUM(fare_amount) AS total_fare_amount,
  SUM(total_amount) AS total_amount,
  SUM(passenger_count) AS total_passengers,
  SUM(trip_distance) AS total_distance
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  DATE(pickup_datetime),
  taxi_type,
  COALESCE(payment_type_name, 'unknown')
