/* @bruin

# Staging layer: clean, deduplicate, and enrich raw trip data.
# - Joins with payment lookup for payment_type_name
# - Deduplicates using composite key (ROW_NUMBER)
# - Filters invalid rows (null PKs, negative amounts)
# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table

columns:
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)"
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: "Trip start time"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Trip end time"
    primary_key: true
    checks:
      - name: not_null
  - name: PULocationID
    type: integer
    description: "Pickup taxi zone ID"
    primary_key: true
    checks:
      - name: not_null
  - name: DOLocationID
    type: integer
    description: "Dropoff taxi zone ID"
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Fare amount in USD"
    primary_key: true
    checks:
      - name: not_null
      - name: non_negative
  - name: passenger_count
    type: float
    description: "Number of passengers"
    checks:
      - name: non_negative
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
    checks:
      - name: non_negative
  - name: payment_type
    type: integer
    description: "Payment type ID"
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment type label from lookup"
  - name: total_amount
    type: float
    description: "Total amount in USD"
    checks:
      - name: non_negative

custom_checks:
  - name: no_duplicate_trips
    description: "Ensures no duplicate rows by composite key (pickup, dropoff, locations, fare)"
    query: |
      SELECT
        (SELECT COUNT(*) FROM staging.trips) - (
          SELECT COUNT(*)
          FROM (
            SELECT DISTINCT pickup_datetime, dropoff_datetime, "PULocationID", "DOLocationID", fare_amount
            FROM staging.trips
          )
        )
    value: 0

@bruin */

WITH windowed AS (
  SELECT *
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
),
enriched AS (
  SELECT
    t.taxi_type,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.pu_location_id AS "PULocationID",
    t.do_location_id AS "DOLocationID",
    t.payment_type,
    p.payment_type_name,
    t.fare_amount,
    t.total_amount
  FROM windowed t
  LEFT JOIN ingestion.payment_lookup p ON t.payment_type = p.payment_type_id
),
filtered AS (
  SELECT *
  FROM enriched
  WHERE pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND "PULocationID" IS NOT NULL
    AND "DOLocationID" IS NOT NULL
    AND fare_amount IS NOT NULL
    AND fare_amount >= 0
),
deduped AS (
  SELECT *
  FROM filtered
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pickup_datetime, dropoff_datetime, "PULocationID", "DOLocationID", fare_amount
    ORDER BY taxi_type
  ) = 1
)
SELECT *
FROM deduped
