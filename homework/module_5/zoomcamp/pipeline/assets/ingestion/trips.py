"""@bruin

# Python ingestion asset: fetch NYC TLC taxi trip data from public parquet endpoint.
# - Uses BRUIN_START_DATE / BRUIN_END_DATE and pipeline variable taxi_types.
# - Keeps data in raw form; staging layer cleans and deduplicates.
name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

# Output columns for metadata, lineage, and quality. Raw parquet may include
# additional columns (e.g. VendorID, airport_fee); we define the key ones here.
columns:
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)"
  - name: extracted_at
    type: timestamp
    description: "When this batch was extracted (lineage/debugging)"
  - name: pickup_datetime
    type: timestamp
    description: "Trip start time (unified from tpep_ or lpep_ column)"
  - name: dropoff_datetime
    type: timestamp
    description: "Trip end time (unified from tpep_ or lpep_ column)"
  - name: passenger_count
    type: float
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: PULocationID
    type: integer
    description: "Pickup taxi zone ID"
  - name: DOLocationID
    type: integer
    description: "Dropoff taxi zone ID"
  - name: payment_type
    type: integer
    description: "Payment type (1=credit, 2=cash, etc.)"
  - name: fare_amount
    type: float
    description: "Fare amount in USD"
  - name: total_amount
    type: float
    description: "Total amount in USD"

@bruin"""

import json
import os
from io import BytesIO
from datetime import datetime

import pandas as pd
import requests


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def _parse_date(s: str) -> datetime:
    """Parse YYYY-MM-DD to datetime for comparison."""
    return datetime.strptime(s.strip()[:10], "%Y-%m-%d")


def _month_range(start_date: str, end_date: str):
    """Yield (year, month) for each month in [start_date, end_date] (inclusive)."""
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    y, m = start.year, start.month
    end_y, end_m = end.year, end.month
    while (y, m) <= (end_y, end_m):
        yield (y, m)
        m += 1
        if m > 12:
            m = 1
            y += 1


def _unify_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add unified pickup_datetime and dropoff_datetime from tpep_ or lpep_ columns."""
    if "tpep_pickup_datetime" in df.columns:
        df = df.copy()
        df["pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    elif "lpep_pickup_datetime" in df.columns:
        df = df.copy()
        df["pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    return df


def materialize():
    """
    Ingest NYC TLC trip parquet files for the run's date range and taxi types.
    Uses BRUIN_START_DATE, BRUIN_END_DATE and BRUIN_VARS (taxi_types).
    """
    start_date = os.environ.get("BRUIN_START_DATE", "").strip()[:10]
    end_date = os.environ.get("BRUIN_END_DATE", "").strip()[:10]
    if not start_date or not end_date:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set (YYYY-MM-DD)")

    vars_json = os.environ.get("BRUIN_VARS", "{}")
    try:
        vars_data = json.loads(vars_json)
    except json.JSONDecodeError:
        vars_data = {}
    taxi_types = vars_data.get("taxi_types", ["yellow"])
    if not isinstance(taxi_types, list):
        taxi_types = [taxi_types]

    extracted_at = datetime.utcnow()
    frames = []

    for taxi_type in taxi_types:
        taxi_type = str(taxi_type).strip().lower()
        for year, month in _month_range(start_date, end_date):
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = BASE_URL + filename
            resp = requests.get(url, timeout=120)
            # Skip 404 (not found) and 403 (forbidden – e.g. future or post–Nov 2025 data)
            if resp.status_code in (403, 404):
                continue
            resp.raise_for_status()

            df = pd.read_parquet(BytesIO(resp.content))
            df = _unify_datetime_columns(df)
            df["taxi_type"] = taxi_type
            df["extracted_at"] = extracted_at
            frames.append(df)

    if not frames:
        # Return empty DataFrame with expected columns so table schema exists
        return pd.DataFrame(columns=[
            "taxi_type", "extracted_at", "pickup_datetime", "dropoff_datetime",
            "passenger_count", "trip_distance", "PULocationID", "DOLocationID",
            "payment_type", "fare_amount", "total_amount",
        ])

    return pd.concat(frames, ignore_index=True)
