#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# =========================
# Schema for trips
# =========================
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]

# =========================
# Ingest parquet trips
# =========================
def ingest_parquet_trips(
    url: str,
    engine,
    target_table: str,
    chunksize: int
):
    print(f"Reading parquet from {url}")
    df = pd.read_parquet(url)

    # create table
    df.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )
    print(f"Table {target_table} created")

    # chunked insert
    for i in tqdm(range(0, len(df), chunksize)):
        df_chunk = df.iloc[i:i + chunksize]
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print(f"Inserted rows {i} - {i + len(df_chunk)}")

    print(f"Done ingesting {target_table}")

# =========================
# Ingest taxi zones (CSV)
# =========================
def ingest_taxi_location(
    url: str,
    engine,
    target_table: str
):
    print(f"Reading taxi zone lookup from {url}")
    df = pd.read_csv(url)

    df.to_sql(
        name=target_table,
        con=engine,
        if_exists="replace",
        index=False
    )

    print(f"Table {target_table} created with {len(df)} rows")

# =========================
# CLI
# =========================
@click.command()
@click.option('--pg-user', default='root')
@click.option('--pg-pass', default='root')
@click.option('--pg-host', default='localhost')
@click.option('--pg-port', default='5432')
@click.option('--pg-db', default='ny_taxi')
@click.option('--year', type=int, required=True)
@click.option('--month', type=int, required=True)
@click.option('--chunksize', default=100000)
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize):

    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    trips_url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"green_tripdata_{year:04d}-{month:02d}.parquet"
    )

    location_url = (
        "https://github.com/DataTalksClub/nyc-tlc-data/releases/"
        "download/misc/taxi_zone_lookup.csv"
    )

    ingest_parquet_trips(
        url=trips_url,
        engine=engine,
        target_table="green_taxi_trips",
        chunksize=chunksize
    )

    ingest_taxi_location(
        url=location_url,
        engine=engine,
        target_table="taxi_location"
    )

if __name__ == '__main__':
    main()
