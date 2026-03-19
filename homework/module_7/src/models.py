import json
from dataclasses import dataclass

import pandas as pd


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tip_amount: float
    passenger_count: float
    lpep_pickup_datetime: str  # epoch milliseconds
    lpep_dropoff_datetime: str  # epoch milliseconds


def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        tip_amount=float(row['tip_amount']),
        passenger_count=(
            float(row['passenger_count'])
            if pd.notnull(row['passenger_count'])
            else None
        ),
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
    )


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)