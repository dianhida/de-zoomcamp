from __future__ import annotations

import argparse
import json
from pathlib import Path

from taxi_spark.yellow_metrics import (
    average_output_parquet_file_size_mb,
    average_partition_size_rows,
    count_records_on_date,
    get_spark,
    longest_trip_hours,
    read_yellow_parquet,
    repartition_and_write_parquet,
)


def main() -> int:
    here = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(description="Module 6 PySpark metrics runner")
    parser.add_argument(
        "--input",
        default=str(here / "yellow_tripdata_2025-11.parquet"),
        help="Input parquet path (default: module_6/yellow_tripdata_2025-11.parquet)",
    )
    parser.add_argument(
        "--output",
        default=str(here / "output" / "yellow_2025_11_p4"),
        help="Output directory for repartitioned parquet",
    )
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument(
        "--pickup-date",
        default="2025-11-15",
        help="Pickup date to count records on (YYYY-MM-DD, default: 2025-11-15)",
    )
    args = parser.parse_args()

    spark = get_spark("module_6_run_metrics")
    try:
        df = read_yellow_parquet(spark, args.input)
        total_rows = int(df.count())

        df_rep = repartition_and_write_parquet(
            df,
            output_dir=args.output,
            partitions=args.partitions,
            mode="overwrite",
        )

        results = {
            "input": str(args.input),
            "output": str(args.output),
            "partitions_requested": int(args.partitions),
            "total_rows": total_rows,
            "avg_partition_rows": average_partition_size_rows(df_rep),
            "avg_output_parquet_file_mb": average_output_parquet_file_size_mb(args.output),
            "records_on_pickup_date": count_records_on_date(df, pickup_date=args.pickup_date),
            "max_trip_hours": longest_trip_hours(df),
        }
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

