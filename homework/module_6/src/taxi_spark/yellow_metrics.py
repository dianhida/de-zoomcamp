from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F


def get_spark(app_name: str = "module_6_yellow_metrics") -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def print_spark_version(spark: SparkSession) -> str:
    version = spark.version
    print(f"Spark version: {version}")
    return version


def read_yellow_parquet(spark: SparkSession, parquet_path: str | Path) -> DataFrame:
    return spark.read.parquet(str(parquet_path))


def read_zone_lookup_csv(spark: SparkSession, csv_path: str | Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(csv_path))
    )


def repartition_and_write_parquet(
    df: DataFrame,
    output_dir: str | Path,
    partitions: int = 4,
    mode: str = "overwrite",
) -> DataFrame:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    df4 = df.repartition(int(partitions))
    df4.write.mode(mode).parquet(str(out))
    return df4


def _partition_row_counts(df: DataFrame) -> list[int]:
    def count_rows_in_partition(it: Iterable[object]) -> Iterable[int]:
        n = 0
        for _ in it:
            n += 1
        yield n

    return [int(x) for x in df.rdd.mapPartitions(count_rows_in_partition).collect()]


def average_partition_size_rows(df: DataFrame) -> float:
    counts = _partition_row_counts(df)
    if not counts:
        return 0.0
    return float(sum(counts) / len(counts))


def average_output_parquet_file_size_mb(output_dir: str | Path) -> float:
    p = Path(output_dir)
    files = [f for f in p.rglob("*.parquet") if f.is_file()]
    if not files:
        return 0.0
    sizes_mb = [f.stat().st_size / (1024 * 1024) for f in files]
    return float(sum(sizes_mb) / len(sizes_mb))


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value

    s = value.strip()
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        pass

    # Accept the homework-style example like "15 november" (defaults year=2025).
    try:
        d = datetime.strptime(s.lower(), "%d %B")
        return date(2025, d.month, d.day)
    except ValueError as e:
        raise ValueError(f"Unsupported date format: {value!r}. Use YYYY-MM-DD.") from e


def count_records_on_date(
    df: DataFrame,
    pickup_date: str | date = "2025-11-15",
    pickup_col: str = "tpep_pickup_datetime",
) -> int:
    d = _parse_date(pickup_date)
    filtered = df.where(F.to_date(F.col(pickup_col)) == F.lit(d.isoformat()))
    return int(filtered.count())


def longest_trip_hours(
    df: DataFrame,
    pickup_col: str = "tpep_pickup_datetime",
    dropoff_col: str = "tpep_dropoff_datetime",
) -> float:
    dur_seconds = (
        F.col(dropoff_col).cast("timestamp").cast("long")
        - F.col(pickup_col).cast("timestamp").cast("long")
    )
    m = (
        df.select((dur_seconds / F.lit(3600.0)).alias("trip_hours"))
        .where(F.col("trip_hours").isNotNull())
        .where(F.col("trip_hours") >= F.lit(0.0))
        .agg(F.max("trip_hours").alias("max_trip_hours"))
        .collect()[0]["max_trip_hours"]
    )
    return float(m) if m is not None else 0.0


@dataclass(frozen=True)
class Metrics:
    avg_partition_rows: float
    avg_output_file_mb: float
    records_on_date: int
    max_trip_hours: float


def compute_metrics(
    spark: SparkSession,
    input_parquet: str | Path,
    output_dir: str | Path,
    partitions: int = 4,
    pickup_date: str | date = "2025-11-15",
) -> Metrics:
    df = read_yellow_parquet(spark, input_parquet)
    df_rep = repartition_and_write_parquet(df, output_dir=output_dir, partitions=partitions)

    return Metrics(
        avg_partition_rows=average_partition_size_rows(df_rep),
        avg_output_file_mb=average_output_parquet_file_size_mb(output_dir),
        records_on_date=count_records_on_date(df, pickup_date=pickup_date),
        max_trip_hours=longest_trip_hours(df),
    )


def least_frequent_pickup_zone_name(
    yellow_df: DataFrame,
    zone_df: DataFrame,
    pulocation_col: str = "PULocationID",
) -> str:
    pickup_counts = (
        yellow_df.groupBy(F.col(pulocation_col).alias("LocationID"))
        .agg(F.count("*").alias("pickup_count"))
    )

    joined = (
        pickup_counts.join(zone_df, on="LocationID", how="left")
        .select("LocationID", "Zone", "pickup_count")
    )

    row = (
        joined.orderBy(F.col("pickup_count").asc(), F.col("Zone").asc())
        .limit(1)
        .collect()[0]
    )

    return row["Zone"]

