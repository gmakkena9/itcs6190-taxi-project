"""Stage 1 - Ingestion (Spark Structured APIs).

Reads the raw NYC Yellow Taxi CSV sample with an explicit schema (never inferred
at scale), reads the taxi-zone lookup, performs a quick data-quality profile, and
persists both as columnar Parquet for the downstream stages. Writing Parquet once
here means transformations, SQL, and MLlib all read a compact, typed format
instead of re-parsing CSV.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from utils import (
    RAW_TRIPS_CSV,
    TRIPS_PARQUET,
    ZONE_LOOKUP_CSV,
    banner,
    ensure_dirs,
    get_spark,
)

TRIP_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
    ]
)


def read_raw_trips(spark: SparkSession, path: str = RAW_TRIPS_CSV) -> DataFrame:
    """Read raw trip CSV with an explicit, typed schema."""
    return (
        spark.read.option("header", True)
        .schema(TRIP_SCHEMA)
        .csv(path)
    )


def read_zones(spark: SparkSession, path: str = ZONE_LOOKUP_CSV) -> DataFrame:
    """Read the taxi-zone lookup (LocationID -> Borough/Zone/service_zone)."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def profile(df: DataFrame) -> None:
    """Print a compact data-quality profile used in the EDA write-up."""
    total = df.count()
    nulls = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    ).collect()[0].asDict()
    bad_distance = df.filter(F.col("trip_distance") <= 0).count()
    bad_fare = df.filter(F.col("fare_amount") < 0).count()
    print(f"Total rows: {total:,}")
    print(f"Zero/negative distance rows: {bad_distance:,}")
    print(f"Negative fare rows: {bad_fare:,}")
    nonzero_nulls = {k: v for k, v in nulls.items() if v}
    print(f"Columns with nulls: {nonzero_nulls or 'none'}")


def run() -> None:
    ensure_dirs()
    spark = get_spark("ingestion")
    banner("STAGE 1 | INGESTION")

    trips = read_raw_trips(spark)
    print("Trip schema:")
    trips.printSchema()
    profile(trips)

    zones = read_zones(spark)
    print(f"\nZone lookup rows: {zones.count()}")

    trips.write.mode("overwrite").parquet(TRIPS_PARQUET)
    print(f"\nWrote curated trips -> {TRIPS_PARQUET}")

    spark.stop()


if __name__ == "__main__":
    run()
