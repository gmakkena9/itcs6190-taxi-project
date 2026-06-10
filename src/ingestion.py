"""Stage 1 - Ingestion (Spark Structured APIs).

Reads NYC Yellow Taxi trip data, profiles its quality, and persists it as
columnar Parquet for the downstream stages. Two sources are supported through a
single code path:

* the bundled **synthetic CSV sample** (default, for offline one-command runs);
* a **real TLC monthly Parquet file** (set ``TAXI_SOURCE=/path/to/file.parquet``),
  used for the validation run described in ``docs/real_data_validation.md``.

For CSV we apply an explicit schema (never inferred at scale). For real Parquet
files — which are self-describing but whose schema drifts across years (e.g.
``passenger_count`` as DOUBLE, and extra columns such as ``RatecodeID``,
``congestion_surcharge``, ``Airport_fee``, ``cbd_congestion_fee``) — we read the
native schema and **normalize** it down to the 13 canonical columns the rest of
the pipeline expects. Either way, writing Parquet once here means
transformations, SQL, and MLlib all read a compact, typed format.
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
    TRIPS_PARQUET,
    banner,
    ensure_dirs,
    get_spark,
    resolve_trip_source,
    resolve_zone_source,
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


# The 13 canonical columns every downstream stage relies on. Real TLC files
# carry ~6 more (RatecodeID, store_and_fwd_flag, extra, mta_tax,
# congestion_surcharge, Airport_fee, cbd_congestion_fee for 2025+); we keep only
# these so the sample and the real data flow through identical code.
CANONICAL_COLS = [f.name for f in TRIP_SCHEMA.fields]


def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read the synthetic CSV sample with an explicit, typed schema."""
    return (
        spark.read.option("header", True)
        .schema(TRIP_SCHEMA)
        .csv(path)
    )


def normalize_trips(df: DataFrame) -> DataFrame:
    """Project any TLC trip DataFrame onto the 13 canonical, typed columns.

    Real monthly Parquet files vary year-to-year: ``passenger_count`` ships as
    DOUBLE, ``improvement_surcharge`` may be absent in older years, and several
    extra fee columns come along. We select the canonical columns (filling a
    null for any genuinely missing one) and cast each to the sample's types so
    the downstream stages are schema-agnostic to the source.
    """
    present = set(df.columns)
    type_map = {f.name: f.dataType for f in TRIP_SCHEMA.fields}
    cols = []
    for name in CANONICAL_COLS:
        if name in present:
            cols.append(F.col(name).cast(type_map[name]).alias(name))
        else:
            cols.append(F.lit(None).cast(type_map[name]).alias(name))
    return df.select(*cols)


def read_raw_trips(spark: SparkSession, path: str | None = None) -> DataFrame:
    """Read trips from CSV or Parquet (auto-detected) as canonical columns.

    ``path`` defaults to whatever :func:`utils.resolve_trip_source` returns (the
    bundled sample, or a ``TAXI_SOURCE`` override). Parquet is read with its
    native self-describing schema and then normalized; CSV uses the explicit
    sample schema.
    """
    path = path or resolve_trip_source()
    if path.endswith(".parquet") or path.endswith(".pq"):
        return normalize_trips(spark.read.parquet(path))
    return read_raw_csv(spark, path)


def read_zones(spark: SparkSession, path: str | None = None) -> DataFrame:
    """Read the taxi-zone lookup (LocationID -> Borough/Zone/service_zone)."""
    path = path or resolve_zone_source()
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

    source = resolve_trip_source()
    fmt = "Parquet (real TLC)" if source.endswith((".parquet", ".pq")) else "CSV (synthetic sample)"
    print(f"Source: {source}\nFormat: {fmt}")

    trips = read_raw_trips(spark, source)
    print("\nTrip schema (normalized to canonical columns):")
    trips.printSchema()
    profile(trips)

    zones = read_zones(spark)
    print(f"\nZone lookup rows: {zones.count()}")

    trips.write.mode("overwrite").parquet(TRIPS_PARQUET)
    print(f"\nWrote curated trips -> {TRIPS_PARQUET}")

    spark.stop()


if __name__ == "__main__":
    run()
