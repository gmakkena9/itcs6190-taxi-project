"""Shared helpers: Spark session construction and project path resolution.

Keeping these in one place means every stage of the pipeline (ingestion,
transformations, streaming, MLlib) opens an identically configured session and
reads/writes from the same well-known locations, regardless of the working
directory the script is launched from.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

# Project root is the parent of the directory containing this file (src/).
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SAMPLE_DIR = os.path.join(DATA_DIR, "sample")
REAL_DIR = os.path.join(DATA_DIR, "real")  # real TLC files (git-ignored)
CURATED_DIR = os.path.join(DATA_DIR, "curated")
OUTPUTS_DIR = os.path.join(DATA_DIR, "outputs")
STREAM_SOURCE_DIR = os.path.join(DATA_DIR, "stream_source")
CHECKPOINT_DIR = os.path.join(DATA_DIR, "_checkpoints")

# Curated datasets produced by the pipeline.
TRIPS_PARQUET = os.path.join(CURATED_DIR, "trips")
FEATURES_PARQUET = os.path.join(CURATED_DIR, "features")

RAW_TRIPS_CSV = os.path.join(SAMPLE_DIR, "yellow_tripdata_sample.csv")
ZONE_LOOKUP_CSV = os.path.join(SAMPLE_DIR, "taxi_zone_lookup.csv")


def resolve_trip_source() -> str:
    """Return the trip-data source the pipeline should ingest.

    Resolution order:
      1. ``TAXI_SOURCE`` environment variable, if set (a CSV or Parquet path);
      2. otherwise the bundled synthetic CSV sample.

    This is the single switch that lets the *same* pipeline run against either
    the offline sample or a real TLC monthly Parquet file (see
    ``docs/real_data_validation.md``).
    """
    return os.environ.get("TAXI_SOURCE", RAW_TRIPS_CSV)


def resolve_zone_source() -> str:
    """Return the zone-lookup source (env override or bundled sample)."""
    return os.environ.get("TAXI_ZONES", ZONE_LOOKUP_CSV)


def get_spark(app_name: str = "ITCS6190-Taxi") -> SparkSession:
    """Return a locally-configured SparkSession tuned for small-data runs.

    Shuffle partitions are kept low (the sample is tiny) and the Spark UI is
    disabled to keep local runs quiet and fast.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def banner(title: str) -> None:
    """Print a visually distinct section header to the console."""
    line = "=" * 70
    print(f"\n{line}\n  {title}\n{line}")


def ensure_dirs() -> None:
    for d in (CURATED_DIR, OUTPUTS_DIR, STREAM_SOURCE_DIR, CHECKPOINT_DIR):
        os.makedirs(d, exist_ok=True)
