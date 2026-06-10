"""Real-data ingestion: a real-TLC-shaped Parquet normalizes to the canonical
13 columns with the right types (passenger_count double -> int, extra fee
columns dropped, missing columns tolerated)."""
from datetime import datetime

from ingestion import CANONICAL_COLS, normalize_trips


def test_normalize_real_schema(spark):
    # A real 2024 yellow row carries extra columns and double passenger_count.
    rows = [
        (2, datetime(2024, 1, 1, 8, 0), datetime(2024, 1, 1, 8, 15), 1.0, 2.5,
         1.0, "N", 161, 138, 1, 12.0, 1.0, 0.5, 2.0, 0.0, 1.0, 19.0, 2.5, 0.0),
    ]
    cols = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
        "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",
        "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge", "Airport_fee",
    ]
    df = spark.createDataFrame(rows, cols)
    out = normalize_trips(df)

    # Only the canonical columns survive, in order.
    assert out.columns == CANONICAL_COLS
    types = dict(out.dtypes)
    assert types["passenger_count"] == "int"   # cast down from double
    assert types["trip_distance"] == "double"
    assert types["tpep_pickup_datetime"] == "timestamp"
    assert out.count() == 1


def test_normalize_tolerates_missing_column(spark):
    # Older files may lack improvement_surcharge; it should appear as null.
    rows = [(2, 2.5, 161, 12.0)]
    df = spark.createDataFrame(
        rows, ["VendorID", "trip_distance", "PULocationID", "fare_amount"]
    )
    out = normalize_trips(df)
    assert out.columns == CANONICAL_COLS
    assert out.filter(out.improvement_surcharge.isNotNull()).count() == 0
