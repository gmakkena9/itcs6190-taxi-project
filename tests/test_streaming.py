"""Streaming: the per-batch aggregation works the same on static data."""
from datetime import datetime, timezone

from streaming import aggregate_zone_stream


def test_aggregate_zone_stream(spark):
    rows = [
        (datetime(2024, 1, 1, 8, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 8, 20, tzinfo=timezone.utc), 3.0, 14.0),
        (datetime(2024, 1, 1, 8, 5, tzinfo=timezone.utc), datetime(2024, 1, 1, 8, 25, tzinfo=timezone.utc), 3.0, 16.0),
        (datetime(2024, 1, 1, 17, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 17, 30, tzinfo=timezone.utc), 4.0, 20.0),
    ]
    df = spark.createDataFrame(
        rows,
        ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount"],
    )
    out = {r["pickup_hour"]: r for r in aggregate_zone_stream(df).collect()}
    assert out[8]["trips"] == 2
    assert out[17]["trips"] == 1
    assert abs(out[8]["avg_fare"] - 15.0) < 0.01
