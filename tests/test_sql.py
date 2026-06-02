"""Transformations + Spark SQL: cleaning rules and hourly demand aggregation."""
from datetime import datetime, timezone

from transformations import add_features, clean_trips, demand_by_hour


def _trips(spark):
    rows = [
        # good 15-min trip at 08:00
        (datetime(2024, 1, 1, 8, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 8, 15, tzinfo=timezone.utc), 2.5, 12.0, 1),
        # good 10-min trip at 08:00
        (datetime(2024, 1, 1, 8, 30, tzinfo=timezone.utc), datetime(2024, 1, 1, 8, 40, tzinfo=timezone.utc), 1.5, 9.0, 1),
        # bad: zero distance -> dropped
        (datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 9, 10, tzinfo=timezone.utc), 0.0, 8.0, 1),
        # bad: negative fare -> dropped
        (datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 9, 10, tzinfo=timezone.utc), 2.0, -3.0, 1),
    ]
    return spark.createDataFrame(
        rows,
        ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
         "fare_amount", "passenger_count"],
    )


def test_clean_drops_bad_rows(spark):
    cleaned = clean_trips(_trips(spark))
    assert cleaned.count() == 2
    assert "trip_duration_min" in cleaned.columns


def test_demand_by_hour(spark):
    featured = add_features(clean_trips(_trips(spark)))
    out = {r["pickup_hour"]: r for r in demand_by_hour(spark, featured).collect()}
    assert out[8]["trips"] == 2
    assert abs(out[8]["avg_duration_min"] - 12.5) < 0.01
