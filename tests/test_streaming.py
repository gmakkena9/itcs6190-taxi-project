"""Streaming: the per-batch aggregation works the same on static data."""
from datetime import datetime

from streaming import aggregate_zone_stream, parse_cli_args


def test_aggregate_zone_stream(spark):
    rows = [
        (datetime(2024, 1, 1, 8, 0), datetime(2024, 1, 1, 8, 20), 3.0, 14.0),
        (datetime(2024, 1, 1, 8, 5), datetime(2024, 1, 1, 8, 25), 3.0, 16.0),
        (datetime(2024, 1, 1, 17, 0), datetime(2024, 1, 1, 17, 30), 4.0, 20.0),
    ]
    df = spark.createDataFrame(
        rows,
        ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount"],
    )
    out = {r["pickup_hour"]: r for r in aggregate_zone_stream(df).collect()}
    assert out[8]["trips"] == 2
    assert out[17]["trips"] == 1
    assert abs(out[8]["avg_fare"] - 15.0) < 0.01


def test_parse_cli_args_default_one_shot():
    """No flags -> one-shot pipeline mode (the run.sh / availableNow path)."""
    is_continuous, timeout = parse_cli_args([])
    assert is_continuous is False
    assert timeout is None


def test_parse_cli_args_continuous_default_timeout():
    """--continuous with no number -> continuous mode, 60s demo window."""
    is_continuous, timeout = parse_cli_args(["--continuous"])
    assert is_continuous is True
    assert timeout == 60


def test_parse_cli_args_continuous_custom_timeout():
    """--continuous 30 -> continuous mode, 30s demo window."""
    is_continuous, timeout = parse_cli_args(["--continuous", "30"])
    assert is_continuous is True
    assert timeout == 30


def test_parse_cli_args_continuous_run_forever():
    """--continuous 0 -> continuous mode, no timeout (run until Ctrl+C)."""
    is_continuous, timeout = parse_cli_args(["--continuous", "0"])
    assert is_continuous is True
    assert timeout is None
