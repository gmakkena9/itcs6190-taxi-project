"""EDA: derived columns (duration, hour, borough) compute correctly."""
from datetime import datetime

from eda import add_eda_columns


def test_add_eda_columns(spark):
    trips = spark.createDataFrame(
        [(datetime(2024, 1, 1, 8, 0), datetime(2024, 1, 1, 8, 15), 2.5, 161, 12.0),
         (datetime(2024, 1, 1, 17, 0), datetime(2024, 1, 1, 17, 30), 4.0, 132, 18.0)],
        ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
         "PULocationID", "fare_amount"],
    )
    zones = spark.createDataFrame(
        [(161, "Manhattan", "Midtown Center", "Yellow Zone"),
         (132, "Queens", "JFK Airport", "Airports")],
        ["LocationID", "Borough", "Zone", "service_zone"],
    )
    out = {r["PULocationID"]: r for r in add_eda_columns(trips, zones).collect()}
    assert abs(out[161]["trip_duration_min"] - 15.0) < 0.01
    assert out[161]["pickup_hour"] == 8
    assert out[132]["pu_borough"] == "Queens"
