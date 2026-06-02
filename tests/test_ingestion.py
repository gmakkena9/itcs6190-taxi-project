"""Ingestion: schema is explicit and parses raw values into the right types."""
from ingestion import TRIP_SCHEMA, read_raw_trips


def test_schema_fields():
    names = [f.name for f in TRIP_SCHEMA.fields]
    assert "tpep_pickup_datetime" in names
    assert "trip_distance" in names
    assert len(names) == 13


def test_read_raw_trips_types(spark, tmp_path):
    csv = tmp_path / "t.csv"
    csv.write_text(
        "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,"
        "trip_distance,PULocationID,DOLocationID,payment_type,fare_amount,"
        "tip_amount,tolls_amount,improvement_surcharge,total_amount\n"
        "1,2024-01-01 08:00:00,2024-01-01 08:15:00,1,2.5,161,138,1,12.0,2.0,0.0,1.0,15.0\n"
    )
    df = read_raw_trips(spark, str(csv))
    assert df.count() == 1
    types = dict(df.dtypes)
    assert types["trip_distance"] == "double"
    assert types["tpep_pickup_datetime"] == "timestamp"
