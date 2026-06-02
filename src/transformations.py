"""Stage 2 - Transformations (Structured APIs + Spark SQL).

Cleans the curated trips, engineers the features the regression model will use,
joins each trip to its pickup-zone borough/zone name, and runs Spark SQL
aggregations to surface demand hotspots. The cleaned feature table is persisted
for the MLlib stage; the SQL aggregates are written to ``data/outputs`` for the
results write-up.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion import read_zones
from utils import (
    FEATURES_PARQUET,
    OUTPUTS_DIR,
    TRIPS_PARQUET,
    banner,
    ensure_dirs,
    get_spark,
)


def clean_trips(df: DataFrame) -> DataFrame:
    """Drop physically impossible / corrupt rows.

    Keeps trips with a positive distance, non-negative fare, a positive duration
    under 3 hours, and a sane average speed. The duration column (the regression
    target) is computed here in minutes.
    """
    dur = (
        F.col("tpep_dropoff_datetime").cast("long")
        - F.col("tpep_pickup_datetime").cast("long")
    ) / 60.0
    out = df.withColumn("trip_duration_min", dur)
    return out.filter(
        (F.col("trip_distance") > 0)
        & (F.col("fare_amount") >= 0)
        & (F.col("trip_duration_min") > 0)
        & (F.col("trip_duration_min") < 180)
        & (F.col("passenger_count") > 0)
    )


def add_features(df: DataFrame) -> DataFrame:
    """Derive temporal and trip-level features for modeling."""
    return (
        df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dow", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("is_weekend", (F.dayofweek("tpep_pickup_datetime").isin(1, 7)).cast("int"))
        .withColumn(
            "is_rush_hour",
            (
                F.col("pickup_hour").between(7, 10)
                | F.col("pickup_hour").between(16, 19)
            ).cast("int"),
        )
        .withColumn(
            "avg_speed_mph",
            F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60.0), 2),
        )
    )


def join_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Attach pickup-zone borough/name via a broadcast join on the small lookup."""
    pu = (
        zones.select(
            F.col("LocationID").alias("PULocationID"),
            F.col("Borough").alias("pu_borough"),
            F.col("Zone").alias("pu_zone"),
        )
    )
    return trips.join(F.broadcast(pu), on="PULocationID", how="left")


def demand_by_hour(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Spark SQL: average duration / fare and trip counts by pickup hour."""
    df.createOrReplaceTempView("trips")
    return spark.sql(
        """
        SELECT pickup_hour,
               COUNT(*)                         AS trips,
               ROUND(AVG(trip_duration_min), 2) AS avg_duration_min,
               ROUND(AVG(trip_distance), 2)     AS avg_distance_mi,
               ROUND(AVG(fare_amount), 2)       AS avg_fare
        FROM trips
        GROUP BY pickup_hour
        ORDER BY pickup_hour
        """
    )


def top_pickup_zones(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Spark SQL: busiest pickup zones (demand hotspots)."""
    df.createOrReplaceTempView("trips")
    return spark.sql(
        """
        SELECT pu_borough, pu_zone,
               COUNT(*)                         AS trips,
               ROUND(AVG(trip_duration_min), 2) AS avg_duration_min,
               ROUND(AVG(fare_amount), 2)       AS avg_fare
        FROM trips
        GROUP BY pu_borough, pu_zone
        ORDER BY trips DESC
        LIMIT 10
        """
    )


def run() -> None:
    ensure_dirs()
    spark = get_spark("transformations")
    banner("STAGE 2 | TRANSFORMATIONS + SPARK SQL")

    raw = spark.read.parquet(TRIPS_PARQUET)
    cleaned = clean_trips(raw)
    print(f"Rows before cleaning: {raw.count():,}  ->  after: {cleaned.count():,}")

    zones = read_zones(spark)
    featured = join_zones(add_features(cleaned), zones).cache()

    print("\nDemand by pickup hour (Spark SQL):")
    by_hour = demand_by_hour(spark, featured)
    by_hour.show(24, truncate=False)

    print("Top pickup zones (Spark SQL):")
    hotspots = top_pickup_zones(spark, featured)
    hotspots.show(truncate=False)

    # Persist feature table for MLlib, plus SQL aggregates for the report.
    featured.write.mode("overwrite").parquet(FEATURES_PARQUET)
    by_hour.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{OUTPUTS_DIR}/demand_by_hour"
    )
    hotspots.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{OUTPUTS_DIR}/top_pickup_zones"
    )
    print(f"\nWrote features -> {FEATURES_PARQUET}")
    print(f"Wrote SQL aggregates -> {OUTPUTS_DIR}/")

    spark.stop()


if __name__ == "__main__":
    run()
