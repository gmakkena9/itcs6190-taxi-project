"""Exploratory Data Analysis (Week 3 deliverable).

Reads the ingested taxi sample, prints summary statistics and sample Spark SQL
queries, and saves a set of visualizations to ``docs/figures/``. This is the
exploratory companion to ``ingestion.py``: it characterizes the data before any
modeling and documents the demand / duration patterns the later stages exploit.

Run after ingestion (or standalone — it falls back to the raw CSV):
    python src/eda.py
"""
from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")  # headless: save figures to disk instead of displaying
import matplotlib.pyplot as plt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion import read_raw_trips, read_zones
from utils import PROJECT_ROOT, TRIPS_PARQUET, banner, get_spark

FIG_DIR = os.path.join(PROJECT_ROOT, "docs", "figures")


def load_trips(spark: SparkSession) -> DataFrame:
    """Load curated Parquet if ingestion has run, else fall back to raw CSV."""
    if os.path.isdir(TRIPS_PARQUET):
        return spark.read.parquet(TRIPS_PARQUET)
    return read_raw_trips(spark)


def add_eda_columns(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Light derived columns for exploration: duration, hour, pickup borough."""
    dur = (
        F.col("tpep_dropoff_datetime").cast("long")
        - F.col("tpep_pickup_datetime").cast("long")
    ) / 60.0
    pu = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pu_borough"),
        F.col("Zone").alias("pu_zone"),
    )
    return (
        trips.withColumn("trip_duration_min", dur)
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .join(F.broadcast(pu), on="PULocationID", how="left")
        .filter((F.col("trip_distance") > 0) & (F.col("trip_duration_min") > 0))
    )


def _save_bar(x, y, title, xlabel, ylabel, path, rotate=0):
    plt.figure(figsize=(10, 4))
    plt.bar([str(v) for v in x], y, color="#1f6f54")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if rotate:
        plt.xticks(rotation=rotate, ha="right")
    plt.tight_layout()
    plt.savefig(path, dpi=110)
    plt.close()


def _save_hist(values, bins, title, xlabel, path):
    plt.figure(figsize=(8, 4))
    plt.hist(values, bins=bins, color="#1f6f54", edgecolor="white")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("count")
    plt.tight_layout()
    plt.savefig(path, dpi=110)
    plt.close()


def run() -> None:
    os.makedirs(FIG_DIR, exist_ok=True)
    spark = get_spark("eda")
    banner("EXPLORATORY DATA ANALYSIS")

    zones = read_zones(spark)
    df = add_eda_columns(load_trips(spark), zones).cache()
    print(f"Rows analyzed: {df.count():,}")

    # --- Summary statistics ---
    print("\nSummary statistics (key numeric columns):")
    df.select("trip_distance", "trip_duration_min", "fare_amount",
              "total_amount", "passenger_count").describe().show()

    # --- Sample Spark SQL queries ---
    df.createOrReplaceTempView("trips")

    print("Trips and average duration by pickup hour (Spark SQL):")
    by_hour = spark.sql(
        """SELECT pickup_hour,
                  COUNT(*) AS trips,
                  ROUND(AVG(trip_duration_min),2) AS avg_duration_min,
                  ROUND(AVG(fare_amount),2) AS avg_fare
           FROM trips GROUP BY pickup_hour ORDER BY pickup_hour"""
    )
    by_hour.show(24, truncate=False)

    print("Trips by pickup borough (Spark SQL):")
    by_borough = spark.sql(
        """SELECT pu_borough,
                  COUNT(*) AS trips,
                  ROUND(AVG(trip_distance),2) AS avg_distance_mi,
                  ROUND(AVG(fare_amount),2) AS avg_fare
           FROM trips GROUP BY pu_borough ORDER BY trips DESC"""
    )
    by_borough.show(truncate=False)

    print("Average fare by payment type (1=card, 2=cash):")
    spark.sql(
        """SELECT payment_type, COUNT(*) AS trips,
                  ROUND(AVG(fare_amount),2) AS avg_fare,
                  ROUND(AVG(tip_amount),2) AS avg_tip
           FROM trips GROUP BY payment_type ORDER BY payment_type"""
    ).show(truncate=False)

    print("Top 10 pickup zones (demand hotspots):")
    spark.sql(
        """SELECT pu_borough, pu_zone, COUNT(*) AS trips
           FROM trips GROUP BY pu_borough, pu_zone
           ORDER BY trips DESC LIMIT 10"""
    ).show(truncate=False)

    # --- Visualizations (collect small aggregates to the driver, then plot) ---
    hour_rows = by_hour.collect()
    _save_bar([r["pickup_hour"] for r in hour_rows], [r["trips"] for r in hour_rows],
              "Trips by pickup hour", "hour of day", "trips",
              os.path.join(FIG_DIR, "trips_by_hour.png"))
    _save_bar([r["pickup_hour"] for r in hour_rows], [r["avg_duration_min"] for r in hour_rows],
              "Average trip duration by pickup hour", "hour of day", "minutes",
              os.path.join(FIG_DIR, "duration_by_hour.png"))

    bor_rows = by_borough.collect()
    _save_bar([r["pu_borough"] for r in bor_rows], [r["trips"] for r in bor_rows],
              "Trips by pickup borough", "borough", "trips",
              os.path.join(FIG_DIR, "trips_by_borough.png"), rotate=30)

    # Histograms: sample to pandas (cap rows so the driver stays light).
    pdf = df.select("trip_distance", "trip_duration_min").limit(20000).toPandas()
    _save_hist(pdf["trip_distance"], 40, "Trip distance distribution",
               "miles", os.path.join(FIG_DIR, "hist_distance.png"))
    _save_hist(pdf["trip_duration_min"], 40, "Trip duration distribution",
               "minutes", os.path.join(FIG_DIR, "hist_duration.png"))

    print(f"\nSaved 5 figures -> {FIG_DIR}")
    spark.stop()


if __name__ == "__main__":
    run()
