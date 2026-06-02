"""Stage 3 - Structured Streaming.

Simulates a real-time feed of taxi trips: the curated sample is split into many
small CSV files dropped into ``data/stream_source``, and a Structured Streaming
query reads that directory one file per trigger, maintains a running aggregation
of trips / average fare / average duration per pickup hour, and writes results.

Using ``trigger(availableNow=True)`` processes every available micro-batch and
then stops, so the job terminates cleanly inside the single-command pipeline
while still exercising the full streaming code path (source -> stateful
aggregation -> sink).
"""
from __future__ import annotations

import glob
import os
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion import TRIP_SCHEMA
from utils import (
    CHECKPOINT_DIR,
    OUTPUTS_DIR,
    RAW_TRIPS_CSV,
    STREAM_SOURCE_DIR,
    banner,
    ensure_dirs,
    get_spark,
)

N_MICRO_BATCHES = 12


def prepare_stream_source(n_batches: int = N_MICRO_BATCHES) -> None:
    """Split the raw sample into ``n_batches`` CSV files for the stream source."""
    if os.path.isdir(STREAM_SOURCE_DIR):
        shutil.rmtree(STREAM_SOURCE_DIR)
    os.makedirs(STREAM_SOURCE_DIR, exist_ok=True)

    with open(RAW_TRIPS_CSV) as fh:
        header = fh.readline()
        rows = fh.readlines()

    chunk = max(1, len(rows) // n_batches)
    for i in range(n_batches):
        part = rows[i * chunk : (i + 1) * chunk] if i < n_batches - 1 else rows[i * chunk :]
        if not part:
            continue
        with open(os.path.join(STREAM_SOURCE_DIR, f"batch_{i:03d}.csv"), "w") as out:
            out.write(header)
            out.writelines(part)


def aggregate_zone_stream(df: DataFrame) -> DataFrame:
    """Aggregation applied to each micro-batch (and reusable on static data).

    Computes trip volume, average fare, and average trip duration per pickup
    hour. Written as a pure transformation so it can be unit-tested on a static
    DataFrame.
    """
    dur = (
        F.col("tpep_dropoff_datetime").cast("long")
        - F.col("tpep_pickup_datetime").cast("long")
    ) / 60.0
    return (
        df.withColumn("trip_duration_min", dur)
        .filter((F.col("trip_distance") > 0) & (F.col("trip_duration_min") > 0))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
        )
    )


def run() -> None:
    ensure_dirs()
    prepare_stream_source()
    spark = get_spark("streaming")
    banner("STAGE 3 | STRUCTURED STREAMING")
    print(f"Streaming {N_MICRO_BATCHES} micro-batches from {STREAM_SOURCE_DIR}")

    stream = (
        spark.readStream.option("header", True)
        .option("maxFilesPerTrigger", 1)
        .schema(TRIP_SCHEMA)
        .csv(STREAM_SOURCE_DIR)
    )

    agg = aggregate_zone_stream(stream)

    ckpt = os.path.join(CHECKPOINT_DIR, "zone_stream")
    if os.path.isdir(ckpt):
        shutil.rmtree(ckpt)

    query = (
        agg.writeStream.outputMode("complete")
        .format("memory")
        .queryName("zone_stream")
        .option("checkpointLocation", ckpt)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    print(f"Micro-batches processed: {query.lastProgress['batchId'] + 1}")
    result = spark.sql(
        "SELECT * FROM zone_stream ORDER BY pickup_hour"
    )
    print("\nFinal streamed aggregation (trips per pickup hour):")
    result.show(24, truncate=False)

    out_dir = f"{OUTPUTS_DIR}/streaming_hourly_summary"
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    result.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir)
    print(f"Wrote streaming summary -> {out_dir}")

    spark.stop()


if __name__ == "__main__":
    run()
