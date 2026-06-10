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
    STREAM_SOURCE_DIR,
    TRIPS_PARQUET,
    banner,
    ensure_dirs,
    get_spark,
    resolve_trip_source,
)

N_MICRO_BATCHES = 12
# Cap rows fed to the simulated stream so a real multi-million-row TLC file
# still demonstrates the streaming path quickly.
STREAM_ROW_CAP = 60_000


def _split_csv(source: str, n_batches: int) -> None:
    """Fast, dependency-free path for the CSV sample: split by lines."""
    with open(source) as fh:
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


def _split_parquet(spark: SparkSession, n_batches: int) -> None:
    """Real-data path: derive micro-batch CSVs from the curated (canonical)
    trips Parquet that ingestion already wrote, capped at ``STREAM_ROW_CAP``."""
    capped = spark.read.parquet(TRIPS_PARQUET).limit(STREAM_ROW_CAP)
    rows = capped.collect()
    cols = capped.columns
    chunk = max(1, len(rows) // n_batches)

    def _fmt(v):
        return "" if v is None else str(v)

    for i in range(n_batches):
        part = rows[i * chunk : (i + 1) * chunk] if i < n_batches - 1 else rows[i * chunk :]
        if not part:
            continue
        with open(os.path.join(STREAM_SOURCE_DIR, f"batch_{i:03d}.csv"), "w") as out:
            out.write(",".join(cols) + "\n")
            for r in part:
                out.write(",".join(_fmt(r[c]) for c in cols) + "\n")


def prepare_stream_source(spark: SparkSession, n_batches: int = N_MICRO_BATCHES) -> None:
    """Split the resolved trip source into ``n_batches`` CSV files.

    Works for both the CSV sample (fast line split) and a real Parquet file
    (sampled from the curated trips written in the ingestion stage).
    """
    if os.path.isdir(STREAM_SOURCE_DIR):
        shutil.rmtree(STREAM_SOURCE_DIR)
    os.makedirs(STREAM_SOURCE_DIR, exist_ok=True)

    source = resolve_trip_source()
    if source.endswith((".parquet", ".pq")):
        _split_parquet(spark, n_batches)
    else:
        _split_csv(source, n_batches)


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
    spark = get_spark("streaming")
    prepare_stream_source(spark)
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
