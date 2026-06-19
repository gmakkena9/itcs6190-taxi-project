# Methodology

The project implements an end-to-end big-data pipeline in Apache Spark that
ingests NYC taxi trips, engineers features, surfaces demand patterns with Spark
SQL, processes a simulated real-time feed with Structured Streaming, and trains
an MLlib regression model to predict trip duration. Every stage runs in Spark
**local mode** and is chained by `run.sh`.

```
generate_sample.py        ->  data/sample/*.csv
       │
ingestion.py              ->  reads CSV (typed schema), writes data/curated/trips (Parquet)
       │
transformations.py        ->  clean + features + zone join + Spark SQL aggregates
       │                       writes data/curated/features (Parquet) + data/outputs/*.csv
       │
streaming.py              ->  Structured Streaming over micro-batched files
       │                       writes data/outputs/streaming_hourly_summary
       │
ml_pipeline.py            ->  MLlib regression, writes data/outputs/ml_metrics.json
```

## 1. Ingestion — Structured APIs (`src/ingestion.py`)
The raw CSV is read with an **explicit `StructType` schema** rather than inferred
types, which is both faster and safer at scale. A short data-quality profile
(row count, null counts, invalid-distance/fare counts) is printed, and the typed
data is written to columnar Parquet so downstream stages avoid re-parsing CSV.

## 2. Transformations & Spark SQL (`src/transformations.py`)
- **Cleaning** removes physically impossible rows and computes the target,
  `trip_duration_min`, from the pickup/dropoff timestamps.
- **Feature engineering** derives temporal features (`pickup_hour`,
  `pickup_dow`, `is_weekend`, `is_rush_hour`) and `avg_speed_mph`.
- **Join** — a `broadcast` join attaches the pickup zone's borough and name from
  the small lookup table (broadcast avoids a shuffle).
- **Spark SQL** — the feature table is registered as a temp view and queried for
  (a) demand and average duration/fare by pickup hour and (b) the top-10 busiest
  pickup zones. Results are written to `data/outputs/`.

## 3. Structured Streaming (`src/streaming.py`)
The sample is split into 12 small CSV files written to `data/stream_source/` to
simulate an arriving feed. A streaming query reads that directory **one file per
trigger** (`maxFilesPerTrigger = 1`) with an explicit schema and maintains a
running aggregation of trips / average fare / average duration per pickup hour
(`outputMode("complete")`). The aggregation logic is factored into a pure
function (`aggregate_zone_stream`) so it can be unit-tested on static data.

Two trigger modes are implemented, matching two different use cases:

- **`run()` — `trigger(availableNow=True)`.** Used by `bash run.sh`. Processes
  every micro-batch file that is on disk when the query starts and then stops
  on its own. This is the right trigger for a single, reproducible,
  non-interactive pipeline run that needs to terminate and produce a final
  output file.
- **`run_continuous()` — `trigger(processingTime="5 seconds")`.** A genuinely
  long-running stream: Spark polls `data/stream_source/` every 5 seconds and
  picks up any new files, indefinitely, until the query is stopped
  (`query.stop()`, Ctrl+C, or a bounded `awaitTermination(timeout)` for demo
  purposes). This is the trigger a real continuous deployment of this job
  would use.

This split came out of a discussion with the instructor: an earlier version of
this stage used `time.sleep()` before starting the `availableNow` query, on the
assumption that this would let in-flight files get fully written before the
stream "missed" them. That doesn't actually solve anything, because
`availableNow` only looks at what's present the moment the query starts — no
amount of sleeping beforehand changes that, and it can't react to files written
*after* the query begins either way. The correct fix, applied here, is to use
`processingTime` for any code path that's actually meant to behave like a
continuous stream, and reserve `availableNow` for the "run once, drain
everything, exit" case that the one-command pipeline needs.

## 4. MLlib Regression (`src/ml_pipeline.py`)
A Spark ML `Pipeline` predicts `trip_duration_min`:
`StringIndexer` + `OneHotEncoder` on the categorical columns (`pu_borough`,
`payment_type`) → `VectorAssembler` over the numeric + encoded features →
regressor. A **Random Forest** (40 trees, depth 8) is the primary model and a
**Linear Regression** serves as an interpretable baseline. The data is split
80/20 (seed 42); both models are scored with RMSE, MAE, and R²; and the metrics
plus the Random Forest feature importances are written to
`data/outputs/ml_metrics.json`.

## Reproducibility & testing
`run.sh` (or `make run`) executes the whole pipeline from data generation to
metrics. `make test` runs a pytest suite that exercises the schema, cleaning
rules, SQL aggregation, streaming aggregation, and model fitting on small
in-memory DataFrames, and the same suite runs in GitHub Actions CI.
