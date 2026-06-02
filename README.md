# ITCS 6190 – Cloud Computing for Data Analysis
## Course Project: NYC Taxi Trip-Duration Analysis with Apache Spark

**Team:** Team 3 (Solo) — Gopi Bharath Makkena (GitHub: @gmakkena9)

**Chosen dataset:** NYC TLC Yellow Taxi Trip Records (+ Taxi Zone Lookup)
Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

---

## Overview
This project builds an end-to-end big-data analytics pipeline on Apache Spark
using New York City Yellow Taxi trip data. The pipeline ingests raw trip records,
cleans and enriches them, surfaces demand patterns across the city, processes a
simulated real-time stream of trip events, and trains a machine learning model to
predict how long a trip will take. It integrates all the required Spark
components — Structured APIs, Spark SQL, Structured Streaming, and MLlib — and is
designed to run end-to-end in Spark local mode with a single command.

The NYC taxi dataset is a good fit because it is large, well-documented, and rich
in the kinds of signals that make analysis interesting: distance, time of day,
day of week, and location all influence both demand and trip duration.

## Dataset size & dimensions
- **Rows:** ~3–4 million trips per monthly file; roughly 60 million rows for
  2024–mid-2025, and about 1.5 billion rows (~50 GB) accumulated since 2009.
- **Columns:** ~18–19 fields per trip record (VendorID, pickup/dropoff datetimes,
  passenger_count, trip_distance, PULocationID, DOLocationID, payment_type,
  fare_amount, tip_amount, tolls_amount, total_amount, etc.); a
  `cbd_congestion_fee` column was added for 2025+. The Taxi Zone Lookup adds 4
  columns (LocationID, Borough, Zone, service_zone) across 265 zones.
- **Format:** published monthly as Parquet (~50 MB compressed per file).
- **Scope used here:** 1–3 monthly files (~3–9M rows) referenced externally, plus
  a small seeded ~40K-row sample (same schema) committed under `data/sample/` for
  offline, one-command reproduction.

## Analytical / predictive questions
1. **Can we predict the duration of a taxi trip (in minutes)** from its distance,
   pickup time of day, day of week, and pickup zone? (Primary regression task.)
2. **How do demand and trip duration vary across time and space** — when and where
   are the busiest pickup zones, and how much does rush-hour congestion increase
   travel time compared with off-peak hours?

## Planned Spark components
- **Structured APIs (DataFrame):** typed-schema ingestion of the raw trip data,
  cleaning invalid records (zero distance, negative fares, impossible durations),
  feature engineering (trip duration, pickup hour, day of week, weekend flag,
  rush-hour flag, average speed), and a broadcast **join** with the taxi-zone
  lookup to attach each trip's pickup borough and zone.
- **Spark SQL:** aggregations summarizing demand-by-hour (trip counts, average
  duration, average fare) and ranking the top pickup zones (demand hotspots).
- **Structured Streaming:** a simulated real-time feed, created by splitting the
  sample into micro-batch files read from a directory source, with a running
  aggregation of trip volume, average fare, and average duration per pickup hour.
- **MLlib:** a regression pipeline (categorical encoding → vector assembly →
  model) predicting trip duration — Random Forest as the primary model with a
  Linear Regression baseline, evaluated with RMSE, MAE, and R².

## Reproducibility
The full pipeline runs from raw data to final outputs with a single command
(`run.sh` / `make run`) in Spark local mode, reproducible on any machine with
Java and Python installed. A unit-test suite and GitHub Actions CI guard the core
logic of each stage.

## Repository structure
```
.
├── data/        # external-data pointer + small committed sample
├── src/         # pipeline stages: ingestion, transformations, streaming, ml_pipeline
├── notebooks/   # exploratory data analysis
├── tests/       # unit tests for each stage
├── docs/        # dataset overview, methodology, results, limitations, reproduction guide
├── run.sh       # one-command end-to-end run
└── Makefile     # run / test / clean targets
```

## Status & roadmap
- **Week 2 — Project Setup:** repository, README, Proposal Issue ✅
- **Week 3 — Ingestion + EDA:** typed ingestion to Parquet, data-quality profile,
  summary stats, Spark SQL queries, and visualizations (`docs/figures/`) ✅
- **Week 4 — Streaming + MLlib:** Structured Streaming aggregation and a
  trip-duration regression model (Random Forest + Linear Regression baseline) ✅
- **Week 5 — Full pipeline + release:** end-to-end integration, finalized `/docs/`,
  tagged release (in progress)

Run the whole pipeline with `bash run.sh` (or `make run`).
