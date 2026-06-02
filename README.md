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
day of week, and location all influence both demand and trip duration. The full
dataset is several gigabytes and will be stored externally per the project rules;
a small, seeded representative sample with the same schema is committed under
`data/sample/` so the pipeline can be reproduced offline.

## Analytical / predictive questions
1. **Can we predict the duration of a taxi trip (in minutes)** from its distance,
   pickup time of day, day of week, and pickup zone? This is the primary
   predictive task and is framed as a regression problem.
2. **How do demand and trip duration vary across time and space** — when and where
   are the busiest pickup zones, and how much does rush-hour congestion increase
   travel time compared with off-peak hours?

## Planned Spark components
- **Structured APIs (DataFrame):** typed-schema ingestion of the raw trip data,
  cleaning out invalid records (zero distance, negative fares, impossible
  durations), feature engineering (trip duration, pickup hour, day of week,
  weekend flag, rush-hour flag, average speed), and a broadcast **join** with the
  taxi-zone lookup to attach each trip's pickup borough and zone.
- **Spark SQL:** aggregations that summarize demand-by-hour (trip counts, average
  duration, average fare) and rank the top pickup zones to reveal demand hotspots.
- **Structured Streaming:** a simulated real-time feed, created by splitting the
  sample into micro-batch files read from a directory source, with a running
  aggregation of trip volume, average fare, and average duration per pickup hour.
- **MLlib:** a regression pipeline (categorical encoding → vector assembly →
  model) predicting trip duration, using a Random Forest as the primary model and
  a Linear Regression baseline, evaluated with RMSE, MAE, and R².

## Reproducibility
The full pipeline will run from raw data to final outputs with a single command
(`run.sh` / `make run`) in Spark local mode, so the results can be reproduced on
any machine with Java and Python installed. A small unit-test suite and GitHub
Actions CI will guard the core logic of each stage.

## Repository structure
.
├── data/        # external-data pointer + small committed sample
├── src/         # pipeline stages: ingestion, transformations, streaming, ml_pipeline
├── notebooks/   # exploratory data analysis
├── tests/       # unit tests for each stage
├── docs/        # dataset overview, methodology, results, limitations, reproduction guide
├── run.sh       # one-command end-to-end run
└── Makefile     # run / test / clean targets
- **Size & dimensions:**
  - **Rows:** ~3 million trips per monthly file; the full archive is very large — <cite index="7-1">roughly 60 million yellow-taxi rows for 2024–May 2025 alone</cite>, and <cite index="3-1">about 1.5 billion rows (~50 GB) accumulated across 2009 onward</cite>.
  - **Columns:** ~19 fields per trip record (e.g. VendorID, pickup/dropoff datetimes, passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, tip_amount, tolls_amount, total_amount); <cite index="10-1">for 2025 onward a cbd_congestion_fee column       was added for congestion pricing</cite>. The Taxi Zone Lookup adds 4 columns (LocationID, Borough, Zone, service_zone) across 265 zones.
  - **Format & file size:** <cite index="10-1">published monthly as Parquet files (the format is used because of the dataset's size)</cite>; a single monthly file is on the order of ~50 MB compressed.
  - **Scope I will use:** 1–3 monthly files (~3–9M rows) referenced externally, with a small seeded ~40K-row sample (same schema) committed under `data/sample/` for offline, one-command runs.
