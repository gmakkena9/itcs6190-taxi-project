# Reproduction Guide

## Prerequisites
- **Java 11+** (Spark requirement) — verify with `java -version`
- **Python 3.9+** — verify with `python3 --version`

## Setup
```bash
git clone <your-repo-url>
cd ITCS6190_CourseProject
pip install -r requirements.txt
```

## Run the full pipeline (one command)
```bash
bash run.sh
# or:
make run
```
This generates the sample, runs ingestion → transformations + SQL → streaming →
MLlib, and writes all artifacts to `data/outputs/`. A full run takes ~1–2 minutes
on a laptop.

## Inspect the outputs
```bash
cat data/outputs/ml_metrics.json                 # model RMSE / MAE / R² + importances
cat data/outputs/demand_by_hour/*.csv            # hourly demand (Spark SQL)
cat data/outputs/top_pickup_zones/*.csv          # demand hotspots
cat data/outputs/streaming_hourly_summary/*.csv  # streamed aggregation
```

## Run the tests
```bash
make test          # or: python3 -m pytest -q
```

## Run a single stage
```bash
python3 data/generate_sample.py   # regenerate sample data
python3 src/ingestion.py
python3 src/transformations.py
python3 src/streaming.py
python3 src/ml_pipeline.py
```

## Clean generated artifacts
```bash
make clean
```

## Using the real (full-scale) dataset
Download one real monthly Yellow Taxi file and the official zone lookup, then
run the same pipeline against it:
```bash
make download-real          # default 2024-01  (or: make download-real M=2024-03)
make validate               # runs run.sh with TAXI_SOURCE/TAXI_ZONES set
```
Ingestion auto-detects Parquet and normalizes its schema to the canonical
columns — no source edits required. See `docs/real_data_validation.md` for the
schema-difference table and validation metrics.

If you prefer to point at a file manually:
```bash
TAXI_SOURCE=/path/to/yellow_tripdata_2024-01.parquet \
TAXI_ZONES=/path/to/taxi_zone_lookup.csv \
bash run.sh
```
