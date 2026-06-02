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
Download one or more monthly Yellow Taxi files and the zone lookup from the
[NYC TLC site](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
then update `RAW_TRIPS_CSV` / `ZONE_LOOKUP_CSV` in `src/utils.py` (or read the
Parquet files directly in `ingestion.py`). No other changes are required.
