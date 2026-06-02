#!/usr/bin/env bash
# One-command, end-to-end pipeline for the NYC Taxi trip-duration project.
# Runs entirely in Spark local mode against the bundled sample dataset.
set -euo pipefail
cd "$(dirname "$0")"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"

echo ">>> [0/5] Generating sample dataset"
python3 data/generate_sample.py

echo ">>> [1/5] Ingestion (Structured APIs)"
python3 src/ingestion.py

echo ">>> [2/5] Transformations + Spark SQL"
python3 src/transformations.py

echo ">>> [3/5] Structured Streaming"
python3 src/streaming.py

echo ">>> [4/5] MLlib regression"
python3 src/ml_pipeline.py

echo ">>> [5/5] Exploratory Data Analysis (figures)"
python3 src/eda.py

echo ">>> Pipeline complete. Outputs in data/outputs/, figures in docs/figures/"
