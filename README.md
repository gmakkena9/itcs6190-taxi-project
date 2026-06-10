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
predict how long a trip will take. It integrates all required Spark components —
**Structured APIs, Spark SQL, Structured Streaming, and MLlib** — and runs
end-to-end in Spark local mode with a single command.

---

## Quick Start (one command)

```bash
git clone <your-repo-url>
cd itcs6190-taxi-project
pip install -r requirements.txt
bash run.sh          # or: make run
```

A full run takes ~1–2 minutes on a laptop. All outputs land in `data/outputs/`.

---

## Repository Structure

```
.
├── data/
│   ├── sample/                 # 40K synthetic trips + 35-zone lookup (committed)
│   ├── generate_sample.py      # reproducible synthetic data generator (seed=42)
│   ├── download_tlc.py         # fetch a real TLC monthly Parquet file
│   └── external.txt            # pointer to the full TLC dataset URL
├── src/
│   ├── utils.py                # SparkSession factory + project path constants
│   ├── ingestion.py            # Stage 1 — typed CSV/Parquet ingestion
│   ├── transformations.py      # Stage 2 — cleaning, feature engineering, Spark SQL
│   ├── streaming.py            # Stage 3 — Structured Streaming simulation
│   ├── ml_pipeline.py          # Stage 4 — MLlib Random Forest + Linear Regression
│   └── eda.py                  # Stage 5 — Spark SQL summaries + matplotlib charts
├── notebooks/                  # Jupyter notebooks (ingestion, EDA, ML, SQL, streaming)
├── tests/                      # pytest unit tests for every pipeline stage
├── docs/
│   ├── dataset_overview.md
│   ├── methodology.md
│   ├── results.md
│   ├── limitations.md
│   ├── eda_findings.md
│   ├── real_data_validation.md
│   ├── reproduction_guide.md
│   ├── figures/                # 5 EDA charts (PNG)
│   └── slides/                 # Presentation (PPTX)
├── run.sh                      # one-command end-to-end pipeline
├── Makefile                    # make run / test / clean / download-real / validate
└── requirements.txt
```

---

## Dataset

| | Full TLC dataset | Committed sample |
|---|---|---|
| Rows | ~3–4M per monthly file; ~1.5B since 2009 | **40,000 trips** |
| Columns | 18–19 (schema drifts year-to-year) | **13 canonical fields** |
| Zone lookup | 265 zones × 4 columns | 35 zones × 4 columns |
| Format | Parquet (monthly, ~50 MB compressed) | CSV |

The pipeline is also validated against a **real TLC Jan-2024 Parquet file (2,964,624 rows)**
— see `docs/real_data_validation.md`.

---

## Spark Components

| Component | Implementation |
|---|---|
| **Structured APIs (DataFrame)** | Typed-schema ingestion, cleaning, feature engineering, broadcast join |
| **Spark SQL** | Hourly demand aggregations, top-10 pickup zone hotspots |
| **Structured Streaming** | 12-file micro-batch simulation, stateful aggregation, `trigger(availableNow=True)` |
| **MLlib** | RandomForestRegressor + LinearRegression pipeline (StringIndexer → OHE → VectorAssembler → model) |

---

## Results

### Data quality
- Raw rows: **40,000** → after cleaning: **39,200** (~2% removed)

### Demand patterns (Spark SQL)
Rush-hour congestion nearly **doubles** trip duration for the same ~2.9 mi distance:

| Window | Hours | Avg Duration | Avg Fare |
|---|---|---|---|
| Overnight | 0–5 | ~10 min | ~$11.4 |
| **Morning rush** | **7–10** | **~18.7 min** | **~$14.6** |
| Midday | 11–15 | ~12.3 min | ~$12.3 |
| **Evening rush** | **16–19** | **~21.1 min** | **~$15.4** |

**Top pickup zones:** Times Sq/Theatre District, Penn Station, Midtown Center, JFK Airport, LaGuardia

### MLlib (80/20 split, seed=42)

| Model | RMSE (min) | MAE (min) | R² |
|---|---|---|---|
| **Random Forest** (40 trees, depth 8) | **3.87** | **2.63** | **0.867** |
| Linear Regression (baseline) | 3.89 | 2.94 | 0.866 |

**Top feature importances:** `trip_distance` (78%), `is_rush_hour` (16%), `pickup_hour` (5%)

---

## Status

| Week | Milestone | Status |
|---|---|---|
| 2 | Project Setup — repo, README, Proposal Issue | ✅ |
| 3 | Ingestion + EDA — typed ingestion, Spark SQL, visualizations | ✅ |
| 4 | Streaming + MLlib — 12-batch simulation, RF + LR models | ✅ |
| 5 | Full pipeline + docs + real-data validation | ✅ |
| 5 (Jun 22–23) | Final Release v1.0.0 + Live Presentation | 🚀 |

---

## Running Against Real Data

```bash
make download-real              # fetch TLC Jan-2024 (default) → data/real/
make validate                   # run full pipeline against real Parquet
# or manually:
TAXI_SOURCE=data/real/yellow_tripdata_2024-01.parquet \
TAXI_ZONES=data/real/taxi_zone_lookup.csv bash run.sh
```

---

## Running Tests

```bash
make test     # or: python3 -m pytest -q
# → 9 tests passing
```
