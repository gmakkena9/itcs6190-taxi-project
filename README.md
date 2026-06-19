# ITCS 6190 — Cloud Computing for Data Analysis
## Course Project: NYC Taxi Trip-Duration Analysis with Apache Spark

[![CI](https://github.com/gmakkena9/itcs6190-taxi-project/actions/workflows/ci.yml/badge.svg)](https://github.com/gmakkena9/itcs6190-taxi-project/actions/workflows/ci.yml)
[![Release](https://img.shields.io/badge/release-v1.0.0-blue)](https://github.com/gmakkena9/itcs6190-taxi-project/releases/tag/v1.0.0)

**Team:** Solo — Gopi Bharath Makkena (GitHub: [@gmakkena9](https://github.com/gmakkena9))

**Chosen dataset:** NYC TLC Yellow Taxi Trip Records + Taxi Zone Lookup
Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

**Live demo:** https://gmakkena9.github.io/itcs6190-taxi-project/

---

## Overview

This project builds an end-to-end big-data analytics pipeline on Apache Spark
using New York City Yellow Taxi trip data. The pipeline ingests raw trip
records, cleans and enriches them, surfaces citywide demand patterns, processes
a simulated real-time stream of trip events, and trains a machine learning
model to predict trip duration. It integrates all required Spark components —
**Structured APIs, Spark SQL, Structured Streaming, and MLlib** — and runs
end-to-end in Spark local mode with a single command.

### Analytical questions
1. Can we predict trip duration (minutes) from distance, pickup time, and zone?
2. How do demand and duration vary across NYC — where are the hotspots, and
   how does rush hour affect travel time?

---

## Quick start (one command)

```bash
git clone https://github.com/gmakkena9/itcs6190-taxi-project.git
cd itcs6190-taxi-project
pip install -r requirements.txt
bash run.sh          # or: make run
```

A full run takes ~1–2 minutes on a laptop. All outputs land in `data/outputs/`.

```bash
make test            # run the 9 unit tests
```

---

## Repository structure

```
.
├── .github/
│   ├── workflows/ci.yml            # GitHub Actions — runs unit tests on every push
│   └── ISSUE_TEMPLATE/             # Proposal + Weekly Check-in issue templates
├── data/
│   ├── sample/                     # 40K synthetic trips + 35-zone lookup (committed)
│   ├── outputs/                    # Pipeline outputs (ML metrics, SQL aggregates, streaming results)
│   ├── generate_sample.py          # Reproducible synthetic data generator (seed=42)
│   ├── download_tlc.py             # Fetch a real TLC monthly Parquet file
│   └── external.txt                # Pointer to the full TLC dataset URL
├── src/
│   ├── utils.py                    # SparkSession factory + project path constants
│   ├── ingestion.py                # Stage 1 — typed CSV/Parquet ingestion
│   ├── transformations.py          # Stage 2 — cleaning, feature engineering, Spark SQL
│   ├── streaming.py                # Stage 3 — Structured Streaming (one-shot + continuous modes)
│   ├── ml_pipeline.py               # Stage 4 — MLlib Random Forest + Linear Regression
│   └── eda.py                      # Stage 5 — Spark SQL summaries + matplotlib charts
├── notebooks/                      # Jupyter notebooks (ingestion, EDA, ML, SQL, streaming)
├── tests/                          # pytest unit tests for every pipeline stage
├── docs/
│   ├── dataset_overview.md
│   ├── methodology.md
│   ├── results.md
│   ├── limitations.md
│   ├── eda_findings.md
│   ├── real_data_validation.md
│   ├── reproduction_guide.md
│   ├── figures/                    # 5 EDA charts (PNG)
│   ├── slides/                     # Presentation (PPTX)
│   └── index.html                  # Live interactive demo (GitHub Pages)
├── run.sh                          # One-command end-to-end pipeline
├── Makefile                        # make run / test / clean / download-real / validate
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
| License | Public domain (NYC Open Data) | Same |

The pipeline is also validated against a **real TLC Jan-2024 Parquet file
(2,964,624 rows)** — see `docs/real_data_validation.md`.

---

## Spark components

| Component | Implementation | File |
|---|---|---|
| **Structured APIs (DataFrame)** | Typed-schema ingestion, cleaning, feature engineering, broadcast join | `src/ingestion.py`, `src/transformations.py` |
| **Spark SQL** | Hourly demand aggregations, top-10 pickup zone hotspots | `src/transformations.py`, `src/eda.py` |
| **Structured Streaming** | 12-file micro-batch simulation, stateful aggregation | `src/streaming.py` |
| **MLlib** | RandomForestRegressor + LinearRegression pipeline (StringIndexer → OHE → VectorAssembler → model) | `src/ml_pipeline.py` |

### Streaming — trigger design

`streaming.py` provides two entry points, chosen deliberately to match two
different real-world needs:

- **`run()`** — used by `bash run.sh`. Uses `trigger(availableNow=True)`,
  which drains every micro-batch file currently on disk and then stops. This
  is correct for a single, reproducible, non-interactive pipeline run.
- **`run_continuous()`** — a genuinely long-running stream. Uses
  `trigger(processingTime="5 seconds")`, so Spark polls the source directory
  every 5 seconds and picks up new files as they arrive, until the query is
  stopped. This is the trigger a real continuous deployment would use.

  ```python
  from streaming import run_continuous
  run_continuous(timeout_seconds=60)   # demo window, or None to run until Ctrl+C
  ```

(Earlier in development this stage used `time.sleep()` before starting the
`availableNow` query to "wait" for files — that doesn't actually help, since
`availableNow` only processes what's present at start time. Switching the
intended-to-be-continuous use case to `processingTime` is the correct fix.)

### Performance optimizations applied

- **Broadcast join** — the 35-row zone lookup is broadcast onto the trip
  DataFrame in `transformations.py`, avoiding a shuffle.
- **Explicit schema (`StructType`)** — avoids the cost of Spark inferring the
  CSV schema by scanning the file twice.
- **Parquet intermediate storage** — `data/curated/` is written in Parquet
  (columnar, compressed) so downstream stages don't re-parse CSV.
- **`coalesce(1)`** on small output aggregates — keeps `data/outputs/` as
  single, readable CSV files instead of many small part-files.

---

## Results

### Data quality
Raw rows: **40,000** → after cleaning: **39,200** (~2% removed: zero-distance,
negative-fare, >3hr, zero-passenger rows).

### Demand patterns (Spark SQL)
Rush-hour congestion nearly **doubles** trip duration for the same ~2.9 mi
average distance:

| Window | Hours | Avg duration | Avg fare |
|---|---|---|---|
| Overnight | 0–5 | ~10 min | ~$11.4 |
| **Morning rush** | **7–10** | **~18.7 min** | **~$14.6** |
| Midday | 11–15 | ~12.3 min | ~$12.3 |
| **Evening rush** | **16–19** | **~21.1 min** | **~$15.4** |

**Top pickup zones:** Times Sq/Theatre District, Penn Station, Midtown
Center, JFK Airport, LaGuardia Airport.

### MLlib (80/20 split, seed=42)

| Model | RMSE (min) | MAE (min) | R² |
|---|---|---|---|
| **Random Forest** (40 trees, depth 8) | **3.87** | **2.63** | **0.867** |
| Linear Regression (baseline) | 3.89 | 2.94 | 0.866 |

**Top feature importances:** `trip_distance` (78.1%), `is_rush_hour` (16.3%),
`pickup_hour` (4.7%).

**Real-data validation** (TLC Jan-2024, 2,964,624 rows): Random Forest
RMSE 6.14 min, R² 0.735, vs Linear Regression RMSE 9.94, R² 0.305 — the
non-linear model's advantage is much larger on real, noisier data than on the
synthetic sample.

---

## Project status

| Week | Milestone | Status |
|---|---|---|
| 2 | Project setup — repo, README, Proposal Issue | ✅ Complete |
| 3 | Ingestion + EDA — typed ingestion, Spark SQL, visualizations | ✅ Complete |
| 4 | Streaming + MLlib — 12-batch simulation, RF + LR models | ✅ Complete |
| 5 | Full pipeline + docs + real-data validation | ✅ Complete |
| 5  | Final release v1.0.0 + live presentation | ✅ Tagged, ready to present |

---

## Deliverables checklist (per project spec)

- [x] **Code package** — runnable Spark pipeline, single-command execution via `run.sh` / `make run`
- [x] **Documentation** in `/docs/`: dataset overview, methodology, results, limitations, reproduction guide
- [x] **Final release** — Git tag `v1.0.0` with release notes, doc links, run instructions
- [x] **Presentation** — `docs/slides/ITCS6190_NYC_Taxi_Presentation.pptx`
- [x] **Live demo** — `docs/index.html`, served via GitHub Pages
- [x] **CI** — GitHub Actions runs the full test suite on every push
- [x] **Weekly Check-in Issues** — progress, blockers, plan, for every week from Week 2

---

## Running against real data

```bash
make download-real              # fetch TLC Jan-2024 (default) → data/real/
make validate                   # run full pipeline against real Parquet
# or manually:
TAXI_SOURCE=data/real/yellow_tripdata_2024-01.parquet \
TAXI_ZONES=data/real/taxi_zone_lookup.csv bash run.sh
```

---

## Running tests

```bash
make test     # or: python3 -m pytest -q
# → 9 tests passing
```

---

## License

MIT — see `LICENSE`.
