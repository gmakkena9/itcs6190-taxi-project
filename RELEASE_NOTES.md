# Release Notes — v1.0.0

**ITCS 6190 · NYC Taxi Trip-Duration Analysis · Summer 2026**
**Author:** Gopi Bharath Makkena (@gmakkena9)

---

## What this release contains

A complete, reproducible end-to-end Apache Spark analytics pipeline on NYC Yellow Taxi trip data,
satisfying all ITCS 6190 project requirements.

### Pipeline stages
| Stage | File | Spark Component |
|---|---|---|
| 0 — Data generation | `data/generate_sample.py` | — |
| 1 — Ingestion | `src/ingestion.py` | Structured APIs (DataFrame) |
| 2 — Transformations + SQL | `src/transformations.py` | Structured APIs + Spark SQL |
| 3 — Streaming | `src/streaming.py` | Structured Streaming |
| 4 — ML Regression | `src/ml_pipeline.py` | MLlib |
| 5 — EDA | `src/eda.py` | Spark SQL + matplotlib |

### Key results
- **Random Forest RMSE:** 3.87 min · **MAE:** 2.63 min · **R²:** 0.867
- **Top feature:** `trip_distance` (78% importance); `is_rush_hour` adds 16%
- **Rush-hour effect confirmed:** 7–10 AM and 4–7 PM nearly double trip duration vs overnight
- **Streaming:** 12 micro-batches processed, aggregation matches batch SQL output exactly
- **Real-data validation:** pipeline runs against 2,964,624-row TLC Jan-2024 Parquet with R² 0.735

### How to run
```bash
bash run.sh         # or: make run
make test           # 9 unit tests
make download-real  # fetch real TLC data
make validate       # run against real data
```

### Documentation
- `docs/dataset_overview.md` — schema, sizing, licensing
- `docs/methodology.md` — full pipeline description
- `docs/results.md` — metrics, visualizations, insights
- `docs/limitations.md` — known constraints
- `docs/real_data_validation.md` — real-data run details
- `docs/reproduction_guide.md` — step-by-step setup
- `docs/slides/ITCS6190_NYC_Taxi_Presentation.pptx` — 9-slide presentation

### Links
- Presentation slides: `docs/slides/ITCS6190_NYC_Taxi_Presentation.pptx`
- Live demo dashboard: see project README for link
