# Results

All figures below come from a single reproducible run (`bash run.sh`) on the
seeded 40,000-row sample. Numbers are deterministic given the fixed seeds.

## Data quality (ingestion)
- Raw rows: **40,000**; after cleaning: **39,200** (~2% removed as invalid —
  zero-distance, negative-fare, or non-positive-duration trips).

## Demand patterns (Spark SQL)
Average trip duration by pickup hour shows a strong, interpretable
**rush-hour effect** — average distance barely changes across the day (~2.9–3.1
mi), but duration roughly doubles during congested windows:

| Window | Hours | Avg duration | Avg fare |
|---|---|---|---|
| Overnight | 0–5 | ~9.6–10.0 min | ~$11.1–12.0 |
| Morning rush | 7–10 | ~18.5–18.9 min | ~$14.7–15.0 |
| Midday | 11–15 | ~12.1–12.4 min | ~$12.3–12.6 |
| Evening rush | 16–19 | ~20.8–21.5 min | ~$15.3–15.7 |

**Top pickup zones (demand hotspots):** Times Sq/Theatre District (~2,300
trips), Penn Station/Madison Sq West, and Midtown Center lead, followed by JFK
and LaGuardia airports — consistent with real NYC demand geography.

Full tables: `data/outputs/demand_by_hour/` and `data/outputs/top_pickup_zones/`.

## Streaming (Structured Streaming)
The streaming job consumed **12 micro-batches** and reproduced the same hourly
demand/duration profile on the simulated live feed, confirming the streaming and
batch aggregations agree. Output: `data/outputs/streaming_hourly_summary/`.

## Trip-duration model (MLlib)
80/20 split (seed 42): **31,369** train / **7,831** test rows.

| Model | RMSE (min) | MAE (min) | R² |
|---|---|---|---|
| **Random Forest** (40 trees, depth 8) | **3.87** | **2.63** | **0.867** |
| Linear Regression (baseline) | 3.89 | 2.94 | 0.866 |

Both models explain ~87% of the variance in trip duration. The Random Forest
edges the baseline on MAE while capturing the non-linear congestion effect.

**Random Forest feature importances:**

| Feature | Importance |
|---|---|
| `trip_distance` | 0.781 |
| `is_rush_hour` | 0.163 |
| `pickup_hour` | 0.047 |
| `passenger_count` | 0.003 |
| `pickup_dow` | 0.003 |

Distance dominates (as expected), but **rush-hour status and pickup hour
together account for ~21%** of the model's predictive power — the model has
learned the time-of-day congestion signal, exactly the behavior the SQL
aggregates revealed. Full metrics: `data/outputs/ml_metrics.json`.

## Takeaway
Time of day is the second-largest driver of trip duration after distance. A
dispatcher predicting ETAs from distance alone would be systematically optimistic
during the 7–10 and 16–19 windows; incorporating an `is_rush_hour` feature
measurably improves predictions.
