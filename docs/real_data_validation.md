# Real-Data Validation Run

Development and CI use the synthetic sample so the project runs offline with one
command. To confirm the pipeline behaves correctly on **authentic data**, the
same code path is validated against a real NYC TLC monthly Yellow Taxi file
(per reviewer feedback requesting at least one real-file run before final
submission).

## How to run it

```bash
# 1. Download one real monthly file + the official zone lookup into data/real/
make download-real            # default 2024-01  (or: make download-real M=2024-03)

# 2. Run the full pipeline against it (ingestion -> SQL -> streaming -> MLlib)
make validate                 # default 2024-01  (or: make validate M=2024-03)
```

`make validate` simply sets two environment variables and calls `run.sh`:

```bash
TAXI_SOURCE=data/real/yellow_tripdata_2024-01.parquet \
TAXI_ZONES=data/real/taxi_zone_lookup.csv \
bash run.sh
```

No source changes are required — `src/utils.resolve_trip_source()` reads
`TAXI_SOURCE`, and ingestion auto-detects Parquet vs CSV.

## What changes for real data (and why the pipeline doesn't break)

Real TLC monthly files differ from the synthetic CSV sample in ways that would
break a naive reader:

| Difference | Real file | Handled by |
|---|---|---|
| Format | Parquet (self-describing) | `read_raw_trips` branches on extension |
| `passenger_count` | DOUBLE (often null) | `normalize_trips` casts to INT |
| Extra columns | `RatecodeID`, `store_and_fwd_flag`, `extra`, `mta_tax`, `congestion_surcharge`, `Airport_fee`, `cbd_congestion_fee` (2025+) | `normalize_trips` projects onto the 13 canonical columns |
| Missing columns (older years) | e.g. no `improvement_surcharge` | filled with typed nulls |
| Real dirty data | genuine zero-distance / negative-fare / zero-duration rows | the existing cleaning filters remove them |

`normalize_trips` projects any TLC trip schema onto the 13 canonical, typed
columns the rest of the pipeline expects, so ingestion, SQL, streaming, and
MLlib are all source-agnostic.

## Validation result

The pipeline was run end-to-end against a **real downloaded TLC monthly file**,
`yellow_tripdata_2024-01.parquet` (2,964,624 trips), via `make download-real`
then `make validate`:

- **Ingestion** normalized the real 19-column schema to the 13 canonical typed
  columns (`passenger_count` DOUBLE → INT, extra fee columns dropped) and read
  all 2,964,624 rows. Data-quality profile: 60,371 zero/negative-distance rows,
  37,448 negative-fare rows, 140,162 null `passenger_count` values.
- **Transformations + Spark SQL** cleaned 2,964,624 → 2,722,217 rows (~8.2%
  removed) and produced the hour-of-day demand table and zone hotspots. Busiest
  real pickup zone: JFK Airport (136,822 trips, ~37 min, ~$63 avg fare).
- **Structured Streaming** consumed its micro-batches and terminated cleanly.
- **MLlib** (80/20 split — 2,177,001 train / 545,216 test):

  | Model | RMSE (min) | MAE (min) | R² |
  |---|---|---|---|
  | Random Forest (40 trees, depth 8) | 6.14 | 3.85 | 0.735 |
  | Linear Regression (baseline) | 9.94 | 6.90 | 0.305 |

  Random Forest feature importances: `trip_distance` 0.686, `pu_borough` 0.130,
  `payment_type` 0.110, `pickup_hour` 0.039.

> **Interpretation.** On real records the R² is 0.735 — lower than the synthetic
> sample's ~0.87, exactly as expected, because real durations are affected by
> weather, events, and live traffic the features don't capture. The Random
> Forest now decisively beats the linear baseline (0.735 vs 0.305): real trip
> duration is genuinely non-linear. The validation confirms both that the code
> path is correct on authentic data and that the cleaning stage is essential
> (~8% of real rows are invalid, vs the ~2% injected into the sample).

> Note: the official zone lookup has 265 zones (vs the 35-zone sample subset),
> so hotspot rankings reflect the full city geography.

**Environment note (Windows).** Spark requires **Java 17** (Java 21 throws
`getSubject is not supported`) and the Hadoop `winutils.exe` / `hadoop.dll`
helpers on `HADOOP_HOME` for writing Parquet on Windows. With those in place the
run completes in a few minutes on a laptop.
