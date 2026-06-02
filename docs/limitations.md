# Limitations

- **Synthetic sample.** To keep the project runnable offline with one command,
  the bundled data is generated, not real TLC records. It deliberately encodes a
  clean distance + time-of-day → duration relationship, so the reported R² (~0.87)
  is higher and tidier than what real, noisier trip data would yield. The code is
  unchanged for real data — point `RAW_TRIPS_CSV` at TLC Parquet files to validate.
- **Zone subset.** The lookup contains 35 representative zones rather than the
  full 265, so hotspot rankings reflect the sampled geography.
- **No external joins.** Real-world duration is also driven by weather, events,
  and live traffic. Those sources are out of scope here; the join demonstrates
  the mechanism (zone enrichment) rather than exhausting useful signals.
- **Streaming is simulated.** Files are replayed from disk with
  `trigger(availableNow=True)` so the job terminates inside the batch pipeline.
  Event-time watermarking, late-data handling, and continuous low-latency
  triggers are not exercised.
- **Modeling scope.** No hyperparameter tuning, cross-validation, or feature
  scaling study. Two models are compared; a production effort would add a
  `CrossValidator` grid and richer features.
- **Local mode only.** Spark runs `local[*]`; cluster behavior (shuffle at scale,
  executor tuning, skew) is not represented.
