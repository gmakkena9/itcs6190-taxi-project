"""Stage 4 - MLlib (trip-duration regression).

Trains a Spark MLlib regression pipeline to predict trip duration (minutes) from
distance, time-of-day, and pickup-zone features. A Random Forest is the primary
model; a Linear Regression serves as an interpretable baseline. Both are
evaluated with RMSE / MAE / R2 on a held-out test split, and the metrics plus the
Random Forest feature importances are written to ``data/outputs/ml_metrics.json``.
"""
from __future__ import annotations

import json
import os

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame

from utils import FEATURES_PARQUET, OUTPUTS_DIR, banner, ensure_dirs, get_spark

TARGET = "trip_duration_min"
NUMERIC = ["trip_distance", "pickup_hour", "pickup_dow", "is_weekend",
           "is_rush_hour", "passenger_count"]
CATEGORICAL = ["pu_borough", "payment_type"]


def build_pipeline(regressor) -> Pipeline:
    """Assemble encode -> vectorize -> model stages for the given regressor."""
    stages = []
    encoded = []
    for col in CATEGORICAL:
        idx = StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
        ohe = OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_ohe")
        stages += [idx, ohe]
        encoded.append(f"{col}_ohe")
    assembler = VectorAssembler(inputCols=NUMERIC + encoded, outputCol="features")
    stages += [assembler, regressor]
    return Pipeline(stages=stages)


def evaluate(predictions: DataFrame) -> dict:
    """Return RMSE / MAE / R2 for a predictions DataFrame."""
    metrics = {}
    for name in ("rmse", "mae", "r2"):
        ev = RegressionEvaluator(
            labelCol=TARGET, predictionCol="prediction", metricName=name
        )
        metrics[name] = round(ev.evaluate(predictions), 4)
    return metrics


def run() -> None:
    ensure_dirs()
    spark = get_spark("ml_pipeline")
    banner("STAGE 4 | MLlib REGRESSION")

    df = (
        spark.read.parquet(FEATURES_PARQUET)
        .select(TARGET, *NUMERIC, *CATEGORICAL)
        .na.drop()
    )
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    print(f"Train rows: {train.count():,}  |  Test rows: {test.count():,}")

    results = {}

    # Primary model: Random Forest.
    rf = RandomForestRegressor(
        labelCol=TARGET, featuresCol="features", numTrees=40, maxDepth=8, seed=42
    )
    rf_model = build_pipeline(rf).fit(train)
    rf_metrics = evaluate(rf_model.transform(test))
    results["random_forest"] = rf_metrics
    print(f"\nRandom Forest  -> {rf_metrics}")

    # Baseline: Linear Regression.
    lr = LinearRegression(labelCol=TARGET, featuresCol="features")
    lr_model = build_pipeline(lr).fit(train)
    lr_metrics = evaluate(lr_model.transform(test))
    results["linear_regression"] = lr_metrics
    print(f"Linear Regr.   -> {lr_metrics}")

    # Feature importances from the fitted Random Forest.
    rf_stage = rf_model.stages[-1]
    assembler = rf_model.stages[-2]
    importances = sorted(
        zip(assembler.getInputCols(), rf_stage.featureImportances.toArray()),
        key=lambda kv: kv[1],
        reverse=True,
    )
    results["rf_feature_importances"] = [
        {"feature": f, "importance": round(float(v), 4)} for f, v in importances
    ]
    print("\nTop Random Forest features:")
    for f, v in importances[:5]:
        print(f"  {f:<22} {v:.4f}")

    out_path = os.path.join(OUTPUTS_DIR, "ml_metrics.json")
    with open(out_path, "w") as fh:
        json.dump(results, fh, indent=2)
    print(f"\nWrote metrics -> {out_path}")

    spark.stop()


if __name__ == "__main__":
    run()
