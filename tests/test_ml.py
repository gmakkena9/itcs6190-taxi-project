"""MLlib: the regression pipeline fits and produces numeric predictions."""
from pyspark.ml.regression import LinearRegression

from ml_pipeline import TARGET, build_pipeline


def test_pipeline_fits_and_predicts(spark):
    rows = []
    for i in range(40):
        dist = 1.0 + i * 0.2
        rows.append(
            (dist * 4.0, dist, (i % 24), (i % 7) + 1, i % 2, 0, i % 5 + 1,
             "Manhattan" if i % 2 else "Queens", 1)
        )
    cols = [TARGET, "trip_distance", "pickup_hour", "pickup_dow", "is_weekend",
            "is_rush_hour", "passenger_count", "pu_borough", "payment_type"]
    df = spark.createDataFrame(rows, cols)
    model = build_pipeline(LinearRegression(labelCol=TARGET, featuresCol="features")).fit(df)
    preds = model.transform(df)
    assert "prediction" in preds.columns
    assert preds.filter(preds.prediction.isNull()).count() == 0
