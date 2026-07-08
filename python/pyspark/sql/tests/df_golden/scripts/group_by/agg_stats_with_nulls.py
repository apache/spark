# Aggregate with nulls.

from pyspark.sql.functions import (
    avg,
    col,
    count,
    kurtosis,
    max,
    min,
    round,
    skewness,
    stddev,
    sum,
    variance,
)

df = spark.table("testData").agg(
    round(skewness(col("a")), 12),
    round(kurtosis(col("a")), 12),
    min(col("a")),
    max(col("a")),
    round(avg(col("a")), 12),
    round(variance(col("a")), 12),
    round(stddev(col("a")), 12),
    sum(col("a")),
    count(col("a")))
