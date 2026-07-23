# Aggregate grouped by literals (hash aggregate).

from pyspark.sql.functions import approx_count_distinct, col, lit

df = (
    spark.table("testData")
    .filter(col("a") == 0)
    .groupBy(lit("foo"))
    .agg(approx_count_distinct(col("a")))
)
