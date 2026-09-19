# Having referencing aggregate expressions is ok.

from pyspark.sql.functions import col, count

df = (
    spark.table("test_agg")
    .agg(count(col("k")).alias("cnt"))
    .filter(col("cnt") > 1)
    .select(col("cnt"))
)
