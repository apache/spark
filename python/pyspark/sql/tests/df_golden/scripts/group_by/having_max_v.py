from pyspark.sql.functions import col, max

df = (
    spark.table("test_agg")
    .groupBy(col("k"))
    .agg(max(col("v")).alias("max_v"))
    .filter(col("max_v") == True)  # noqa: E712
    .select(col("k"), col("max_v"))
)
