# having

from pyspark.sql.functions import col, every

df = (
    spark.table("test_agg")
    .groupBy(col("k"))
    .agg(every(col("v")).alias("every_v"))
    .filter(col("every_v") == False)  # noqa: E712
    .select(col("k"), col("every_v"))
)
