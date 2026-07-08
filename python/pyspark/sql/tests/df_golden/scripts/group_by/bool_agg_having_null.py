from pyspark.sql.functions import col, every

df = (spark.table("test_agg")
    .groupBy(col("k"))
    .agg(every(col("v")).alias("every_v"))
    .filter(col("every_v").isNull())
    .select(col("k"), col("every_v")))
