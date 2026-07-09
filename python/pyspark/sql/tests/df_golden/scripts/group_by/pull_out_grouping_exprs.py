# PullOutGroupingExpressions also pulls out grouping expressions from inside
# AggregateExpressions

from pyspark.sql.functions import col, first, when

df = (
    spark.table("testData")
    .groupBy(col("a").isNull())
    .agg(when(~col("a").isNull(), 0).otherwise(1), first(col("a").isNull()).alias("c"))
)
