# Aggregate grouped by literals (sort aggregate).

from pyspark.sql.functions import col, lit, max, struct

df = spark.table("testData").filter(col("a") == 0).groupBy(lit("foo")).agg(max(struct(col("a"))))
