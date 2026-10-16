# Aggregate with complex GroupBy expressions.

from pyspark.sql.functions import col, count

df = spark.table("testData").groupBy(col("a") + col("b")).agg(count(col("b")))
