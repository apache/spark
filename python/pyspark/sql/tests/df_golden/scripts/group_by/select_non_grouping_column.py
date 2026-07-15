# Aggregate with empty GroupBy expressions.

from pyspark.sql.functions import col, count

df = spark.table("testData").select(col("a"), count(col("b")))
