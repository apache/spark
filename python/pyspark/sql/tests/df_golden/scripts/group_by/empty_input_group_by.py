# Aggregate with empty input and non-empty GroupBy expressions.

from pyspark.sql.functions import col, count, lit

df = spark.table("testData").filter(lit(False)).groupBy(col("a")).agg(count(lit(1)))
