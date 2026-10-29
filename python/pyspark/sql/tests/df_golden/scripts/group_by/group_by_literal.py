# Aggregate grouped by literals.

from pyspark.sql.functions import col, count, lit

df = spark.table("testData").groupBy(lit("foo")).agg(count(col("a")))
