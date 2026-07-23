# Aggregate with empty input and empty GroupBy expressions.

from pyspark.sql.functions import count, lit

df = spark.table("testData").filter(lit(False)).agg(count(lit(1)))
