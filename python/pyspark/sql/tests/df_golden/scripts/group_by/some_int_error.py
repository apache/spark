# input type checking Short

from pyspark.sql.functions import lit, some

df = spark.table("test_agg").select(some(lit(1)))
