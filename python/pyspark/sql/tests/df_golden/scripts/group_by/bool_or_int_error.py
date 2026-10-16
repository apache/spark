# input type checking Long

from pyspark.sql.functions import bool_or, lit

df = spark.table("test_agg").select(bool_or(lit(1)))
