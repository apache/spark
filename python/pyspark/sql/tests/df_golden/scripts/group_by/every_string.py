# input type checking String

from pyspark.sql.functions import every, lit

df = spark.table("test_agg").select(every(lit("true")))
