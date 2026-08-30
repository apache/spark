# input type checking Int

from pyspark.sql.functions import every, lit

df = spark.table("test_agg").select(every(lit(1)))
