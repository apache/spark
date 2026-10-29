from pyspark.sql import Window
from pyspark.sql.functions import bool_and, col

df = spark.table("test_agg").select(
    col("k"), col("v"), bool_and(col("v")).over(Window.partitionBy("k").orderBy("v"))
)
