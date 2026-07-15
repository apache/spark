from pyspark.sql import Window
from pyspark.sql.functions import bool_or, col

df = spark.table("test_agg").select(
    col("k"), col("v"), bool_or(col("v")).over(Window.partitionBy("k").orderBy("v"))
)
