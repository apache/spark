from pyspark.sql import Window
from pyspark.sql.functions import col, some

df = spark.table("test_agg").select(
    col("k"), col("v"),
    some(col("v")).over(Window.partitionBy("k").orderBy("v")))
