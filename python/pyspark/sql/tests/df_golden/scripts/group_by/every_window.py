# every/some/any aggregates/bool_and/bool_or are supported as windows expression.

from pyspark.sql import Window
from pyspark.sql.functions import col, every

df = spark.table("test_agg").select(
    col("k"), col("v"), every(col("v")).over(Window.partitionBy("k").orderBy("v"))
)
