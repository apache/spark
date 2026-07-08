# empty table

from pyspark.sql.functions import bool_and, bool_or, col, every, lit, some

df = spark.table("test_agg").filter(lit(1) == 0).agg(
    every(col("v")), some(col("v")), bool_or(col("v")),
    bool_and(col("v")), bool_or(col("v")))
