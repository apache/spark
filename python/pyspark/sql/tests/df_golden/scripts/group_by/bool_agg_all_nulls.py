# all null values

from pyspark.sql.functions import bool_and, bool_or, col, every, some

df = spark.table("test_agg").filter(col("k") == 4).agg(
    every(col("v")), some(col("v")), bool_or(col("v")),
    bool_and(col("v")), bool_or(col("v")))
