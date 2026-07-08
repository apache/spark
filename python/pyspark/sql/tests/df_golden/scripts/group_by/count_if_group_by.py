from pyspark.sql.functions import col, count_if

df = spark.table("testData").groupBy(col("a") + 1).agg(
    count_if((col("a") + 1) == col("b")))
