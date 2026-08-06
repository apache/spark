from pyspark.sql.functions import col, count

df = spark.table("testData").groupBy(col("b")).agg(col("a"), count(col("b")))
