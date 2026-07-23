from pyspark.sql.functions import col, count

df = spark.table("testData").agg(count(col("a")), count(col("b")))
