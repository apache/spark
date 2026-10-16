from pyspark.sql.functions import col, count

df = spark.table("testData").groupBy(col("a")).agg(count(col("a")), count(col("b")))
