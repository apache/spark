from pyspark.sql.functions import count, lit

df = spark.table("testData").filter(lit(False)).agg(count(lit(1))).select(lit(1))
