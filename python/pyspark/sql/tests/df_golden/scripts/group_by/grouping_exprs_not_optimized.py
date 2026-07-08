# SPARK-34581: Don't optimize out grouping expressions from aggregate expressions
# without aggregate function

from pyspark.sql.functions import col, count

df = spark.table("testData").groupBy(col("a").isNull()).agg(
    ~col("a").isNull(), count("*").alias("c"))
