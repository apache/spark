# struct() in group by

from pyspark.sql.functions import col, count, lit, struct

df = spark.table("testData").groupBy(struct((col("a") + 0.1).alias("aa"))).agg(count(lit(1)))
