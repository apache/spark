from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession\
	.builder\
        .appName("DV2Test")\
	.getOrCreate()


df = spark.read.format("csv").options(header='true').load("/home/shivsood/myspark_scripts/text.csv")
df.show(5)

#Read
df_jdbc = spark.read.options(header='true').format("jdbcv2").load()
df_jdbc.show(3)


#Write
#df.filter( col("rollnum") == "38" ).write.format("jdbcv2").save()

