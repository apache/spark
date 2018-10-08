package org.apache.spark.sql



import java.util.Properties

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object MyTest {
  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val sparkContext = spark.sparkContext
    val url = "jdbc:h2:mem:testdb2"
    var conn: java.sql.Connection = null
    val url1 = "jdbc:h2:mem:testdb3"
    var conn1: java.sql.Connection = null
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")


    val arr2x2 = Array[Row](Row.apply("dave", 42), Row.apply("mary", 222))
    val arr1x2 = Array[Row](Row.apply("fred", 3))
    val schema2 = StructType(
      StructField("name", StringType) ::
        StructField("id", IntegerType) :: Nil)
    val df = spark.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    val df2 = spark.createDataFrame(sparkContext.parallelize(arr1x2), schema2)

    df.write.mode(SaveMode.Ignore).jdbc(url1, "TEST.DROPTEST", properties)
    spark.read.jdbc(url1, "TEST.DROPTEST", properties).count()
    spark.read.jdbc(url1, "TEST.DROPTEST", properties).collect()
    spark.stop()
  }
}
