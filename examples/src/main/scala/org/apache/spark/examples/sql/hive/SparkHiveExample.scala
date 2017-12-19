/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.sql.hive

// $example on:spark_hive$
import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
// $example off:spark_hive$

object SparkHiveExample {

  // $example on:spark_hive$
  case class Record(key: Int, value: String)
  // $example off:spark_hive$

  def main(args: Array[String]) {
    // When working with Hive, one must instantiate `SparkSession` with Hive support, including
    // connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined
    // functions. Users who do not have an existing Hive deployment can still enable Hive support.
    // When not configured by the hive-site.xml, the context automatically creates `metastore_db`
    // in the current directory and creates a directory configured by `spark.sql.warehouse.dir`,
    // which defaults to the directory `spark-warehouse` in the current directory that the spark
    // application is started.

    // $example on:spark_hive$
    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM src").show()
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()
    // +--------------------+
    // |               value|
    // +--------------------+
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // ...

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    // +---+------+---+------+
    // |key| value|key| value|
    // +---+------+---+------+
    // |  2| val_2|  2| val_2|
    // |  4| val_4|  4| val_4|
    // |  5| val_5|  5| val_5|
    // ...
    // $example off:spark_hive$

    // $example on:spark_hive$
    // to save DataFrame to Hive Managed table as Parquet format

    /*
     * 1. Create Hive Database / Schema with location at HDFS if you want to mentioned explicitly else default
     * warehouse location will be used to store Hive table Data.
     * Ex: CREATE DATABASE IF NOT EXISTS database_name LOCATION hdfs_path;
     * You don't have to explicitly give location for each table, every tables under specified schema will be located at
     * location given while creating schema.
     * 2. Create Hive Managed table with storage format as 'Parquet'
     * Ex: CREATE TABLE records(key int, value string) STORED AS PARQUET;
     */
    val hiveTableDF = sql("SELECT * FROM records").toDF()
    hiveTableDF.write.mode(SaveMode.Overwrite).saveAsTable("database_name.records")

    // to save DataFrame to Hive External table as compatible parquet format.
    /*
     * 1. Create Hive External table with storage format as parquet.
     * Ex: CREATE EXTERNAL TABLE records(key int, value string) STORED AS PARQUET;
     * Since we are not explicitly providing hive database location, it automatically takes default warehouse location
     * given to 'spark.sql.warehouse.dir' while creating SparkSession with enableHiveSupport().
     * For example, we have given '/user/hive/warehouse/' as a Hive Warehouse location. It will create schema directories
     * under '/user/hive/warehouse/' as '/user/hive/warehouse/database_name.db' and '/user/hive/warehouse/database_name'.
     */

    // to make Hive parquet format compatible with spark parquet format
    spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")
    // Multiple parquet files could be created accordingly to volume of data under directory given.
    val hiveExternalTableLocation = s"/user/hive/warehouse/database_name.db/records"
    hiveTableDF.write.mode(SaveMode.Overwrite).parquet(hiveExternalTableLocation)

    // turn on flag for Dynamic Partitioning
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    // You can create partitions in Hive table, so downstream queries run much faster.
    hiveTableDF.write.mode(SaveMode.Overwrite).partitionBy("key")
      .parquet(hiveExternalTableLocation)
    /*
    If Data volume is very huge, then every partitions would have many small-small files which may harm
    downstream query performance due to File I/O, Bandwidth I/O, Network I/O, Disk I/O.
    To improve performance you can create single parquet file under each partition directory using 'repartition'
    on partitioned key for Hive table.
     */
    hiveTableDF.repartition($"key").write.mode(SaveMode.Overwrite)
      .partitionBy("key").parquet(hiveExternalTableLocation)

    /*
     You can also do coalesce to control number of files under each partitions, repartition does full shuffle and equal
     data distribution to all partitions. here coalesce can reduce number of files to given 'Int' argument without
     full data shuffle.
     */
    // coalesce of 10 could create 10 parquet files under each partitions,
    // if data is huge and make sense to do partitioning.
    hiveTableDF.coalesce(10).write.mode(SaveMode.Overwrite)
      .partitionBy("key").parquet(hiveExternalTableLocation)
    // $example off:spark_hive$
    
    spark.stop()
  }
}
