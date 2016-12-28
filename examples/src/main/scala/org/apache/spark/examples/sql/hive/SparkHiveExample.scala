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
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
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
    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
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

    // The items in DaraFrames are of type Row, which allows you to access each column by ordinal.
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

    spark.stop()
  }
}
