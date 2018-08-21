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
package org.apache.spark.sql.execution.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Benchmark

/**
 * Benchmark to measure data source write performance.
 * By default it measures 4 data source format: Parquet, ORC, JSON, CSV:
 *  spark-submit --class <this class> <spark sql test jar>
 * To measure specified formats, run it with arguments:
 *  spark-submit --class <this class> <spark sql test jar> format1 [format2] [...]
 */
object DataSourceWriteBenchmark {
  val conf = new SparkConf()
    .setAppName("DataSourceWriteBenchmark")
    .setIfMissing("spark.master", "local[1]")
    .set("spark.sql.parquet.compression.codec", "snappy")
    .set("spark.sql.orc.compression.codec", "snappy")

  val spark = SparkSession.builder.config(conf).getOrCreate()

  // Set default configs. Individual cases will change them if necessary.
  spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

  val tempTable = "temp"
  val numRows = 1024 * 1024 * 15

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  def writeNumeric(table: String, format: String, benchmark: Benchmark, dataType: String): Unit = {
    spark.sql(s"create table $table(id $dataType) using $format")
    benchmark.addCase(s"Output Single $dataType Column") { _ =>
      spark.sql(s"INSERT OVERWRITE TABLE $table SELECT CAST(id AS $dataType) AS c1 FROM $tempTable")
    }
  }

  def writeIntString(table: String, format: String, benchmark: Benchmark): Unit = {
    spark.sql(s"CREATE TABLE $table(c1 INT, c2 STRING) USING $format")
    benchmark.addCase("Output Int and String Column") { _ =>
      spark.sql(s"INSERT OVERWRITE TABLE $table SELECT CAST(id AS INT) AS " +
        s"c1, CAST(id AS STRING) AS c2 FROM $tempTable")
    }
  }

  def writePartition(table: String, format: String, benchmark: Benchmark): Unit = {
    spark.sql(s"CREATE TABLE $table(p INT, id INT) USING $format PARTITIONED BY (p)")
    benchmark.addCase("Output Partitions") { _ =>
      spark.sql(s"INSERT OVERWRITE TABLE $table SELECT CAST(id AS INT) AS id," +
        s" CAST(id % 2 AS INT) AS p FROM $tempTable")
    }
  }

  def writeBucket(table: String, format: String, benchmark: Benchmark): Unit = {
    spark.sql(s"CREATE TABLE $table(c1 INT, c2 INT) USING $format CLUSTERED BY (c2) INTO 2 BUCKETS")
    benchmark.addCase("Output Buckets") { _ =>
      spark.sql(s"INSERT OVERWRITE TABLE $table SELECT CAST(id AS INT) AS " +
        s"c1, CAST(id AS INT) AS c2 FROM $tempTable")
    }
  }

  def main(args: Array[String]): Unit = {
    val tableInt = "tableInt"
    val tableDouble = "tableDouble"
    val tableIntString = "tableIntString"
    val tablePartition = "tablePartition"
    val tableBucket = "tableBucket"
    val formats: Seq[String] = if (args.isEmpty) {
      Seq("Parquet", "ORC", "JSON", "CSV")
    } else {
      args
    }
    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz
    Parquet writer benchmark:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Output Single Int Column                      1815 / 1932          8.7         115.4       1.0X
    Output Single Double Column                   1877 / 1878          8.4         119.3       1.0X
    Output Int and String Column                  6265 / 6543          2.5         398.3       0.3X
    Output Partitions                             4067 / 4457          3.9         258.6       0.4X
    Output Buckets                                5608 / 5820          2.8         356.6       0.3X

    ORC writer benchmark:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Output Single Int Column                      1201 / 1239         13.1          76.3       1.0X
    Output Single Double Column                   1542 / 1600         10.2          98.0       0.8X
    Output Int and String Column                  6495 / 6580          2.4         412.9       0.2X
    Output Partitions                             3648 / 3842          4.3         231.9       0.3X
    Output Buckets                                5022 / 5145          3.1         319.3       0.2X

    JSON writer benchmark:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Output Single Int Column                      1988 / 2093          7.9         126.4       1.0X
    Output Single Double Column                   2854 / 2911          5.5         181.4       0.7X
    Output Int and String Column                  6467 / 6653          2.4         411.1       0.3X
    Output Partitions                             4548 / 5055          3.5         289.1       0.4X
    Output Buckets                                5664 / 5765          2.8         360.1       0.4X

    CSV writer benchmark:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Output Single Int Column                      3025 / 3190          5.2         192.3       1.0X
    Output Single Double Column                   3575 / 3634          4.4         227.3       0.8X
    Output Int and String Column                  7313 / 7399          2.2         464.9       0.4X
    Output Partitions                             5105 / 5190          3.1         324.6       0.6X
    Output Buckets                                6986 / 6992          2.3         444.1       0.4X
    */
    withTempTable(tempTable) {
      spark.range(numRows).createOrReplaceTempView(tempTable)
      formats.foreach { format =>
        withTable(tableInt, tableDouble, tableIntString, tablePartition, tableBucket) {
          val benchmark = new Benchmark(s"$format writer benchmark", numRows)
          writeNumeric(tableInt, format, benchmark, "Int")
          writeNumeric(tableDouble, format, benchmark, "Double")
          writeIntString(tableIntString, format, benchmark)
          writePartition(tablePartition, format, benchmark)
          writeBucket(tableBucket, format, benchmark)
          benchmark.run()
        }
      }
    }
  }
}
