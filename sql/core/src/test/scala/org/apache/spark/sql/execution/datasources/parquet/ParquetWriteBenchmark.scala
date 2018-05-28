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
package org.apache.spark.sql.execution.datasources.parquet

import java.io.File

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{Benchmark, Utils}

/**
 * Benchmark to measure parquet write performance.
 * To run this:
 *  spark-submit --class <this class> --jars <spark sql test jar>
 */
object ParquetWriteBenchmark {
  val tempTable = "temp"
  val table = "t"
  val format = "parquet"
  val numRows = 1024 * 1024 * 15
  val conf = new SparkConf()
  val benchmark = new Benchmark(s"$format writer benchmark", numRows)

  conf.set("spark.sql.parquet.compression.codec", "snappy")

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("parquet-write-benchmark")
    .config(conf)
    .getOrCreate()

  // Set default configs. Individual cases will change them if necessary.
  spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")


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

  def writeInt(table: String, benchmark: Benchmark): Unit = {
    spark.sql(s"create table $table(c1 INT, c2 STRING) using $format")
    benchmark.addCase("Output Single Int Column") { _ =>
      spark.sql(s"INSERT overwrite table $table select cast(id as INT) as " +
        s"c1, cast(id as STRING) as c2 from $tempTable")
    }
  }

  def writeIntString(table: String, benchmark: Benchmark): Unit = {
    spark.sql(s"create table $table(c1 INT, c2 STRING) using $format")
    benchmark.addCase("Output Int and String Column") { _ =>
      spark.sql(s"INSERT overwrite table $table select cast(id as INT) as " +
        s"c1, cast(id as STRING) as c2 from $tempTable")
    }
  }

  def writePartition(table: String, benchmark: Benchmark): Unit = {
    spark.sql(s"create table $table(p INT, id INT) using $format PARTITIONED BY (p)")
    benchmark.addCase("Output Partitions") { _ =>
      spark.sql(s"INSERT overwrite table $table select cast(id as INT) as id," +
        s" cast(id % 2 as INT) as p from $tempTable")
    }
  }

  def writeBucket(table: String, benchmark: Benchmark): Unit = {
    spark.sql(s"create table $table(c1 INT, c2 INT) using $format CLUSTERED BY (c2) INTO 2 BUCKETS")
    benchmark.addCase("Output Buckets") { _ =>
      spark.sql(s"INSERT overwrite table $table select cast(id as INT) as " +
        s"c1, cast(id as INT) as c2 from $tempTable")
    }
  }

  def main(args: Array[String]): Unit = {
    val tableInt = "tableInt"
    val tableIntString = "tableIntString"
    val tablePartition = "tablePartition"
    val tableBucket = "tableBucket"
    withTempTable(tempTable) {
      spark.range(numRows).createOrReplaceTempView(tempTable)
      withTable(tableInt, tableIntString, tablePartition, tableBucket) {
        val benchmark = new Benchmark(s"$format writer benchmark", numRows)
        writeInt(tableInt, benchmark)
        writeIntString(tableIntString, benchmark)
        writePartition(tablePartition, benchmark)
        writeBucket(tableBucket, benchmark)
        benchmark.run()
      }
    }
  }
}
