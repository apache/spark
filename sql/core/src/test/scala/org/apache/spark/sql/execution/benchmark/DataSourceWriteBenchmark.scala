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

  def writeInt(table: String, format: String, benchmark: Benchmark): Unit = {
    spark.sql(s"create table $table(c1 INT, c2 STRING) using $format")
    benchmark.addCase("Output Single Int Column") { _ =>
      spark.sql(s"INSERT overwrite table $table select cast(id as INT) as " +
        s"c1, cast(id as STRING) as c2 from $tempTable")
    }
  }

  def writeIntString(table: String, format: String, benchmark: Benchmark): Unit = {
    spark.sql(s"create table $table(c1 INT, c2 STRING) using $format")
    benchmark.addCase("Output Int and String Column") { _ =>
      spark.sql(s"INSERT overwrite table $table select cast(id as INT) as " +
        s"c1, cast(id as STRING) as c2 from $tempTable")
    }
  }

  def writePartition(table: String, format: String, benchmark: Benchmark): Unit = {
    spark.sql(s"create table $table(p INT, id INT) using $format PARTITIONED BY (p)")
    benchmark.addCase("Output Partitions") { _ =>
      spark.sql(s"INSERT overwrite table $table select cast(id as INT) as id," +
        s" cast(id % 2 as INT) as p from $tempTable")
    }
  }

  def writeBucket(table: String, format: String, benchmark: Benchmark): Unit = {
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
    // If the
    val formats: Seq[String] = if (args.isEmpty) {
      Seq("Parquet", "ORC", "JSON", "CSV")
    } else {
      args
    }
    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz
    Parquet writer benchmark:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Output Single Int Column                      6054 / 6070          2.6         384.9       1.0X
    Output Int and String Column                  5784 / 5800          2.7         367.8       1.0X
    Output Partitions                             3891 / 3904          4.0         247.4       1.6X
    Output Buckets                                5446 / 5729          2.9         346.2       1.1X

    ORC writer benchmark:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Output Single Int Column                      5734 / 5823          2.7         364.6       1.0X
    Output Int and String Column                  5802 / 5839          2.7         368.9       1.0X
    Output Partitions                             3384 / 3671          4.6         215.1       1.7X
    Output Buckets                                4950 / 4988          3.2         314.7       1.2X

    JSON writer benchmark:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Output Single Int Column                      5576 / 5594          2.8         354.5       1.0X
    Output Int and String Column                  5550 / 5620          2.8         352.9       1.0X
    Output Partitions                             3727 / 4100          4.2         237.0       1.5X
    Output Buckets                                5316 / 5852          3.0         338.0       1.0X

    CSV writer benchmark:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Output Single Int Column                      7064 / 8714          2.2         449.1       1.0X
    Output Int and String Column                  7114 / 7663          2.2         452.3       1.0X
    Output Partitions                             5771 / 6228          2.7         366.9       1.2X
    Output Buckets                                7414 / 7479          2.1         471.3       1.0X
    */
    withTempTable(tempTable) {
      spark.range(numRows).createOrReplaceTempView(tempTable)
      formats.foreach { format =>
        withTable(tableInt, tableIntString, tablePartition, tableBucket) {
          val benchmark = new Benchmark(s"$format writer benchmark", numRows)
          writeInt(tableInt, format, benchmark)
          writeIntString(tableIntString, format, benchmark)
          writePartition(tablePartition, format, benchmark)
          writeBucket(tableBucket, format, benchmark)
          benchmark.run()
        }
      }
    }
  }
}
