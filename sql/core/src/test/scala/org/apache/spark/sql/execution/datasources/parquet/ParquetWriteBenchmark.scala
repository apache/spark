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
  val conf = new SparkConf()
  conf.set("spark.sql.parquet.compression.codec", "snappy")

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("parquet-write-benchmark")
    .config(conf)
    .getOrCreate()

  // Set default configs. Individual cases will change them if necessary.
  spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

  def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(spark.conf.get(key)).toOption)
    (keys, values).zipped.foreach(spark.conf.set)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  def runSQL(name: String, sql: String, values: Int): Unit = {
    withTempTable("t1") {
      spark.range(values).createOrReplaceTempView("t1")
      val benchmark = new Benchmark(name, values)
      benchmark.addCase("Parquet Writer") { _ =>
        withTempPath { dir =>
          spark.sql(sql).write.parquet(dir.getCanonicalPath)
        }
      }
      benchmark.run()
    }
  }

  def intWriteBenchmark(values: Int): Unit = {
    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    Output Single Int Column:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Writer                                2536 / 2610          6.2         161.3       1.0X
    */
    runSQL(name = "Output Single Int Column",
      sql = "select cast(id as INT) as id from t1",
      values = values)
  }

  def intStringWriteBenchmark(values: Int): Unit = {
    /*
    Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

    Output Int and String Column:            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Writer                                4644 / 4673          2.3         442.9       1.0X
    */
    runSQL(name = "Output Int and String Column",
      sql = "select cast(id as INT) as c1, cast(id as STRING) as c2 from t1",
      values = values)
  }

  def partitionTableWriteBenchmark(values: Int): Unit = {
    withTempTable("t1") {
      spark.range(values).createOrReplaceTempView("t1")
      val benchmark = new Benchmark("Partitioned Table", values)
      benchmark.addCase("Parquet Writer") { _ =>
        withTempPath { dir =>
          spark.sql("select id % 2 as p, cast(id as INT) as id from t1")
            .write.partitionBy("p").parquet(dir.getCanonicalPath)
        }
      }

      /*
      Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

      Partitioned Table:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      ---------------------------------------------------------------------------------------------
      Parquet Writer                             4163 / 4173          3.8         264.7       1.0X
      */
      benchmark.run()
    }
  }

  def main(args: Array[String]): Unit = {
    intWriteBenchmark(1024 * 1024 * 15)
    intStringWriteBenchmark(1024 * 1024 * 10)
    partitionTableWriteBenchmark(1024 * 1024 * 15)
  }
}
