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
package org.apache.spark.sql

import java.util.Random

import scala.util.Try

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Benchmark

/**
 * Benchmark to measure performance of columnar storage for dataframe cache.
 * To run this:
 *  bin/spark-submit --class org.apache.spark.sql.DataFrameCacheBenchmark
 *    sql/core/target/spark-sql_*-tests.jar
 *    [float datasize scale] [double datasize scale] [master URL]
 */
case class DataFrameCacheBenchmark(masterURL: String) {
  val conf = new SparkConf()
  val sc = new SparkContext(
    (if (masterURL == null) "local[1]" else masterURL), "test-sql-context", conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  // Set default configs. Individual cases will change them if necessary.
  sqlContext.conf.setConfString(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(sqlContext.conf.getConfString(key)).toOption)
    (keys, values).zipped.foreach(sqlContext.conf.setConfString)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => sqlContext.conf.setConfString(key, value)
        case (key, None) => sqlContext.conf.unsetConf(key)
      }
    }
  }

  def floatSumBenchmark(values: Int, iters: Int = 5): Unit = {
    val suites = Seq(("InternalRow", "false"), ("ColumnVector", "true"))

    val benchmarkPT = new Benchmark("Float Sum with PassThrough cache", values, iters)
    val rand1 = new Random(511)
    val dfPassThrough = sc.parallelize(0 to values - 1, 1)
      .map(i => rand1.nextFloat()).toDF().cache()
    dfPassThrough.count()       // force to create df.cache()
    suites.foreach {
      case (str, value) =>
        benchmarkPT.addCase(s"$str codegen") { iter =>
          withSQLConf(SQLConf. COLUMN_VECTOR_CODEGEN.key -> value) {
            dfPassThrough.agg(sum("value")).collect
          }
        }
    }

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_66-b17 on Linux 2.6.32-504.el6.x86_64
    Intel(R) Xeon(R) CPU E5-2667 v2 @ 3.30GHz
    Float Sum with PassThrough cache:   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    InternalRow codegen                       476 /  483         66.1          15.1       1.0X
    ColumnVector codegen                       91 /  103        343.8           2.9       5.2X
    */

    benchmarkPT.run()
    dfPassThrough.unpersist(true)
    System.gc()
  }

  def doubleSumBenchmark(values: Int, iters: Int = 5): Unit = {
    val suites = Seq(("InternalRow", "false"), ("ColumnVector", "true"))

    val benchmarkPT = new Benchmark("Double Sum with PassThrough cache", values, iters)
    val rand1 = new Random(511)
    val dfPassThrough = sc.parallelize(0 to values - 1, 1)
      .map(i => rand1.nextDouble()).toDF().cache()
    dfPassThrough.count()       // force to create df.cache()
    suites.foreach {
      case (str, value) =>
        benchmarkPT.addCase(s"$str codegen") { iter =>
          withSQLConf(SQLConf. COLUMN_VECTOR_CODEGEN.key -> value) {
            dfPassThrough.agg(sum("value")).collect
          }
        }
    }

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_66-b17 on Linux 2.6.32-504.el6.x86_64
    Intel(R) Xeon(R) CPU E5-2667 v2 @ 3.30GHz
    Double Sum with PassThrough cache:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    InternalRow codegen                       290 /  306         54.3          18.4       1.0X
    ColumnVector codegen                       95 /  101        165.7           6.0       3.1X
    */

    benchmarkPT.run()
    dfPassThrough.unpersist(true)
    System.gc()
  }

  def run(f: Int, d: Int): Unit = {
    floatSumBenchmark(1024 * 1024 * f)
    doubleSumBenchmark(1024 * 1024 * d)
  }
}

object DataFrameCacheBenchmark {
  def main(args: Array[String]): Unit = {
    val f = if (args.length > 0) args(0).toInt else 30
    val d = if (args.length > 1) args(1).toInt else 15
    val masterURL = if (args.length > 2) args(2) else "local[1]"

    val benchmark = DataFrameCacheBenchmark(masterURL)
    benchmark.run(f, d)
  }
}
