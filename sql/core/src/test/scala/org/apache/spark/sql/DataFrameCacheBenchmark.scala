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
 *  spark-submit --class <this class> <spark sql test jar>
 */
object DataFrameCacheBenchmark {
  val conf = new SparkConf()
  val sc = new SparkContext("local[1]", "test-sql-context", conf)
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

  def intSumBenchmark(values: Int, iters: Int = 5): Unit = {
    val suites = Seq(("InternalRow", "false"), ("ColumnVector", "true"))

    val benchmarkPT = new Benchmark("Int Sum with PassThrough cache", values, iters)
    val rand1 = new Random(511)
    val dfPassThrough = sc.parallelize(0 to values - 1, 1).map(i => rand1.nextInt()).toDF.cache()
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
    Int Sum with PassThrough cache:     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    InternalRow codegen                       462 /  466         68.1          14.7       1.0X
    ColumnVector codegen                       94 /  100        336.3           3.0       4.9X
    */

    benchmarkPT.run()
    dfPassThrough.unpersist(true)
    System.gc()

    val benchmarkRL = new Benchmark("Int Sum with RunLength cache", values, iters)
    val dfRunLength = sc.parallelize(0 to values - 1, 1)
      .map(i => (i, (i / 1024).toInt)).toDF("k", "v").cache()
    dfRunLength.count()         // force to create df.cache()
    suites.foreach {
      case (str, value) =>
        benchmarkRL.addCase(s"$str codegen") { iter =>
          withSQLConf(SQLConf. COLUMN_VECTOR_CODEGEN.key -> value) {
            dfRunLength.agg(sum("v")).collect()
          }
        }
    }

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_66-b17 on Linux 2.6.32-504.el6.x86_64
    Intel(R) Xeon(R) CPU E5-2667 v2 @ 3.30GHz
    Int Sum with RunLength cache:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    InternalRow codegen                       492 /  553         63.9          15.7       1.0X
    ColumnVector codegen                      175 /  180        179.5           5.6       2.8X
    */

    benchmarkRL.run()
    dfRunLength.unpersist(true)
    System.gc()

    val rand2 = new Random(42)
    val benchmarkDIC = new Benchmark("Int Sum with Dictionary cache", values, iters)
    val dfDictionary = sc.parallelize(0 to values - 1, 1)
      .map(i => (i, (rand2.nextInt() % 1023))).toDF("k", "v").cache()
    dfDictionary.count()        // force to create df.cache()
    suites.foreach {
      case (str, value) =>
        benchmarkDIC.addCase(s"$str codegen") { iter =>
          withSQLConf(SQLConf. COLUMN_VECTOR_CODEGEN.key -> value) {
            dfDictionary.agg(sum("v")).collect()
          }
        }
    }

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_66-b17 on Linux 2.6.32-504.el6.x86_64
    Intel(R) Xeon(R) CPU E5-2667 v2 @ 3.30GHz
    Int Sum with Dictionary cache:      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    InternalRow codegen                       599 /  610         52.5          19.0       1.0X
    ColumnVector codegen                      320 /  327         98.3          10.2       1.9X
    */

    benchmarkDIC.run()
    dfDictionary.unpersist(true)
    System.gc()

    val benchmarkIDelta = new Benchmark("Int Sum with Delta cache", values, iters)
    val dfIntDelta = sc.parallelize(0 to values - 1, 1)
      .map(i => (i, i)).toDF("k", "v").cache()
    dfIntDelta.count()          // force to create df.cache()
    suites.foreach {
      case (str, value) =>
        benchmarkIDelta.addCase(s"$str codegen") { iter =>
          withSQLConf(SQLConf. COLUMN_VECTOR_CODEGEN.key -> value) {
            dfIntDelta.agg(sum("v")).collect()
          }
        }
    }

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_66-b17 on Linux 2.6.32-504.el6.x86_64
    Intel(R) Xeon(R) CPU E5-2667 v2 @ 3.30GHz
    Int Sum with Delta cache:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    InternalRow codegen                       467 /  512         67.4          14.8       1.0X
    ColumnVector codegen                      247 /  250        127.4           7.8       1.9X
    */

    benchmarkIDelta.run()
    dfIntDelta.unpersist(true)
    System.gc()
  }

  def longSumBenchmark(values: Int, iters: Int = 5): Unit = {
    val suites = Seq(("InternalRow", "false"), ("ColumnVector", "true"))

    val benchmarkPT = new Benchmark("Long Sum with PassThrough cache", values, iters)
    val rand1 = new Random(511)
    val dfPassThrough = sc.parallelize(0 to values - 1, 1).map(i => rand1.nextLong()).toDF().cache()
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
    Long Sum with PassThrough cache:    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    InternalRow codegen                       382 /  420         41.2          24.3       1.0X
    ColumnVector codegen                       89 /  101        176.2           5.7       4.3X
    */

    benchmarkPT.run()
    dfPassThrough.unpersist(true)
    System.gc()
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

  def main(args: Array[String]): Unit = {
    longSumBenchmark(1024 * 1024 * 15)
    doubleSumBenchmark(1024 * 1024 * 15)
    floatSumBenchmark(1024 * 1024 * 30)
    intSumBenchmark(1024 * 1024 * 30)
  }
}
