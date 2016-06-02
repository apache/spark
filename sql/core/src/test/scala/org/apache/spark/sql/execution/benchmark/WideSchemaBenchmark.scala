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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.functions._
import org.apache.spark.util.Benchmark

/**
 * Benchmark for performance with very wide and nested DataFrames.
 * To run this:
 *  build/sbt "sql/test-only *WideSchemaBenchmark"
 */
class WideSchemaBenchmark extends SparkFunSuite {
  private val scaleFactor = 100000
  private val widthsToTest = Seq(1, 10, 100, 1000, 5000)  // crashes on higher values
  private val depthsToTest = Seq(1, 10, 100, 250)
  assert(scaleFactor > widthsToTest.max)

  private lazy val sparkSession = SparkSession.builder
    .master("local[1]")
    .appName("microbenchmark")
    .getOrCreate()

  import sparkSession.implicits._

  ignore("parsing large select expressions") {
    val benchmark = new Benchmark("parsing large select", 1)
    for (width <- widthsToTest) {
      val selectExpr = (1 to width).map(i => s"id as a_$i")
      benchmark.addTimerCase(s"$width select expressions") { timer =>
        timer.startTiming()
        sparkSession.range(1).toDF.selectExpr(selectExpr: _*)
        timer.stopTiming()
      }
    }
    benchmark.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
    Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
    parsing large select                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ----------------------------------------------------------------------------------------------
    1 select expressions                          15 /   19          0.0    15464923.0       1.0X
    10 select expressions                         26 /   30          0.0    26131438.0       0.6X
    100 select expressions                        46 /   58          0.0    45719540.0       0.3X
    1000 select expressions                      285 /  328          0.0   285407972.0       0.1X
    5000 select expressions                     1482 / 1645          0.0  1482298845.0       0.0X
    */
  }

  ignore("many column field read") {
    val benchmark = new Benchmark("many column field read", scaleFactor)
    for (width <- widthsToTest) {
      // normalize by width to keep constant data size
      val numRows = scaleFactor / width
      val selectExpr = (1 to width).map(i => s"id as a_$i")
      val df = sparkSession.range(numRows).toDF.selectExpr(selectExpr: _*).cache()
      df.count()  // force caching
      benchmark.addTimerCase(s"$width cols x $numRows rows") { timer =>
        timer.startTiming()
        df.selectExpr("sum(a_1)").collect()
        timer.stopTiming()
      }
    }
    benchmark.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
    Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
    many column field read:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ----------------------------------------------------------------------------------------------
    1 cols x 100000 rows                          57 /   67          1.7         572.9       1.0X
    10 cols x 10000 rows                          35 /   43          2.8         354.1       1.6X
    100 cols x 1000 rows                          49 /   67          2.0         492.4       1.2X
    1000 cols x 100 rows                         428 /  459          0.2        4281.8       0.1X
    5000 cols x 20 rows                         2170 / 2253          0.0       21695.6       0.0X
    */
  }

  ignore("wide struct field read") {
    val benchmark = new Benchmark("wide struct field read", scaleFactor)
    for (width <- widthsToTest) {
      val numRows = scaleFactor / width
      var datum: String = "{"
      for (i <- 1 to width) {
        if (i == 1) {
          datum += s""""value_$i": 1"""
        } else {
          datum += s""", "value_$i": 1"""
        }
      }
      datum += "}"
      val df = sparkSession.read.json(sparkSession.range(numRows).map(_ => datum).rdd).cache()
      df.count()  // force caching
      benchmark.addTimerCase(s"$width wide x $numRows rows") { timer =>
        timer.startTiming()
        df.selectExpr("sum(value_1)").collect()
        timer.stopTiming()
      }
    }
    benchmark.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
    Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
    wide struct field read:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ----------------------------------------------------------------------------------------------
    1 wide x 100000 rows                          33 /   41          3.0         333.9       1.0X
    10 wide x 10000 rows                          45 /   52          2.2         445.1       0.8X
    100 wide x 1000 rows                          86 /   95          1.2         856.1       0.4X
    1000 wide x 100 rows                         265 /  322          0.4        2646.4       0.1X
    5000 wide x 20 rows                         1882 / 2043          0.1       18817.4       0.0X
    */
  }

  ignore("deeply nested struct field read") {
    val benchmark = new Benchmark("deeply nested struct field read", scaleFactor)
    for (depth <- depthsToTest) {
      val numRows = scaleFactor / depth
      var datum: String = "{\"value\": 1}"
      var selector: String = "value"
      for (i <- 1 to depth) {
        datum = "{\"value\": " + datum + "}"
        selector = selector + ".value"
      }
      val df = sparkSession.read.json(sparkSession.range(numRows).map(_ => datum).rdd).cache()
      df.count()  // force caching
      benchmark.addTimerCase(s"$depth deep x $numRows rows") { timer =>
        timer.startTiming()
        df.selectExpr(s"sum($selector)").collect()
        timer.stopTiming()
      }
    }
    benchmark.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
    Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
    deeply nested struct field read:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ----------------------------------------------------------------------------------------------
    1 deep x 100000 rows                          45 /   55          2.2         450.4       1.0X
    10 deep x 10000 rows                          52 /   63          1.9         515.1       0.9X
    100 deep x 1000 rows                         100 /  119          1.0        1004.0       0.4X
    250 deep x 400 rows                          534 /  623          0.2        5342.4       0.1X
    */
  }

  //
  // The following benchmarks are for reference: the schema is not actually wide for them.
  //

  ignore("wide array field read") {
    val benchmark = new Benchmark("wide array field read", scaleFactor)
    for (width <- widthsToTest) {
      val numRows = scaleFactor / width
      var datum: String = "{\"value\": ["
      for (i <- 1 to width) {
        if (i == 1) {
          datum += "1"
        } else {
          datum += ", 1"
        }
      }
      datum += "]}"
      val df = sparkSession.read.json(sparkSession.range(numRows).map(_ => datum).rdd).cache()
      df.count()  // force caching
      benchmark.addTimerCase(s"$width wide x $numRows rows") { timer =>
        timer.startTiming()
        df.selectExpr("sum(value[0])").collect()
        timer.stopTiming()
      }
    }
    benchmark.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
    Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
    wide array field read:                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ----------------------------------------------------------------------------------------------
    1 wide x 100000 rows                          67 /   73          1.5         666.0       1.0X
    10 wide x 10000 rows                          43 /   46          2.3         425.8       1.6X
    100 wide x 1000 rows                          35 /   39          2.8         351.6       1.9X
    1000 wide x 100 rows                          34 /   36          3.0         336.4       2.0X
    5000 wide x 20 rows                           30 /   32          3.3         301.9       2.2X
    */
  }

  ignore("wide map field read") {
    val benchmark = new Benchmark("wide array field read", scaleFactor)
    for (width <- widthsToTest) {
      val numRows = scaleFactor / width
      val datum = Tuple1((1 to width).map(i => ("value_" + i -> 1)).toMap)
      val df = sparkSession.range(numRows).map(_ => datum).cache()
      df.count()  // force caching
      benchmark.addTimerCase(s"$width wide x $numRows rows") { timer =>
        timer.startTiming()
        df.selectExpr("sum(_1[\"value_1\"])").collect()
        timer.stopTiming()
      }
    }
    benchmark.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
    Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
    wide array field read:                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ----------------------------------------------------------------------------------------------
    1 wide x 100000 rows                          53 /   65          1.9         533.1       1.0X
    10 wide x 10000 rows                          40 /   51          2.5         404.5       1.3X
    100 wide x 1000 rows                          36 /   38          2.8         357.0       1.5X
    1000 wide x 100 rows                          34 /   36          3.0         338.4       1.6X
    5000 wide x 20 rows                           34 /   48          2.9         343.4       1.6X
    */
  }
}
