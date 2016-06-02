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
  private val widthsToTest = Seq(1, 10, 100, 1000, 2500)
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
      benchmark.addCase(s"$width select expressions") { iter =>
        sparkSession.range(1).toDF.selectExpr(selectExpr: _*)
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

  test("many column field read and write") {
    val benchmark = new Benchmark("many column field r/w", scaleFactor)
    for (width <- widthsToTest) {
      // normalize by width to keep constant data size
      val numRows = scaleFactor / width
      val selectExpr = (1 to width).map(i => s"id as a_$i")
      val df = sparkSession.range(numRows).toDF.selectExpr(selectExpr: _*).cache()
      df.count()  // force caching
      benchmark.addCase(s"$width cols x $numRows rows (read)") { iter =>
        df.selectExpr("sum(a_1)").collect()
      }
      benchmark.addCase(s"$width cols x $numRows rows (write)") { iter =>
        df.selectExpr("*", "hash(a_1) as f").selectExpr("sum(a_1)", "sum(f)").collect()
      }
    }
    benchmark.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
    Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
    many column field r/w:                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ----------------------------------------------------------------------------------------------
    1 cols x 100000 rows (read)                   41 /   51          2.4         414.5       1.0X
    1 cols x 100000 rows (write)                  52 /   68          1.9         520.1       0.8X
    10 cols x 10000 rows (read)                   43 /   49          2.3         426.7       1.0X
    10 cols x 10000 rows (write)                  48 /   65          2.1         478.3       0.9X
    100 cols x 1000 rows (read)                   84 /   91          1.2         840.4       0.5X
    100 cols x 1000 rows (write)                 103 /  122          1.0        1032.1       0.4X
    1000 cols x 100 rows (read)                  458 /  468          0.2        4575.5       0.1X
    1000 cols x 100 rows (write)                 849 /  875          0.1        8494.0       0.0X
    2500 cols x 40 rows (read)                  1077 / 1135          0.1       10770.3       0.0X
    2500 cols x 40 rows (write)                 2251 / 2351          0.0       22510.8       0.0X
    */
  }

  test("wide struct field read and write") {
    val benchmark = new Benchmark("wide struct field r/w", scaleFactor)
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
      benchmark.addCase(s"$width wide x $numRows rows (read)") { iter =>
        df.selectExpr("sum(value_1)").collect()
      }
      benchmark.addCase(s"$width wide x $numRows rows (write)") { iter =>
        df.selectExpr("*", "hash(value_1) as f").selectExpr(s"sum(value_1)", "sum(f)").collect()
      }
    }
    benchmark.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
    Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
    wide struct field r/w:                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ----------------------------------------------------------------------------------------------
    1 wide x 100000 rows (read)                   31 /   38          3.2         309.3       1.0X
    1 wide x 100000 rows (write)                  55 /   61          1.8         551.6       0.6X
    10 wide x 10000 rows (read)                   51 /   53          2.0         506.6       0.6X
    10 wide x 10000 rows (write)                  60 /   71          1.7         596.5       0.5X
    100 wide x 1000 rows (read)                   74 /   86          1.4         737.4       0.4X
    100 wide x 1000 rows (write)                 133 /  154          0.8        1332.1       0.2X
    1000 wide x 100 rows (read)                  377 /  441          0.3        3767.3       0.1X
    1000 wide x 100 rows (write)                 688 /  828          0.1        6880.8       0.0X
    2500 wide x 40 rows (read)                   974 / 1044          0.1        9738.1       0.0X
    2500 wide x 40 rows (write)                 2081 / 2256          0.0       20805.8       0.0X
    */
  }

  test("deeply nested struct field read and write") {
    val benchmark = new Benchmark("deeply nested struct field r/w", scaleFactor)
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
      benchmark.addCase(s"$depth deep x $numRows rows (read)") { iter =>
        df.selectExpr(s"sum($selector)").collect()
      }
      benchmark.addCase(s"$depth deep x $numRows rows (write)") { iter =>
        df.selectExpr("*", s"$selector as f").selectExpr(s"sum($selector)", "sum(f)").collect()
      }
    }
    benchmark.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
    Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
    deeply nested struct field r/w:        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ----------------------------------------------------------------------------------------------
    1 deep x 100000 rows (read)                   38 /   49          2.6         382.2       1.0X
    1 deep x 100000 rows (write)                  38 /   45          2.6         382.1       1.0X
    10 deep x 10000 rows (read)                   48 /   61          2.1         483.8       0.8X
    10 deep x 10000 rows (write)                  93 /  104          1.1         931.2       0.4X
    100 deep x 1000 rows (read)                  151 /  162          0.7        1513.2       0.3X
    100 deep x 1000 rows (write)                 903 / 1004          0.1        9030.5       0.0X
    250 deep x 400 rows (read)                   653 /  735          0.2        6526.8       0.1X
    250 deep x 400 rows (write)                 8749 / 9217          0.0       87490.4       0.0X
    */
  }

  test("bushy struct field read and write") {
    val benchmark = new Benchmark("bushy struct field r/w", scaleFactor)
    for (width <- Seq(1, 10, 100, 500)) {
      val numRows = scaleFactor / width
      var numNodes = 1
      var datum: String = "{\"value\": 1}"
      var selector: String = "value"
      var depth = 1
      while (numNodes < width) {
        numNodes *= 2
        datum = s"""{"left_$depth": $datum, "right_$depth": $datum}"""
        selector = s"left_$depth." + selector
        depth += 1
      }
      // TODO(ekl) seems like the json parsing is actually the majority of the time, perhaps
      // we should benchmark that too separately.
      val df = sparkSession.read.json(sparkSession.range(numRows).map(_ => datum).rdd).cache()
      df.count()  // force caching
      benchmark.addCase(s"$numNodes nodes x $depth deep x $numRows rows (read)") { iter =>
        df.selectExpr(s"sum($selector)").collect()
      }
      benchmark.addCase(s"$numNodes nodes x $depth deep x $numRows rows (write)") { iter =>
        df.selectExpr("*", s"$selector as f").selectExpr(s"sum($selector)", "sum(f)").collect()
      }
    }
    benchmark.run()

    /*
    OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
    Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
    bushy struct field r/w:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ----------------------------------------------------------------------------------------------
    1 nodes x 1 deep x 100000 rows (read)         57 /   69          1.7         571.7       1.0X
    1 nodes x 1 deep x 100000 rows (write)        61 /   75          1.6         614.4       0.9X
    16 nodes x 5 deep x 10000 rows (read)         44 /   55          2.3         439.0       1.3X
    16 nodes x 5 deep x 10000 rows (write)       100 /  126          1.0         999.5       0.6X
    128 nodes x 8 deep x 1000 rows (read)         53 /   61          1.9         532.9       1.1X
    128 nodes x 8 deep x 1000 rows (write)       530 /  656          0.2        5295.6       0.1X
    512 nodes x 10 deep x 200 rows (read)         98 /  134          1.0         979.2       0.6X
    512 nodes x 10 deep x 200 rows (write)      6698 / 7124          0.0       66977.0       0.0X
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
      benchmark.addCase(s"$width wide x $numRows rows") { iter =>
        df.selectExpr("sum(value[0])").collect()
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
      benchmark.addCase(s"$width wide x $numRows rows") { iter =>
        df.selectExpr("sum(_1[\"value_1\"])").collect()
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
