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

import java.io.File

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.functions._
import org.apache.spark.util.{Benchmark, Utils}

/**
 * Benchmark for performance with very wide and nested DataFrames.
 * To run this:
 *  build/sbt "sql/test-only *WideSchemaBenchmark"
 */
class WideSchemaBenchmark extends SparkFunSuite with BeforeAndAfterEach {
  private val scaleFactor = 100000
  private val widthsToTest = Seq(1, 10, 100, 1000, 2500)
  private val depthsToTest = Seq(1, 10, 100, 250)
  assert(scaleFactor > widthsToTest.max)

  private lazy val sparkSession = SparkSession.builder
    .master("local[1]")
    .appName("microbenchmark")
    .getOrCreate()

  import sparkSession.implicits._

  private var tmpFiles: List[File] = Nil

  override def afterEach() {
    try {
      for (tmpFile <- tmpFiles) {
        Utils.deleteRecursively(tmpFile)
      }
    } finally {
      tmpFiles = Nil
      super.afterEach()
    }
  }

  /**
   * Writes the given DataFrame to parquet at a temporary location, and returns a DataFrame
   * backed by the written parquet files.
   */
  private def saveAsParquet(df: DataFrame): DataFrame = {
    val tmpFile = File.createTempFile("WideSchemaBenchmark", "tmp")
    tmpFiles ::= tmpFile
    tmpFile.delete()
    df.write.parquet(tmpFile.getAbsolutePath)
    assert(tmpFile.isDirectory())
    sparkSession.read.parquet(tmpFile.getAbsolutePath)
  }

  /**
   * Adds standard set of cases to a benchmark given a dataframe and field to select.
   */
  private def addCases(
      benchmark: Benchmark,
      df: DataFrame,
      desc: String,
      selector: String): Unit = {
    benchmark.addCase(desc + " (read in-mem)") { iter =>
      df.selectExpr(s"sum($selector)").collect()
    }
    benchmark.addCase(desc + " (write in-mem)") { iter =>
      df.selectExpr("*", s"hash($selector) as f").selectExpr(s"sum($selector)", "sum(f)").collect()
    }
    val parquet = saveAsParquet(df)
    benchmark.addCase(desc + " (read parquet)") { iter =>
      parquet.selectExpr(s"sum($selector) as f").collect()
    }
    benchmark.addCase(desc + " (write parquet)") { iter =>
      saveAsParquet(df.selectExpr(s"sum($selector) as f"))
    }
  }

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
parsing large select:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
------------------------------------------------------------------------------------------------
1 select expressions                            22 /   25          0.0    22053737.0       1.0X
10 select expressions                            8 /   13          0.0     8288520.0       2.7X
100 select expressions                          29 /   32          0.0    29481040.0       0.7X
1000 select expressions                        268 /  276          0.0   268183159.0       0.1X
2500 select expressions                        683 /  691          0.0   683422241.0       0.0X
*/
  }

  ignore("many column field read and write") {
    val benchmark = new Benchmark("many column field r/w", scaleFactor)
    for (width <- widthsToTest) {
      // normalize by width to keep constant data size
      val numRows = scaleFactor / width
      val selectExpr = (1 to width).map(i => s"id as a_$i")
      val df = sparkSession.range(numRows).toDF.selectExpr(selectExpr: _*).cache()
      df.count()  // force caching
      addCases(benchmark, df, s"$width cols x $numRows rows", "a_1")
    }
    benchmark.run()

/*
OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
many column field r/w:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
------------------------------------------------------------------------------------------------
1 cols x 100000 rows (read in-mem)              26 /   33          3.8         262.9       1.0X
1 cols x 100000 rows (write in-mem)             40 /   51          2.5         401.6       0.7X
1 cols x 100000 rows (read parquet)             37 /   57          2.7         374.3       0.7X
1 cols x 100000 rows (write parquet)           105 /  157          0.9        1054.9       0.2X
10 cols x 10000 rows (read in-mem)              26 /   39          3.8         260.5       1.0X
10 cols x 10000 rows (write in-mem)             37 /   44          2.7         367.4       0.7X
10 cols x 10000 rows (read parquet)             31 /   39          3.3         305.1       0.9X
10 cols x 10000 rows (write parquet)            86 /  137          1.2         860.2       0.3X
100 cols x 1000 rows (read in-mem)              40 /   64          2.5         401.2       0.7X
100 cols x 1000 rows (write in-mem)            112 /  139          0.9        1118.3       0.2X
100 cols x 1000 rows (read parquet)             35 /   52          2.9         349.8       0.8X
100 cols x 1000 rows (write parquet)           150 /  182          0.7        1497.1       0.2X
1000 cols x 100 rows (read in-mem)             304 /  362          0.3        3043.6       0.1X
1000 cols x 100 rows (write in-mem)            647 /  729          0.2        6467.8       0.0X
1000 cols x 100 rows (read parquet)            194 /  235          0.5        1937.7       0.1X
1000 cols x 100 rows (write parquet)           511 /  521          0.2        5105.0       0.1X
2500 cols x 40 rows (read in-mem)              915 /  924          0.1        9148.2       0.0X
2500 cols x 40 rows (write in-mem)            1856 / 1933          0.1       18558.1       0.0X
2500 cols x 40 rows (read parquet)             802 /  881          0.1        8019.3       0.0X
2500 cols x 40 rows (write parquet)           1268 / 1291          0.1       12681.6       0.0X
*/
  }

  ignore("wide shallowly nested struct field read and write") {
    val benchmark = new Benchmark(
      "wide shallowly nested struct field r/w", scaleFactor)
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
      datum = s"""{"a": {"b": {"c": $datum, "d": $datum}, "e": $datum}}"""
      val df = sparkSession.read.json(sparkSession.range(numRows).map(_ => datum).rdd).cache()
      df.count()  // force caching
      addCases(benchmark, df, s"$width wide x $numRows rows", "a.b.c.value_1")
    }
    benchmark.run()

/*
Java HotSpot(TM) 64-Bit Server VM 1.7.0_80-b15 on Linux 4.2.0-36-generic
Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
wide shallowly nested struct field r/w:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
------------------------------------------------------------------------------------------------
1 wide x 100000 rows (read in-mem)             100 /  125          1.0         997.7       1.0X
1 wide x 100000 rows (write in-mem)            130 /  147          0.8        1302.9       0.8X
1 wide x 100000 rows (read parquet)            195 /  228          0.5        1951.4       0.5X
1 wide x 100000 rows (write parquet)           248 /  259          0.4        2479.7       0.4X
10 wide x 10000 rows (read in-mem)              76 /   89          1.3         757.2       1.3X
10 wide x 10000 rows (write in-mem)             90 /  116          1.1         900.0       1.1X
10 wide x 10000 rows (read parquet)             90 /  135          1.1         903.9       1.1X
10 wide x 10000 rows (write parquet)           222 /  240          0.4        2222.8       0.4X
100 wide x 1000 rows (read in-mem)              71 /   91          1.4         710.8       1.4X
100 wide x 1000 rows (write in-mem)            252 /  324          0.4        2522.4       0.4X
100 wide x 1000 rows (read parquet)            310 /  329          0.3        3095.9       0.3X
100 wide x 1000 rows (write parquet)           253 /  267          0.4        2525.7       0.4X
1000 wide x 100 rows (read in-mem)             144 /  160          0.7        1439.5       0.7X
1000 wide x 100 rows (write in-mem)           2055 / 2326          0.0       20553.9       0.0X
1000 wide x 100 rows (read parquet)            750 /  925          0.1        7496.8       0.1X
1000 wide x 100 rows (write parquet)           235 /  317          0.4        2347.5       0.4X
2500 wide x 40 rows (read in-mem)              219 /  227          0.5        2190.9       0.5X
2500 wide x 40 rows (write in-mem)            5177 / 5423          0.0       51773.2       0.0X
2500 wide x 40 rows (read parquet)            1642 / 1714          0.1       16417.7       0.1X
2500 wide x 40 rows (write parquet)            357 /  381          0.3        3568.2       0.3X
*/
  }

  ignore("wide struct field read and write") {
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
      addCases(benchmark, df, s"$width wide x $numRows rows", "value_1")
    }
    benchmark.run()

/*
OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
wide struct field r/w:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
------------------------------------------------------------------------------------------------
1 wide x 100000 rows (read in-mem)              22 /   37          4.6         216.8       1.0X
1 wide x 100000 rows (write in-mem)             37 /   54          2.7         365.6       0.6X
1 wide x 100000 rows (read parquet)             27 /   44          3.6         274.7       0.8X
1 wide x 100000 rows (write parquet)           155 /  183          0.6        1546.3       0.1X
10 wide x 10000 rows (read in-mem)              27 /   40          3.7         272.1       0.8X
10 wide x 10000 rows (write in-mem)             32 /   44          3.2         315.7       0.7X
10 wide x 10000 rows (read parquet)             31 /   44          3.2         309.8       0.7X
10 wide x 10000 rows (write parquet)           151 /  169          0.7        1509.3       0.1X
100 wide x 1000 rows (read in-mem)              37 /   62          2.7         374.4       0.6X
100 wide x 1000 rows (write in-mem)             81 /   96          1.2         805.6       0.3X
100 wide x 1000 rows (read parquet)             31 /   44          3.3         307.3       0.7X
100 wide x 1000 rows (write parquet)           174 /  209          0.6        1745.0       0.1X
1000 wide x 100 rows (read in-mem)             308 /  339          0.3        3082.4       0.1X
1000 wide x 100 rows (write in-mem)            672 /  696          0.1        6717.7       0.0X
1000 wide x 100 rows (read parquet)            182 /  228          0.5        1821.2       0.1X
1000 wide x 100 rows (write parquet)           484 /  497          0.2        4841.2       0.0X
2500 wide x 40 rows (read in-mem)              727 /  786          0.1        7268.4       0.0X
2500 wide x 40 rows (write in-mem)            1734 / 1782          0.1       17341.5       0.0X
2500 wide x 40 rows (read parquet)             882 /  935          0.1        8816.8       0.0X
2500 wide x 40 rows (write parquet)            935 /  982          0.1        9351.9       0.0X
*/
  }

  ignore("deeply nested struct field read and write") {
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
      addCases(benchmark, df, s"$depth deep x $numRows rows", selector)
    }
    benchmark.run()

/*
OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
deeply nested struct field r/w:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
------------------------------------------------------------------------------------------------
1 deep x 100000 rows (read in-mem)              24 /   39          4.2         239.0       1.0X
1 deep x 100000 rows (write in-mem)             34 /   47          3.0         335.1       0.7X
1 deep x 100000 rows (read parquet)             45 /   51          2.2         446.1       0.5X
1 deep x 100000 rows (write parquet)            86 /  108          1.2         859.4       0.3X
10 deep x 10000 rows (read in-mem)              28 /   38          3.6         275.1       0.9X
10 deep x 10000 rows (write in-mem)             43 /   64          2.3         427.1       0.6X
10 deep x 10000 rows (read parquet)             44 /   59          2.3         438.1       0.5X
10 deep x 10000 rows (write parquet)            85 /  110          1.2         853.6       0.3X
100 deep x 1000 rows (read in-mem)              79 /  100          1.3         785.5       0.3X
100 deep x 1000 rows (write in-mem)            776 /  800          0.1        7760.3       0.0X
100 deep x 1000 rows (read parquet)           3302 / 3394          0.0       33021.2       0.0X
100 deep x 1000 rows (write parquet)           226 /  243          0.4        2259.0       0.1X
250 deep x 400 rows (read in-mem)              610 /  639          0.2        6104.0       0.0X
250 deep x 400 rows (write in-mem)            8526 / 8531          0.0       85256.9       0.0X
250 deep x 400 rows (read parquet)          54968 / 55069          0.0      549681.4       0.0X
250 deep x 400 rows (write parquet)            714 /  718          0.1        7143.0       0.0X
*/
  }

  ignore("bushy struct field read and write") {
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
      addCases(benchmark, df, s"$numNodes x $depth deep x $numRows rows", selector)
    }
    benchmark.run()

/*
OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
bushy struct field r/w:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
------------------------------------------------------------------------------------------------
1 x 1 deep x 100000 rows (read in-mem)         21 /   27          4.7         212.6       1.0X
1 x 1 deep x 100000 rows (write in-mem)        27 /   38          3.8         265.8       0.8X
1 x 1 deep x 100000 rows (read parquet)        26 /   32          3.9         259.1       0.8X
1 x 1 deep x 100000 rows (write parquet)      150 /  169          0.7        1499.5       0.1X
16 x 5 deep x 10000 rows (read in-mem)         26 /   45          3.9         258.7       0.8X
16 x 5 deep x 10000 rows (write in-mem)        54 /   58          1.9         535.1       0.4X
16 x 5 deep x 10000 rows (read parquet)        60 /   84          1.7         595.8       0.4X
16 x 5 deep x 10000 rows (write parquet)      179 /  184          0.6        1787.5       0.1X
128 x 8 deep x 1000 rows (read in-mem)         26 /   40          3.8         261.4       0.8X
128 x 8 deep x 1000 rows (write in-mem)       592 /  592          0.2        5915.3       0.0X
128 x 8 deep x 1000 rows (read parquet)       203 /  251          0.5        2031.8       0.1X
128 x 8 deep x 1000 rows (write parquet)      105 /  131          1.0        1045.2       0.2X
512 x 10 deep x 200 rows (read in-mem)        101 /  125          1.0        1007.4       0.2X
512 x 10 deep x 200 rows (write in-mem)      6778 / 6943          0.0       67781.1       0.0X
512 x 10 deep x 200 rows (read parquet)       958 / 1071          0.1        9584.9       0.0X
512 x 10 deep x 200 rows (write parquet)      173 /  207          0.6        1726.1       0.1X
*/
  }

  ignore("wide array field read and write") {
    val benchmark = new Benchmark("wide array field r/w", scaleFactor)
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
      addCases(benchmark, df, s"$width wide x $numRows rows", "value[0]")
    }
    benchmark.run()

/*
OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
wide array field r/w:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
------------------------------------------------------------------------------------------------
1 wide x 100000 rows (read in-mem)              27 /   45          3.7         268.0       1.0X
1 wide x 100000 rows (write in-mem)             37 /   52          2.7         368.3       0.7X
1 wide x 100000 rows (read parquet)             52 /   65          1.9         524.9       0.5X
1 wide x 100000 rows (write parquet)           102 /  139          1.0        1016.7       0.3X
10 wide x 10000 rows (read in-mem)              20 /   26          5.0         201.7       1.3X
10 wide x 10000 rows (write in-mem)             26 /   35          3.8         259.8       1.0X
10 wide x 10000 rows (read parquet)             39 /   59          2.5         393.8       0.7X
10 wide x 10000 rows (write parquet)           120 /  143          0.8        1201.4       0.2X
100 wide x 1000 rows (read in-mem)              24 /   31          4.2         240.1       1.1X
100 wide x 1000 rows (write in-mem)             26 /   35          3.8         264.1       1.0X
100 wide x 1000 rows (read parquet)             30 /   47          3.4         296.8       0.9X
100 wide x 1000 rows (write parquet)           109 /  147          0.9        1094.8       0.2X
1000 wide x 100 rows (read in-mem)              20 /   38          5.0         200.6       1.3X
1000 wide x 100 rows (write in-mem)             24 /   32          4.1         242.3       1.1X
1000 wide x 100 rows (read parquet)             47 /   55          2.1         470.1       0.6X
1000 wide x 100 rows (write parquet)           146 /  164          0.7        1465.0       0.2X
2500 wide x 40 rows (read in-mem)               20 /   28          5.1         196.1       1.4X
2500 wide x 40 rows (write in-mem)              25 /   27          4.0         249.3       1.1X
2500 wide x 40 rows (read parquet)              33 /   48          3.0         332.0       0.8X
2500 wide x 40 rows (write parquet)            149 /  176          0.7        1489.3       0.2X
*/
  }

  ignore("wide map field read and write") {
    val benchmark = new Benchmark("wide map field r/w", scaleFactor)
    for (width <- widthsToTest) {
      val numRows = scaleFactor / width
      val datum = Tuple1((1 to width).map(i => ("value_" + i -> 1)).toMap)
      val df = sparkSession.range(numRows).map(_ => datum).toDF.cache()
      df.count()  // force caching
      addCases(benchmark, df, s"$width wide x $numRows rows", "_1[\"value_1\"]")
    }
    benchmark.run()

/*
OpenJDK 64-Bit Server VM 1.8.0_66-internal-b17 on Linux 4.2.0-36-generic
Intel(R) Xeon(R) CPU E5-1650 v3 @ 3.50GHz
wide map field r/w:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
------------------------------------------------------------------------------------------------
1 wide x 100000 rows (read in-mem)              27 /   42          3.7         270.9       1.0X
1 wide x 100000 rows (write in-mem)             40 /   63          2.5         403.4       0.7X
1 wide x 100000 rows (read parquet)             71 /  114          1.4         705.8       0.4X
1 wide x 100000 rows (write parquet)           169 /  184          0.6        1689.7       0.2X
10 wide x 10000 rows (read in-mem)              22 /   35          4.6         216.6       1.3X
10 wide x 10000 rows (write in-mem)             29 /   34          3.5         285.6       0.9X
10 wide x 10000 rows (read parquet)             61 /   81          1.6         610.3       0.4X
10 wide x 10000 rows (write parquet)           150 /  172          0.7        1504.7       0.2X
100 wide x 1000 rows (read in-mem)              21 /   29          4.8         207.9       1.3X
100 wide x 1000 rows (write in-mem)             30 /   57          3.3         304.9       0.9X
100 wide x 1000 rows (read parquet)             36 /   61          2.8         356.7       0.8X
100 wide x 1000 rows (write parquet)           108 /  136          0.9        1075.7       0.3X
1000 wide x 100 rows (read in-mem)              22 /   31          4.5         223.0       1.2X
1000 wide x 100 rows (write in-mem)             33 /   41          3.0         332.0       0.8X
1000 wide x 100 rows (read parquet)             49 /   66          2.0         493.6       0.5X
1000 wide x 100 rows (write parquet)           127 /  139          0.8        1265.9       0.2X
2500 wide x 40 rows (read in-mem)               23 /   34          4.4         226.0       1.2X
2500 wide x 40 rows (write in-mem)              33 /   42          3.1         326.6       0.8X
2500 wide x 40 rows (read parquet)              36 /   48          2.8         359.2       0.8X
2500 wide x 40 rows (write parquet)            155 /  168          0.6        1549.2       0.2X
*/
  }
}
