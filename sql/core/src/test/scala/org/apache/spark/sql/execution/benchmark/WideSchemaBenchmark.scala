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

import org.apache.spark.util.{Benchmark, BenchmarkBase => FileBenchmarkBase, Utils}

/**
 * Benchmark for performance with very wide and nested DataFrames.
 * To run this benchmark:
 * 1. without sbt: bin/spark-submit --class <this class> <spark sql test jar>
 * 2. build/sbt "sql/test:runMain <this class>"
 * 3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *    Results will be written to "benchmarks/WideSchemaBenchmark-results.txt".
 */
object WideSchemaBenchmark extends FileBenchmarkBase {
  private val scaleFactor = 100000
  private val widthsToTest = Seq(1, 100, 2500)
  private val depthsToTest = Seq(1, 100, 250)
  assert(scaleFactor > widthsToTest.max)

  private lazy val sparkSession = SparkSession.builder
    .master("local[1]")
    .appName("microbenchmark")
    .getOrCreate()

  import sparkSession.implicits._

  def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir().getCanonicalFile
    try f(dir) finally Utils.deleteRecursively(dir)
  }

  /**
   * Writes the given DataFrame to parquet at a temporary location, and returns a DataFrame
   * backed by the written parquet files.
   */
  private def saveAsParquet(dir: File, df: DataFrame): DataFrame = {
    df.write.mode(SaveMode.Overwrite).parquet(dir.getAbsolutePath)
    assert(dir.isDirectory())
    sparkSession.read.parquet(dir.getAbsolutePath)
  }

  /**
   * Adds standard set of cases to a benchmark given a dataframe and field to select.
   */
  private def addCases(
      benchmark: Benchmark,
      dir: File,
      df: DataFrame,
      desc: String,
      selector: String): Unit = {
    val readDir = File.createTempFile("read", "", dir)
    val writeDir = File.createTempFile("write", "", dir)

    benchmark.addCase(desc + " (read in-mem)") { iter =>
      df.selectExpr(s"sum($selector)").collect()
    }
    benchmark.addCase(desc + " (exec in-mem)") { iter =>
      df.selectExpr("*", s"hash($selector) as f").selectExpr(s"sum($selector)", "sum(f)").collect()
    }
    benchmark.addCase(desc + " (read parquet)") { iter =>
      saveAsParquet(readDir, df).selectExpr(s"sum($selector) as f").collect()
    }
    benchmark.addCase(desc + " (write parquet)") { iter =>
      saveAsParquet(writeDir, df.selectExpr(s"sum($selector) as f"))
    }
  }

  override def benchmark(): Unit = {
    runBenchmark("parsing large select expressions") {
      withTempDir { dir =>
        val benchmark = new Benchmark("parsing large select", 1, output = output)
        for (width <- widthsToTest) {
          val selectExpr = (1 to width).map(i => s"id as a_$i")
          benchmark.addCase(s"$width select expressions") { iter =>
            sparkSession.range(1).toDF.selectExpr(selectExpr: _*)
          }
        }
        benchmark.run()
      }
    }

    runBenchmark("many column field read and write") {
      withTempDir { dir =>
        val benchmark = new Benchmark("many column field r/w", scaleFactor, output = output)
        for (width <- widthsToTest) {
          // normalize by width to keep constant data size
          val numRows = scaleFactor / width
          val selectExpr = (1 to width).map(i => s"id as a_$i")
          val df = sparkSession.range(numRows).toDF.selectExpr(selectExpr: _*).cache()
          df.count() // force caching
          addCases(benchmark, dir, df, s"$width cols x $numRows rows", "a_1")
        }
        benchmark.run()
      }
    }

    runBenchmark("wide shallowly nested struct field read and write") {
      withTempDir { dir =>
        val benchmark = new Benchmark(
          "wide shallowly nested struct field r/w", scaleFactor, output = output)
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
          val df = sparkSession.read.json(sparkSession.range(numRows).map(_ => datum)).cache()
          df.count() // force caching
          addCases(benchmark, dir, df, s"$width wide x $numRows rows", "a.b.c.value_1")
        }
        benchmark.run()
      }
    }

    runBenchmark("deeply nested struct field read and write") {
      withTempDir { dir =>
        val benchmark =
          new Benchmark("deeply nested struct field r/w", scaleFactor, output = output)
        for (depth <- depthsToTest) {
          val numRows = scaleFactor / depth
          var datum: String = "{\"value\": 1}"
          var selector: String = "value"
          for (i <- 1 to depth) {
            datum = "{\"value\": " + datum + "}"
            selector = selector + ".value"
          }
          val df = sparkSession.read.json(sparkSession.range(numRows).map(_ => datum)).cache()
          df.count() // force caching
          addCases(benchmark, dir, df, s"$depth deep x $numRows rows", selector)
        }
        benchmark.run()
      }
    }

    runBenchmark("bushy struct field read and write") {
      withTempDir { dir =>
        val benchmark = new Benchmark("bushy struct field r/w", scaleFactor, output = output)
        for (width <- Seq(1, 100, 1000)) {
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
          val df = sparkSession.read.json(sparkSession.range(numRows).map(_ => datum)).cache()
          df.count() // force caching
          addCases(benchmark, dir, df, s"$numNodes x $depth deep x $numRows rows", selector)
        }
        benchmark.run()
      }
    }

    runBenchmark("wide array field read and write") {
      withTempDir { dir =>
        val benchmark = new Benchmark("wide array field r/w", scaleFactor, output = output)
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
          val df = sparkSession.read.json(sparkSession.range(numRows).map(_ => datum)).cache()
          df.count() // force caching
          addCases(benchmark, dir, df, s"$width wide x $numRows rows", "value[0]")
        }
        benchmark.run()
      }
    }

    runBenchmark("wide map field read and write") {
      withTempDir { dir =>
        val benchmark = new Benchmark("wide map field r/w", scaleFactor, output = output)
        for (width <- widthsToTest) {
          val numRows = scaleFactor / width
          val datum = Tuple1((1 to width).map(i => ("value_" + i -> 1)).toMap)
          val df = sparkSession.range(numRows).map(_ => datum).toDF.cache()
          df.count() // force caching
          addCases(benchmark, dir, df, s"$width wide x $numRows rows", "_1[\"value_1\"]")
        }
        benchmark.run()
      }
    }
  }
}
