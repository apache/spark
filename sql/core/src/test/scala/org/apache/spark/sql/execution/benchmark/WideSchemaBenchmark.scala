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

import java.io.File

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.util.Utils

/**
 * Benchmark for performance with very wide and nested DataFrames.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/WideSchemaBenchmark-results.txt".
 * }}}
 */
object WideSchemaBenchmark extends SqlBasedBenchmark {
  private val scaleFactor = 100000
  private val widthsToTest = Seq(1, 100, 2500)
  private val depthsToTest = Seq(1, 100, 250)
  assert(scaleFactor > widthsToTest.max)

  import spark.implicits._

  private var tmpFiles: List[File] = Nil

  private def deleteTmpFiles(): Unit = tmpFiles.foreach(Utils.deleteRecursively)

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
    spark.read.parquet(tmpFile.getAbsolutePath)
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
      df.selectExpr(s"sum($selector)").noop()
    }
    benchmark.addCase(desc + " (exec in-mem)") { iter =>
      df.selectExpr("*", s"hash($selector) as f").selectExpr(s"sum($selector)", "sum(f)").noop()
    }
    val parquet = saveAsParquet(df)
    benchmark.addCase(desc + " (read parquet)") { iter =>
      parquet.selectExpr(s"sum($selector) as f").noop()
    }
    benchmark.addCase(desc + " (write parquet)") { iter =>
      saveAsParquet(df.selectExpr(s"sum($selector) as f"))
    }
  }

  def parsingLargeSelectExpressions(): Unit = {
    val benchmark = new Benchmark("parsing large select", 1, output = output)
    for (width <- widthsToTest) {
      val selectExpr = (1 to width).map(i => s"id as a_$i")
      benchmark.addCase(s"$width select expressions") { iter =>
        spark.range(1).toDF().selectExpr(selectExpr: _*)
      }
    }
    benchmark.run()
  }

  def optimizeLargeSelectExpressions(): Unit = {
    val benchmark = new Benchmark("optimize large select", 1, output = output)
    Seq(100, 1000, 10000).foreach { width =>
      val columns = (1 to width).map(i => s"id as c_$i")
      val df = spark.range(1).selectExpr(columns: _*).cache()
      df.count()  // force caching
      benchmark.addCase(s"$width columns") { iter =>
        df.withColumn("id", lit(1)).withColumn("name", lit("name")).queryExecution.optimizedPlan
      }
    }
    benchmark.run()
  }

  def manyColumnFieldReadAndWrite(): Unit = {
    val benchmark = new Benchmark("many column field r/w", scaleFactor, output = output)
    for (width <- widthsToTest) {
      // normalize by width to keep constant data size
      val numRows = scaleFactor / width
      val selectExpr = (1 to width).map(i => s"id as a_$i")
      val df = spark.range(numRows).toDF().selectExpr(selectExpr: _*).cache()
      df.count()  // force caching
      addCases(benchmark, df, s"$width cols x $numRows rows", "a_1")
    }
    benchmark.run()
  }

  def wideShallowlyNestedStructFieldReadAndWrite(): Unit = {
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
      val df = spark.read.json(spark.range(numRows).map(_ => datum)).cache()
      df.count()  // force caching
      addCases(benchmark, df, s"$width wide x $numRows rows", "a.b.c.value_1")
    }
    benchmark.run()
  }

  def deeplyNestedStructFieldReadAndWrite(): Unit = {
    val benchmark = new Benchmark("deeply nested struct field r/w", scaleFactor, output = output)
    for (depth <- depthsToTest) {
      val numRows = scaleFactor / depth
      var datum: String = "{\"value\": 1}"
      var selector: String = "value"
      for (i <- 1 to depth) {
        datum = "{\"value\": " + datum + "}"
        selector = selector + ".value"
      }
      val df = spark.read.json(spark.range(numRows).map(_ => datum)).cache()
      df.count()  // force caching
      addCases(benchmark, df, s"$depth deep x $numRows rows", selector)
    }
    benchmark.run()
  }

  def bushyStructFieldReadAndWrite(): Unit = {
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
      val df = spark.read.json(spark.range(numRows).map(_ => datum)).cache()
      df.count()  // force caching
      addCases(benchmark, df, s"$numNodes x $depth deep x $numRows rows", selector)
    }
    benchmark.run()
  }


  def wideArrayFieldReadAndWrite(): Unit = {
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
      val df = spark.read.json(spark.range(numRows).map(_ => datum)).cache()
      df.count()  // force caching
      addCases(benchmark, df, s"$width wide x $numRows rows", "value[0]")
    }
    benchmark.run()
  }

  def wideMapFieldReadAndWrite(): Unit = {
    val benchmark = new Benchmark("wide map field r/w", scaleFactor, output = output)
    for (width <- widthsToTest) {
      val numRows = scaleFactor / width
      val datum = Tuple1((1 to width).map(i => ("value_" + i -> 1)).toMap)
      val df = spark.range(numRows).map(_ => datum).toDF().cache()
      df.count()  // force caching
      addCases(benchmark, df, s"$width wide x $numRows rows", "_1[\"value_1\"]")
    }
    benchmark.run()
  }

  def runBenchmarkWithDeleteTmpFiles(benchmarkName: String)(func: => Any): Unit = {
    runBenchmark(benchmarkName) {
      func
    }
    deleteTmpFiles()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmarkWithDeleteTmpFiles("parsing large select expressions") {
      parsingLargeSelectExpressions()
    }

    runBenchmarkWithDeleteTmpFiles("optimize large select expressions") {
      optimizeLargeSelectExpressions()
    }

    runBenchmarkWithDeleteTmpFiles("many column field read and write") {
      manyColumnFieldReadAndWrite()
    }

    runBenchmarkWithDeleteTmpFiles("wide shallowly nested struct field read and write") {
      wideShallowlyNestedStructFieldReadAndWrite()
    }

    runBenchmarkWithDeleteTmpFiles("deeply nested struct field read and write") {
      deeplyNestedStructFieldReadAndWrite()
    }

    runBenchmarkWithDeleteTmpFiles("bushy struct field read and write") {
      bushyStructFieldReadAndWrite()
    }

    runBenchmarkWithDeleteTmpFiles("wide array field read and write") {
      wideArrayFieldReadAndWrite()
    }

    runBenchmarkWithDeleteTmpFiles("wide map field read and write") {
      wideMapFieldReadAndWrite()
    }
  }
}
