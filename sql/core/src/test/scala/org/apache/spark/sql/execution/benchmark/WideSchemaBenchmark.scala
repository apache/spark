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

import java.io.{File, FileOutputStream, OutputStream}

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.functions._
import org.apache.spark.util.{Benchmark, Utils}

/**
 * Benchmark for performance with very wide and nested DataFrames.
 * To run this:
 *  build/sbt "sql/test-only *WideSchemaBenchmark"
 *
 * Results will be written to "sql/core/benchmarks/WideSchemaBenchmark-results.txt".
 */
class WideSchemaBenchmark extends SparkFunSuite with BeforeAndAfterEach {
  private val scaleFactor = 100000
  private val widthsToTest = Seq(1, 100, 2500)
  private val depthsToTest = Seq(1, 100, 250)
  assert(scaleFactor > widthsToTest.max)

  private lazy val sparkSession = SparkSession.builder
    .master("local[1]")
    .appName("microbenchmark")
    .getOrCreate()

  import sparkSession.implicits._

  private var tmpFiles: List[File] = Nil
  private var out: OutputStream = null

  override def beforeAll() {
    super.beforeAll()
    out = new FileOutputStream(new File("benchmarks/WideSchemaBenchmark-results.txt"))
  }

  override def afterAll() {
    super.afterAll()
    out.close()
  }

  override def afterEach() {
    super.afterEach()
    for (tmpFile <- tmpFiles) {
      Utils.deleteRecursively(tmpFile)
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
    benchmark.addCase(desc + " (exec in-mem)") { iter =>
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
    val benchmark = new Benchmark("parsing large select", 1, output = Some(out))
    for (width <- widthsToTest) {
      val selectExpr = (1 to width).map(i => s"id as a_$i")
      benchmark.addCase(s"$width select expressions") { iter =>
        sparkSession.range(1).toDF.selectExpr(selectExpr: _*)
      }
    }
    benchmark.run()
  }

  ignore("many column field read and write") {
    val benchmark = new Benchmark("many column field r/w", scaleFactor, output = Some(out))
    for (width <- widthsToTest) {
      // normalize by width to keep constant data size
      val numRows = scaleFactor / width
      val selectExpr = (1 to width).map(i => s"id as a_$i")
      val df = sparkSession.range(numRows).toDF.selectExpr(selectExpr: _*).cache()
      df.count()  // force caching
      addCases(benchmark, df, s"$width cols x $numRows rows", "a_1")
    }
    benchmark.run()
  }

  ignore("wide shallowly nested struct field read and write") {
    val benchmark = new Benchmark(
      "wide shallowly nested struct field r/w", scaleFactor, output = Some(out))
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
  }

  ignore("deeply nested struct field read and write") {
    val benchmark = new Benchmark("deeply nested struct field r/w", scaleFactor, output = Some(out))
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
  }

  ignore("bushy struct field read and write") {
    val benchmark = new Benchmark("bushy struct field r/w", scaleFactor, output = Some(out))
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
      val df = sparkSession.read.json(sparkSession.range(numRows).map(_ => datum).rdd).cache()
      df.count()  // force caching
      addCases(benchmark, df, s"$numNodes x $depth deep x $numRows rows", selector)
    }
    benchmark.run()
  }

  ignore("wide array field read and write") {
    val benchmark = new Benchmark("wide array field r/w", scaleFactor, output = Some(out))
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
  }

  ignore("wide map field read and write") {
    val benchmark = new Benchmark("wide map field r/w", scaleFactor, output = Some(out))
    for (width <- widthsToTest) {
      val numRows = scaleFactor / width
      val datum = Tuple1((1 to width).map(i => ("value_" + i -> 1)).toMap)
      val df = sparkSession.range(numRows).map(_ => datum).toDF.cache()
      df.count()  // force caching
      addCases(benchmark, df, s"$width wide x $numRows rows", "_1[\"value_1\"]")
    }
    benchmark.run()
  }
}
