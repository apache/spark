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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.internal.SQLConf

/**
 * Synthetic benchmark for nested fields predicate push down performance for Parquet datasource.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ParquetNestedPredicatePushDownBenchmark-results.txt".
 * }}}
 */
object ParquetNestedPredicatePushDownBenchmark extends SqlBasedBenchmark {

  private val N = 100 * 1024 * 1024
  private val NUMBER_OF_ITER = 10

  private val df: DataFrame = spark
    .range(1, N, 1, 4)
    .toDF("id")
    .selectExpr("id", "STRUCT(id x, STRUCT(CAST(id AS STRING) z) y) nested")
    .sort("id")

  private def addCase(
      benchmark: Benchmark,
      inputPath: String,
      enableNestedPD: String,
      name: String,
      withFilter: DataFrame => DataFrame): Unit = {
    val loadDF = spark.read.parquet(inputPath)
    benchmark.addCase(name) { _ =>
      withSQLConf((SQLConf.NESTED_PREDICATE_PUSHDOWN_FILE_SOURCE_LIST.key, enableNestedPD)) {
        withFilter(loadDF).noop()
      }
    }
  }

  private def createAndRunBenchmark(name: String, withFilter: DataFrame => DataFrame): Unit = {
    withTempPath { tempDir =>
      val outputPath = tempDir.getCanonicalPath
      df.write.mode(SaveMode.Overwrite).parquet(outputPath)
      val benchmark = new Benchmark(name, N, NUMBER_OF_ITER, output = output)
      addCase(
        benchmark,
        outputPath,
        enableNestedPD = "",
        "Without nested predicate Pushdown",
        withFilter)
      addCase(
        benchmark,
        outputPath,
        enableNestedPD = "parquet",
        "With nested predicate Pushdown",
        withFilter)
      benchmark.run()
    }
  }

  /**
   * Benchmark for sorted data with a filter which allows to filter out all the row groups
   * when nested fields predicate push down enabled
   */
  def runLoadNoRowGroupWhenPredicatePushedDown(): Unit = {
    createAndRunBenchmark("Can skip all row groups", _.filter("nested.x < 0"))
  }

  /**
   * Benchmark with a filter which allows to load only some row groups
   * when nested fields predicate push down enabled
   */
  def runLoadSomeRowGroupWhenPredicatePushedDown(): Unit = {
    createAndRunBenchmark("Can skip some row groups", _.filter("nested.x = 100"))
  }

  /**
   * Benchmark with a filter which still requires to
   * load all the row groups on sorted data to see if we introduce too much
   * overhead or not if enable nested predicate push down.
   */
  def runLoadAllRowGroupsWhenPredicatePushedDown(): Unit = {
    createAndRunBenchmark("Can skip no row groups", _.filter(s"nested.x >= 0 and nested.x <= $N"))
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runLoadNoRowGroupWhenPredicatePushedDown()
    runLoadSomeRowGroupWhenPredicatePushedDown()
    runLoadAllRowGroupsWhenPredicatePushedDown()
  }
}
