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

import scala.concurrent.duration._

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.classic
import org.apache.spark.sql.execution.QueryExecution

/**
 * Benchmark to measure the overhead of cloning the analyzer for transactional query execution.
 * Each transactional query creates a new [[Analyzer]] instance via
 * [[Analyzer.withCatalogManager]], which shares all rules with the original but carries a
 * per-query [[org.apache.spark.sql.connector.catalog.CatalogManager]]. This benchmark checks
 * whether the cloning introduces measurable overhead.
 *
 * To run this benchmark:
 * {{{
 *   build/sbt "sql/Test/runMain <this class>"
 * }}}
 */
object AnalyzerBenchmark extends SqlBasedBenchmark {

  private val numRows = 100
  private val queries = Seq(
    "simple select"     -> "SELECT id, val FROM t1",
    "join"              -> "SELECT t1.id, t2.val FROM t1 JOIN t2 ON t1.id = t2.id",
    "wide schema"       -> s"SELECT ${(1 to 100).map(i => s"col_$i").mkString(", ")} FROM wide_t"
  )

  private def setupTables(): Unit = {
    spark.range(numRows).selectExpr("id", "id * 2 as val").createOrReplaceTempView("t1")
    spark.range(numRows).selectExpr("id", "id * 3 as val").createOrReplaceTempView("t2")
    spark.range(numRows)
      .selectExpr((1 to numRows).map(i => s"id as col_$i"): _*)
      .createOrReplaceTempView("wide_t")
  }

  /**
   * Measures analysis time for a pre-parsed plan, comparing the session analyzer against a
   * cloned analyzer created via [[Analyzer.withCatalogManager]].
   *
   * Two cases:
   *  - "session analyzer"           : baseline, uses the session analyzer directly.
   *  - "cloned analyzer (per query)": analyzer cloned every iteration; reflects the full
   *                                   per-transactional-query cost (clone + analysis).
   */
  def analysisBenchmark(): Unit = {
    for ((name, sql) <- queries) {
      runBenchmark(s"analysis overhead $name") {
        val benchmark = new Benchmark(
          name = s"analysis overhead $name",
          // Per row measurements are not meaningful here.
          valuesPerIteration = numRows,
          minTime = 10.seconds,
          output = output)
        val catalogManager = spark.sessionState.catalogManager

        benchmark.addCase("session analyzer") { _ =>
          val plan = spark.sessionState.sqlParser.parsePlan(sql)
          new QueryExecution(spark.asInstanceOf[classic.SparkSession], plan).analyzed
        }

        benchmark.addCase("cloned analyzer (per query)") { _ =>
          val cloned = spark.sessionState.analyzer.withCatalogManager(catalogManager)
          val plan = spark.sessionState.sqlParser.parsePlan(sql)
          new QueryExecution(spark.asInstanceOf[classic.SparkSession],
            plan, analyzerOpt = Some(cloned)).analyzed
        }

        benchmark.run()
      }
    }
  }

  /**
   * Micro-benchmark for [[Analyzer.withCatalogManager]] in isolation: measures the cost of
   * instantiating the anonymous [[Analyzer]] subclass, independent of analysis work.
   */
  def cloneCostBenchmark(): Unit = {
    runBenchmark("analyzer clone cost") {
      val numRows = 1 // Per row measurements are not meaningful here.
      val benchmark = new Benchmark(
        name = "analyzer clone cost",
        valuesPerIteration = numRows,
        output = output)
      val catalogManager = spark.sessionState.catalogManager

      benchmark.addCase("withCatalogManager") { _ =>
        spark.sessionState.analyzer.withCatalogManager(catalogManager)
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    setupTables()
    cloneCostBenchmark()
    analysisBenchmark()
  }
}
