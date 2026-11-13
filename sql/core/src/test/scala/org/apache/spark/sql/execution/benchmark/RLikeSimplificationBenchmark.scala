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
import org.apache.spark.sql.catalyst.expressions.{Contains, RLike}
import org.apache.spark.sql.functions.{concat, lit, lower, rlike, upper}
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmarks literal-alternation RLIKE evaluation against the simplified Contains OR-tree.
 * The cases vary input length and alternative count, and include a deterministic nontrivial
 * input that the OR-tree may evaluate once per alternative.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/RLikeSimplificationBenchmark-results.txt".
 * }}}
 */
object RLikeSimplificationBenchmark extends SqlBasedBenchmark {

  import spark.implicits._

  private val numRows = 1000
  private val numIters = 3

  private def runCase(
      name: String,
      inputLength: Int,
      numAlternatives: Int,
      expensiveInput: Boolean = false): Unit = {
    val alternatives = (0 until numAlternatives).map { i =>
      if (i == 0) "foo" else s"literal$i"
    }
    val pattern = alternatives.mkString(".*(", "|", ").*")
    val padding = "x" * inputLength
    val df = spark.range(numRows).select(
      concat(lit(padding), $"id".cast("string")).as("value"))
    val input = if (expensiveInput) lower(upper($"value")) else $"value"
    def query = df.select(rlike(input, lit(pattern)))

    // Do not time a comparison unless the switch actually selects the intended expression.
    val disabledPlan = withSQLConf(SQLConf.RLIKE_SIMPLIFICATION_ENABLED.key -> "false") {
      query.queryExecution.optimizedPlan
    }
    val disabledExpressions = disabledPlan.flatMap(_.expressions)
    require(
      disabledExpressions.exists(_.exists(_.isInstanceOf[RLike])),
      s"Expected original RLIKE in disabled mode: ${disabledPlan.treeString}")
    val enabledPlan = withSQLConf(SQLConf.RLIKE_SIMPLIFICATION_ENABLED.key -> "true") {
      query.queryExecution.optimizedPlan
    }
    val enabledExpressions = enabledPlan.flatMap(_.expressions)
    require(
      enabledExpressions.exists(_.exists(_.isInstanceOf[Contains])),
      s"Expected Contains in enabled mode: ${enabledPlan.treeString}")
    require(
      !enabledExpressions.exists(_.exists(_.isInstanceOf[RLike])),
      s"Expected no RLIKE in enabled mode: ${enabledPlan.treeString}")

    val benchmark = new Benchmark(name, numRows, minNumIters = numIters, output = output)
    benchmark.addCase("Disabled: original RLIKE", numIters = numIters) { _ =>
      withSQLConf(SQLConf.RLIKE_SIMPLIFICATION_ENABLED.key -> "false") {
        query.noop()
      }
    }
    benchmark.addCase("Enabled: simplified Contains", numIters = numIters) { _ =>
      withSQLConf(SQLConf.RLIKE_SIMPLIFICATION_ENABLED.key -> "true") {
        query.noop()
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runCase("Short input, 4 alternatives", inputLength = 32, numAlternatives = 4)
    runCase("Long input, 4 alternatives", inputLength = 256, numAlternatives = 4)
    runCase("Short input, 32 alternatives", inputLength = 32, numAlternatives = 32)
    runCase("Long input, 32 alternatives", inputLength = 256, numAlternatives = 32)
    runCase(
      "Expensive deterministic input, 32 alternatives",
      inputLength = 32,
      numAlternatives = 32,
      expensiveInput = true)
  }
}
