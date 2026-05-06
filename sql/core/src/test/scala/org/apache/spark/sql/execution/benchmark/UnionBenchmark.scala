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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

/**
 * Benchmark Union codegen fusion.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <sql/core test jar>,<spark catalyst jar> <sql/core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/UnionBenchmark-results.txt".
 * }}}
 */
object UnionBenchmark extends SqlBasedBenchmark {

  // Multi-core master is required: with `local[1]`, each `Range` child produces
  // a single partition, `UnionExec.outputPartitioning` collapses to
  // `SinglePartition`, and fusion is denied (it applies only on the
  // `UnknownPartitioning` path). Use `local[4]` so children have multiple
  // partitions and the Union ends up on the fusable path.
  override def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getCanonicalName)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 4)
      .config("spark.ui.enabled", false)
      .getOrCreate()
  }

  // Plain children: each child is a bare Range, all children share the same type.
  private def buildPlainQuery(n: Int): DataFrame = {
    val dfs = (0 until n).map(i =>
      spark.range(i * 10000L, i * 10000L + 10000L).toDF("id"))
    dfs.reduce((a, b) => a.union(b)).filter(col("id") > 0)
  }

  // Type widening: alternate int and long children so UnionExec needs to
  // insert `Cast` nodes in `perChildProjections`, adding per-row work in
  // each helper method.
  private def buildWideningQuery(n: Int): DataFrame = {
    val dfs = (0 until n).map { i =>
      val df = spark.range(i * 10000L, i * 10000L + 10000L).toDF("id")
      if (i % 2 == 0) df.select(col("id").cast(IntegerType).as("id"))
      else df
    }
    dfs.reduce((a, b) => a.union(b)).filter(col("id") > 0)
  }

  // Fatter helpers: each child has its own per-child filter + projection,
  // which inflates the generated helper method body and stresses HotSpot
  // inlining/compilation.
  private def buildPerChildOpsQuery(n: Int): DataFrame = {
    val dfs = (0 until n).map { i =>
      spark.range(i * 10000L, i * 10000L + 10000L).toDF("id")
        .filter(col("id") > i.toLong)
        .select((col("id") + 1).as("id"))
    }
    dfs.reduce((a, b) => a.union(b)).filter(col("id") > 0)
  }

  // TPC-DS-style shape: union of per-channel projections followed by
  // grouping aggregation. Matches Q2 (wswscs + weekly sums), Q5 (per-channel
  // rollups), and similar ETL queries where UNION ALL feeds an aggregate.
  private def buildDownstreamAggQuery(n: Int): DataFrame = {
    val dfs = (0 until n).map(i =>
      spark.range(i * 10000L, i * 10000L + 10000L).toDF("id"))
    dfs.reduce((a, b) => a.union(b))
      .groupBy((col("id") % 10).as("bucket"))
      .count()
  }

  private def runScenario(
      name: String,
      build: Int => DataFrame,
      ns: Seq[Int]): Unit = {
    ns.foreach { n =>
      runBenchmark(s"$name (N=$n)") {
        val benchmark = new Benchmark(
          s"$name, N=$n",
          valuesPerIteration = 10000L * n,
          output = output)

        benchmark.addCase(s"codegen=off") { _ =>
          withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
            build(n).noop()
          }
        }

        benchmark.addCase(s"codegen=on") { _ =>
          withSQLConf(
            SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true",
            SQLConf.WHOLESTAGE_UNION_MAX_CHILDREN.key -> "8192") {
            build(n).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val ns = Seq(2, 4, 8, 16, 32, 64, 128, 256, 512, 1024)
    runScenario("Union plain", buildPlainQuery, ns)
    runScenario("Union type widening", buildWideningQuery, ns)
    runScenario("Union per-child ops", buildPerChildOpsQuery, ns)
    runScenario("Union + downstream aggregate", buildDownstreamAggQuery, ns)
  }

  private def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val saved = pairs.map { case (k, _) => k -> spark.conf.getOption(k) }
    try {
      pairs.foreach { case (k, v) => spark.conf.set(k, v) }
      f
    } finally {
      saved.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
    }
  }
}
