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
package org.apache.spark.sql.execution.columnar

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmarks for the DSv2-backed in-memory cache path, measuring the impact of
 * column pruning, filter pushdown, and planning overhead compared with the pre-DSv2
 * InMemoryRelation approach.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/InMemoryCacheDSv2Benchmark-results.txt".
 * }}}
 */
object InMemoryCacheDSv2Benchmark extends SqlBasedBenchmark {

  private val numRows = 1000000
  private val numIters = 5

  /**
   * Benchmarks column pruning: reading 2 of 10 columns from a cached wide table.
   * Under the DSv2 path, column pruning is applied via SupportsPushDownRequiredColumns,
   * so InMemoryTableScanExec only deserializes the 2 requested columns.
   * The "no pruning" case reads all 10 columns, simulating the pre-DSv2 behaviour.
   */
  def columnPruningBenchmark(): Unit = {
    val df = spark.range(numRows).select(
      (0 until 10).map(i => (col("id") + i).alias(s"c$i")): _*
    ).cache()
    df.count() // materialize the cache

    val benchmark = new Benchmark(
      s"Column pruning - $numRows rows, 10 cols, select 2",
      numRows, output = output)

    // Use sum() to force actual column deserialization (count() gets optimized away).
    // "pruning" case: DSv2 column pruning deserializes only 2 of 10 columns.
    // "baseline" case: all 10 columns are needed and deserialized (simulates pre-DSv2 behaviour
    // where the full row is always deserialized even when only some columns are needed).
    benchmark.addCase("sum 2 of 10 cols (column pruning via DSv2)") { _ =>
      df.select("c0", "c1").agg(sum("c0") + sum("c1")).collect()
    }

    benchmark.addCase("sum all 10 cols (no pruning - pre-DSv2 baseline)") { _ =>
      df.agg(sum("c0") + sum("c1") + sum("c2") + sum("c3") + sum("c4") +
        sum("c5") + sum("c6") + sum("c7") + sum("c8") + sum("c9")).collect()
    }

    benchmark.run()
    df.unpersist()
  }

  /**
   * Benchmarks filter pushdown: a selective predicate on a cached table.
   * Under the DSv2 path, filters are pushed via SupportsPushDownV2Filters, enabling
   * per-batch min/max pruning inside InMemoryTableScanExec (category-2 push-down).
   * The "no push" case applies the filter outside the scan via a post-scan FilterExec,
   * but must still read all batches - this is the same behaviour as the pre-DSv2 path.
   *
   * Note: both cases produce identical results; the difference is how many columnar
   * batches are inspected before row-level filtering.
   */
  def filterPushdownBenchmark(): Unit = {
    // Use sorted data so that batch-level min/max pruning is maximally effective.
    val df = spark.range(numRows).select(col("id").alias("c0")).cache()
    df.count() // materialize the cache

    val benchmark = new Benchmark(
      s"Filter pushdown - $numRows rows, selective filter (c0 < 1000)",
      numRows, output = output)

    benchmark.addCase("filter c0 < 1000 (pushed to scan, batch pruning)") { _ =>
      df.filter(col("c0") < 1000).count()
    }

    benchmark.addCase("filter c0 < 1000 (count with full scan for comparison)") { _ =>
      df.count()
    }

    benchmark.run()
    df.unpersist()
  }

  /**
   * Benchmarks planning overhead: how long the optimizer takes for a simple cached scan.
   * The DSv2 path runs additional optimizer rules (V2ScanRelationPushDown batch) compared
   * with the pre-DSv2 InMemoryRelation path. This case measures total plan->execute time
   * without caching queryExecution results.
   */
  def planningOverheadBenchmark(): Unit = {
    val numPlanIters = 1000
    val df = spark.range(numRows).select(col("id").alias("c0")).cache()
    df.count() // materialize the cache

    val benchmark = new Benchmark(
      s"Planning overhead - $numPlanIters plan-only iterations",
      numPlanIters, output = output)

    benchmark.addCase("optimizedPlan (DSv2 path, V2ScanRelationPushDown)") { _ =>
      var i = 0
      while (i < numPlanIters) {
        df.filter(col("c0") > 0).queryExecution.optimizedPlan
        i += 1
      }
    }

    benchmark.run()
    df.unpersist()
  }

  /**
   * Benchmarks a full aggregate query end-to-end on a cached multi-column table to
   * measure real-world combined overhead of planning + execution.
   */
  def endToEndAggregateBenchmark(): Unit = {
    val df = spark.range(numRows).select(
      (col("id") % 100).alias("key"),
      col("id").alias("val")
    ).cache()
    df.count()

    val benchmark = new Benchmark(
      s"End-to-end aggregate (groupBy + sum) on $numRows rows",
      numRows, output = output)

    benchmark.addCase("groupBy(key).sum(val) - DSv2 path") { _ =>
      df.groupBy("key").agg(sum("val")).count()
    }

    benchmark.run()
    df.unpersist()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // AQE off for deterministic planning
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      runBenchmark("In-memory cache: column pruning") {
        columnPruningBenchmark()
      }
      runBenchmark("In-memory cache: filter pushdown") {
        filterPushdownBenchmark()
      }
      runBenchmark("In-memory cache: planning overhead") {
        planningOverheadBenchmark()
      }
      runBenchmark("In-memory cache: end-to-end aggregate") {
        endToEndAggregateBenchmark()
      }
    }
  }
}
