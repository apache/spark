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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Benchmark to measure performance of map lookup operations.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.MapLookupBenchmark"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *        "sql/Test/runMain org.apache.spark.sql.execution.benchmark.MapLookupBenchmark"
 *      Results will be written to "benchmarks/MapLookupBenchmark-results.txt".
 * }}}
 */
object MapLookupBenchmark extends SqlBasedBenchmark {
  private val NUMBER_OF_ITER = 3

  override def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .appName("MapLookupBenchmark")
      .config("spark.driver.memory", "6g")
      .config("spark.executor.memory", "6g")
      .getOrCreate()
  }

  private def run(
      mapSize: Int,
      hitRate: Double,
      keyType: DataType): Unit = {
    val numRows = 10000

    val benchmark = new Benchmark(
      s"MapLookup (size=$mapSize, hit=$hitRate, type=$keyType)",
      numRows,
      NUMBER_OF_ITER,
      output = output)

    import spark.implicits._

    // Create a DataFrame with a single column 'm' containing the map,
    // and 'k' containing the key to lookup.
    // Use `typedLit` to create the map literal directly, which makes `m` a foldable
    // expression so the hash index can be built once and reused across rows.
    val keys = (0 until mapSize).toArray
    val map = keys.zip(keys.map(_.toString)).toMap
    val mapCol = typedLit(map)

    // Generate lookup keys
    val lookupKeys = (0 until numRows).map { i =>
      if (i < numRows * hitRate) keys(i % mapSize) else -1
    }

    val lookupDf = lookupKeys.toDF("key").select(mapCol.as("m"), $"key")

    // Only measure `GetMapValue` here: `ElementAt` on a map delegates to the same
    // `GetMapValueUtil` machinery, so its numbers would track `GetMapValue` exactly.
    val expr = col("m").getItem(col("key"))

    benchmark.addCase("GetMapValue interpreted - Linear Lookup") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> Int.MaxValue.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(expr).noop()
      }
    }

    benchmark.addCase("GetMapValue interpreted - Hash Lookup") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> 0.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(expr).noop()
      }
    }

    benchmark.addCase("GetMapValue codegen - Linear Lookup") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> Int.MaxValue.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(expr).noop()
      }
    }

    benchmark.addCase("GetMapValue codegen - Hash Lookup") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> 0.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(expr).noop()
      }
    }

    benchmark.run()
    System.gc()
  }

  /**
   * Benchmark map lookup when the map comes from a row column (non-foldable). This is the
   * common production shape (e.g. `SELECT my_map_col['k'] FROM t`). Hash lookup must not be
   * used here regardless of threshold - with the foldable gate, `MAP_LOOKUP_HASH_THRESHOLD=0`
   * still routes to linear scan. This benchmark verifies there is no per-row regression.
   */
  private def runMapCol(
      mapSize: Int,
      hitRate: Double,
      keyType: DataType): Unit = {
    val numRows = 10000

    val benchmark = new Benchmark(
      s"MapLookup: non-foldable map column (size=$mapSize, hit=$hitRate, type=$keyType)",
      numRows,
      NUMBER_OF_ITER,
      output = output)

    import spark.implicits._

    val keys = (0 until mapSize).toArray
    val map = keys.zip(keys.map(_.toString)).toMap

    val lookupKeys = (0 until numRows).map { i =>
      if (i < numRows * hitRate) keys(i % mapSize) else -1
    }

    // Column `m` is a non-foldable attribute reference: each row carries its own map value.
    val lookupDf = lookupKeys.map(k => (map, k)).toDF("m", "key")

    // Only measure `GetMapValue` here (see `run`); `ElementAt` on a map takes the same path.
    val expr = col("m").getItem(col("key"))

    // Non-foldable maps always take the linear path regardless of threshold. Setting
    // threshold=0 (the most aggressive hash-path setting) verifies the foldable gate still
    // blocks the hash path - if it were ever reintroduced for non-foldable maps, this
    // benchmark would regress visibly.
    benchmark.addCase("GetMapValue interpreted") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> 0.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(expr).noop()
      }
    }

    benchmark.addCase("GetMapValue codegen") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> 0.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(expr).noop()
      }
    }

    benchmark.run()
    System.gc()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // Three sizes spanning three orders of magnitude: 10000 is well above the default
    // threshold (hash wins clearly), 1000 is at the threshold, and 10 is well below it
    // (hash overhead dominates -- justifies the threshold default). Upper bound is capped
    // because task serialization of a 1M-entry literal exceeds sbt's default 8g heap.
    for (size <- Seq(10000, 1000, 10)) {
      run(size, 1.0, IntegerType)
      run(size, 0.5, IntegerType)
      run(size, 0.0, IntegerType)
    }
    // Non-foldable map column carries one map per row, so total memory scales with
    // `numRows * mapSize`. Cap `mapSize` at 1000 here to stay safely below the driver heap.
    for (size <- Seq(1000, 100, 10)) {
      runMapCol(size, 1.0, IntegerType)
      runMapCol(size, 0.5, IntegerType)
      runMapCol(size, 0.0, IntegerType)
    }
  }
}
