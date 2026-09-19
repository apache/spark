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
 * Synthetic benchmark for row-group skipping on shredded Variant columns (SPARK-55817).
 *
 * The optimization pushes a predicate on a shredded Variant field to the physical typed_value leaf
 * (guarded so residual fallbacks are never skipped), letting Parquet skip row groups the leaf
 * min/max cannot match. The lift depends on the layout: the field must be shredded, the predicate a
 * literal comparison, the data sorted on that field, and a file must hold many row groups. This
 * benchmark writes such a layout (sorted on the shredded field, small block size so a single file
 * has many row groups) and compares scan time with the optimization on vs off.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to
 *      "benchmarks/VariantShreddedPredicatePushdownBenchmark-results.txt".
 * }}}
 */
object VariantShreddedPredicatePushdownBenchmark extends SqlBasedBenchmark {

  private val N = 20 * 1024 * 1024
  private val NUMBER_OF_ITER = 10

  // A single-column shredded Variant dataset with an object field `a` sorted ascending, so that a
  // literal predicate on `a` maps to a contiguous range of row groups.
  private val df: DataFrame = spark
    .range(0, N, 1, 1)
    .selectExpr("parse_json('{\"a\":' || id || '}') AS v")

  // Same, but every row also carries a key `z` outside the shredding schema, so the whole partial
  // object lands in the top-level residual `v.value` (non-null on every row). `a` is still fully
  // shredded into the typed leaf. This is the normal layout for real Variant data (the inferred
  // shredding schema is capped, so extra keys are common), and it is where the flat
  // `or(leaf, isNotNull(residual)...)` guard could never skip -- the tighter
  // `or(leaf, and(anyResidualNotNull, isNull(leaf)))` guard skips via the "leaf has no nulls" arm.
  private val dfPartialObject: DataFrame = spark
    .range(0, N, 1, 1)
    .selectExpr("parse_json('{\"a\":' || id || ', \"z\":\"outside\"}') AS v")

  // Confs to write the Variant column shredded, forcing `a` to a bigint typed leaf. A small block
  // size makes the writer emit many row groups per file.
  private val writeConf = Seq(
    SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key -> "true",
    SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true",
    SQLConf.VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key -> "a bigint")

  private def addCase(
      benchmark: Benchmark,
      inputPath: String,
      enablePushdown: String,
      name: String,
      withFilter: DataFrame => DataFrame): Unit = {
    val loadDF = spark.read.parquet(inputPath).selectExpr("variant_get(v, '$.a', 'bigint') AS a")
    benchmark.addCase(name) { _ =>
      withSQLConf(
        SQLConf.VARIANT_SHREDDED_PREDICATE_PUSHDOWN_ENABLED.key -> enablePushdown,
        SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true") {
        withFilter(loadDF).noop()
      }
    }
  }

  // A tiny block size makes one file hold many row groups, which is the layout row-group skipping
  // needs. `blockSize = None` uses the Parquet default (one row group for this dataset), used to
  // show the skip-none overhead disappears when a file is not deliberately dense.
  private def createAndRunBenchmark(
      name: String,
      withFilter: DataFrame => DataFrame,
      data: DataFrame = df,
      blockSize: Option[Int] = Some(128 * 1024)): Unit = {
    withTempPath { tempDir =>
      val outputPath = tempDir.getCanonicalPath
      withSQLConf(writeConf: _*) {
        val writer = data.write.mode(SaveMode.Overwrite)
        blockSize.foreach(bs => writer.option("parquet.block.size", bs.toString))
        writer.parquet(outputPath)
      }
      val benchmark = new Benchmark(name, N, NUMBER_OF_ITER, output = output)
      addCase(benchmark, outputPath, enablePushdown = "false",
        "Without shredded predicate pushdown", withFilter)
      addCase(benchmark, outputPath, enablePushdown = "true",
        "With shredded predicate pushdown", withFilter)
      benchmark.run()
    }
  }

  /**
   * Filter that matches nothing, so the leaf min/max lets Parquet skip every row group when the
   * optimization is on.
   */
  def runSkipAllRowGroups(): Unit = {
    createAndRunBenchmark("Can skip all row groups", _.filter("a < 0"))
  }

  /**
   * Highly selective filter matching only the last few row groups of the sorted data.
   */
  def runSkipSomeRowGroups(): Unit = {
    createAndRunBenchmark("Can skip some row groups", _.filter(s"a > ${(N * 0.99).toLong}"))
  }

  /**
   * Filter that matches the whole range, so no row group can be skipped -- measures the overhead
   * of building and evaluating the pushed predicate when it never helps. Written with the tiny
   * block size (many row groups), so this is the worst case for the overhead: it is paid per row
   * group. Compare with `runSkipNoRowGroupsDefaultBlockSize`.
   */
  def runSkipNoRowGroups(): Unit = {
    createAndRunBenchmark("Can skip no row groups", _.filter(s"a >= 0 and a <= $N"))
  }

  /**
   * Same skip-none filter but written at the default Parquet block size (one row group for this
   * dataset), the layout a default-configured writer produces. The per-row-group overhead
   * effectively vanishes here -- it scales with row-group count, the same knob as the benefit.
   */
  def runSkipNoRowGroupsDefaultBlockSize(): Unit = {
    createAndRunBenchmark("Can skip no row groups (default block size)",
      _.filter(s"a >= 0 and a <= $N"), blockSize = None)
  }

  /**
   * Same selective filter as `runSkipSomeRowGroups`, but on data whose objects carry a key outside
   * the shredding schema (so the top-level residual is non-null on every row). This is the layout
   * where the earlier flat OR guard could never skip; the tighter guard still skips here.
   */
  def runSkipSomeRowGroupsPartialObject(): Unit = {
    createAndRunBenchmark("Can skip some row groups (partial object)",
      _.filter(s"a > ${(N * 0.99).toLong}"), data = dfPartialObject)
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runSkipAllRowGroups()
    runSkipSomeRowGroups()
    runSkipNoRowGroups()
    runSkipNoRowGroupsDefaultBlockSize()
    runSkipSomeRowGroupsPartialObject()
  }
}
