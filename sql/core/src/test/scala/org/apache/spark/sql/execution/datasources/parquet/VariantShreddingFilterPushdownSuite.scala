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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.File

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.AccumulatorContext

/**
 * End-to-end tests for row-group skipping on shredded Variant columns in Parquet (SPARK-55817).
 *
 * When a Variant column is written with shredding enabled, each extracted scalar field is stored
 * as a typed Parquet leaf column (e.g. `v.typed_value.a.typed_value` for `$.a`) carrying min/max
 * statistics. On the DSv1 path, PushVariantIntoScan rewrites
 * `variant_get(v, '$.a', 'bigint') > 999` into a struct-field access `v.`0` > 999`, and (when
 * `spark.sql.variant.shreddedPredicatePushdown.enabled` is true) ParquetFilters maps `v.`0`` to
 * the physical leaf and OR-s in an IS NOT NULL guard on every untyped residual `value` column
 * along the path.
 *
 * Scope: the optimization fires on the DSv1 read path only. On the DSv2 path variant extraction is
 * pushed through the separate SupportsPushDownVariantExtractions mechanism, and the filter is never
 * rewritten into `v.`0``, so it cannot be pushed for row-group skipping (see the comment in
 * ParquetScanBuilder). DSv2 reads remain correct -- the variant filter is applied post-scan -- they
 * just do not skip row groups. These tests therefore assert skipping only on DSv1, and assert
 * correctness on both DSv1 and DSv2.
 *
 * The central correctness concern is soundness under fallback: shredding is per-row and per-file
 * best-effort, so values that don't fit the shredded type (overflow / type mismatch) or that are
 * in a file that doesn't shred the path are stored in an opaque residual with `typed_value` NULL.
 * Parquet min/max excludes NULLs, so a naive leaf-only predicate could skip a row group that still
 * holds a matching row. These tests mix typed and fallback rows in a single row group and assert
 * that no matching row is ever dropped and results equal the no-pushdown baseline.
 */
class VariantShreddingFilterPushdownSuite extends QueryTest with ParquetTest
    with SharedSparkSession {

  // Base configs to write shredded Variant Parquet files.
  private def writeConf(forceSchema: String): Seq[(String, String)] = Seq(
    SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key -> "true",
    SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true",
    SQLConf.VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key -> forceSchema,
    // Keep the physical group unannotated so the schema is a plain shredded struct.
    SQLConf.PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE.key -> "false")

  /**
   * Counts how many Parquet row groups are actually read by the given DataFrame, using the
   * accumulator technique from ParquetFilterSuite. Only meaningful with the vectorized reader,
   * which reports the row-group count into a registered NumRowGroupsAcc.
   */
  private def countRowGroupsRead(df: DataFrame): Int = {
    val accu = new NumRowGroupsAcc
    sparkContext.register(accu)
    try {
      df.foreachPartition((it: Iterator[Row]) => it.foreach(_ => accu.add(0)))
      accu.value
    } finally {
      AccumulatorContext.remove(accu.id)
    }
  }

  /**
   * Writes a JSON-per-row Variant Parquet file coalesced to a single partition with a tiny block
   * size so the writer emits multiple row groups. `jsonExpr` is the SQL expression producing the
   * JSON string per `id` in `range(0, numRows, 1, 1)`.
   */
  private def writeShredded(
      dir: File,
      forceSchema: String,
      jsonExpr: String,
      numRows: Int,
      blockSize: Int = 512): Unit = {
    withSQLConf(writeConf(forceSchema): _*) {
      spark.sql(
        s"""SELECT parse_json($jsonExpr) AS v
           |FROM range(0, $numRows, 1, 1)""".stripMargin)
        .coalesce(1)
        .write
        .option("parquet.block.size", blockSize)
        .mode("overwrite")
        .parquet(dir.getAbsolutePath)
    }
  }

  // Run `block` with pushdown enabled, across the {DSv1, DSv2} x {vectorized, non-vectorized} grid.
  // `dsv1` is passed so a test can assert row-group skipping only on the DSv1 path.
  private def forEachReader(block: (Boolean, Boolean) => Unit): Unit = {
    Seq("parquet" -> true, "" -> false).foreach { case (useV1, dsv1) =>
      Seq(true, false).foreach { vectorized =>
        withSQLConf(
          SQLConf.USE_V1_SOURCE_LIST.key -> useV1,
          SQLConf.VARIANT_SHREDDED_PREDICATE_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorized.toString,
          SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true") {
          withClue(s"(dsv1=$dsv1, vectorized=$vectorized) ") {
            block(dsv1, vectorized)
          }
        }
      }
    }
  }

  // Read the same query with pushdown disabled: the baseline that must never lose rows.
  private def baseline(read: => DataFrame): Seq[Row] = {
    withSQLConf(
      SQLConf.VARIANT_SHREDDED_PREDICATE_PUSHDOWN_ENABLED.key -> "false",
      SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true") {
      read.collect().toSeq
    }
  }

  test("overflow fallback: matching row in residual is not dropped") {
    withTempDir { dir =>
      // `a` shredded as tinyint. id 0..49 -> a=id (fits, typed). id 50 -> a=1500 (overflows
      // tinyint, stored in the residual with typed_value NULL). All 51 rows fit one row group
      // (blockSize large enough), so the typed leaf stats are min=0,max=49; a leaf-only `a > 999`
      // would drop the row group and lose the 1500 row.
      val jsonExpr =
        "case when id = 50 then '{\"a\":1500}' else '{\"a\":' || id || '}' end"
      writeShredded(dir, "a tinyint", jsonExpr, numRows = 51, blockSize = 1024 * 1024)

      def read: DataFrame = spark.read.parquet(dir.getAbsolutePath)
        .selectExpr("variant_get(v, '$.a', 'bigint') AS a")
        .where("a > 999")
      val expected = baseline(read)
      assert(expected == Seq(Row(1500L)), s"baseline should return the overflow row, got $expected")

      forEachReader { (dsv1, vectorized) =>
        // The single row group has a non-null residual (the overflow row), so the IS NOT NULL
        // guard must keep it: results include the residual row. On DSv1 with the vectorized
        // reader we also assert the row group is not skipped -- this directly validates the
        // Parquet or(leaf, notEq(residual, null)) semantics.
        checkAnswer(read, expected)
        if (dsv1 && vectorized) {
          val all = spark.read.parquet(dir.getAbsolutePath)
            .selectExpr("variant_get(v, '$.a', 'bigint') AS a")
          assert(countRowGroupsRead(read) == countRowGroupsRead(all),
            "Row group with a non-null residual must NOT be skipped")
        }
      }
    }
  }

  test("type-mismatch fallback: string values in a numeric-shredded field are not dropped") {
    withTempDir { dir =>
      // `a` shredded as bigint. Even rows -> a is a number (typed); odd rows -> a is a string
      // (type mismatch -> residual, typed_value NULL). Use try_variant_get so the string rows
      // resolve to NULL (filtered out) rather than raising a strict-cast error, and assert the
      // matching numeric rows are still returned.
      val jsonExpr =
        "case when id % 2 = 0 then '{\"a\":' || (id + 1000) || '}' " +
        "else '{\"a\":\"str' || id || '\"}' end"
      writeShredded(dir, "a bigint", jsonExpr, numRows = 20, blockSize = 1024 * 1024)

      def read: DataFrame = spark.read.parquet(dir.getAbsolutePath)
        .selectExpr("try_variant_get(v, '$.a', 'bigint') AS a")
        .where("a > 1005")
      val expected = baseline(read)
      assert(expected.nonEmpty, "baseline should return the matching numeric rows")

      forEachReader { (_, _) =>
        checkAnswer(read, expected)
      }
    }
  }

  test("file without the shredded path: value read from residual, predicate not pushed") {
    withTempDir { dir =>
      // Force a shredding schema that does NOT contain `a`; `$.a` lives entirely in the opaque
      // top-level residual. Nothing is pushed for `$.a`; results must still be correct.
      val jsonExpr = "'{\"a\":' || id || '}'"
      writeShredded(dir, "b bigint", jsonExpr, numRows = 20, blockSize = 1024 * 1024)

      def read: DataFrame = spark.read.parquet(dir.getAbsolutePath)
        .selectExpr("variant_get(v, '$.a', 'bigint') AS a")
        .where("a > 9")
      val expected = baseline(read)
      assert(expected == (10L to 19L).map(Row(_)), s"unexpected baseline: $expected")

      forEachReader { (_, _) =>
        checkAnswer(read.orderBy("a"), expected)
      }
    }
  }

  test("residual-null happy path: a row group is skipped (DSv1) and results are correct") {
    withTempDir { dir =>
      // Homogeneous typed data across two row groups. All values shred cleanly (residuals all
      // NULL), so the optimization fires and one row group is skipped on DSv1.
      val jsonExpr = "'{\"a\":' || id || '}'"
      // Small block size -> at least two row groups: [0,999] and [1000,1999].
      writeShredded(dir, "a bigint", jsonExpr, numRows = 2000, blockSize = 512)

      forEachReader { (dsv1, vectorized) =>
        val filtered = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a', 'bigint') AS a")
          .where("a > 999")
        val all = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a', 'bigint') AS a")
        checkAnswer(filtered.orderBy("a"), (1000L to 1999L).map(Row(_)))
        if (dsv1 && vectorized) {
          assert(countRowGroupsRead(filtered) < countRowGroupsRead(all),
            "Expected at least one row group to be skipped by the shredded leaf statistics")
        }
      }
    }
  }

  test("multi-level $.a.b: skip fires on DSv1 and results are correct") {
    withTempDir { dir =>
      // `a` shredded as struct<b bigint>. Homogeneous nested typed data across two row groups so
      // the skip fires on the nested leaf `v.typed_value.a.typed_value.b.typed_value`.
      val typedJson = "'{\"a\":{\"b\":' || id || '}}'"
      writeShredded(dir, "a struct<b bigint>", typedJson, numRows = 2000, blockSize = 512)

      forEachReader { (dsv1, vectorized) =>
        val filtered = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a.b', 'bigint') AS b")
          .where("b > 999")
        val all = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a.b', 'bigint') AS b")
        checkAnswer(filtered.orderBy("b"), (1000L to 1999L).map(Row(_)))
        if (dsv1 && vectorized) {
          assert(countRowGroupsRead(filtered) < countRowGroupsRead(all),
            "Expected a row group to be skipped by the nested shredded leaf statistics")
        }
      }
    }
  }

  test("multi-level $.a.b: fallback at an intermediate level is not dropped") {
    withTempDir { dir =>
      // `a` shredded as struct<b bigint>. One row stores `a` as a plain number (not an object):
      // the whole `a` subtree cannot be shredded, so `a` goes to the L1 residual
      // (v.typed_value.a.value) with its typed_value NULL. The IS NOT NULL guard on that L1
      // residual must prevent skipping a row group that contains it. Row 6 has a matching nested
      // value so a leaf-only predicate would be tempted to skip.
      val jsonExpr =
        "case when id = 5 then '{\"a\":9999}' " +
        "when id = 6 then '{\"a\":{\"b\":5000}}' " +
        "else '{\"a\":{\"b\":' || id || '}}' end"
      writeShredded(dir, "a struct<b bigint>", jsonExpr, numRows = 20, blockSize = 1024 * 1024)

      def read: DataFrame = spark.read.parquet(dir.getAbsolutePath)
        .selectExpr("variant_get(v, '$.a.b', 'bigint') AS b")
        .where("b > 999")
      val expected = baseline(read)
      // Row 6 (b=5000) matches; rows 0..4,7..19 have b <= 19; row 5 has no `b` (a is a scalar).
      assert(expected.contains(Row(5000L)), s"baseline should include the matching row: $expected")

      forEachReader { (_, _) =>
        checkAnswer(read, expected)
      }
    }
  }
}
