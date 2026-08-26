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

import org.apache.spark.SparkException
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
 * the physical leaf and guards it so a row group is skipped only when the leaf cannot match AND
 * every value for the path is provably in the leaf (see `makeShreddedFilter`).
 *
 * Scope: the optimization fires on both DSv1 and DSv2 read paths. DSv2 performs the regular filter
 * pushdown before variant extraction pushdown, so after variant extraction rewrites predicates into
 * `v.`0`` struct accesses it runs a second Parquet-only predicate pushdown for those rewritten
 * variant predicates. These tests assert skipping when the vectorized reader exposes the row-group
 * count, and assert correctness on both vectorized and non-vectorized readers.
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

  // Base configs to write shredded Variant Parquet files. `annotate` controls whether the physical
  // variant group carries the VARIANT logical-type annotation (the production default is true).
  private def writeConf(forceSchema: String, annotate: Boolean): Seq[(String, String)] = Seq(
    SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key -> "true",
    SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true",
    SQLConf.VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key -> forceSchema,
    SQLConf.PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE.key -> annotate.toString)

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
      blockSize: Int = 512,
      annotate: Boolean = true): Unit = {
    withSQLConf(writeConf(forceSchema, annotate): _*) {
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

  // Assert that a row group whose only match for `$.a > 999` is a residual fallback (a value the
  // int64 leaf cannot hold, so it lands in v.typed_value.a.value with the leaf NULL) is not
  // dropped. `fallbackJson` is the JSON for the fallback row. The leaf min/max is 0..49, so only
  // the guard keeps the row group; a leaf-only predicate would drop it and lose the row (#54598).
  private def checkResidualFallbackNotDropped(fallbackJson: String): Unit = {
    withTempDir { dir =>
      val jsonExpr =
        "case when id = 50 then '" + fallbackJson + "' else '{\"a\":' || id || '}' end"
      writeShredded(dir, "a bigint", jsonExpr, numRows = 51, blockSize = 1024 * 1024)

      def read: DataFrame = spark.read.parquet(dir.getAbsolutePath)
        .selectExpr("try_variant_get(v, '$.a', 'bigint') AS a")
        .where("a > 999")
      val expected = baseline(read)
      assert(expected == Seq(Row(1500L)), s"baseline should return the fallback row, got $expected")

      forEachReader { (_, vectorized) =>
        // The row group's only match is in the residual with a NULL leaf, so the guard must keep
        // it: results include the fallback row and the row group is not skipped.
        checkAnswer(read, expected)
        if (vectorized) {
          val all = spark.read.parquet(dir.getAbsolutePath)
            .selectExpr("try_variant_get(v, '$.a', 'bigint') AS a")
          assert(countRowGroupsRead(read) == countRowGroupsRead(all),
            "Row group whose only match is a residual fallback must NOT be skipped")
        }
      }
    }
  }

  test("residual fallback beyond the leaf's min/max is not dropped") {
    // Different fallback encodings that all miss the int64 leaf: a non-integral decimal that
    // 1500.5 rounds to 1500, and a string "1500". Both are read back as bigint 1500.
    checkResidualFallbackNotDropped("{\"a\":1500.5}")
    checkResidualFallbackNotDropped("{\"a\":\"1500\"}")
  }

  test("negated predicate over an all-fallback row group is not dropped") {
    withTempDir { dir =>
      // `a` shredded as bigint. Both rows are non-integral decimals the int64 leaf cannot hold, so
      // both land in the residual (typed leaf entirely NULL, residual has no nulls). The path is
      // still pushable (bigint extraction over a bigint leaf). A naive negated push would rewrite
      // `!= 700` into and(notEq(leaf), eq(residual, null)) and skip the row group -- losing both
      // rows. The negation guard must prevent pushing, so {500, 600} come back.
      val jsonExpr = "case when id = 0 then '{\"a\":500.5}' else '{\"a\":600.5}' end"
      writeShredded(dir, "a bigint", jsonExpr, numRows = 2, blockSize = 1024 * 1024)

      Seq(
        "try_variant_get(v, '$.a', 'bigint') != 700" -> Seq(Row(500L), Row(600L)),
        "try_variant_get(v, '$.a', 'bigint') NOT IN (700, 800)" -> Seq(Row(500L), Row(600L))
      ).foreach { case (predicate, want) =>
        def read: DataFrame = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("try_variant_get(v, '$.a', 'bigint') AS a")
          .where(predicate)
        assert(baseline(read).sortBy(_.getLong(0)) == want, s"baseline for $predicate")
        forEachReader { (_, _) =>
          checkAnswer(read, want)
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

  test("residual-null happy path: a row group is skipped and results are correct") {
    withTempDir { dir =>
      // Homogeneous typed data across two row groups. All values shred cleanly (residuals all
      // NULL), so the optimization fires and one row group is skipped on DSv1.
      val jsonExpr = "'{\"a\":' || id || '}'"
      // Small block size -> at least two row groups: [0,999] and [1000,1999].
      writeShredded(dir, "a bigint", jsonExpr, numRows = 2000, blockSize = 512)

      forEachReader { (_, vectorized) =>
        val filtered = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a', 'bigint') AS a")
          .where("a > 999")
        val all = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a', 'bigint') AS a")
        checkAnswer(filtered.orderBy("a"), (1000L to 1999L).map(Row(_)))
        if (vectorized) {
          assert(countRowGroupsRead(filtered) < countRowGroupsRead(all),
            "Expected at least one row group to be skipped by the shredded leaf statistics")
        }
      }
    }
  }

  test("partial object with a non-shredded sibling key still skips (leaf has no nulls)") {
    withTempDir { dir =>
      // Every row also carries a key `z` outside the shredding schema, so the whole partial object
      // lands in the top-level residual v.value -- it is non-null on every row. `a` is still fully
      // shredded into the typed leaf (no nulls). The flat OR guard could never skip here (v.value
      // never all-null); the tighter guard skips via the "leaf has no nulls" arm. Sorted on `a`
      // across two row groups so `a > 999` can drop the first.
      val jsonExpr = "'{\"a\":' || id || ', \"z\":\"outside\"}'"
      writeShredded(dir, "a bigint", jsonExpr, numRows = 2000, blockSize = 512)

      forEachReader { (_, vectorized) =>
        val filtered = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a', 'bigint') AS a").where("a > 999")
        val all = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a', 'bigint') AS a")
        checkAnswer(filtered.orderBy("a"), (1000L to 1999L).map(Row(_)))
        if (vectorized) {
          assert(countRowGroupsRead(filtered) < countRowGroupsRead(all),
            "Expected skipping despite a non-null top-level residual (partial object)")
        }
      }
    }
  }

  test("multi-level $.a.b: skip fires and results are correct") {
    withTempDir { dir =>
      // `a` shredded as struct<b bigint>. Homogeneous nested typed data across two row groups so
      // the skip fires on the nested leaf `v.typed_value.a.typed_value.b.typed_value`.
      val typedJson = "'{\"a\":{\"b\":' || id || '}}'"
      writeShredded(dir, "a struct<b bigint>", typedJson, numRows = 2000, blockSize = 512)

      forEachReader { (_, vectorized) =>
        val filtered = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a.b', 'bigint') AS b")
          .where("b > 999")
        val all = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a.b', 'bigint') AS b")
        checkAnswer(filtered.orderBy("b"), (1000L to 1999L).map(Row(_)))
        if (vectorized) {
          assert(countRowGroupsRead(filtered) < countRowGroupsRead(all),
            "Expected a row group to be skipped by the nested shredded leaf statistics")
        }
      }
    }
  }

  test("multi-level $.a.b: nested residual fallback beyond the leaf's min/max is not dropped") {
    withTempDir { dir =>
      // `a` shredded as struct<b bigint>. Rows 0..19 shred cleanly, so the nested leaf
      // v.typed_value.a.typed_value.b.typed_value is min 0 / max 19. Row 20 stores `b` as a
      // non-integral decimal the int64 leaf cannot hold, so it lands in
      // v.typed_value.a.typed_value.b.value with the leaf NULL. `b > 999` matches only that row and
      // the leaf min/max cannot match it, so only the guard keeps the row group -- this makes the
      // nested leaf-level residual load-bearing (a leaf-only predicate would drop it).
      val jsonExpr =
        "case when id = 20 then '{\"a\":{\"b\":1500.5}}' else '{\"a\":{\"b\":' || id || '}}' end"
      writeShredded(dir, "a struct<b bigint>", jsonExpr, numRows = 21, blockSize = 1024 * 1024)

      def read: DataFrame = spark.read.parquet(dir.getAbsolutePath)
        .selectExpr("try_variant_get(v, '$.a.b', 'bigint') AS b")
        .where("b > 999")
      assert(baseline(read) == Seq(Row(1500L)), "baseline should return the nested fallback row")

      forEachReader { (_, vectorized) =>
        checkAnswer(read, Seq(Row(1500L)))
        if (vectorized) {
          val all = spark.read.parquet(dir.getAbsolutePath)
            .selectExpr("try_variant_get(v, '$.a.b', 'bigint') AS b")
          assert(countRowGroupsRead(read) == countRowGroupsRead(all),
            "Row group whose only match is a nested residual fallback must NOT be skipped")
        }
      }
    }
  }

  test("unannotated variant layout: skip and fallback still work") {
    // The suite writes the production-default annotated layout everywhere else; this test covers
    // the unannotated physical layout explicitly. Both a skip-eligible query and a residual
    // fallback must behave correctly.
    withTempDir { dir =>
      writeShredded(dir, "a bigint", "'{\"a\":' || id || '}'", numRows = 2000, blockSize = 512,
        annotate = false)
      forEachReader { (_, vectorized) =>
        val filtered = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a', 'bigint') AS a").where("a > 999")
        val all = spark.read.parquet(dir.getAbsolutePath)
          .selectExpr("variant_get(v, '$.a', 'bigint') AS a")
        checkAnswer(filtered.orderBy("a"), (1000L to 1999L).map(Row(_)))
        if (vectorized) {
          assert(countRowGroupsRead(filtered) < countRowGroupsRead(all),
            "Expected a row group to be skipped on the unannotated layout")
        }
      }
    }

    withTempDir { dir =>
      // Residual fallback under the unannotated layout: the row whose only match is in the residual
      // (NULL leaf) must survive.
      writeShredded(dir, "a bigint",
        "case when id = 50 then '{\"a\":1500.5}' else '{\"a\":' || id || '}' end",
        numRows = 51, blockSize = 1024 * 1024, annotate = false)
      def read: DataFrame = spark.read.parquet(dir.getAbsolutePath)
        .selectExpr("try_variant_get(v, '$.a', 'bigint') AS a").where("a > 999")
      assert(baseline(read) == Seq(Row(1500L)))
      forEachReader { (_, _) =>
        checkAnswer(read, Seq(Row(1500L)))
      }
    }
  }

  test("deferCastError=true: strict non-string cast does not fire; try_variant_get still does") {
    // With deferCastError, a strict cast to a non-string, non-variant type is rewritten into
    // UnwrapVariantCastError, which is not translated to a pushable filter -- so shredded pushdown
    // does not fire for it (results still correct). try_variant_get (failOnError=false) and string
    // targets are unaffected and still fire. Results must be correct in every combination.
    withTempDir { dir =>
      writeShredded(dir, "a bigint", "'{\"a\":' || id || '}'", numRows = 2000, blockSize = 512)
      Seq("true", "false").foreach { defer =>
        withSQLConf(
          SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
          SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR.key -> defer,
          SQLConf.VARIANT_SHREDDED_PREDICATE_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
          SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true") {
          withClue(s"(deferCastError=$defer) ") {
            // Strict cast: correct either way (does not fire when defer=true).
            val strict = spark.read.parquet(dir.getAbsolutePath)
              .selectExpr("variant_get(v, '$.a', 'bigint') AS a").where("a > 999")
            checkAnswer(strict.orderBy("a"), (1000L to 1999L).map(Row(_)))
            // try_variant_get: unaffected by deferCastError and still skips a row group on DSv1.
            val tryGet = spark.read.parquet(dir.getAbsolutePath)
              .selectExpr("try_variant_get(v, '$.a', 'bigint') AS a").where("a > 999")
            val all = spark.read.parquet(dir.getAbsolutePath)
              .selectExpr("try_variant_get(v, '$.a', 'bigint') AS a")
            checkAnswer(tryGet.orderBy("a"), (1000L to 1999L).map(Row(_)))
            assert(countRowGroupsRead(tryGet) < countRowGroupsRead(all),
              "try_variant_get should still skip regardless of deferCastError")
          }
        }
      }
    }
  }

  test("strict variant_get preserves INVALID_VARIANT_CAST on a residual fallback (not empty)") {
    // The worst failure mode: a thrown cast error silently becoming an empty result. `a` shredded
    // as int; row 50 stores 3000000000, which overflows int32 so it lands in the residual with the
    // leaf NULL (extraction type matches the leaf exactly, so the path is pushed). With
    // deferCastError=false (default) the scan casts eagerly, so strict `variant_get(v,'$.a','int')`
    // must raise INVALID_VARIANT_CAST -- a leaf-only push would drop the row group (leaf max 49)
    // and return empty instead. The guard keeps the row group, so the error is preserved.
    withTempDir { dir =>
      val jsonExpr = "case when id = 50 then '{\"a\":3000000000}' else '{\"a\":' || id || '}' end"
      writeShredded(dir, "a int", jsonExpr, numRows = 51, blockSize = 1024 * 1024)
      Seq("parquet", "").foreach { useV1 =>
        Seq(true, false).foreach { vectorized =>
          withSQLConf(
            SQLConf.USE_V1_SOURCE_LIST.key -> useV1,
            SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR.key -> "false",
            SQLConf.VARIANT_SHREDDED_PREDICATE_PUSHDOWN_ENABLED.key -> "true",
            SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
            SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorized.toString,
            SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true") {
            withClue(s"(useV1='$useV1', vectorized=$vectorized) ") {
              val e = intercept[SparkException] {
                spark.read.parquet(dir.getAbsolutePath)
                  .selectExpr("variant_get(v, '$.a', 'int') AS a").where("a > 999").collect()
              }
              assert(findCause(e, "INVALID_VARIANT_CAST"),
                s"Expected INVALID_VARIANT_CAST to be preserved, got: $e")
            }
          }
        }
      }
    }
  }

  // Walk an exception's cause chain for a Spark error condition (message substring).
  private def findCause(e: Throwable, condition: String): Boolean = {
    var cur: Throwable = e
    while (cur != null) {
      if (Option(cur.getMessage).exists(_.contains(condition))) return true
      cur = cur.getCause
    }
    false
  }
}
