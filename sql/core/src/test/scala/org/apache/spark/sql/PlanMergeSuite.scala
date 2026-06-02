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

import org.apache.spark.sql.catalyst.optimizer.MergeSubplans
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class PlanMergeSuite extends SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  setupTestData()

  test("Merge non-correlated scalar subqueries") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  (SELECT avg(key) FROM testData),
            |  (SELECT sum(key) FROM testData),
            |  (SELECT count(distinct key) FROM testData)
          """.stripMargin)

        checkAnswer(df, Row(50.5, 5050, 100) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-correlated scalar subqueries in a subquery") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT (
            |  SELECT
            |    SUM(
            |      (SELECT avg(key) FROM testData) +
            |      (SELECT sum(key) FROM testData) +
            |      (SELECT count(distinct key) FROM testData))
            |   FROM testData
            |)
          """.stripMargin)

        checkAnswer(df, Row(520050.0) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 5,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-correlated scalar subqueries from different levels") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  (SELECT avg(key) FROM testData),
            |  (
            |    SELECT
            |      SUM(
            |        (SELECT sum(key) FROM testData)
            |      )
            |    FROM testData
            |  )
          """.stripMargin)

        checkAnswer(df, Row(50.5, 505000) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-correlated scalar subqueries from different parent plans") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  (
            |    SELECT
            |      SUM(
            |        (SELECT avg(key) FROM testData)
            |      )
            |    FROM testData
            |  ),
            |  (
            |    SELECT
            |      SUM(
            |        (SELECT sum(key) FROM testData)
            |      )
            |    FROM testData
            |  )
          """.stripMargin)

        checkAnswer(df, Row(5050.0, 505000) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 4,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-correlated scalar subqueries with conflicting names") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  (SELECT avg(key) AS key FROM testData),
            |  (SELECT sum(key) AS key FROM testData),
            |  (SELECT count(distinct key) AS key FROM testData)
          """.stripMargin)

        checkAnswer(df, Row(50.5, 5050, 100) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-grouping aggregates") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT *
            |FROM (SELECT avg(key) FROM testData)
            |JOIN (SELECT sum(key) FROM testData)
            |JOIN (SELECT count(distinct key) FROM testData)
          """.stripMargin)

        checkAnswer(df, Row(50.5, 5050, 100) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-grouping aggregates from different levels") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  first(avg_key),
            |  (
            |    -- Using `testData2` makes the whole subquery plan non-mergeable to the
            |    -- non-grouping aggregate subplan in the main plan, which uses `testData`, but its
            |    -- aggregate subplan with `sum(key)` is mergeable
            |    SELECT first(sum_key)
            |    FROM (SELECT sum(key) AS sum_key FROM testData)
            |    JOIN testData2
            |  ),
            |  first(count_key)
            |FROM (SELECT avg(key) AS avg_key, count(distinct key) as count_key FROM testData)
            |JOIN testData3
          """.stripMargin)

        checkAnswer(df, Row(50.5, 5050, 100) :: Nil)

        val plan = df.queryExecution.executedPlan

        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-grouping aggregate and subquery") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  first(avg_key),
            |  (
            |    -- In this case the whole scalar subquery plan is mergeable to the non-grouping
            |    -- aggregate subplan in the main plan.
            |    SELECT sum(key) AS sum_key FROM testData
            |  ),
            |  first(count_key)
            |FROM (SELECT avg(key) AS avg_key, count(distinct key) as count_key FROM testData)
            |JOIN testData3
          """.stripMargin)

        checkAnswer(df, Row(50.5, 5050, 100) :: Nil)

        val plan = df.queryExecution.executedPlan

        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("SPARK-40618: Regression test for merging subquery bug with nested subqueries") {
    // This test contains a subquery expression with another subquery expression nested inside.
    // It acts as a regression test to ensure that the MergeScalarSubqueries rule does not attempt
    // to merge them together.
    withTable("t1", "t2") {
      sql("create table t1(col int) using csv")
      checkAnswer(sql("select(select sum((select sum(col) from t1)) from t1)"), Row(null))

      checkAnswer(sql(
        """
          |select
          |  (select sum(
          |    (select sum(
          |        (select sum(col) from t1))
          |     from t1))
          |  from t1)
          |""".stripMargin),
        Row(null))

      sql("create table t2(col int) using csv")
      checkAnswer(sql(
        """
          |select
          |  (select sum(
          |    (select sum(
          |        (select sum(col) from t1))
          |     from t2))
          |  from t1)
          |""".stripMargin),
        Row(null))
    }
  }

  test("SPARK-42346: Rewrite distinct aggregates after merging subqueries") {
    withTempView("t1") {
      Seq((1, 2), (3, 4)).toDF("c1", "c2").createOrReplaceTempView("t1")

      checkAnswer(sql(
        """
          |SELECT
          |  (SELECT count(distinct c1) FROM t1),
          |  (SELECT count(distinct c2) FROM t1)
          |""".stripMargin),
        Row(2, 2))

      // In this case we don't merge the subqueries as `RewriteDistinctAggregates` kicks off for the
      // 2 subqueries first but `MergeScalarSubqueries` is not prepared for the `Expand` nodes that
      // are inserted by the rewrite.
      checkAnswer(sql(
        """
          |SELECT
          |  (SELECT count(distinct c1) + sum(distinct c2) FROM t1),
          |  (SELECT count(distinct c2) + sum(distinct c1) FROM t1)
          |""".stripMargin),
        Row(8, 6))
    }
  }

  test("SPARK-40193: Merge non-grouping scalar subqueries with different filter conditions") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
        val df = sql(
          """
            |SELECT
            |  (SELECT sum(key) FROM testData WHERE key > 50),
            |  (SELECT sum(key) FROM testData WHERE key <= 50)
          """.stripMargin)

        checkAnswer(df, Row(3775, 1275) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 1,
          "Missing or unexpected ReusedSubqueryExec in the plan")
      }
    }
  }

  test("SPARK-40193: Merge non-grouping scalar subqueries where only one has a filter") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
        // ObjectSerializerPruning produces different scan shapes depending on whether a Filter is
        // present. Disabling the rule makes both scans identical so PlanMerger can merge them.
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.ObjectSerializerPruning") {
        val df = sql(
          """
            |SELECT
            |  (SELECT sum(key) FROM testData),
            |  (SELECT sum(key) FROM testData WHERE key > 50)
          """.stripMargin)

        checkAnswer(df, Row(5050, 3775) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 1,
          "Missing or unexpected ReusedSubqueryExec in the plan")
      }
    }
  }

  // ===========================================================================
  // SupportsScanMerging: V2 file-source scan merging, at parity with V1.
  //
  // The merge logic lives in the FileScan base, so it is format-agnostic. Tests are organized in
  // two groups:
  //   1. "merge semantics" -- exhaustive coverage of the merge taxonomy (differing columns /
  //      data filters / partition filters / through-join, plus negatives). These use Parquet as
  //      the representative format; repeating every shape on every format would only re-test the
  //      shared FileScan logic.
  //   2. "format coverage" -- one representative query run across every built-in file format, to
  //      confirm each concrete Scan inherits the merge (its SupportsScanMerging hooks + `options`
  //      wiring work).
  // Each test runs through the V1 and V2 file-source paths (USE_V1_SOURCE_LIST) and asserts they
  // produce identical results and identical merge structure.
  // ===========================================================================

  private val allFileSources = Seq("avro", "csv", "json", "kafka", "orc", "text", "parquet")

  // USE_V1_SOURCE_LIST routes a source through V1 when it is listed (its V2 path is disabled) and
  // through V2 when omitted. `allViaV1` routes every source through V1; `viaV2(fmt)` routes `fmt`
  // through V2 and everything else through V1.
  private val allViaV1: String = allFileSources.mkString(",")
  private def viaV2(format: String): String = allFileSources.filterNot(_ == format).mkString(",")

  // Counts (SubqueryExec, ReusedSubqueryExec) in the executed plan -- a merge
  // collapses N subqueries into 1 SubqueryExec + (N-1) ReusedSubqueryExec.
  private def subqueryCounts(df: DataFrame): (Int, Int) = {
    val plan = df.queryExecution.executedPlan
    val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
    val reusedSubqueryIds = collectWithSubqueries(plan) {
      case rs: ReusedSubqueryExec => rs.child.id
    }
    (subqueryIds.size, reusedSubqueryIds.size)
  }

  /**
   * Runs `queryFn(table)` through the V1 file-source path and the V2 file-source path and
   * asserts that both produce identical results AND identical merge behavior (same
   * SubqueryExec / ReusedSubqueryExec counts). This is the core V1/V2 capability-parity
   * guard: a query that merges under V1 must merge identically under V2, and vice versa.
   *
   * Uses Parquet as the representative format -- the merge logic is format-agnostic (lives in
   * FileScan), so the merge-semantics tests built on this helper need only one format. Per-format
   * coverage is provided separately by the "format coverage" tests.
   */
  private def assertV1V2Parity(
      data: DataFrame,
      query: String => String,
      expected: Seq[Row],
      expectedSubqueries: Int,
      expectedReused: Int,
      extraConfs: Seq[(String, String)] = Seq.empty): Unit = {
    Seq(false, true).foreach { enableAQE =>
      Seq("V1" -> allViaV1, "V2" -> viaV2("parquet")).foreach { case (label, v1List) =>
        withTempPath { path =>
          val pathStr = path.getAbsolutePath
          data.write.mode("overwrite").parquet(pathStr)
          withSQLConf(
            (Seq(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
              SQLConf.USE_V1_SOURCE_LIST.key -> v1List) ++ extraConfs): _*) {
            val df = spark.sql(query(s"parquet.`$pathStr`"))
            withClue(s"[$label, AQE=$enableAQE] results: ") {
              checkAnswer(df, expected)
            }
            val (nSub, nReused) = subqueryCounts(df)
            withClue(s"[$label, AQE=$enableAQE] SubqueryExec count: ") {
              assert(nSub == expectedSubqueries)
            }
            withClue(s"[$label, AQE=$enableAQE] ReusedSubqueryExec count: ") {
              assert(nReused == expectedReused)
            }
          }
        }
      }
    }
  }


  /**
   * Like [[assertV1V2Parity]] but compares the V1 path directly against the V2 path instead of
   * against hard-coded expectations. Asserts:
   *   1. V1 and V2 return identical rows.
   *   2. V1 and V2 have identical merge structure (same SubqueryExec / ReusedSubqueryExec counts).
   *   3. The merge actually fired -- the merged plan has strictly fewer distinct SubqueryExec
   *      than the same query with MergeSubplans excluded. Without this the parity check would
   *      pass vacuously if the optimization silently stopped firing in BOTH paths.
   * Used for composition-style queries where the exact merged counts are not worth predicting --
   * the point is that V2 behaves exactly like V1, and that a merge genuinely happens.
   *
   * Note: the "merge fired" guard checks for a reduction in distinct SubqueryExec, so this helper
   * is only suitable for scalar-subquery / joined-subquery merges (not main-plan merges that
   * leave no SubqueryExec, e.g. UNION-of-aggregates).
   */
  private def assertV1V2Consistent(
      data: DataFrame,
      query: String => String,
      extraConfs: Seq[(String, String)] = Seq.empty,
      format: String = "parquet"): Unit = {
    Seq(false, true).foreach { enableAQE =>
      withTempPath { path =>
        val pathStr = path.getAbsolutePath
        data.write.mode("overwrite").format(format).save(pathStr)
        def run(v1List: String, confs: Seq[(String, String)]): (Seq[Row], (Int, Int)) = {
          withSQLConf(
            (Seq(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
              SQLConf.USE_V1_SOURCE_LIST.key -> v1List) ++ confs): _*) {
            val df = spark.sql(query(s"$format.`$pathStr`"))
            (df.collect().toSeq, subqueryCounts(df))
          }
        }
        val (v1Rows, v1Counts) = run(allViaV1, extraConfs)
        val (v2Rows, v2Counts) = run(viaV2(format), extraConfs)
        // Baseline with the merge rule excluded, to prove the merge actually fired.
        val (_, offCounts) = run(
          allViaV1, extraConfs :+ (SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> MergeSubplans.ruleName))
        withClue(s"[AQE=$enableAQE] V1 vs V2 rows: ") {
          assert(v1Rows.map(_.toString).sorted == v2Rows.map(_.toString).sorted)
        }
        withClue(s"[AQE=$enableAQE] V1 vs V2 (SubqueryExec, ReusedSubqueryExec) counts: ") {
          assert(v1Counts == v2Counts)
        }
        withClue(s"[AQE=$enableAQE] merge did not fire (merged $v1Counts vs off $offCounts): ") {
          assert(v1Counts._1 < offCounts._1)
        }
      }
    }
  }

  test("V1/V2 parity: many scalar subqueries over one relation, differing in filter and " +
      "aggregate") {
    // Integration shape: scalar subqueries over a single relation where some share a filter but
    // compute different aggregates (strict / column-union leaf merge) and others differ in their
    // filter (relaxed / OR-widen leaf merge). 3 buckets x {count, avg(disc), avg(paid)}.
    // (This is the structure of TPC-DS q9.)
    val data = spark.range(60).selectExpr(
      "id + 1 as qty", "cast(id as double) as disc", "cast(id * 2 as double) as paid")
    assertV1V2Consistent(
      data,
      tbl =>
        s"""
           |SELECT
           |  CASE WHEN (SELECT count(*) FROM $tbl WHERE qty BETWEEN 1 AND 20) > 10
           |    THEN (SELECT avg(disc) FROM $tbl WHERE qty BETWEEN 1 AND 20)
           |    ELSE (SELECT avg(paid) FROM $tbl WHERE qty BETWEEN 1 AND 20) END b1,
           |  CASE WHEN (SELECT count(*) FROM $tbl WHERE qty BETWEEN 21 AND 40) > 10
           |    THEN (SELECT avg(disc) FROM $tbl WHERE qty BETWEEN 21 AND 40)
           |    ELSE (SELECT avg(paid) FROM $tbl WHERE qty BETWEEN 21 AND 40) END b2,
           |  CASE WHEN (SELECT count(*) FROM $tbl WHERE qty BETWEEN 41 AND 60) > 10
           |    THEN (SELECT avg(disc) FROM $tbl WHERE qty BETWEEN 41 AND 60)
           |    ELSE (SELECT avg(paid) FROM $tbl WHERE qty BETWEEN 41 AND 60) END b3
         """.stripMargin,
      extraConfs = Seq(
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.FILE_SCAN_MERGE_IGNORE_PUSHED_DATA_FILTERS.key -> "true"))
  }

  // ---- Format coverage -------------------------------------------------------
  // One representative query (differing columns + differing filter, exercising both the strict
  // column-union and relaxed OR-widen leaf paths) run across every built-in file format, to
  // confirm each concrete FileScan inherits the merge. Parquet is included so the same shape is
  // checked uniformly. CSV/Text are omitted because their all-string round-trip is not
  // type-faithful for these aggregates; their scans share the identical FileScan merge path.
  Seq("parquet", "orc", "json").foreach { fmt =>
    test(s"V1/V2 parity, format coverage ($fmt): differing columns and filter") {
      val data = spark.range(100).selectExpr(
        "id as a", "id * 2 as b", "cast(id % 5 as int) as c")
      assertV1V2Consistent(
        data,
        tbl =>
          s"""
             |SELECT
             |  (SELECT sum(a) FROM $tbl WHERE c = 1),
             |  (SELECT sum(b) FROM $tbl WHERE c = 2)
           """.stripMargin,
        extraConfs = Seq(
          SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true",
          SQLConf.FILE_SCAN_MERGE_IGNORE_PUSHED_DATA_FILTERS.key -> "true"),
        format = fmt)
    }
  }

  test("V1/V2 parity: merge subqueries differing only in projected columns") {
    val data = spark.range(100).selectExpr("id as a", "id * 2 as b", "id % 5 as c")
    assertV1V2Parity(
      data,
      tbl =>
        s"""
           |SELECT
           |  (SELECT sum(a) FROM $tbl WHERE c = 1),
           |  (SELECT sum(b) FROM $tbl WHERE c = 1)
         """.stripMargin,
      expected = Row(
        (0 until 100).filter(_ % 5 == 1).sum,
        (0 until 100).filter(_ % 5 == 1).map(_ * 2L).sum) :: Nil,
      expectedSubqueries = 1,
      expectedReused = 1)
  }

  test("V1/V2 parity: merge subqueries differing in filter (symmetric propagation)") {
    val data = spark.range(100).selectExpr("id as a", "id % 5 as c")
    assertV1V2Parity(
      data,
      tbl =>
        s"""
           |SELECT
           |  (SELECT sum(a) FROM $tbl WHERE c = 1),
           |  (SELECT sum(a) FROM $tbl WHERE c = 2)
         """.stripMargin,
      expected = Row(
        (0 until 100).filter(_ % 5 == 1).sum,
        (0 until 100).filter(_ % 5 == 2).sum) :: Nil,
      expectedSubqueries = 1,
      expectedReused = 1,
      extraConfs = Seq(
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.FILE_SCAN_MERGE_IGNORE_PUSHED_DATA_FILTERS.key -> "true"))
  }

  test("V1/V2 parity: merge subqueries differing in BOTH column and filter") {
    val data = spark.range(100).selectExpr("id as a", "id * 2 as b", "id % 5 as c")
    assertV1V2Parity(
      data,
      tbl =>
        s"""
           |SELECT
           |  (SELECT sum(a) FROM $tbl WHERE c = 1),
           |  (SELECT sum(b) FROM $tbl WHERE c = 2)
         """.stripMargin,
      expected = Row(
        (0 until 100).filter(_ % 5 == 1).sum,
        (0 until 100).filter(_ % 5 == 2).map(_ * 2L).sum) :: Nil,
      expectedSubqueries = 1,
      expectedReused = 1,
      extraConfs = Seq(
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.FILE_SCAN_MERGE_IGNORE_PUSHED_DATA_FILTERS.key -> "true"))
  }

  test("V1/V2 parity: no merge for filter difference when symmetric propagation off") {
    val data = spark.range(100).selectExpr("id as a", "id % 5 as c")
    assertV1V2Parity(
      data,
      tbl =>
        s"""
           |SELECT
           |  (SELECT sum(a) FROM $tbl WHERE c = 1),
           |  (SELECT sum(a) FROM $tbl WHERE c = 2)
         """.stripMargin,
      expected = Row(
        (0 until 100).filter(_ % 5 == 1).sum,
        (0 until 100).filter(_ % 5 == 2).sum) :: Nil,
      expectedSubqueries = 2,
      expectedReused = 0,
      extraConfs = Seq(
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "false"))
  }

  test("V1/V2 parity: merge subqueries differing in partition filter") {
    val expP1 = (0 until 100).filter(_ % 5 == 1).sum
    val expP2 = (0 until 100).filter(_ % 5 == 2).sum
    Seq(false, true).foreach { enableAQE =>
      Seq("V1" -> allViaV1, "V2" -> viaV2("parquet")).foreach { case (label, v1List) =>
        withTempPath { path =>
          val pathStr = path.getAbsolutePath
          spark.range(100).selectExpr("id as a", "id % 5 as p")
            .write.mode("overwrite").partitionBy("p").parquet(pathStr)
          withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
            SQLConf.USE_V1_SOURCE_LIST.key -> v1List,
            SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true",
            SQLConf.FILE_SCAN_MERGE_IGNORE_PUSHED_DATA_FILTERS.key -> "true") {
            val df = spark.sql(
              s"""
                |SELECT
                |  (SELECT sum(a) FROM parquet.`$pathStr` WHERE p = 1),
                |  (SELECT sum(a) FROM parquet.`$pathStr` WHERE p = 2)
              """.stripMargin)
            withClue(s"[$label, AQE=$enableAQE] results: ") {
              checkAnswer(df, Row(expP1, expP2) :: Nil)
            }
          }
        }
      }
    }
  }

  test("V1/V2 parity: same partition filter, differing data filter (relaxed path, partitioned)") {
    // Both subqueries share the partition filter (p = 1) but differ in a data-column filter
    // (c = 1 vs c = 2) over PARTITIONED data. This is the relaxed leaf-merge path on partitioned
    // input: only the data filter is OR-widened; the (equal) partition filter is pushed as-is.
    // Expected: p=1 (odd ids) AND c=1 -> ids 1,11,...,91; p=1 AND c=2 -> ids 7,17,...,97.
    val expC1 = (0 until 100).filter(id => id % 2 == 1 && id % 5 == 1).sum
    val expC2 = (0 until 100).filter(id => id % 2 == 1 && id % 5 == 2).sum
    Seq(false, true).foreach { enableAQE =>
      withTempPath { path =>
        val pathStr = path.getAbsolutePath
        spark.range(100)
          .selectExpr("id as a", "cast(id % 5 as int) as c", "cast(id % 2 as int) as p")
          .write.mode("overwrite").partitionBy("p").parquet(pathStr)
        def run(v1List: String, extra: Seq[(String, String)]): (Seq[Row], (Int, Int)) =
          withSQLConf(
            (Seq(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
              SQLConf.USE_V1_SOURCE_LIST.key -> v1List,
              SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true",
              SQLConf.FILE_SCAN_MERGE_IGNORE_PUSHED_DATA_FILTERS.key -> "true") ++ extra): _*) {
            val df = spark.sql(
              s"""
                |SELECT
                |  (SELECT sum(a) FROM parquet.`$pathStr` WHERE p = 1 AND c = 1),
                |  (SELECT sum(a) FROM parquet.`$pathStr` WHERE p = 1 AND c = 2)
              """.stripMargin)
            withClue(s"[AQE=$enableAQE, $v1List] results: ") {
              checkAnswer(df, Row(expC1, expC2) :: Nil)
            }
            (df.collect().toSeq, subqueryCounts(df))
          }
        val (_, v1Counts) = run(allViaV1, Seq.empty)
        val (_, v2Counts) = run(viaV2("parquet"), Seq.empty)
        val (_, offCounts) =
          run(allViaV1, Seq(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> MergeSubplans.ruleName))
        withClue(s"[AQE=$enableAQE] V1 vs V2 merge counts: ") { assert(v1Counts == v2Counts) }
        withClue(s"[AQE=$enableAQE] merge did not fire ($v1Counts vs off $offCounts): ") {
          assert(v1Counts._1 < offCounts._1)
        }
      }
    }
  }

  test("V1/V2 parity: joined aggregate subqueries differing in a join-side filter (through-join)") {
    // Integration shape: several count(*) subqueries over the same fact joined to dimensions,
    // each differing only in a filter on a DIMENSION (here `td.t_hour`). The differing filter is
    // on a join input, so merging requires through-join filter propagation. Under V2 the dimension
    // scans differ in their pushed filter (OR-widened by the leaf merge) while the fact/other-dim
    // scans merge strictly -- exercising the V2 leaf merge together with join-crossing propagation.
    // (This is the structure of TPC-DS q88.)
    Seq(false, true).foreach { enableAQE =>
      withTempDir { dir =>
        val ssPath = new java.io.File(dir, "ss").getAbsolutePath
        val hdPath = new java.io.File(dir, "hd").getAbsolutePath
        val tdPath = new java.io.File(dir, "td").getAbsolutePath
        // td: t_time_sk 0..29, hours cycling 8/9/10.
        spark.range(30).selectExpr("id as t_time_sk", "8 + cast(id % 3 as int) as t_hour")
          .write.mode("overwrite").parquet(tdPath)
        // hd: demo 0..4, dep_count = demo.
        spark.range(5).selectExpr("id as hd_demo_sk", "cast(id as int) as hd_dep_count")
          .write.mode("overwrite").parquet(hdPath)
        // ss: fact linking to td and hd.
        spark.range(300).selectExpr(
          "cast(id % 30 as int) as ss_sold_time_sk", "cast(id % 5 as int) as ss_hdemo_sk")
          .write.mode("overwrite").parquet(ssPath)

        def run(v1List: String, extra: Seq[(String, String)]): (Seq[String], (Int, Int)) =
          withSQLConf(
            (Seq(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
              SQLConf.USE_V1_SOURCE_LIST.key -> v1List,
              SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true",
              SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true",
              SQLConf.FILE_SCAN_MERGE_IGNORE_PUSHED_DATA_FILTERS.key -> "true") ++ extra): _*) {
            def bucket(hour: Int): String =
              s"""(SELECT count(*) FROM parquet.`$ssPath` ss, parquet.`$hdPath` hd,
                 |  parquet.`$tdPath` td
                 | WHERE ss.ss_hdemo_sk = hd.hd_demo_sk AND ss.ss_sold_time_sk = td.t_time_sk
                 |   AND hd.hd_dep_count = 3 AND td.t_hour = $hour)""".stripMargin
            val df = spark.sql(
              s"SELECT ${bucket(8)} h8, ${bucket(9)} h9, ${bucket(10)} h10")
            (df.collect().map(_.toString).sorted.toSeq, subqueryCounts(df))
          }
        val (v1Rows, v1Counts) = run(allViaV1, Seq.empty)
        val (v2Rows, v2Counts) = run(viaV2("parquet"), Seq.empty)
        val (_, offCounts) =
          run(allViaV1, Seq(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> MergeSubplans.ruleName))
        withClue(s"[AQE=$enableAQE] V1 vs V2 rows: ") { assert(v1Rows == v2Rows) }
        withClue(s"[AQE=$enableAQE] V1 vs V2 merge counts: ") { assert(v1Counts == v2Counts) }
        withClue(s"[AQE=$enableAQE] merge did not fire (merged $v1Counts vs off $offCounts): ") {
          assert(v1Counts._1 < offCounts._1)
        }
      }
    }
  }

  test("SPARK-56677: Merge scalar subqueries with filter propagation through Join") {
    // subquery1 has no filter; subquery2 filters on b > 1 (a column from the right side of the join
    // that is not part of the join condition). Predicate pushdown can only push this filter to
    // testData2, not to testData, so only the right child differs between the two subqueries.
    Seq(false, true).foreach { enableAQE =>
      Seq(true, false).foreach { filterPropagationThroughJoinEnabled =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
          SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key ->
            filterPropagationThroughJoinEnabled.toString,
          // ObjectSerializerPruning produces different scan shapes depending on whether a Filter is
          // present. Disabling the rule makes both scans identical so PlanMerger can merge them.
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
            "org.apache.spark.sql.catalyst.optimizer.ObjectSerializerPruning") {
          val df = sql(
            """
              |SELECT
              |  (SELECT sum(key) FROM testData JOIN testData2 ON key = a),
              |  (SELECT sum(key) FROM testData JOIN testData2 ON key = a WHERE b > 1)
            """.stripMargin)

          checkAnswer(df, Row(12, 6) :: Nil)

          val plan = df.queryExecution.executedPlan
          val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
          val reusedSubqueryIds = collectWithSubqueries(plan) {
            case rs: ReusedSubqueryExec => rs.child.id
          }

          if (filterPropagationThroughJoinEnabled) {
            assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
            assert(reusedSubqueryIds.size == 1,
              "Missing or unexpected ReusedSubqueryExec in the plan")
          } else {
            assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
            assert(reusedSubqueryIds.size == 0,
              "Missing or unexpected ReusedSubqueryExec in the plan")
          }
        }
      }
    }
  }
}
