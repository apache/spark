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

package org.apache.spark.sql.execution.joins

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.planning.{IntervalOverlapJoin, LessPartialRangeJoin}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftOuter, LeftSemi}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DoubleType

/**
 * Planner and end-to-end coverage for broadcast range join.
 *
 * Which condition is recognized: `ExtractRangeJoinKeysSuite`.
 * What the index returns: `RangeIndexSuite`.
 * What the operator returns: `RangeJoinSuite`.
 * This suite checks that the planner chooses the operator, and that the SQL
 * result matches broadcast nested loop join where that comparison is the point.
 */
class RangeJoinSQLSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {

  private val pointCond =
    "s.ip_add_int >= l.BEGIN_IP2LONG AND s.ip_add_int <= l.END_IP2LONG"

  private def findRangeJoin(plan: SparkPlan): Seq[BroadcastRangeJoinExec] = {
    collect(plan) { case j: BroadcastRangeJoinExec => j }
  }

  /** `n` range joins sit in one whole-stage, not in nested stages or input adapters. */
  private def oneCodegenStageOwns(plan: SparkPlan, n: Int): Boolean = {
    exists(plan) {
      case w: WholeStageCodegenExec =>
        def count(p: SparkPlan): Int = p match {
          case _: InputAdapter | _: WholeStageCodegenExec => 0
          case j: BroadcastRangeJoinExec => 1 + j.children.map(count).sum
          case other => other.children.map(count).sum
        }
        count(w.child) == n
      case _ => false
    }
  }

  /** The range join is compiled into a whole-stage, not hidden under an input adapter. */
  private def rangeJoinIsCodegen(plan: SparkPlan): Boolean = {
    exists(plan) {
      case w: WholeStageCodegenExec =>
        def contains(p: SparkPlan): Boolean = p match {
          case _: InputAdapter => false
          case _: BroadcastRangeJoinExec => true
          case other => other.children.exists(contains)
        }
        contains(w.child)
      case _ => false
    }
  }

  private def findBroadcastNestedLoopJoin(plan: SparkPlan): Seq[BroadcastNestedLoopJoinExec] = {
    collect(plan) { case j: BroadcastNestedLoopJoinExec => j }
  }

  private def findCartesianProduct(plan: SparkPlan): Seq[CartesianProductExec] = {
    collect(plan) { case c: CartesianProductExec => c }
  }

  private def withRangeJoin(extra: (String, String)*)(f: => Unit): Unit = {
    withSQLConf((Seq(SQLConf.BROADCAST_RANGE_JOIN_ENABLED.key -> "true") ++ extra): _*)(f)
  }

  /**
   * One query per key family and join type that a range join can plan, shared by
   * "whole-stage codegen covers each range-join probe" and "interpreted execution
   * covers each range-join probe" so the two coverage lists cannot drift apart.
   */
  private def probeCoverageQueries: Seq[String] = Seq(
    s"SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l ON $pointCond",
    s"SELECT s.id, l.zip_id FROM signin_ip s LEFT JOIN ip_lookup l ON $pointCond",
    s"SELECT s.id, l.zip_id FROM signin_ip s RIGHT JOIN ip_lookup l ON $pointCond",
    s"SELECT s.id FROM signin_ip s LEFT SEMI JOIN ip_lookup l ON $pointCond",
    s"SELECT s.id FROM signin_ip s LEFT ANTI JOIN ip_lookup l ON $pointCond",
    "SELECT p.p, r.lo, r.hi FROM dec_points p JOIN dec_ranges r ON p.p >= r.lo AND p.p <= r.hi",
    "SELECT /*+ BROADCAST(r) */ p.s, r.lo, r.hi FROM str_points p JOIN str_ranges r " +
      "ON p.s >= r.lo AND p.s <= r.hi",
    "SELECT a.lo, a.hi, b.lo, b.hi FROM intervals_a a JOIN intervals_b b " +
      "ON a.lo < b.hi AND b.lo < a.hi",
    "SELECT a.lo, a.hi, b.lo, b.hi FROM intervals_a a LEFT JOIN intervals_b b ON a.hi < b.lo",
    "SELECT /*+ BROADCAST(a) */ a.lo, b.hi FROM intervals_a a JOIN intervals_b b ON a.lo < b.hi",
    // DOUBLE keys stay on the boxed interval index (see setupDoubleIntervalViews), unlike
    // the INT two-key overlap above, which is long-specialized.
    "SELECT a.lo, a.hi, b.lo, b.hi FROM dbl_ranges_a a JOIN dbl_ranges_b b " +
      "ON a.lo < b.hi AND b.lo < a.hi",
    // Double and string keys stay on the boxed point index, so these queries reach
    // its generated probe code (or, interpreted, the per-index dispatch in
    // BroadcastRangeJoinExec.makeCandidateFn), which int partial ranges never do.
    // The partial-range fixtures include probe values equal to build keys, so an
    // inclusive (<=/>=) scan wired to its exclusive counterpart by mistake would
    // drop those boundary matches.
    "SELECT p.d, b.lo FROM dbl_partial_points p JOIN dbl_partial_bounds b ON p.d < b.lo",
    "SELECT p.d, b.lo FROM dbl_partial_bounds b JOIN dbl_partial_points p ON p.d > b.lo",
    "SELECT p.d, b.lo FROM dbl_partial_points p JOIN dbl_partial_bounds b ON p.d <= b.lo",
    "SELECT p.d, b.lo FROM dbl_partial_bounds b JOIN dbl_partial_points p ON p.d >= b.lo",
    "SELECT p.s, b.lo FROM str_partial_points p JOIN str_partial_bounds b ON p.s < b.lo",
    "SELECT p.s, b.lo FROM str_partial_bounds b JOIN str_partial_points p ON p.s > b.lo",
    "SELECT p.s, b.lo FROM str_partial_points p JOIN str_partial_bounds b ON p.s <= b.lo",
    "SELECT p.s, b.lo FROM str_partial_bounds b JOIN str_partial_points p ON p.s >= b.lo")

  private def setupIpLookupViews(): Unit = {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW signin_ip(id, ip_add_int) AS VALUES
        |  ('1', 1467630666L),
        |  ('2', 1186929717L),
        |  ('3', 3501544352L),
        |  ('31', 3501544320L),
        |  ('32', 3501544383L),
        |  ('33', 3501544330L),
        |  ('34', 3501544390L),
        |  ('4', 1626243295L),
        |  ('41', 1626243072L),
        |  ('42', 1626243327L),
        |  ('5', 1206355995L),
        |  ('5', 1206355995L),
        |  ('6', CAST(NULL AS BIGINT)),
        |  ('7', 0L)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW ip_lookup(
        |    BEGIN_IP2LONG, END_IP2LONG, TWO_CHAR_CNTRY_CD, zip_id) AS VALUES
        |  (1467630592L, 1467630847L, 'de', 2380520L),
        |  (1186929664L, 1186930175L, 'us', 6723936L),
        |  (1186929664L, 1186930175L, 'us', 6723936L),
        |  (3501544320L, 3501544383L, 'ca', 12804631L),
        |  (3501544330L, 3501544390L, 'ca', 12804631001L),
        |  (1626243072L, 1626243327L, 'us', 8849753L),
        |  (1206355968L, 1206356223L, 'us', 10138801L),
        |  (CAST(NULL AS BIGINT), 1206356223L, 'us', 10138801L),
        |  (1206355968L, CAST(NULL AS BIGINT), 'us', 10138801L),
        |  (CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), 'us', 10138801L)
        |""".stripMargin)
  }

  private def setupIntervalViews(): Unit = {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW intervals_a(lo, hi) AS VALUES
        |  (-1, 0), (0, 1), (0, 2), (1, 5)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW intervals_b(lo, hi) AS VALUES
        |  (-2, -1), (-4, -2), (1, 3), (5, 7)
        |""".stripMargin)
  }

  /**
   * Two-column DOUBLE ranges, so a two-key interval-overlap query here builds a boxed
   * `IntervalIndex` (DoubleType has no bit-preserving long encoding) with both bounds
   * supplied by the streamed side - the `case intervals: IntervalIndex` branch of
   * `BroadcastRangeJoinExec.makeCandidateFn` that `intervals_a`/`intervals_b` (INT,
   * long-specialized) never reaches.
   */
  private def setupDoubleIntervalViews(): Unit = {
    // An unsuffixed literal like `1.0` resolves to DECIMAL, not DOUBLE (only `1.0D` or a
    // cast does), and DECIMAL(2, 1) maps to LongIntervalIndex, defeating the point of this
    // view. The `D` suffix keeps these columns on the boxed IntervalIndex path.
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dbl_ranges_a(lo, hi) AS VALUES
        |  (-1.0D, 0.0D), (0.0D, 1.0D), (0.0D, 2.0D), (1.0D, 5.0D)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dbl_ranges_b(lo, hi) AS VALUES
        |  (-2.0D, -1.0D), (-4.0D, -2.0D), (1.0D, 3.0D), (5.0D, 7.0D)
        |""".stripMargin)
    assert(sql("SELECT lo, hi FROM dbl_ranges_a").schema.fields.forall(_.dataType == DoubleType))
    assert(sql("SELECT lo, hi FROM dbl_ranges_b").schema.fields.forall(_.dataType == DoubleType))
  }

  private def setupDecimalViews(): Unit = {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dec_ranges(lo, hi) AS VALUES
        |  (CAST(0.00 AS DECIMAL(10, 2)), CAST(2.50 AS DECIMAL(10, 2))),
        |  (CAST(3.00 AS DECIMAL(10, 2)), CAST(5.00 AS DECIMAL(10, 2)))
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dec_points(p) AS VALUES
        |  (CAST(1.25 AS DECIMAL(10, 2))),
        |  (CAST(3.00 AS DECIMAL(10, 2))),
        |  (CAST(9.00 AS DECIMAL(10, 2)))
        |""".stripMargin)
  }

  private def setupCollatedStrings(): Unit = {
    // 'M' and 'm' compare equal under UNICODE_CI and differ as bytes.
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW str_ranges(lo, hi) AS VALUES
        |  (CAST('a' AS STRING COLLATE UNICODE_CI), CAST('a' AS STRING COLLATE UNICODE_CI)),
        |  (CAST('b' AS STRING COLLATE UNICODE_CI), CAST('b' AS STRING COLLATE UNICODE_CI)),
        |  (CAST('c' AS STRING COLLATE UNICODE_CI), CAST('M' AS STRING COLLATE UNICODE_CI)),
        |  (CAST('m' AS STRING COLLATE UNICODE_CI), CAST('z' AS STRING COLLATE UNICODE_CI))
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW str_points(s) AS VALUES
        |  (CAST('M' AS STRING COLLATE UNICODE_CI)),
        |  (CAST('m' AS STRING COLLATE UNICODE_CI))
        |""".stripMargin)
  }

  /**
   * Points and bounds for single-inequality joins on boxed key families. Each points
   * view includes values equal to a bound, so the inclusive (<=/>=) queries in
   * `probeCoverageQueries` exercise the boundary, not just the strict (</>) ones.
   */
  private def setupPartialRangeViews(): Unit = {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dbl_partial_points(d) AS VALUES
        |  (0.5), (1.0), (3.0), (4.0), (6.0), (7.5),
        |  (CAST(NULL AS DOUBLE))
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dbl_partial_bounds(lo) AS VALUES
        |  (1.0), (4.0), (7.5)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW str_partial_points(s) AS VALUES
        |  ('a'), ('b'), ('m'), ('n'), ('z')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW str_partial_bounds(lo) AS VALUES
        |  ('b'), ('n')
        |""".stripMargin)
  }

  private def setupDateAndTimestampViews(): Unit = {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dt_ranges(lo, hi) AS VALUES
        |  (DATE'2024-01-01', DATE'2024-01-31'),
        |  (DATE'2024-06-01', DATE'2024-06-30')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dt_points(d) AS VALUES
        |  (DATE'2024-01-15'),
        |  (DATE'2024-05-01'),
        |  (DATE'2024-06-15')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW ts_ranges(lo, hi) AS VALUES
        |  (TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-01 12:00:00')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW ts_points(t) AS VALUES
        |  (TIMESTAMP'2024-01-01 06:00:00'),
        |  (TIMESTAMP'2024-01-01 18:00:00')
        |""".stripMargin)
  }

  test("range join is disabled by default and falls back to nested loop join") {
    setupIpLookupViews()
    val df = sql(s"SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l ON $pointCond")
    assert(findRangeJoin(df.queryExecution.executedPlan).isEmpty)
    assert(findBroadcastNestedLoopJoin(df.queryExecution.executedPlan).size == 1)
  }

  test("point-in-range matches the nested loop result") {
    setupIpLookupViews()
    val query = s"SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l ON $pointCond"
    val expected = sql(query).collect()
    withRangeJoin() {
      val df = sql(query)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, expected)
    }
  }

  test("BETWEEN is planned as a point-in-range join") {
    setupIpLookupViews()
    val query =
      "SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l " +
        "ON s.ip_add_int BETWEEN l.BEGIN_IP2LONG AND l.END_IP2LONG"
    val expected = sql(query).collect()
    withRangeJoin(SQLConf.ALWAYS_INLINE_COMMON_EXPR.key -> "false") {
      val df = sql(query)
      val planned = findRangeJoin(df.queryExecution.executedPlan)
      assert(planned.size == 1)
      // BETWEEN lowers to one point expression. The other side keeps (low, high).
      val keyCounts = Set(planned.head.leftKeys.length, planned.head.rightKeys.length)
      assert(keyCounts == Set(1, 2))
      checkAnswer(df, expected)
    }
  }

  test("hints select range join, nested loop, or cartesian") {
    setupIpLookupViews()
    setupIntervalViews()
    val expected = sql(
      s"SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l ON $pointCond").collect()
    withRangeJoin() {
      val broadcast = sql(
        "SELECT /*+ BROADCAST(l) */ s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l " +
          s"ON $pointCond")
      val joins = findRangeJoin(broadcast.queryExecution.executedPlan)
      assert(joins.size == 1 && joins.head.buildSide == BuildRight)
      checkAnswer(broadcast, expected)

      val dualHintInner = sql(
        "SELECT /*+ BROADCAST(s), BROADCAST(l) */ s.id, l.zip_id " +
          s"FROM signin_ip s JOIN ip_lookup l ON $pointCond")
      val dualJoins = findRangeJoin(dualHintInner.queryExecution.executedPlan)
      assert(dualJoins.size == 1 && dualJoins.head.buildSide == BuildRight)
      checkAnswer(dualHintInner, expected)

      val dualHintLeftOuter = sql(
        "SELECT /*+ BROADCAST(s), BROADCAST(l) */ s.id, l.zip_id " +
          s"FROM signin_ip s LEFT JOIN ip_lookup l ON $pointCond")
      val dualOuterJoins = findRangeJoin(dualHintLeftOuter.queryExecution.executedPlan)
      assert(dualOuterJoins.size == 1 && dualOuterJoins.head.buildSide == BuildRight)

      val unsupported = sql(
        "SELECT /*+ BROADCAST(a) */ a.lo, b.hi FROM intervals_a a LEFT JOIN intervals_b b " +
          "ON a.lo < b.hi")
      val loops = findBroadcastNestedLoopJoin(unsupported.queryExecution.executedPlan)
      assert(findRangeJoin(unsupported.queryExecution.executedPlan).isEmpty)
      assert(loops.size == 1 && loops.head.buildSide == BuildLeft)

      val cartesian = sql(
        "SELECT /*+ SHUFFLE_REPLICATE_NL(s) */ s.id, l.zip_id " +
          s"FROM signin_ip s JOIN ip_lookup l ON $pointCond")
      val plan = cartesian.queryExecution.executedPlan
      assert(findRangeJoin(plan).isEmpty)
      assert(findCartesianProduct(plan).size == 1)
    }
  }

  test("supported join types match nested loop; full outer does not") {
    setupIpLookupViews()
    Seq(
      s"SELECT s.id, l.zip_id FROM signin_ip s LEFT JOIN ip_lookup l ON $pointCond",
      s"SELECT s.id, l.zip_id FROM signin_ip s RIGHT JOIN ip_lookup l ON $pointCond",
      s"SELECT s.id FROM signin_ip s LEFT SEMI JOIN ip_lookup l ON $pointCond",
      s"SELECT s.id FROM signin_ip s LEFT ANTI JOIN ip_lookup l ON $pointCond"
    ).foreach { query =>
      val expected = sql(query).collect()
      withRangeJoin() {
        val df = sql(query)
        assert(findRangeJoin(df.queryExecution.executedPlan).size == 1, query)
        checkAnswer(df, expected)
      }
    }
    val fullOuter =
      s"SELECT s.id, l.zip_id FROM signin_ip s FULL OUTER JOIN ip_lookup l ON $pointCond"
    withRangeJoin() {
      assert(findRangeJoin(sql(fullOuter).queryExecution.executedPlan).isEmpty)
    }
  }

  test("partial range left outer broadcasts the right side") {
    setupIntervalViews()
    // No b.lo is strictly greater than a.hi (5), so the left row (1, 5) is kept with nulls.
    val query =
      "SELECT a.lo, a.hi, b.lo, b.hi FROM intervals_a a LEFT JOIN intervals_b b " +
        "ON a.hi < b.lo"
    val expected = sql(query).collect()
    assert(expected.exists(_.anyNull))
    withRangeJoin() {
      val df = sql(query)
      val planned = findRangeJoin(df.queryExecution.executedPlan)
      assert(planned.size == 1)
      assert(planned.head.joinType == LeftOuter)
      assert(planned.head.buildSide == BuildRight)
      assert(planned.head.rangeJoinKind == LessPartialRangeJoin(strict = true))
      checkAnswer(df, expected)
    }
  }

  test("strict inequalities over duplicated build keys match the nested loop join") {
    // 10,000 build rows over 100 distinct integer keys, 100 per key. Under `<`, a point
    // index that kept the probe's own key would surface 100 duplicates that
    // all fail the condition before the first passing row; the strict scan
    // must skip them, and semi/anti must still match the nested loop answers.
    sql("CREATE OR REPLACE TEMP VIEW dup_keys(k) AS SELECT id % 100 FROM range(10000)")
    sql("CREATE OR REPLACE TEMP VIEW probe_keys(k) AS SELECT id FROM range(100)")
    val operators = Seq("<", "<=", ">", ">=")
    val joinTypes = Seq("JOIN", "LEFT SEMI JOIN", "LEFT ANTI JOIN")
    for (op <- operators; joinType <- joinTypes) {
      // BROADCAST(b) pins dup_keys as the build side (BuildRight). Left semi/anti
      // already require BuildRight, but a plain JOIN would otherwise pick probe_keys
      // (the smaller side) as build, leaving the duplicated keys on the streamed side.
      val query =
        s"SELECT /*+ BROADCAST(b) */ p.k FROM probe_keys p $joinType dup_keys b ON p.k $op b.k"
      val expected = sql(query).collect()
      withRangeJoin() {
        val df = sql(query)
        assert(findRangeJoin(df.queryExecution.executedPlan).size == 1, query)
        checkAnswer(df, expected)
      }
    }
    // Inner joins can also build the left side; repeat with dup_keys on the left and
    // pinned as build so the strict scan is exercised under BuildLeft too.
    for (op <- operators) {
      val query =
        s"SELECT /*+ BROADCAST(b) */ p.k FROM dup_keys b JOIN probe_keys p ON b.k $op p.k"
      val expected = sql(query).collect()
      withRangeJoin() {
        val df = sql(query)
        val joins = findRangeJoin(df.queryExecution.executedPlan)
        assert(joins.size == 1 && joins.head.buildSide == BuildLeft, query)
        checkAnswer(df, expected)
      }
    }
  }

  test("a residual predicate is still evaluated") {
    sql("CREATE OR REPLACE TEMP VIEW complex_a(id, lo, hi) AS VALUES (1, 0, 10), (2, 0, 10)")
    sql("CREATE OR REPLACE TEMP VIEW complex_b(id, point) AS VALUES (1, 5), (2, 5)")
    // Both ranges cover both points, so the range predicate matches all 4 pairs.
    // a.id <> b.id is not an equi-join key and filters that down to 2.
    val query =
      "SELECT a.id, b.id FROM complex_a a JOIN complex_b b " +
        "ON b.point >= a.lo AND b.point <= a.hi AND a.id <> b.id"
    val expected = sql(query).collect()
    assert(expected.length == 2)
    withRangeJoin() {
      val df = sql(query)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, expected)
    }
  }

  test("interval overlap is planned with two keys on each side") {
    setupIntervalViews()
    val query =
      "SELECT a.lo, a.hi, b.lo, b.hi FROM intervals_a a JOIN intervals_b b " +
        "ON a.lo < b.hi AND b.lo < a.hi"
    val expected = sql(query).collect()
    withRangeJoin() {
      val df = sql(query)
      val planned = findRangeJoin(df.queryExecution.executedPlan)
      assert(planned.size == 1)
      assert(planned.head.rangeJoinKind == IntervalOverlapJoin)
      assert(planned.head.leftKeys.length == 2)
      assert(planned.head.rightKeys.length == 2)
      checkAnswer(df, expected)
    }
  }

  test("interval overlap with an inverted interval matches the nested loop result") {
    sql("CREATE OR REPLACE TEMP VIEW inverted_a(lo, hi) AS VALUES (0, 5), (1, 2), (-4, 10)")
    sql("CREATE OR REPLACE TEMP VIEW inverted_b(lo, hi) AS VALUES (3, 1), (7, 9)")
    val query =
      "SELECT a.lo, a.hi, b.lo, b.hi FROM inverted_a a " +
        "JOIN inverted_b b ON a.lo < b.hi AND b.lo < a.hi"
    // (3, 1) accepts rows with a.lo < 1 and a.hi > 3, so it has to reach the result.
    val expected = sql(query).collect()
    assert(expected.exists(r => r.getInt(2) == 3 && r.getInt(3) == 1), expected.toSeq)
    assert(expected.length == 3, expected.toSeq)
    // Broadcast each side in turn so the inverted interval lands on the index once.
    Seq("/*+ BROADCAST(a) */", "/*+ BROADCAST(b) */").foreach { hint =>
      val hinted = query.replace("SELECT ", s"SELECT $hint ")
      withRangeJoin() {
        val df = sql(hinted)
        assert(findRangeJoin(df.queryExecution.executedPlan).size == 1, hinted)
        checkAnswer(df, expected)
      }
    }
  }

  test("a scalar subquery in the condition is planned") {
    setupIpLookupViews()
    // The extra conjunct names both sides, so it stays on the join. The subquery
    // has to be planned from that condition.
    val query =
      "SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l " +
        s"ON $pointCond " +
        "AND s.ip_add_int + l.zip_id > (SELECT min(ip_add_int) FROM signin_ip)"
    val expected = sql(query).collect()
    withRangeJoin() {
      val df = sql(query)
      val planned = findRangeJoin(df.queryExecution.executedPlan)
      assert(planned.size == 1)
      val cond = planned.head.condition.get
      val subquery = cond.collectFirst { case s: ScalarSubquery => s.plan }
      assert(subquery.exists(plan =>
        plan.isInstanceOf[SubqueryExec] || plan.isInstanceOf[ReusedSubqueryExec]), cond)
      checkAnswer(df, expected)
    }
  }

  test("collated strings match the nested loop result on either build side") {
    setupCollatedStrings()
    val cond = "p.s >= r.lo AND p.s <= r.hi"
    Seq(
      (s"SELECT /*+ BROADCAST(r) */ p.s, r.lo, r.hi FROM str_points p JOIN str_ranges r ON $cond",
        BuildRight),
      (s"SELECT /*+ BROADCAST(p) */ p.s, r.lo, r.hi FROM str_points p JOIN str_ranges r ON $cond",
        BuildLeft)
    ).foreach { case (query, buildSide) =>
      val expected = sql(query).collect()
      // Each point matches the interval ending at 'M' and the one starting at 'm'.
      assert(expected.length == 4, query)
      withRangeJoin() {
        val df = sql(query)
        val joins = findRangeJoin(df.queryExecution.executedPlan)
        assert(joins.size == 1 && joins.head.buildSide == buildSide, query)
        checkAnswer(df, expected)
      }
    }
  }

  test("CHAR and VARCHAR range joins match the nested loop result, including " +
      "trailing-space-only values") {
    // CHAR(n) is blank-padded on read (ApplyCharTypePadding), so this only exercises the
    // index's own ordering if the padding survives into the probe: a temp view backed by
    // VALUES never carries the char/varchar catalog metadata that padding keys off, so this
    // needs real catalog tables.
    withTable("char_points", "char_ranges", "varchar_points") {
      sql("CREATE TABLE char_points(p CHAR(5)) USING parquet")
      sql("INSERT INTO char_points VALUES ('ab'), ('ab   '), ('zz')")
      sql("CREATE TABLE char_ranges(lo CHAR(5), hi CHAR(5)) USING parquet")
      sql("INSERT INTO char_ranges VALUES ('aa', 'ab'), ('ab   ', 'ab   ')")
      sql("CREATE TABLE varchar_points(p VARCHAR(5)) USING parquet")
      sql("INSERT INTO varchar_points VALUES ('ab'), ('zz')")

      val charQuery =
        "SELECT /*+ BROADCAST(r) */ p.p, r.lo, r.hi FROM char_points p JOIN char_ranges r " +
          "ON p.p >= r.lo AND p.p <= r.hi"
      // CHAR(5) blank-pads 'ab' to 'ab   ', so it matches both the ('aa', 'ab') and the
      // ('ab   ', 'ab   ') range: trailing-space-only values must compare equal, not just
      // round-trip unchanged.
      val varcharQuery =
        "SELECT /*+ BROADCAST(r) */ p.p, r.lo, r.hi FROM varchar_points p JOIN char_ranges r " +
          "ON p.p >= r.lo AND p.p <= r.hi"

      Seq(charQuery, varcharQuery).foreach { query =>
        val expected = sql(query).collect()
        assert(expected.nonEmpty, query)
        withRangeJoin() {
          val df = sql(query)
          assert(findRangeJoin(df.queryExecution.executedPlan).size == 1, query)
          checkAnswer(df, expected)
        }
      }
    }
  }

  test("date and timestamp range joins match the nested loop result") {
    setupDateAndTimestampViews()
    val dateQuery =
      "SELECT p.d, r.lo, r.hi FROM dt_points p JOIN dt_ranges r ON p.d BETWEEN r.lo AND r.hi"
    val tsQuery =
      "SELECT p.t, r.lo, r.hi FROM ts_points p JOIN ts_ranges r ON p.t BETWEEN r.lo AND r.hi"
    Seq(dateQuery, tsQuery).foreach { query =>
      val expected = sql(query).collect()
      withRangeJoin() {
        val df = sql(query)
        assert(findRangeJoin(df.queryExecution.executedPlan).size == 1, query)
        checkAnswer(df, expected)
      }
    }
  }

  test("boolean range joins match the nested loop result, including a null point") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW bool_ranges(lo, hi) AS VALUES
        |  (false, true),
        |  (true, true)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW bool_points(p) AS VALUES
        |  (false),
        |  (true),
        |  (CAST(NULL AS BOOLEAN))
        |""".stripMargin)
    val query = "SELECT p.p, r.lo, r.hi FROM bool_points p JOIN bool_ranges r " +
      "ON p.p >= r.lo AND p.p <= r.hi"
    val expected = sql(query).collect()
    withRangeJoin() {
      val df = sql(query)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1, query)
      checkAnswer(df, expected)
    }
  }

  test("whole-stage codegen covers each range-join probe") {
    setupIpLookupViews()
    setupDecimalViews()
    setupCollatedStrings()
    setupIntervalViews()
    setupDoubleIntervalViews()
    setupPartialRangeViews()
    probeCoverageQueries.foreach { query =>
      val expected = sql(query).collect()
      withRangeJoin(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FALLBACK.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        assert(rangeJoinIsCodegen(df.queryExecution.executedPlan), query)
      }
    }
  }

  test("interpreted execution covers each range-join probe") {
    // Mirrors "whole-stage codegen covers each range-join probe" with codegen off, so the
    // interpreted paths (innerJoin/outerJoin/leftExistenceJoin) are exercised for every key
    // family, not just the Long-specialized one the earlier codegen-only tests happen to hit.
    setupIpLookupViews()
    setupDecimalViews()
    setupCollatedStrings()
    setupIntervalViews()
    setupDoubleIntervalViews()
    setupPartialRangeViews()
    probeCoverageQueries.foreach { query =>
      val expected = sql(query).collect()
      withRangeJoin(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        assert(findRangeJoin(df.queryExecution.executedPlan).size == 1, query)
        assert(!rangeJoinIsCodegen(df.queryExecution.executedPlan), query)
      }
    }
  }

  test("boolean and date/timestamp range joins match the nested loop result " +
      "with codegen off") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW bool_ranges2(lo, hi) AS VALUES
        |  (false, true),
        |  (true, true)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW bool_points2(p) AS VALUES
        |  (false),
        |  (true),
        |  (CAST(NULL AS BOOLEAN))
        |""".stripMargin)
    setupDateAndTimestampViews()
    val queries = Seq(
      "SELECT p.p, r.lo, r.hi FROM bool_points2 p JOIN bool_ranges2 r " +
        "ON p.p >= r.lo AND p.p <= r.hi",
      "SELECT p.d, r.lo, r.hi FROM dt_points p JOIN dt_ranges r ON p.d BETWEEN r.lo AND r.hi",
      "SELECT p.t, r.lo, r.hi FROM ts_points p JOIN ts_ranges r ON p.t BETWEEN r.lo AND r.hi")
    queries.foreach { query =>
      val expected = sql(query).collect()
      withRangeJoin(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        assert(findRangeJoin(df.queryExecution.executedPlan).size == 1, query)
        assert(!rangeJoinIsCodegen(df.queryExecution.executedPlan), query)
      }
    }
  }

  test("two interval range joins in one whole-stage match nested loop") {
    sql("CREATE OR REPLACE TEMP VIEW chain_a(id, lo, hi) AS VALUES (1, 0, 10), (2, 20, 21)")
    sql(
      "CREATE OR REPLACE TEMP VIEW chain_b(id, lo, hi) AS VALUES " +
        "(10, 1, 4), (11, 2, 8), (12, 30, 40)")
    sql(
      "CREATE OR REPLACE TEMP VIEW chain_c(id, lo, hi) AS VALUES " +
        "(20, 0, 5), (21, 3, 9), (22, 100, 110)")
    val query =
      """
        |SELECT /*+ BROADCAST(b), BROADCAST(c) */ a.id, b.id, c.id
        |FROM chain_a a
        |JOIN chain_b b ON a.lo < b.hi AND b.lo < a.hi
        |JOIN chain_c c ON b.lo < c.hi AND c.lo < b.hi
      """.stripMargin
    val expected = sql(query).collect()
    // Row 1 of a overlaps two rows of b, so the outer probe is still live when
    // the inner probe runs.
    assert(expected.length > 2)
    withRangeJoin(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      SQLConf.CODEGEN_FALLBACK.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(query)
      checkAnswer(df, expected)
      val plan = df.queryExecution.executedPlan
      val joins = findRangeJoin(plan)
      assert(joins.size == 2 && joins.forall(_.rangeJoinKind == IntervalOverlapJoin))
      assert(oneCodegenStageOwns(plan, 2))
    }
  }

  test("semi and anti range joins fused outside an interval join match nested loop") {
    sql("CREATE OR REPLACE TEMP VIEW exist_a(id, lo, hi) AS VALUES (1, 0, 100)")
    sql(
      "CREATE OR REPLACE TEMP VIEW exist_b(id, lo, hi) AS VALUES " +
        "(10, 1, 4), (11, 2, 8), (12, 50, 60)")
    sql(
      "CREATE OR REPLACE TEMP VIEW exist_c(id, lo, hi) AS VALUES " +
        "(20, 0, 5), (21, 3, 9), (22, 100, 110)")
    // b10 and b11 each overlap two rows of c, so semi and anti stop before that
    // probe is drained. b12 is still waiting on the outer probe.
    // The complete semi condition references both a and b on the left side of the semi/anti join,
    // so the semi/anti stays above that join and both probes share one stage.
    val overlap = "b.lo < c.hi AND c.lo < b.hi AND a.lo < c.hi"
    val semi =
      s"""
         |SELECT /*+ BROADCAST(b), BROADCAST(c) */ a.id, b.id
         |FROM exist_a a
         |JOIN exist_b b ON a.lo < b.hi AND b.lo < a.hi
         |LEFT SEMI JOIN exist_c c ON $overlap
       """.stripMargin
    val anti =
      s"""
         |SELECT /*+ BROADCAST(b), BROADCAST(c) */ a.id, b.id
         |FROM exist_a a
         |JOIN exist_b b ON a.lo < b.hi AND b.lo < a.hi
         |LEFT ANTI JOIN exist_c c ON $overlap
       """.stripMargin
    Seq((semi, LeftSemi), (anti, LeftAnti)).foreach { case (query, existence) =>
      val expected = sql(query).collect()
      assert(expected.nonEmpty, query)
      withRangeJoin(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FALLBACK.key -> "false",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        val plan = df.queryExecution.executedPlan
        val joins = findRangeJoin(plan)
        assert(joins.size == 2, query)
        assert(joins.forall(_.rangeJoinKind == IntervalOverlapJoin), query)
        assert(joins.map(_.joinType).toSet == Set(Inner, existence), query)
        assert(oneCodegenStageOwns(plan, 2), query + "\n" + plan.treeString)
      }
    }
  }

  test("AQE plans a range join for each of two joins of the same relation") {
    setupIntervalViews()
    sql("CREATE OR REPLACE TEMP VIEW reuse_points(point) AS VALUES (-3), (1), (3), (6)")
    val query =
      "SELECT p.point, i1.lo, i1.hi, i2.lo, i2.hi FROM reuse_points p " +
        "JOIN intervals_a i1 ON p.point >= i1.lo AND p.point < i1.hi " +
        "JOIN intervals_a i2 ON p.point >= i2.lo AND p.point < i2.hi"
    val expected = sql(query).collect()
    withRangeJoin(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      val df = sql(query)
      checkAnswer(df, expected)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 2)
    }
  }

  test("an interval index that reaches maxBroadcastTableSize fails the query") {
    sql("CREATE OR REPLACE TEMP VIEW tiny_ranges(lo, hi) AS VALUES (0, 10), (1, 11)")
    withRangeJoin(
      SQLConf.MAX_BROADCAST_TABLE_SIZE.key -> "1",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(
        "SELECT p.v, r.lo, r.hi FROM VALUES (5) p(v) JOIN tiny_ranges r " +
          "ON p.v >= r.lo AND p.v <= r.hi")
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      val thrown = intercept[Exception] {
        df.collect()
      }
      val ex = Iterator.iterate(thrown: Throwable)(_.getCause)
        .takeWhile(_ != null)
        .collectFirst { case s: SparkException => s }
        .getOrElse(throw thrown)
      assert(ex.getCondition == "_LEGACY_ERROR_TEMP_2249")
    }
  }

  test("outer loop's stream-reuse ordering holds when the join falls out of codegen") {
    // An output wider than spark.sql.codegen.maxFields excludes the join itself from
    // whole-stage codegen (CollapseCodegenStages.supportCodegen), but a streamed child
    // that supports codegen on its own still gets its own whole-stage region beneath an
    // InputAdapter. range() is such a child: its generated iterator reuses one output row
    // across calls, unlike a VALUES view (LocalTableScanExec), which CollapseCodegenStages
    // never makes a whole-stage root and which hands out its own rows. outerJoin's loop
    // must check `streamRow` before `stream.hasNext`, or this reused row is overwritten
    // while candidates for the current streamed row are still pending.
    sql(
      "CREATE OR REPLACE TEMP VIEW stream_reuse_bounds(b) AS VALUES " +
        "(10L), (20L), (30L), (40L), (50L)")
    val leftOuter =
      "SELECT /*+ BROADCAST(b) */ s.id, b.b FROM range(3) s LEFT JOIN stream_reuse_bounds b " +
        "ON s.id < b.b"
    val rightOuter =
      "SELECT /*+ BROADCAST(b) */ b.b, s.id FROM stream_reuse_bounds b RIGHT JOIN range(3) s " +
        "ON s.id < b.b"
    Seq(leftOuter, rightOuter).foreach { query =>
      val expected = sql(query).collect()
      // Every streamed id (0, 1, 2) is less than all 5 bounds, so the outer loop holds
      // one streamed row across several candidates before advancing to the next.
      assert(expected.length == 15, query)
      withRangeJoin(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FALLBACK.key -> "false",
        SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        val plan = df.queryExecution.executedPlan
        assert(findRangeJoin(plan).size == 1, query)
        assert(!rangeJoinIsCodegen(plan), s"$query\n${plan.treeString}")
        val wholeStages = collect(plan) { case w: WholeStageCodegenExec => w }
        assert(wholeStages.exists(w => collect(w.child) { case r: RangeExec => r }.nonEmpty),
          s"range() must still run under its own whole-stage codegen region: " +
            s"$query\n${plan.treeString}")
        checkAnswer(df, expected)
      }
    }
  }

  test("range join preserves the streamed side's partitioning for every supported " +
    "(join type, build side) pair") {
    withTable("rj_bucketed", "rj_small") {
      spark.range(15).toDF("k").write.bucketBy(4, "k").saveAsTable("rj_bucketed")
      spark.range(6).toDF("k").write.saveAsTable("rj_small")

      // One query per (joinType, buildSide) pair BroadcastRangeJoinExec.supports admits,
      // selecting the streamed side's bucketed `k`. An extra shuffle before the aggregation
      // below would mean outputPartitioning stopped following the streamed side.
      val queries = Seq(
        "SELECT /*+ BROADCAST(s) */ b.k AS k FROM rj_bucketed b " +
          "JOIN rj_small s ON b.k < s.k + 100", // Inner, BuildRight
        "SELECT /*+ BROADCAST(s) */ b.k AS k FROM rj_bucketed b " +
          "LEFT JOIN rj_small s ON b.k < s.k + 100", // LeftOuter, BuildRight
        "SELECT /*+ BROADCAST(s) */ b.k AS k FROM rj_bucketed b " +
          "LEFT SEMI JOIN rj_small s ON b.k < s.k + 100", // LeftSemi, BuildRight
        "SELECT /*+ BROADCAST(s) */ b.k AS k FROM rj_bucketed b " +
          "LEFT ANTI JOIN rj_small s ON b.k < s.k + 100", // LeftAnti, BuildRight
        "SELECT /*+ BROADCAST(s) */ b.k AS k FROM rj_small s " +
          "JOIN rj_bucketed b ON s.k < b.k + 100", // Inner, BuildLeft
        "SELECT /*+ BROADCAST(s) */ b.k AS k FROM rj_small s " +
          "RIGHT JOIN rj_bucketed b ON s.k < b.k + 100") // RightOuter, BuildLeft

      withRangeJoin(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        queries.foreach { query =>
          val plan =
            sql(s"SELECT k, COUNT(*) FROM ($query) GROUP BY k").queryExecution.executedPlan
          assert(findRangeJoin(plan).size == 1, query)
          assert(collect(plan) { case e: ShuffleExchangeExec => e }.isEmpty,
            s"unexpected shuffle before the aggregation:\n$query\n${plan.treeString}")
        }
      }
    }
  }

  test("range join preserves the streamed side's ordering for every supported " +
    "(join type, build side) pair") {
    withTable("rj_t1", "rj_t2", "rj_t3", "rj_t4", "rj_merge") {
      spark.range(15).toDF("k").write.saveAsTable("rj_t1")
      spark.range(6).toDF("k").write.saveAsTable("rj_t2")
      spark.range(9).toDF("k").write.saveAsTable("rj_t3")
      spark.range(11).toDF("k").write.saveAsTable("rj_t4")
      spark.range(8).toDF("k").write.saveAsTable("rj_merge")

      // left_t and right_t are each the output of a sort-merge join, so each already comes
      // out sorted on its own key regardless of the range join below. One query per
      // (joinType, buildSide) pair BroadcastRangeJoinExec.supports admits, selecting the
      // streamed side's key as k0: if outputOrdering stopped following the streamed side,
      // the outer sort-merge join against rj_merge would need to re-sort that side.
      def query(joinType: String, selectExpr: String): String =
        s"""
           |SELECT /*+ MERGE(rj_merge) */ k0, rj_merge.k
           |FROM (
           |  SELECT $selectExpr
           |  FROM (SELECT /*+ MERGE(rj_t3) */ rj_t1.k AS k1 FROM rj_t1 JOIN rj_t3
           |    ON rj_t1.k = rj_t3.k) left_t
           |  $joinType JOIN
           |  (SELECT /*+ MERGE(rj_t4) */ rj_t2.k AS k2 FROM rj_t2 JOIN rj_t4
           |    ON rj_t2.k = rj_t4.k) right_t
           |  ON k1 < k2 + 100
           |) t
           |JOIN rj_merge ON k0 = rj_merge.k
         """.stripMargin

      val queries = Seq(
        query("INNER", "/*+ BROADCAST(right_t) */ k1 AS k0"), // Inner, BuildRight
        query("LEFT", "/*+ BROADCAST(right_t) */ k1 AS k0"), // LeftOuter, BuildRight
        query("LEFT SEMI", "/*+ BROADCAST(right_t) */ k1 AS k0"), // LeftSemi, BuildRight
        query("LEFT ANTI", "/*+ BROADCAST(right_t) */ k1 AS k0"), // LeftAnti, BuildRight
        query("INNER", "/*+ BROADCAST(left_t) */ k2 AS k0"), // Inner, BuildLeft
        query("RIGHT", "/*+ BROADCAST(left_t) */ k2 AS k0")) // RightOuter, BuildLeft

      // Excludes left semi/anti push-down: otherwise a LEFT SEMI/ANTI range join is
      // pushed below left_t's own sort-merge join, moving it off of left_t's sorted
      // output entirely and defeating the ordering this test means to pin.
      withRangeJoin(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.PushDownLeftSemiAntiJoin") {
        queries.foreach { q =>
          val plan = sql(q).queryExecution.executedPlan
          assert(findRangeJoin(plan).size == 1, q)
          def hasRangeJoin(node: SparkPlan): Boolean = exists(node) {
            case _: BroadcastRangeJoinExec => true
            case _ => false
          }
          // CollapseCodegenStages wraps a codegen-supporting node (Sort included) in its
          // own WholeStageCodegenExec, so the direct child here is never a bare SortExec.
          def unwrapCodegen(node: SparkPlan): SparkPlan = node match {
            case w: WholeStageCodegenExec => unwrapCodegen(w.child)
            case i: InputAdapter => unwrapCodegen(i.child)
            case other => other
          }
          val outerJoins = collect(plan) {
            case j: SortMergeJoinExec if hasRangeJoin(j) => j
          }
          assert(outerJoins.size == 1, s"$q\n${plan.treeString}")
          val rangeSide = Seq(outerJoins.head.left, outerJoins.head.right)
            .find(hasRangeJoin).get
          assert(!unwrapCodegen(rangeSide).isInstanceOf[SortExec],
            s"range join side should already be sorted, no extra Sort needed:\n$q\n" +
              plan.treeString)
        }
      }
    }
  }
}
