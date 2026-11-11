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
import org.apache.spark.sql.types.{Decimal, DecimalType, DoubleType}

/**
 * Planner and end-to-end coverage for broadcast range join.
 *
 * Which condition is recognized: `ExtractRangeJoinKeysSuite`.
 * What the index returns: `RangeRelationSuite`.
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
   * Runs `outerQuery`/`semiQuery`/`antiQuery` (same build side, same join condition,
   * differing only in join type) against their nested-loop answers, then asserts each
   * plans as a single range join on `BuildRight` with the matching join type. Shared by
   * the boxed-key-family coverage tests below, which otherwise only differ in which
   * views and condition they probe.
   */
  private def checkOuterSemiAntiAgainstNestedLoop(
      outerQuery: String,
      semiQuery: String,
      antiQuery: String): Unit = {
    val expectedOuter = sql(outerQuery).collect()
    val expectedSemi = sql(semiQuery).collect()
    val expectedAnti = sql(antiQuery).collect()
    assert(expectedOuter.exists(_.anyNull), expectedOuter.toSeq)
    assert(expectedAnti.nonEmpty, expectedAnti.toSeq)
    Seq(true, false).foreach { codegen =>
      withRangeJoin(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString,
        SQLConf.CODEGEN_FALLBACK.key -> "false") {
        Seq(
          (outerQuery, expectedOuter, LeftOuter),
          (semiQuery, expectedSemi, LeftSemi),
          (antiQuery, expectedAnti, LeftAnti)).foreach { case (query, expected, joinType) =>
          val df = sql(query)
          val planned = findRangeJoin(df.queryExecution.executedPlan)
          assert(planned.size == 1, (query, codegen))
          assert(planned.head.joinType == joinType, (query, codegen))
          assert(planned.head.buildSide == BuildRight, (query, codegen))
          checkAnswer(df, expected)
        }
      }
    }
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
    // DOUBLE keys are long-specialized too (see setupDoubleIntervalViews), same as the
    // INT two-key overlap above; str_ranges_a/b below cover the boxed interval index.
    "SELECT a.lo, a.hi, b.lo, b.hi FROM dbl_ranges_a a JOIN dbl_ranges_b b " +
      "ON a.lo < b.hi AND b.lo < a.hi",
    // STRING keys stay on the boxed interval index (see setupStringIntervalViews), with
    // two real, distinct per-row bounds on each side - unlike str_points/str_ranges below,
    // where the streamed side probes with one point used as both bounds.
    "SELECT a.lo, a.hi, b.lo, b.hi FROM str_ranges_a a JOIN str_ranges_b b " +
      "ON a.lo < b.hi AND b.lo < a.hi",
    // Wide decimal and string keys stay on the boxed point index (DOUBLE does not -
    // see setupPartialRangeViews), so these queries reach its generated probe code
    // (or, interpreted, the per-index dispatch in
    // BroadcastRangeJoinExec.makeCandidateFn), which int partial ranges never do.
    // The partial-range fixtures include probe values equal to build keys, so an
    // inclusive (<=/>=) scan wired to its exclusive counterpart by mistake would
    // drop those boundary matches.
    "SELECT p.d, b.lo FROM wide_dec_partial_points p JOIN wide_dec_partial_bounds b " +
      "ON p.d < b.lo",
    "SELECT p.d, b.lo FROM wide_dec_partial_bounds b JOIN wide_dec_partial_points p " +
      "ON p.d > b.lo",
    "SELECT p.d, b.lo FROM wide_dec_partial_points p JOIN wide_dec_partial_bounds b " +
      "ON p.d <= b.lo",
    "SELECT p.d, b.lo FROM wide_dec_partial_bounds b JOIN wide_dec_partial_points p " +
      "ON p.d >= b.lo",
    "SELECT p.s, b.lo FROM str_partial_points p JOIN str_partial_bounds b ON p.s < b.lo",
    "SELECT p.s, b.lo FROM str_partial_bounds b JOIN str_partial_points p ON p.s > b.lo",
    "SELECT p.s, b.lo FROM str_partial_points p JOIN str_partial_bounds b ON p.s <= b.lo",
    "SELECT p.s, b.lo FROM str_partial_bounds b JOIN str_partial_points p ON p.s >= b.lo",
    // FLOAT keys route through the same DoublePrefixComparator bridge as DOUBLE, building
    // a LongPointIndex; -0.0F/0.0F and NaN rows pin that build and probe agree on the
    // widened-to-double encoding (see setupFloatPartialViews).
    "SELECT p.f, b.lo FROM flt_partial_points p JOIN flt_partial_bounds b ON p.f < b.lo",
    // Boolean, date, and timestamp keys each map to the long family differently
    // (0/1, int, long); the boolean fixture includes a null point. These three
    // queries are covered, under both codegen modes, by this pair of tests.
    "SELECT p.p, r.lo, r.hi FROM bool_points p JOIN bool_ranges r ON p.p >= r.lo AND p.p <= r.hi",
    "SELECT p.d, r.lo, r.hi FROM dt_points p JOIN dt_ranges r ON p.d BETWEEN r.lo AND r.hi",
    "SELECT p.t, r.lo, r.hi FROM ts_points p JOIN ts_ranges r ON p.t BETWEEN r.lo AND r.hi")

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
   * Two-column STRING ranges, so a two-key interval-overlap query here builds a boxed
   * `IntervalIndex` - the `case intervals: IntervalIndex` branch of
   * `BroadcastRangeJoinExec.makeCandidateFn` that `intervals_a`/`intervals_b` (INT) and
   * `dbl_ranges_a`/`dbl_ranges_b` (DOUBLE) never reach, since both map to one
   * order-preserving long (see `RangeIndex.longKey`) and build `LongIntervalIndex`
   * instead. StringType has no such mapping, so this stays on the boxed index with
   * two real, distinct per-row bounds on each side.
   */
  private def setupStringIntervalViews(): Unit = {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW str_ranges_a(lo, hi) AS VALUES
        |  ('b', 'c'), ('c', 'd'), ('c', 'e'), ('d', 'h')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW str_ranges_b(lo, hi) AS VALUES
        |  ('a', 'b'), ('z', 'zz'), ('d', 'f'), ('h', 'j')
        |""".stripMargin)
  }

  /**
   * Two-column DOUBLE ranges. `DoubleType` maps to one order-preserving long (see
   * `RangeIndex.longKey`), so a two-key interval-overlap query here builds a
   * `LongIntervalIndex`, same as `intervals_a`/`intervals_b` (INT); boxed `IntervalIndex`
   * coverage comes from `str_ranges_a`/`str_ranges_b` above instead. This view still
   * covers `DoubleType` specifically: NaN and signed-zero ordering, and the long
   * encoding's `DoublePrefixComparator` bridge.
   */
  private def setupDoubleIntervalViews(): Unit = {
    // An unsuffixed literal like `1.0` resolves to DECIMAL, not DOUBLE (only `1.0D` or a
    // cast does), and DECIMAL(2, 1) maps to LongIntervalIndex too, defeating the point of
    // this view. The `D` suffix keeps these columns typed DOUBLE, which routes here through
    // `RangeIndex.longKey`'s `DoublePrefixComparator` bridge rather than DECIMAL's unscaled
    // long.
    //
    // Under `a.lo < b.hi AND b.lo < a.hi`, three rows pin that bridge against real SQL
    // comparison by making a match's outcome hinge on a special value rather than just
    // tolerating its presence:
    //   - a's (-0.0D, 100.0D) against b's (-100.0D, 0.0D): SQL's `-0.0 = 0.0` makes the
    //     `a.lo < b.hi` clause false, so no match is expected; an encoding that sorts
    //     -0.0 strictly below 0.0 instead of folding it would wrongly produce one.
    //   - a's (-200.0D, 0.0D) against b's (-0.0D, 200.0D) pins the same -0.0/0.0 equality
    //     on the other clause, `b.lo < a.hi`.
    //   - a's (0.0D, DOUBLE('NaN')): every b row whose `lo` clears 0.0 must still match
    //     through `b.lo < NaN`, since NaN is the SQL maximum; an encoding that does not
    //     order NaN above every finite double would drop those matches.
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dbl_ranges_a(lo, hi) AS VALUES
        |  (-1.0D, 0.0D), (0.0D, 1.0D), (0.0D, 2.0D), (1.0D, 5.0D),
        |  (-0.0D, 100.0D), (-200.0D, 0.0D), (0.0D, DOUBLE('NaN'))
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dbl_ranges_b(lo, hi) AS VALUES
        |  (-2.0D, -1.0D), (-4.0D, -2.0D), (1.0D, 3.0D), (5.0D, 7.0D),
        |  (-100.0D, 0.0D), (-0.0D, 200.0D), (50.0D, 60.0D)
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
   *
   * DOUBLE is not among these families: like the INT partial-range queries elsewhere,
   * it maps to one order-preserving long (see `RangeIndex.longKey`) and builds a
   * `LongPointIndex`. `DECIMAL(20, 2)` (precision 20, above `Decimal.MAX_LONG_DIGITS`)
   * stands in for the boxed numeric case instead.
   */
  private def setupPartialRangeViews(): Unit = {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW wide_dec_partial_points(d) AS VALUES
        |  (CAST(0.5 AS DECIMAL(20, 2))), (CAST(1.0 AS DECIMAL(20, 2))),
        |  (CAST(3.0 AS DECIMAL(20, 2))), (CAST(4.0 AS DECIMAL(20, 2))),
        |  (CAST(6.0 AS DECIMAL(20, 2))), (CAST(7.5 AS DECIMAL(20, 2))),
        |  (CAST(NULL AS DECIMAL(20, 2)))
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW wide_dec_partial_bounds(lo) AS VALUES
        |  (CAST(1.0 AS DECIMAL(20, 2))), (CAST(4.0 AS DECIMAL(20, 2))),
        |  (CAST(7.5 AS DECIMAL(20, 2)))
        |""".stripMargin)
    assert(sql("SELECT d FROM wide_dec_partial_points").schema.fields.forall { f =>
      f.dataType.isInstanceOf[DecimalType] &&
        f.dataType.asInstanceOf[DecimalType].precision > Decimal.MAX_LONG_DIGITS
    })
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

  private def setupFloatPartialViews(): Unit = {
    // CAST(-0.0 AS FLOAT) would cast the unsuffixed literal -0.0, a DECIMAL whose zero
    // carries no sign, producing +0.0F; CAST(-0.0D AS FLOAT) casts the signed DOUBLE
    // literal instead, so the row is genuine negative zero. Paired with the 0.0F bound
    // below, `p.f < b.lo` pins that: SQL treats -0.0F = 0.0F, so the strict comparison
    // is false, and an encoding that sorts -0.0F strictly below 0.0F instead of folding
    // it would wrongly match.
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW flt_partial_points(f) AS VALUES
        |  (CAST(0.5 AS FLOAT)), (CAST(1.0 AS FLOAT)), (CAST(4.0 AS FLOAT)),
        |  (CAST(-0.0D AS FLOAT)), (FLOAT('NaN'))
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW flt_partial_bounds(lo) AS VALUES
        |  (CAST(1.0 AS FLOAT)), (CAST(0.0 AS FLOAT))
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

  private def setupBooleanViews(): Unit = {
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

  test("a hint that resolves no broadcast side still allows a by-size range join") {
    // MERGE does not apply to a non-equi join (no equi-join keys to sort-merge on), so
    // it resolves no side for either a hinted range join or a hinted nested loop join.
    // Without `getNonEquiJoinBuildSide`'s `case None => withoutHint`, which retries the
    // by-size choice when a hint resolves no broadcast side, this would plan a nested
    // loop join here even though both sides are well within the broadcast threshold,
    // same as the unhinted query.
    setupIpLookupViews()
    val unhinted = sql(
      s"SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l ON $pointCond")
    withRangeJoin() {
      val hinted = sql(
        "SELECT /*+ MERGE(s) */ s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l " +
          s"ON $pointCond")
      assert(findRangeJoin(hinted.queryExecution.executedPlan).size == 1)
      checkAnswer(hinted, unhinted)
    }
  }

  test("neither side broadcastable falls back to nested loop, not an unbounded range join") {
    // getNonEquiJoinBuildSide's last resort still returns a build side for a left
    // outer join when nothing is broadcastable. BroadcastRangeJoinExec.supports is
    // only structural, so upgrading that side to a range join would broadcast a
    // relation that was never confirmed small (and can fail at maxBroadcastTableSize).
    setupIpLookupViews()
    withRangeJoin(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df = sql(
        s"SELECT s.id, l.zip_id FROM signin_ip s LEFT JOIN ip_lookup l ON $pointCond")
      assert(findRangeJoin(df.queryExecution.executedPlan).isEmpty)
      assert(findBroadcastNestedLoopJoin(df.queryExecution.executedPlan).size == 1)
    }
  }

  test("right join keeps its unconfirmed build-left side a nested loop join") {
    // The RIGHT JOIN counterpart of the test above, reaching the build-left arm of
    // sideBroadcastQualified: the last resort builds the left side for a right outer
    // join, and that arm is the only check that does not upgrade it to a range join
    // over a relation no hint or size test confirmed small.
    setupIpLookupViews()
    withRangeJoin(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df = sql(
        s"SELECT s.id, l.zip_id FROM signin_ip s RIGHT JOIN ip_lookup l ON $pointCond")
      assert(findRangeJoin(df.queryExecution.executedPlan).isEmpty)
      val nested = findBroadcastNestedLoopJoin(df.queryExecution.executedPlan)
      assert(nested.size == 1)
      assert(nested.head.buildSide == BuildLeft)
    }
  }

  test("a BROADCAST hint alone, with by-size qualification disabled, still plans a " +
      "range join that builds the hinted side") {
    // Every hinted range-join query elsewhere in this suite hints a side that also
    // qualifies by size under the default broadcast threshold, so sideBroadcastQualified's
    // size-based disjunct alone would plan the same range join either way, and the hint
    // disjunct's own contribution goes untested. With the threshold disabled here, only
    // hintToBroadcastLeft/hintToBroadcastRight can still qualify a side: deleting either
    // one would silently replan a BROADCAST-hinted range join over a large table (such as
    // an IP range table) as a broadcast nested loop join instead, with correct but slower
    // results that only the (CI-skipped) benchmark would notice.
    setupIpLookupViews()
    val unhinted = sql(
      s"SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l ON $pointCond")
    withRangeJoin(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val hintedRight = sql(
        "SELECT /*+ BROADCAST(l) */ s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l " +
          s"ON $pointCond")
      val rightJoins = findRangeJoin(hintedRight.queryExecution.executedPlan)
      assert(rightJoins.size == 1 && rightJoins.head.buildSide == BuildRight)
      checkAnswer(hintedRight, unhinted)

      // Same query and hinted table, with the join order swapped so `l` -- the interval
      // side the hint targets -- is now the physically left input, pinning the
      // hintToBroadcastLeft arm instead.
      val hintedLeft = sql(
        "SELECT /*+ BROADCAST(l) */ s.id, l.zip_id FROM ip_lookup l JOIN signin_ip s " +
          s"ON $pointCond")
      val leftJoins = findRangeJoin(hintedLeft.queryExecution.executedPlan)
      assert(leftJoins.size == 1 && leftJoins.head.buildSide == BuildLeft)
      checkAnswer(hintedLeft, unhinted)
    }
  }

  test("full outer does not plan a range join") {
    setupIpLookupViews()
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

  test("a null streamed key in a partial range join is not a match, not a crash") {
    // The LongPointIndex scan arm's null guard (and the null term the generated probe
    // shares with it) keep a NULL streamed key away from `points.toLong`: for a compact
    // decimal, `toLong(null)` calls `toUnscaledLong` on a null Decimal and throws, while
    // an int key would instead quietly probe with 0. wide_dec_partial_points' NULL row
    // only reaches an inner join, where the inferred not-null filter strips it before it
    // gets here; left outer and left anti preserve a null-keyed streamed row instead of
    // filtering it, so they are what actually reaches the guard.
    sql("CREATE OR REPLACE TEMP VIEW null_key_ints(a) AS VALUES (CAST(NULL AS INT)), (1), (10)")
    sql("CREATE OR REPLACE TEMP VIEW int_bounds(b) AS VALUES (5)")
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW null_key_decimals(a) AS VALUES
        |  (CAST(NULL AS DECIMAL(10, 2))), (CAST(1.0 AS DECIMAL(10, 2))),
        |  (CAST(10.0 AS DECIMAL(10, 2)))
        |""".stripMargin)
    sql("CREATE OR REPLACE TEMP VIEW decimal_bounds(b) AS VALUES (CAST(5.0 AS DECIMAL(10, 2)))")
    assert(sql("SELECT a FROM null_key_decimals").schema.fields.head.dataType
      .asInstanceOf[DecimalType].precision <= Decimal.MAX_LONG_DIGITS)

    Seq("null_key_ints" -> "int_bounds", "null_key_decimals" -> "decimal_bounds")
      .foreach { case (points, bounds) =>
        val outerQuery = s"SELECT p.a, b.b FROM $points p LEFT JOIN $bounds b ON p.a < b.b"
        val antiQuery = s"SELECT p.a FROM $points p LEFT ANTI JOIN $bounds b ON p.a < b.b"
        val expectedOuter = sql(outerQuery).collect()
        val expectedAnti = sql(antiQuery).collect()
        assert(expectedOuter.exists(_.anyNull), points)
        assert(expectedAnti.exists(_.anyNull), points)

        Seq(true, false).foreach { codegen =>
          withRangeJoin(
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString,
            SQLConf.CODEGEN_FALLBACK.key -> "false") {
            val outer = sql(outerQuery)
            assert(findRangeJoin(outer.queryExecution.executedPlan).size == 1, (points, codegen))
            checkAnswer(outer, expectedOuter)

            val anti = sql(antiQuery)
            assert(findRangeJoin(anti.queryExecution.executedPlan).size == 1, (points, codegen))
            checkAnswer(anti, expectedAnti)
          }
        }
      }
  }

  test("boxed IntervalIndex family (STRING) matches the nested loop result under left " +
      "outer, semi, and anti joins") {
    // probeCoverageQueries only ever joins str_ranges_a/b (the boxed IntervalIndex family,
    // since StringType has no order-preserving long encoding) with a plain INNER JOIN; every
    // LeftOuter/LeftSemi/LeftAnti case in that list runs on the long-specialized family
    // instead (BIGINT via signin_ip/ip_lookup). Left outer's null-padding and left semi/anti's
    // short-circuit over a boxed-row candidate iterator are a different code path per key
    // type than the long family, so this pins the untested (boxed index) x (non-inner join
    // type) cell directly.
    setupStringIntervalViews()
    val cond = "a.lo < b.hi AND b.lo < a.hi"
    val outerQuery =
      s"SELECT a.lo, a.hi, b.lo, b.hi FROM str_ranges_a a LEFT JOIN str_ranges_b b ON $cond"
    val semiQuery =
      s"SELECT a.lo, a.hi FROM str_ranges_a a LEFT SEMI JOIN str_ranges_b b ON $cond"
    val antiQuery =
      s"SELECT a.lo, a.hi FROM str_ranges_a a LEFT ANTI JOIN str_ranges_b b ON $cond"
    checkOuterSemiAntiAgainstNestedLoop(outerQuery, semiQuery, antiQuery)
  }

  test("boxed PointIndex family (wide DECIMAL) matches the nested loop result under left " +
      "outer, semi, and anti joins") {
    // probeCoverageQueries only ever joins wide_dec_partial_points/bounds (the boxed
    // PointIndex family, since DECIMAL(20, 2) exceeds Decimal.MAX_LONG_DIGITS and has no long
    // encoding) with a plain INNER JOIN, where the inferred not-null filter also strips the
    // NULL point row before it reaches the join. Left outer, semi, and anti preserve every
    // row instead, so this is the only place the boxed PointIndex's null-padding and
    // short-circuit behavior is exercised end to end, rather than just the long-specialized
    // family covered elsewhere.
    setupPartialRangeViews()
    val outerQuery =
      "SELECT p.d, b.lo FROM wide_dec_partial_points p LEFT JOIN wide_dec_partial_bounds b " +
        "ON p.d < b.lo"
    val semiQuery =
      "SELECT p.d FROM wide_dec_partial_points p LEFT SEMI JOIN wide_dec_partial_bounds b " +
        "ON p.d < b.lo"
    val antiQuery =
      "SELECT p.d FROM wide_dec_partial_points p LEFT ANTI JOIN wide_dec_partial_bounds b " +
        "ON p.d < b.lo"
    checkOuterSemiAntiAgainstNestedLoop(outerQuery, semiQuery, antiQuery)
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

  test("an inclusive interval overlap treats -0.0 and 0.0 as equal on either build side") {
    // dbl_ranges_a/b (setupDoubleIntervalViews) pin -0.0/0.0 equality under the suite's
    // strict `<` interval-overlap query: a comparator that sorts -0.0 strictly below 0.0
    // wrongly turns a false clause true there. This is the complementary direction under
    // an inclusive `<=` overlap: +0.0 and -0.0 here touch only at that shared point, so
    // the match holds only if -0.0 <= 0.0 and 0.0 <= -0.0 both hold, i.e. only if they
    // compare equal; a comparator that instead orders them as distinct values would drop
    // the match. Broadcasting each side in turn puts +0.0 and -0.0 on the build side once
    // each, so the index conversion and the streamed probe's conversion are each checked
    // on both values.
    sql("CREATE OR REPLACE TEMP VIEW inc_zero_pos(lo, hi) AS VALUES (0.0D, 0.0D)")
    sql("CREATE OR REPLACE TEMP VIEW inc_zero_neg(lo, hi) AS VALUES (-0.0D, -0.0D)")
    val query =
      "SELECT a.lo, a.hi, b.lo, b.hi FROM inc_zero_pos a JOIN inc_zero_neg b " +
        "ON a.lo <= b.hi AND b.lo <= a.hi"
    val expected = sql(query).collect()
    assert(expected.length == 1, expected.toSeq)
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

      // CHAR(5) blank-pads 'ab' to 'ab   ', so it matches both the ('aa', 'ab') and the
      // ('ab   ', 'ab   ') range: trailing-space-only values must compare equal, not just
      // round-trip unchanged.
      val charQuery =
        "SELECT /*+ BROADCAST(r) */ p.p, r.lo, r.hi FROM char_points p JOIN char_ranges r " +
          "ON p.p >= r.lo AND p.p <= r.hi"
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

  test("whole-stage codegen covers each range-join probe") {
    setupIpLookupViews()
    setupDecimalViews()
    setupCollatedStrings()
    setupIntervalViews()
    setupStringIntervalViews()
    setupDoubleIntervalViews()
    setupPartialRangeViews()
    setupFloatPartialViews()
    setupDateAndTimestampViews()
    setupBooleanViews()
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
    setupStringIntervalViews()
    setupDoubleIntervalViews()
    setupPartialRangeViews()
    setupFloatPartialViews()
    setupDateAndTimestampViews()
    setupBooleanViews()
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
    // whole-stage codegen (CollapseCodegenStages.supportCodegen). CollapseCodegenStages
    // then wraps the streamed range() child directly in its own WholeStageCodegenExec;
    // InputAdapter is only inserted under a parent that still supports codegen, so this
    // plan has no InputAdapter. range()'s generated iterator reuses one output row across
    // calls, unlike a VALUES view (LocalTableScanExec), which never becomes a whole-stage
    // root and hands out its own rows. outerJoin's loop must check `streamRow` before
    // `stream.hasNext`, or this reused row is overwritten while candidates for the
    // current streamed row are still pending.
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
