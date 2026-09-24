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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.planning.{IntervalOverlapJoin, LessPartialRangeJoin}
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.execution.{
  InputAdapter, ReusedSubqueryExec, ScalarSubquery, SparkPlan, SubqueryExec,
  WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.{
  BroadcastNestedLoopJoinExec, BroadcastRangeJoinExec, CartesianProductExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

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
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
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
    // (1, 5) is below every b.lo, so the left row is kept with nulls.
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
      assert(planned.head.rangeJoin == LessPartialRangeJoin)
      checkAnswer(df, expected)
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
      assert(planned.head.rangeJoin == IntervalOverlapJoin)
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

  test("whole-stage codegen covers inner, outer, semi, decimal, and string") {
    setupIpLookupViews()
    setupDecimalViews()
    setupCollatedStrings()
    val queries = Seq(
      s"SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l ON $pointCond",
      s"SELECT s.id, l.zip_id FROM signin_ip s LEFT JOIN ip_lookup l ON $pointCond",
      s"SELECT s.id, l.zip_id FROM signin_ip s RIGHT JOIN ip_lookup l ON $pointCond",
      s"SELECT s.id FROM signin_ip s LEFT SEMI JOIN ip_lookup l ON $pointCond",
      "SELECT p.p, r.lo, r.hi FROM dec_points p JOIN dec_ranges r " +
        "ON p.p >= r.lo AND p.p <= r.hi",
      "SELECT /*+ BROADCAST(r) */ p.s, r.lo, r.hi FROM str_points p JOIN str_ranges r " +
        "ON p.s >= r.lo AND p.s <= r.hi")
    queries.foreach { query =>
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
}
