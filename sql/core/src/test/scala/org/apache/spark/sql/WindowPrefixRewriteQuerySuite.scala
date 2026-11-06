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

import org.scalatest.Tag

import org.apache.spark.sql.catalyst.expressions.{Alias, CurrentRow, Lag, NamedExpression,
  RowFrame, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

/**
 * Differential tests for `RewriteSlidingFramesAsPrefixDifferences`: the original query (config
 * off) and the rewritten query (config on) must return identical results, and the rewrite must
 * not add exchanges or sorts.
 *
 * The rule only applies with `spark.sql.ansi.enabled = false`, which these tests set together
 * with `spark.sql.optimizer.windowPrefixRewrite.enabled`.
 */
class WindowPrefixRewriteQuerySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  /**
   * Runs the query with the rewrite flag set to `enabled` and returns the collected rows
   * and the executed plan. Collection happens inside the config scope: a DataFrame is lazy,
   * so forcing it after `withSQLConf` has restored the flag would plan with the rewrite off
   * on both sides and make the differential comparison a tautology. AQE is disabled so the
   * executed plan directly shows the window operators.
   */
  private def run(enabled: Boolean)(sqlText: String): (Array[Row], StructType, SparkPlan) =
    withSQLConf(
      SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key -> enabled.toString,
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.sql(sqlText)
      val rows = df.collect()
      (rows, df.schema, df.queryExecution.executedPlan)
    }

  /**
   * Runs the query in both modes and compares the full projection, ignoring row order, and
   * asserts the window operator count grows by exactly `expectedExtraWindows`. A positive
   * delta additionally requires the rewritten plan to be that many (difference window,
   * prefix window) pairs, verified by `assertPrefixRewriteShape`; a zero delta requires
   * identical counts, i.e. the rewrite did not fire. `expectedPlainWindows` counts window
   * operators the rewrite left untouched and is only meaningful with a positive delta.
   *
   * The delta is one per rewritten window operator. `ExtractWindowExpressions` groups the
   * window expressions of one (PARTITION BY, ORDER BY) spec into one operator per level
   * and `CollapseWindow` merges stacked levels that reference the base columns: the
   * mergeable stacked query below gains one operator (both frames share a single prefix
   * window), while a level that sums another level's window output cannot merge and gains
   * its own pair. A query with a second spec the rewrite does not touch keeps that
   * operator and accounts for it via `expectedPlainWindows`.
   */
  private def assertIdentical(
      sqlText: String,
      expectedExtraWindows: Int,
      expectedPlainWindows: Int = 0): Unit = {
    val (originalRows, originalSchema, originalPlan) = run(false)(sqlText)
    val (rewrittenRows, rewrittenSchema, rewrittenPlan) = run(true)(sqlText)
    QueryTest.sameRows(originalRows.toSeq, rewrittenRows.toSeq).foreach(fail(_))
    assert(rewrittenSchema === originalSchema)
    val originalWindows = collectWindows(originalPlan).size
    val rewrittenWindows = collectWindows(rewrittenPlan).size
    assert(rewrittenWindows === originalWindows + expectedExtraWindows,
      s"expected the window operator count to change from $originalWindows to " +
        s"${originalWindows + expectedExtraWindows}, but it was $rewrittenWindows: $sqlText")
    if (expectedExtraWindows > 0) {
      assertPrefixRewriteShape(rewrittenPlan, expectedPlainWindows)
    }
  }

  private def collectWindows(plan: SparkPlan): Seq[WindowExec] = plan.collect {
    case w: WindowExec => w
  }

  /**
   * Descends through adaptive-plan wrappers to the operators under test: an
   * [[AdaptiveSparkPlanExec]] is a leaf whose current plan is a query stage, and a
   * [[QueryStageExec]] in turn hides its inner plan from tree traversal, so plan
   * inspection must unwrap both instead of collecting over them.
   */
  private def unwrap(plan: SparkPlan): SparkPlan = plan match {
    case a: AdaptiveSparkPlanExec => unwrap(a.executedPlan)
    case s: QueryStageExec => unwrap(s.plan)
    case p => p
  }

  /**
   * Asserts the structural contract of the rewrite on the executed plan: apart from
   * `expectedPlainWindows` operators the rewrite left untouched, every window operator is
   * part of a (difference window, prefix window) pair: a difference operator computing lag
   * differences whose inputs are running aggregates computed by the operator directly
   * below it, both over the same partition and order spec, so one sort serves the pair.
   * `CollapseWindow` cannot merge a pair back because the difference window's lag
   * expressions reference the prefix window's outputs.
   */
  private def assertPrefixRewriteShape(plan: SparkPlan, expectedPlainWindows: Int): Unit = {
    val prefixFrame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)

    def lagWindows(ne: NamedExpression): Seq[(Lag, WindowSpecDefinition)] = ne.collect {
      case WindowExpression(l: Lag, spec: WindowSpecDefinition) => (l, spec)
    }

    def isPrefixWindow(w: WindowExec): Boolean =
      w.windowExpression.nonEmpty && w.windowExpression.forall {
        case Alias(WindowExpression(_, WindowSpecDefinition(_, _, `prefixFrame`)), _) => true
        case _ => false
      }

    // Classify every window operator: a pure running-prefix window, a window holding lag
    // differences (possibly alongside members of the group that did not qualify), or a
    // window the rewrite left untouched, e.g. one over a different (PARTITION BY,
    // ORDER BY) spec.
    val windows = collectWindows(plan)
    val (prefixWindows, rest) = windows.partition(isPrefixWindow)
    val (differenceWindows, plainWindows) =
      rest.partition(w => w.windowExpression.exists(lagWindows(_).nonEmpty))
    assert(differenceWindows.nonEmpty, s"expected a rewritten window operator in: $plan")
    assert(plainWindows.size === expectedPlainWindows,
      s"expected $expectedPlainWindows untouched window operators in: $plan")

    // The rewrite emits one prefix window per rewritten operator, so the two kinds pair
    // up one-to-one.
    assert(differenceWindows.size === prefixWindows.size,
      s"expected one prefix window per difference window in: $plan")

    differenceWindows.foreach { outer =>
      // The prefix window a difference window reads from sits directly below it, through
      // any intervening Project and possibly alongside an untouched window for another
      // spec.
      val inner = collectWindows(outer.child).find(prefixWindows.contains)
      assert(inner.isDefined,
        s"expected the difference window's child to contain its running-prefix window in: $plan")

      // Both operators share one partition and order spec (the same expression instances
      // within one plan), which is why the rewrite adds no sort or exchange.
      assert(inner.get.partitionSpec == outer.partitionSpec)
      assert(inner.get.orderSpec == outer.orderSpec)

      // Each lag reads one of the prefix window's running aggregates at a fixed offset
      // (its frame is n PRECEDING to n PRECEDING).
      val prefixAttrs = inner.get.windowExpression.map(_.toAttribute)
      outer.windowExpression.flatMap(lagWindows).foreach { case (lag, spec) =>
        assert(prefixAttrs.exists(_.semanticEquals(lag.input)),
          s"lag input ${lag.input} is not a running prefix computed by the inner window")
        assert(spec.frameSpecification match {
          case SpecifiedWindowFrame(RowFrame, lower, upper) => lower == upper
          case _ => false
        }, s"lag frame must be a fixed offset frame (n PRECEDING to n PRECEDING): $spec")
      }
    }
  }

  private def setupView(sqlText: String): Unit = spark.sql(sqlText).createOrReplaceTempView("t")

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: org.scalactic.source.Position): Unit =
    super.test(testName, testTags: _*)(
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false")(testFun))(pos)

  test("rewrite produces identical results for multiple widths, measures and nullability") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 10 AS k, id AS o,
          |  CASE WHEN id % 5 = 0 THEN CAST(NULL AS BIGINT) ELSE (id * 37) % 1000 END AS vn,
          |  (id * 13) % 500 AS v
          |FROM range(0, 500)
        """.stripMargin)
      assertIdentical(
        """
          |SELECT k, o, vn, v,
          |  sum(vn) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s2,
          |  sum(vn) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS s29,
          |  sum(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS s6,
          |  sum(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) AS s179
          |FROM t
        """.stripMargin,
        expectedExtraWindows = 1)
    }
  }

  test("duplicate (key, order) rows") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 5 AS k, id % 3 AS o, (id * 7) % 100 AS v
          |FROM range(0, 100)
        """.stripMargin)
      val sql =
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s2,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s4
          |FROM t
        """.stripMargin
      assertIdentical(sql, expectedExtraWindows = 0)
    }
  }

  test("duplicate (key, order) rows above the cost gate") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 5 AS k, id % 3 AS o, (id * 7) % 100 AS v
          |FROM range(0, 100)
        """.stripMargin)
      // Widths 3 + 14 = 17 >= 16, so the rewrite fires. With tied order keys the frame
      // contents depend on the tie order, so identical results prove the prefix window and
      // the lag window see the same row order (a single shared sort).
      val sql =
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s2,
          |  sum(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS s13
          |FROM t
        """.stripMargin
      assertIdentical(sql, expectedExtraWindows = 1)
    }
  }

  test("short partitions: fewer rows than the frame width") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 20 AS k, id % 20 AS o, id AS v
          |FROM range(0, 200)
        """.stripMargin)
      assertIdentical(
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS s29
          |FROM t
        """.stripMargin,
        expectedExtraWindows = 1)
    }
  }

  test("all-NULL measures within a partition return NULL, not 0") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 2 AS k, id AS o,
          |  CASE WHEN id % 10 = 9 THEN id ELSE CAST(NULL AS BIGINT) END AS vn
          |FROM range(0, 50)
        """.stripMargin)
      // Widths 4 + 16 = 20 clear the cost gate, so the rewrite fires and the count
      // guard is exercised: vn is non-NULL only at id % 10 = 9 (all in k = 1), so a
      // k = 1 row whose frame contains none of them has an all-NULL frame while the
      // running prefix is non-NULL - the exact case the guard's NULL arm exists for.
      // Without the guard the prefix difference there is 0, not NULL.
      val sql =
        """
          |SELECT k, o, vn,
          |  sum(vn) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s3,
          |  sum(vn) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 15 PRECEDING AND CURRENT ROW) AS s15
          |FROM t
        """.stripMargin
      assertIdentical(sql, expectedExtraWindows = 1)
    }
  }

  test("under-gate widths with a nullable measure are not rewritten") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 2 AS k, id AS o,
          |  CASE WHEN id % 10 = 9 THEN id ELSE CAST(NULL AS BIGINT) END AS vn
          |FROM range(0, 50)
        """.stripMargin)
      // Widths 4 + 1 = 5 stay under the cost gate, so the group is rejected before the
      // NULL guard matters and the plan must be untouched.
      val sql =
        """
          |SELECT k, o, vn,
          |  sum(vn) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s3,
          |  sum(vn) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 0 PRECEDING AND CURRENT ROW) AS s0
          |FROM t
        """.stripMargin
      assertIdentical(sql, expectedExtraWindows = 0)
    }
  }

  test("narrow integral measures (tinyint, smallint, int) are rewritten identically") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 10 AS k, id AS o,
          |  CAST(id % 7 AS TINYINT) AS b,
          |  CASE WHEN id % 5 = 0 THEN CAST(NULL AS SMALLINT) ELSE CAST(id % 13 AS SMALLINT) END
          |    AS ns,
          |  CAST(id % 101 AS INT) AS i
          |FROM range(0, 300)
        """.stripMargin)
      assertIdentical(
        """
          |SELECT k, o, b, ns, i,
          |  sum(b) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sb,
          |  sum(ns) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS sns,
          |  sum(i) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS si
          |FROM t
        """.stripMargin,
        expectedExtraWindows = 1)
    }
  }

  test("window without ORDER BY is not rewritten") {
    withTempView("t") {
      setupView("SELECT id % 3 AS k, id AS v FROM range(0, 60)")
      val sql = """
        |SELECT k, v,
        |  sum(v) OVER (PARTITION BY k ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS s49
        |FROM t
      """.stripMargin
      assertIdentical(sql, expectedExtraWindows = 0)
    }
  }

  test("min, max, avg, float sums, counts and try_sum are not rewritten") {
    withTempView("t") {
      setupView("SELECT id % 3 AS k, id AS o, id AS v, id % 7 AS f FROM range(0, 90)")
      assertIdentical(
        """
          |SELECT k, o, v, f,
          |  min(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS mn,
          |  max(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS mx
          |FROM t
        """.stripMargin,
        expectedExtraWindows = 0)
      assertIdentical(
        """
          |SELECT k, o, v,
          |  sum(v * 0.5) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW)
          |    AS ds
          |FROM t
        """.stripMargin,
        expectedExtraWindows = 0)
      assertIdentical(
        """
          |SELECT k, o, v,
          |  avg(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS av
          |FROM t
        """.stripMargin,
        expectedExtraWindows = 0)
      assertIdentical(
        """
          |SELECT k, o, v,
          |  count(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW)
          |    AS cn
          |FROM t
        """.stripMargin,
        expectedExtraWindows = 0)
      assertIdentical(
        """
          |SELECT k, o, v,
          |  try_sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW)
          |    AS ts
          |FROM t
        """.stripMargin,
        expectedExtraWindows = 0)
      // Mixed group: sum qualifies, the other members keep their frames.
      val mixed =
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS ss,
          |  count(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW)
          |    AS cn
          |FROM t
        """.stripMargin
      assertIdentical(mixed, expectedExtraWindows = 1)
    }
  }

  test("filtered sums are not rewritten") {
    withTempView("t") {
      setupView("SELECT id % 3 AS k, id AS o, id AS v FROM range(0, 90)")
      // Width 32 alone clears the cost gate, but the FILTER clause disqualifies the sum,
      // so the plan must be untouched.
      val sql =
        """
          |SELECT k, o, v,
          |  sum(v) FILTER (WHERE v % 2 = 0) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS sf
          |FROM t
        """.stripMargin
      assertIdentical(sql, expectedExtraWindows = 0)
    }
  }

  test("decimal sums are not rewritten") {
    withTempView("t") {
      setupView("SELECT id % 3 AS k, id AS o, CAST(id AS DECIMAL(10, 2)) AS v FROM range(0, 90)")
      val sql =
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS ds
          |FROM t
        """.stripMargin
      assertIdentical(sql, expectedExtraWindows = 0)
    }
  }

  test("prefix overflow with wrapping sums is bit-identical") {
    withTempView("t") {
      // The prefix passes 2^63 while every frame fits; a frame sum itself overflows too.
      setupView(
        """
          |SELECT 1 AS k, id AS o, element_at(
          |  array(4611686018427387904L, 4611686018427387904L, -4611686018427387904L,
          |        9223372036854775807L, 1L), CAST(id + 1 AS INT)) AS v
          |FROM range(0, 5)
        """.stripMargin)
      val sql =
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s1,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s2,
          |  sum(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS s31
          |FROM t
        """.stripMargin
      assertIdentical(sql, expectedExtraWindows = 1)
    }
  }

  test("sums in an ANSI session are not rewritten") {
    withTempView("t") {
      setupView("SELECT id % 3 AS k, id AS o, id AS v FROM range(0, 90)")
      val sql = """
        |SELECT k, o, v,
        |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS ss
        |FROM t
      """.stripMargin
      withSQLConf(
        SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = spark.sql(sql)
        df.collect()
        assert(collectWindows(df.queryExecution.executedPlan).size === 1)
      }
    }
  }

  test("the rewrite is disabled by default") {
    withTempView("t") {
      setupView("SELECT id % 3 AS k, id AS o, id AS v FROM range(0, 90)")
      // The performance-tuning docs list the flag's default as false; a flag-unset run
      // must leave the width-32 group as a single plain window operator. A flipped
      // default would rewrite it into a (difference window, prefix window) pair and
      // fail the count. Unwrap a possible adaptive wrapper: this test pins the flag,
      // not the AQE setting.
      val df = spark.sql(
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW)
          |    AS s
          |FROM t
        """.stripMargin)
      df.collect()
      assert(collectWindows(unwrap(df.queryExecution.executedPlan)).size === 1)
    }
  }

  test("the rewrite adds no exchange or sort") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 10 AS k, id AS o,
          |  CASE WHEN id % 5 = 0 THEN CAST(NULL AS BIGINT) ELSE id END AS vn,
          |  id AS v
          |FROM range(0, 500)
        """.stripMargin)
      val sql = """
        |SELECT k, o, vn, v,
        |  sum(vn) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS s6,
        |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS s29
        |FROM t
      """.stripMargin
      val before = withSQLConf(
        SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key -> "false",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        spark.sql(sql).queryExecution.executedPlan
      }
      val after = withSQLConf(
        SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        spark.sql(sql).queryExecution.executedPlan
      }
      def counts(plan: SparkPlan): (Int, Int) = (
        plan.collect { case e: ShuffleExchangeLike => e }.size,
        plan.collect { case s: SortExec => s }.size)
      val (beforeExchanges, beforeSorts) = counts(before)
      val (afterExchanges, afterSorts) = counts(after)
      assert(afterExchanges === beforeExchanges)
      assert(afterSorts === beforeSorts)
      // The two WindowExec operators are served by a single sort.
      assert(afterSorts === 1)
    }
  }

  test("stacked sliding windows over the same measure share one prefix window") {
    withTempView("t") {
      setupView("SELECT id % 10 AS k, id AS o, id AS v FROM range(0, 500)")
      // Both levels slide over the same (PARTITION BY, ORDER BY) spec, so with the flag off
      // CollapseWindow merges them into a single window operator. The rewrite must see that
      // merged window: it then adds one prefix window shared by both frames, keeping the
      // window operator count at original + 1. If the rewrite runs before CollapseWindow,
      // each level instead grows its own prefix window that the lag references pin in place,
      // and the count goes from 1 to 3.
      assertIdentical(
        """
          |SELECT k, o, s1,
          |  sum(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS s2
          |FROM (
          |  SELECT k, o, v,
          |    sum(v) OVER (PARTITION BY k ORDER BY o
          |      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS s1
          |  FROM t
          |)
        """.stripMargin,
        expectedExtraWindows = 1)
    }
  }

  test("stacked sliding windows over different measures keep separate prefix windows") {
    withTempView("t") {
      setupView("SELECT id % 10 AS k, id AS o, id AS v FROM range(0, 500)")
      // Unlike the test above, the upper level sums the lower level's output, so the two
      // windows can never merge: the outer references s1, which is the inner window's
      // output, and CollapseWindow's references guard rejects that merge even without the
      // rewrite. Each rewritten level keeps its own prefix window, so the operator count
      // goes from 2 to 4. This pins the baseline a cross-window prefix-reuse change would
      // improve on.
      assertIdentical(
        """
          |SELECT k, o,
          |  sum(s1) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS s2
          |FROM (
          |  SELECT k, o, v,
          |    sum(v) OVER (PARTITION BY k ORDER BY o
          |      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS s1
          |  FROM t
          |)
        """.stripMargin,
        expectedExtraWindows = 2)
    }
  }

  test("a qualifying group is rewritten independently of a different-spec window operator") {
    withTempView("t") {
      setupView("SELECT id % 10 AS k, id % 7 AS k2, id AS o, id AS v FROM range(0, 300)")
      // The width-30 (k, o) group qualifies on its own and gains one (difference window,
      // prefix window) pair; the width-4 (k2, o) window has a different spec, so the
      // rewrite leaves it untouched and it keeps its own operator.
      val sql =
        """
          |SELECT k, k2, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS s29,
          |  sum(v) OVER (PARTITION BY k2 ORDER BY o
          |    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s4
          |FROM t
        """.stripMargin
      assertIdentical(sql, expectedExtraWindows = 1, expectedPlainWindows = 1)
    }
  }

  test("rewrite produces identical results under AQE") {
    withTempView("t") {
      setupView("SELECT id % 3 AS k, id AS o, id AS v FROM range(0, 300)")
      // The differential helper pins AQE off so the executed plan shows the window
      // operators directly; production sessions run with AQE on, so replay the
      // differential under AQE and unwrap AdaptiveSparkPlanExec for the assertions.
      val sql =
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW)
          |    AS s
          |FROM t
        """.stripMargin

      def runAqe(enabled: Boolean): (Array[Row], SparkPlan) =
        withSQLConf(
          SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key -> enabled.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
          val df = spark.sql(sql)
          val rows = df.collect()
          val executed = df.queryExecution.executedPlan
          assert(executed.isInstanceOf[AdaptiveSparkPlanExec],
            s"expected AQE to plan the query, got: $executed")
          (rows, unwrap(executed))
        }

      val (originalRows, originalPlan) = runAqe(enabled = false)
      val (rewrittenRows, rewrittenPlan) = runAqe(enabled = true)
      QueryTest.sameRows(originalRows.toSeq, rewrittenRows.toSeq).foreach(fail(_))
      val originalWindows = collectWindows(originalPlan).size
      val rewrittenWindows = collectWindows(rewrittenPlan).size
      assert(rewrittenWindows === originalWindows + 1,
        s"expected the window operator count to change from $originalWindows to " +
          s"${originalWindows + 1} under AQE, but it was $rewrittenWindows: $sql")
      assertPrefixRewriteShape(rewrittenPlan, expectedPlainWindows = 0)
    }
  }

  test("rewrite produces identical results with whole-stage codegen disabled") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 10 AS k, id AS o,
          |  CASE WHEN id % 5 = 0 THEN CAST(NULL AS BIGINT) ELSE id END AS vn,
          |  id AS v
          |FROM range(0, 300)
        """.stripMargin)
      // WindowExec's interpreted path (codegen off) is a separate implementation of both
      // the sliding frame fold and the emitted lag windows, so the differential must
      // hold there too, not only on the whole-stage path. The nullable measure keeps
      // the count guard's CASE expression on the interpreted path as well.
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        assertIdentical(
          """
            |SELECT k, o, vn, v,
            |  sum(vn) OVER (PARTITION BY k ORDER BY o
            |    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS s6,
            |  sum(v) OVER (PARTITION BY k ORDER BY o
            |    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS s29
            |FROM t
          """.stripMargin,
          expectedExtraWindows = 1)
      }
    }
  }

  test("randomised differential test") {
    withTempView("t") {
      val rand = new scala.util.Random(42)
      val widths = Seq(1, 2, 3, 7, 29, 90, 179)
      for (_ <- 0 until 20) {
        val partMod = 1 + rand.nextInt(20)
        val numRows = partMod * (1 + rand.nextInt(40))
        val nullEvery = 1 + rand.nextInt(6)
        spark.range(0, numRows).select(
          ($"id" % partMod).as("k"),
          // Half the runs use a low-cardinality order key, so tied rows exercise the
          // single-shared-sort invariant the rewrite relies on.
          (if (rand.nextBoolean()) $"id" else $"id" % (1 + rand.nextInt(5))).as("o"),
          functions.when($"id" % nullEvery === 0, functions.lit(null).cast("bigint"))
            .otherwise(($"id" * (1 + rand.nextInt(97))) % 100000)
            .as("vn"),
          $"id".as("v")).createOrReplaceTempView("t")
        var selectedWidths = rand.shuffle(widths).take(1 + rand.nextInt(4))
        while (selectedWidths.sum < 16) {
          selectedWidths = selectedWidths :+ widths(rand.nextInt(widths.length))
        }
        val sumClauses = selectedWidths
          .map { w =>
            s"sum(${if (rand.nextBoolean()) "vn" else "v"}) OVER " +
              s"(PARTITION BY k ORDER BY o ROWS BETWEEN ${w - 1} PRECEDING AND CURRENT ROW) AS s$w"
          }
          .mkString(",\n  ")
        assertIdentical(s"SELECT k, o, vn, v, $sumClauses FROM t", expectedExtraWindows = 1)
      }
    }
  }
}
