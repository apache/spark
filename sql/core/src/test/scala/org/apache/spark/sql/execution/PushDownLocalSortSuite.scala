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

package org.apache.spark.sql.execution

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, AttributeReference, IsNotNull, LessThan, Literal, Rand, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.exchange.ValidateRequirements
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType


abstract class PushDownLocalSortSuiteBase
    extends SharedSparkSession
    with AdaptiveSparkPlanHelper {

  private def checkNumSorts(df: DataFrame, count: Int): Unit = {
    // Execute first so that, under AQE, the final adaptive plan (after query-stage materialization
    // and any replanning) is inspected rather than the initial plan.
    df.collect()
    val plan = df.queryExecution.executedPlan
    assert(collectWithSubqueries(plan) { case s: SortExec => s }.length == count)
  }

  private def checkSorts(query: String, enabledCount: Int, disabledCount: Int): Unit = {
    withSQLConf(SQLConf.PUSH_DOWN_LOCAL_SORT_ENABLED.key -> "true") {
      val df = sql(query)
      checkNumSorts(df, enabledCount)
      val result = df.collect()
      withSQLConf(SQLConf.PUSH_DOWN_LOCAL_SORT_ENABLED.key -> "false") {
        val df = sql(query)
        checkNumSorts(df, disabledCount)
        checkAnswer(df, result)
      }
    }
  }

  test("Push a wider local sort down across stacked windows with prefix-compatible order specs") {
    withTempView("t") {
      spark.range(100).selectExpr("id % 10 as a", "id % 7 as b", "id as c")
        .createOrReplaceTempView("t")
      // The narrower window (order by b) is listed first, so it is planned closest to the leaf and
      // the wider window (order by b, c) ends up above it. Without the rule two local sorts
      // ([a, b] below the inner window, [a, b, c] above it) are computed. The rule widens the
      // lower sort to [a, b, c] so it serves both windows and drops the upper sort, leaving a
      // single [a, b, c] sort.
      val query =
        """
          |SELECT a, b, c,
          |  RANK() OVER (PARTITION BY a ORDER BY b) AS rk,
          |  ROW_NUMBER() OVER (PARTITION BY a ORDER BY b, c) AS rn
          |FROM t
          |""".stripMargin
      checkSorts(query, 1, 2)
    }
  }

  test("No-op when the wider sort is already below the narrower one") {
    withTempView("t") {
      spark.range(100).selectExpr("id % 10 as a", "id % 7 as b", "id as c")
        .createOrReplaceTempView("t")
      // The wider window (order by b, c) is listed first, so it is planned closest to the leaf and
      // the narrower window (order by b) ends up above it. `EnsureRequirements` inserts only one
      // sort here: the wider window's [a, b, c] sort already satisfies the narrower window's
      // [a, b] requirement, so no second sort is added. This rule only pushes a wider sort down,
      // so it does not fire; the single sort is unchanged whether it is on or off.
      val query =
        """
          |SELECT a, b, c,
          |  ROW_NUMBER() OVER (PARTITION BY a ORDER BY b, c) AS rn,
          |  RANK() OVER (PARTITION BY a ORDER BY b) AS rk
          |FROM t
          |""".stripMargin
      checkSorts(query, 1, 1)
    }
  }

  test("Push-down still applies and stays correct with a filter on a window output") {
    withTempView("t") {
      spark.range(200).selectExpr("id % 10 as a", "id % 7 as b", "id as c")
        .createOrReplaceTempView("t")
      val query =
        """
          |SELECT * FROM (
          |  SELECT a, b, c,
          |    RANK() OVER (PARTITION BY a ORDER BY b) AS rk,
          |    ROW_NUMBER() OVER (PARTITION BY a ORDER BY b, c) AS rn
          |  FROM t
          |) WHERE rn > 1
          |""".stripMargin
      // The filter on rn sits above both windows and does not affect the two sorts that feed them,
      // so the push-down still reduces 2 sorts to 1 and the results are unchanged.
      checkSorts(query, 1, 2)
    }
  }

  test("Push a wider sort down through a window to feed a sort aggregate above it") {
    withTempView("t") {
      // `c` varies within each (a, b) group (70 % 13 != 0) and `b` repeats within each partition
      // `a`, so widening the window's sort from [a, b] to [a, b, c] genuinely reorders rows within
      // the window's `ORDER BY b` ties. `RANK()` is tie-safe -- its value does not depend on the
      // order of tied rows -- so the result stays stable while the sort count still drops.
      spark.range(500).selectExpr("id % 10 as a", "id % 7 as b", "id % 13 as c")
        .createOrReplaceTempView("t")
      // Plan shape within one stage: shuffle -> Sort([a,b,c]) -> Window([a],[b]) -> Sort([a,b,c])
      // -> SortAggregate(group by a,b,c). The window needs [a,b] and the sort aggregate needs the
      // wider [a,b,c]; the aggregate's grouping keys are clustered-compatible with the window's
      // partitioning, so no shuffle separates them. The wider [a,b,c] sort feeding the aggregate is
      // pushed down through the window, replacing the window's [a,b] sort and serving both.
      // `collect_list` with object-hash aggregation off forces a sort aggregate.
      withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
        val query =
          """
            |SELECT a, b, c, collect_list(rk) AS cl
            |FROM (SELECT a, b, c, RANK() OVER (PARTITION BY a ORDER BY b) AS rk FROM t)
            |GROUP BY a, b, c
            |""".stripMargin
        checkSorts(query, 1, 2)
      }
    }
  }

  test("Push a wider sort down through a renaming project by rewriting the ordering") {
    withTempView("t") {
      spark.range(200).selectExpr("id % 10 as a", "id % 7 as b", "id % 5 as c")
        .createOrReplaceTempView("t")
      // The inner window sorts by [a, b]; a project then renames b to bb; the outer window sorts by
      // [a, bb, c] (wider). The wider sort's key `bb` is the renamed `b`, so pushing it down
      // through the renaming project requires rewriting the ordering from bb back to b. After the
      // rewrite the [a, b, c] sort is pushed through the inner window, replacing its [a, b] sort.
      val query =
        """
          |SELECT a, bb, c, rn1,
          |  ROW_NUMBER() OVER (PARTITION BY a ORDER BY bb, c) AS rn2
          |FROM (
          |  SELECT a, b AS bb, c, rn1 FROM (
          |    SELECT a, b, c, RANK() OVER (PARTITION BY a ORDER BY b) AS rn1 FROM t
          |  )
          |)
          |""".stripMargin
      checkSorts(query, 1, 2)
    }
  }

  test("Negative: no push-down when the two orderings are disjoint") {
    withTempView("t") {
      spark.range(300).selectExpr("id % 10 as a", "id % 7 as b", "id % 5 as c")
        .createOrReplaceTempView("t")
      // Two windows over the same partition but disjoint order columns (b vs c). Neither ordering
      // covers the other, so neither local sort can be pushed onto the other: both survive.
      val query =
        """
          |SELECT a, b, c,
          |  RANK() OVER (PARTITION BY a ORDER BY b) AS rk,
          |  RANK() OVER (PARTITION BY a ORDER BY c) AS rc
          |FROM t
          |""".stripMargin
      checkSorts(query, 2, 2)
    }
  }

  test("Negative: no push-down when a shuffle separates the two sorts") {
    withTempView("t") {
      spark.range(300).selectExpr("id % 10 as a", "id % 7 as b", "id % 5 as c")
        .createOrReplaceTempView("t")
      // Two windows partitioned by different keys (a vs b) force a shuffle between their local
      // sorts. A local sort is never pushed across a shuffle, so both sorts survive.
      val query =
        """
          |SELECT a, b,
          |  RANK() OVER (PARTITION BY a ORDER BY b) AS rk,
          |  RANK() OVER (PARTITION BY b ORDER BY a) AS rb
          |FROM t
          |""".stripMargin
      checkSorts(query, 2, 2)
    }
  }

  test("Negative: no push-down when the sort directions differ") {
    withTempView("t") {
      spark.range(300).selectExpr("id % 10 as a", "id % 7 as b", "id % 5 as c")
        .createOrReplaceTempView("t")
      // The inner window orders by b ascending; the outer orders by b descending then c. The outer
      // ordering [a, b DESC, c] does not cover the inner [a, b ASC] because the directions differ,
      // so the wider sort cannot be pushed down and both sorts survive.
      val query =
        """
          |SELECT a, b, c,
          |  RANK() OVER (PARTITION BY a ORDER BY b) AS rk,
          |  ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC, c) AS rn
          |FROM t
          |""".stripMargin
      checkSorts(query, 2, 2)
    }
  }

  test("Negative: no push-down when an intermediate project computes the ordering column") {
    withTempView("t") {
      spark.range(300).selectExpr("id % 10 as a", "id % 7 as b", "id % 5 as c")
        .createOrReplaceTempView("t")
      // Between the two window sorts a project derives `bx = b + 1` (an expression alias, not a
      // plain rename). The outer window orders by [a, bx, c]; `bx` is only an expression alias, so
      // it is not rewritten back to `b` and is absent from the project's input. The push-down is
      // rejected because the rewritten ordering still references a column the child does not
      // produce, and both sorts survive.
      val query =
        """
          |SELECT a, bx, c, rn1,
          |  ROW_NUMBER() OVER (PARTITION BY a ORDER BY bx, c) AS rn2
          |FROM (
          |  SELECT a, b + 1 AS bx, c, rn1 FROM (
          |    SELECT a, b, c, RANK() OVER (PARTITION BY a ORDER BY b) AS rn1 FROM t
          |  )
          |)
          |""".stripMargin
      checkSorts(query, 2, 2)
    }
  }

  test("Negative: do not push through a non-deterministic project") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val scan = LocalTableScanExec(Seq(a, b), Nil, None)
    val orderA = SortOrder(a, Ascending) :: Nil
    val orderAB = SortOrder(a, Ascending) :: SortOrder(b, Ascending) :: Nil

    // SortExec([a, b]) <- Project[a, b, rand(0)] <- SortExec([a]). Pushing the sort below the
    // project would change which rows the seeded random stream is evaluated over, so the rule must
    // not cross a non-deterministic project (mirrors `EliminateSorts.canEliminateSort`).
    val lower = SortExec(orderA, global = false, scan)
    val project = ProjectExec(Seq(a, b, Alias(Rand(Literal(0L)), "r")()), lower)
    val upper = SortExec(orderAB, global = false, project)

    withSQLConf(SQLConf.PUSH_DOWN_LOCAL_SORT_ENABLED.key -> "true") {
      assert(PushDownLocalSort(upper).fastEquals(upper), "the plan must be left unchanged")
    }
  }

  test("Negative: do not push through a non-deterministic filter") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val scan = LocalTableScanExec(Seq(a, b), Nil, None)
    val orderA = SortOrder(a, Ascending) :: Nil
    val orderAB = SortOrder(a, Ascending) :: SortOrder(b, Ascending) :: Nil

    // SortExec([a, b]) <- Filter(rand(0) < 0.5) <- SortExec([a]). Even with cardinality-reducer
    // traversal enabled, a non-deterministic filter must not be crossed: the surviving row set
    // would change if the sort moved below it.
    val lower = SortExec(orderA, global = false, scan)
    val filter = FilterExec(LessThan(Rand(Literal(0L)), Literal(0.5)), lower)
    val upper = SortExec(orderAB, global = false, filter)

    withSQLConf(
        SQLConf.PUSH_DOWN_LOCAL_SORT_ENABLED.key -> "true",
        SQLConf.PUSH_DOWN_LOCAL_SORT_THROUGH_CARDINALITY_REDUCER_ENABLED.key -> "true") {
      assert(PushDownLocalSort(upper).fastEquals(upper), "the plan must be left unchanged")
    }
  }

  test("Cardinality reducer: do not cross a filter unless explicitly enabled") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val scan = LocalTableScanExec(Seq(a, b), Nil, None)
    val orderA = SortOrder(a, Ascending) :: Nil
    val orderAB = SortOrder(a, Ascending) :: SortOrder(b, Ascending) :: Nil

    // SortExec([a, b]) <- Filter(a IS NOT NULL) <- SortExec([a]). A filter is a cardinality
    // reducer, so it is only crossed when the reducer config is on.
    val lower = SortExec(orderA, global = false, scan)
    val filter = FilterExec(IsNotNull(a), lower)
    val upper = SortExec(orderAB, global = false, filter)

    withSQLConf(SQLConf.PUSH_DOWN_LOCAL_SORT_ENABLED.key -> "true") {
      // Reducer traversal off (default): the filter is not crossed, both sorts survive.
      withSQLConf(
          SQLConf.PUSH_DOWN_LOCAL_SORT_THROUGH_CARDINALITY_REDUCER_ENABLED.key -> "false") {
        assert(PushDownLocalSort(upper).fastEquals(upper), "must not cross the filter by default")
      }
      // Reducer traversal on: the sort is pushed below the filter and the two sorts become one.
      withSQLConf(
          SQLConf.PUSH_DOWN_LOCAL_SORT_THROUGH_CARDINALITY_REDUCER_ENABLED.key -> "true") {
        val rewritten = PushDownLocalSort(upper)
        assert(rewritten.collect { case s: SortExec => s }.length == 1)
      }
    }
  }

  test("Plan-level: push a wider sort down through order-preserving operators") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val scan = LocalTableScanExec(Seq(a, b), Nil, None)
    val orderA = SortOrder(a, Ascending) :: Nil
    val orderAB = SortOrder(a, Ascending) :: SortOrder(b, Ascending) :: Nil

    // SortExec([a, b]) <- Filter <- Project <- SortExec([a]) : the wider sort above is pushed down
    // to widen the lower [a] sort into [a, b], and the upper sort is dropped, leaving one sort.
    // Crossing the `Filter` requires the cardinality-reducer config.
    val lower = SortExec(orderA, global = false, scan)
    val project = ProjectExec(Seq(a, b), lower)
    val filter = FilterExec(IsNotNull(a), project)
    val upper = SortExec(orderAB, global = false, filter)

    withSQLConf(
        SQLConf.PUSH_DOWN_LOCAL_SORT_ENABLED.key -> "true",
        SQLConf.PUSH_DOWN_LOCAL_SORT_THROUGH_CARDINALITY_REDUCER_ENABLED.key -> "true") {
      val rewritten = PushDownLocalSort(upper)
      val sorts = rewritten.collect { case s: SortExec => s }
      assert(sorts.length == 1, "the two sorts become one")
      // The single remaining sort is the wide [a, b] sort, sitting at the bottom of the chain.
      assert(sorts.head.sortOrder == orderAB)
      assert(sorts.head.child.isInstanceOf[LocalTableScanExec])
      // The rewritten plan still re-exposes the [a, b] ordering that the dropped upper sort gave.
      assert(SortOrder.orderingSatisfies(rewritten.outputOrdering, orderAB))
      // Every operator's required ordering is still satisfied.
      assert(ValidateRequirements.validate(rewritten, UnspecifiedDistribution))
    }
  }

  test("Plan-level: rewrite the ordering through a renaming project when pushing down") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val scan = LocalTableScanExec(Seq(a, b), Nil, None)
    val orderA = SortOrder(a, Ascending) :: Nil
    val bb = Alias(b, "bb")()
    val orderABB = SortOrder(a, Ascending) :: SortOrder(bb.toAttribute, Ascending) :: Nil

    // SortExec([a, bb]) <- Project([a, b AS bb]) <- SortExec([a]) : the upper sort is over the
    // renamed `bb`. Pushing it below the project rewrites `bb` back to `b`, widening the lower sort
    // to [a, b]; the upper sort is dropped and the project re-exposes [a, bb] above.
    val lower = SortExec(orderA, global = false, scan)
    val project = ProjectExec(Seq(a, bb), lower)
    val upper = SortExec(orderABB, global = false, project)

    withSQLConf(SQLConf.PUSH_DOWN_LOCAL_SORT_ENABLED.key -> "true") {
      val rewritten = PushDownLocalSort(upper)
      val sorts = rewritten.collect { case s: SortExec => s }
      assert(sorts.length == 1, "the two sorts become one")
      // The single remaining sort is the rewritten [a, b] sort in the project's input space.
      assert(sorts.head.sortOrder == SortOrder(a, Ascending) :: SortOrder(b, Ascending) :: Nil)
      assert(sorts.head.child.isInstanceOf[LocalTableScanExec])
      // Above, the project re-exposes the [a, bb] ordering the dropped upper sort provided.
      assert(SortOrder.orderingSatisfies(rewritten.outputOrdering, orderABB))
      assert(ValidateRequirements.validate(rewritten, UnspecifiedDistribution))
    }
  }

  test("Plan-level: push a wider sort down past three stacked sorts in a single pass") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val c = AttributeReference("c", IntegerType)()
    val scan = LocalTableScanExec(Seq(a, b, c), Nil, None)
    val orderA = SortOrder(a, Ascending) :: Nil
    val orderAB = SortOrder(a, Ascending) :: SortOrder(b, Ascending) :: Nil
    val orderABC =
      SortOrder(a, Ascending) :: SortOrder(b, Ascending) :: SortOrder(c, Ascending) :: Nil

    // Sort([a,b,c]) <- Filter <- Sort([a,b]) <- Project <- Sort([a]) : the widest ordering is
    // pushed all the way to the bottom sort, and both intermediate sorts are dropped, leaving one.
    // Crossing the `Filter` requires the cardinality-reducer config.
    val bottom = SortExec(orderA, global = false, scan)
    val project = ProjectExec(Seq(a, b, c), bottom)
    val middle = SortExec(orderAB, global = false, project)
    val filter = FilterExec(IsNotNull(a), middle)
    val top = SortExec(orderABC, global = false, filter)

    withSQLConf(
        SQLConf.PUSH_DOWN_LOCAL_SORT_ENABLED.key -> "true",
        SQLConf.PUSH_DOWN_LOCAL_SORT_THROUGH_CARDINALITY_REDUCER_ENABLED.key -> "true") {
      val rewritten = PushDownLocalSort(top)
      val sorts = rewritten.collect { case s: SortExec => s }
      assert(sorts.length == 1, "the three sorts become one")
      assert(sorts.head.sortOrder == orderABC)
      assert(sorts.head.child.isInstanceOf[LocalTableScanExec])
      assert(SortOrder.orderingSatisfies(rewritten.outputOrdering, orderABC))
      assert(ValidateRequirements.validate(rewritten, UnspecifiedDistribution))
    }
  }
}

class PushDownLocalSortSuite extends PushDownLocalSortSuiteBase
  with DisableAdaptiveExecutionSuite

class PushDownLocalSortSuiteAE extends PushDownLocalSortSuiteBase
  with EnableAdaptiveExecutionSuite
