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
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper,
  DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec,
  ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class RemoveRedundantAggregatesSuiteBase
    extends SharedSparkSession
    with AdaptiveSparkPlanHelper {

  private def aggNodes(df: DataFrame): Seq[BaseAggregateExec] = {
    val plan = df.queryExecution.executedPlan
    collectWithSubqueries(plan) { case a: BaseAggregateExec => a }
  }

  private def isComplete(a: BaseAggregateExec): Boolean =
    // Grouping-only aggregates have empty aggregateExpressions; treat them as complete
    // since there are no function modes to check.
    a.aggregateExpressions.isEmpty ||
      a.aggregateExpressions.forall(_.mode == Complete)

  // Run query with the rule enabled, assert it collapsed to a single Complete agg, then
  // re-run with the rule disabled and use that as the correctness reference.
  private def checkCollapsed(query: String): DataFrame = {
    val df = sql(query)
    val aggs = aggNodes(df)
    assert(aggs.length == 1, s"Expected 1 agg node but got ${aggs.length}: $aggs")
    assert(isComplete(aggs.head), s"Expected Complete mode but got: ${aggs.head}")
    val disabledDf = withSQLConf(SQLConf.REMOVE_REDUNDANT_AGGREGATES_ENABLED.key -> "false") {
      sql(query)
    }
    assert(df.schema == disabledDf.schema,
      "collapsed node output schema must match the original final agg schema")
    checkAnswer(df, disabledDf.collect())
    df
  }

  // Run query with the rule enabled, assert it kept two agg nodes, then re-run with the
  // rule disabled and use that as the correctness reference.
  private def checkNotCollapsed(query: String): DataFrame = {
    val df = sql(query)
    val aggs = aggNodes(df)
    assert(aggs.length == 2, s"Expected 2 agg nodes but got ${aggs.length}: $aggs")
    val disabledDf = withSQLConf(SQLConf.REMOVE_REDUNDANT_AGGREGATES_ENABLED.key -> "false") {
      sql(query)
    }
    assert(df.schema == disabledDf.schema,
      "non-collapsed node output schema must match the disabled-rule schema")
    checkAnswer(df, disabledDf.collect())
    df
  }

  // Shared test data: t1 is a fact table with many rows per (a, b) key;
  // t2 is a lookup table with exactly one row per (a, b) key. Joining t1 with t2
  // on (a, b) and then aggregating by (a, b) tests that the collapsed Complete-mode
  // node accumulates all raw t1 rows per key -- a bug in buffer initialization would
  // produce wrong aggregate values since each key has multiple input rows.
  //
  // t1: a in [0,4], b in [0,9] (50 distinct pairs, 20 rows each = 1000 rows total)
  // t2: exactly the same 50 pairs, one row each -- a cross join of range(5) x range(10)
  //     ensures no duplicates and Long-typed keys matching t1 without implicit casts.
  private def createFactAndLookup(): Unit = {
    spark.range(0, 1000, 1, 4)
      .selectExpr("(id % 5) as a", "(id % 10) as b", "cast(id as double) as v")
      .createOrReplaceTempView("t1")
    sql("SELECT a.id as a, b.id as b FROM range(5) a CROSS JOIN range(10) b")
      .createOrReplaceTempView("t2")
  }

  test("HashAgg: grouping only, no aggregate functions") {
    withTempView("t1", "t2") {
      createFactAndLookup()
      checkCollapsed(
        """SELECT /*+ MERGE(t1, t2) */ t1.a, t1.b
          |FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b
          |GROUP BY t1.a, t1.b
          |""".stripMargin)
    }
  }

  test("HashAgg: global aggregate over single-partition child collapses") {
    withTempView("t") {
      // A single-partition child satisfies AllTuples, so EnsureRequirements skips the
      // shuffle between Partial and Final for a global aggregate.
      spark.range(0, 100, 1, 1)
        .selectExpr("cast(id as double) as v")
        .createOrReplaceTempView("t")
      checkCollapsed("SELECT sum(v) as s FROM t")
    }
  }

  test("HashAgg: with SUM aggregate function") {
    withTempView("t1", "t2") {
      // Each of the 50 (a, b) keys matches 20 t1 rows. sum(v) accumulates 20 raw
      // values per key. If Complete mode used mergeExpressions (the Final path) it
      // would treat raw doubles as pre-aggregated buffers and produce wrong sums.
      createFactAndLookup()
      val df = checkCollapsed(
        """SELECT /*+ MERGE(t1, t2) */ t1.a, t1.b, sum(t1.v) as s
          |FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b
          |GROUP BY t1.a, t1.b
          |""".stripMargin)
      assert(aggNodes(df).head.isInstanceOf[HashAggregateExec])
    }
  }

  test("HashAgg: strict-subset child partitioning collapses") {
    withTempView("t1", "t2") {
      // The child is clustered on a strict subset (a) of the GROUP BY keys (a, b).
      // HashPartitioning(a) satisfies ClusteredDistribution(a, b), so EnsureRequirements
      // skips the shuffle and the partial+final pair collapses to a single Complete agg.
      createFactAndLookup()
      checkCollapsed(
        """SELECT /*+ MERGE(t1, t2) */ t1.a, t1.b, sum(t1.v) as s
          |FROM t1 JOIN t2 ON t1.a = t2.a
          |GROUP BY t1.a, t1.b
          |""".stripMargin)
    }
  }

  test("HashAgg: with AVG (multi-buffer aggregate)") {
    withTempView("t1", "t2") {
      // AVG maintains two buffer slots (sum + count). With 20 raw rows per key,
      // a wrong buffer initialisation would produce an incorrect average.
      createFactAndLookup()
      checkCollapsed(
        """SELECT /*+ MERGE(t1, t2) */ t1.a, t1.b, avg(t1.v) as av
          |FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b
          |GROUP BY t1.a, t1.b
          |""".stripMargin)
    }
  }

  test("ObjectHashAgg: collect_list collapses to Complete mode") {
    withTempView("t1", "t2") {
      // collect_list must accumulate all 20 matching t1.v values per key.
      // A wrong buffer initialisation would produce an empty or truncated list.
      createFactAndLookup()
      val df = checkCollapsed(
        """SELECT /*+ MERGE(t1, t2) */ t1.a, t1.b,
          |  sort_array(collect_list(t1.v)) as vals
          |FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b
          |GROUP BY t1.a, t1.b
          |""".stripMargin)
      // collect_list is a TypedImperativeAggregate -> ObjectHashAggregateExec.
      assert(aggNodes(df).head.isInstanceOf[ObjectHashAggregateExec])
    }
  }

  test("SortAgg: collapses to Complete mode when shuffle is skipped") {
    withSQLConf(
        SQLConf.USE_OBJECT_HASH_AGG.key -> "false",
        SQLConf.PREFER_SORTMERGEJOIN.key -> "true") {
      withTempView("t1", "t2") {
        // ObjectHash disabled -> SortAggregateExec. The join on exactly the grouping
        // keys makes EnsureRequirements skip the shuffle, triggering the rule.
        // 20 raw rows per key means a wrong buffer path would produce wrong lists.
        createFactAndLookup()
        val df = checkCollapsed(
          """SELECT /*+ MERGE(t1, t2) */ t1.a, t1.b,
            |  sort_array(collect_list(t1.v)) as vals
            |FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b
            |GROUP BY t1.a, t1.b
            |""".stripMargin)
        // ObjectHash disabled -> falls through to SortAggregateExec.
        assert(aggNodes(df).head.isInstanceOf[SortAggregateExec])
      }
    }
  }

  test("Exchange between partial and final prevents collapsing") {
    withTempView("t") {
      // A plain range scan gives no special partitioning, so EnsureRequirements inserts an
      // Exchange between Partial and Final. The rule must NOT fire.
      spark.range(0, 100, 1, 4)
        .selectExpr("id % 10 as k", "cast(id as double) as v")
        .createOrReplaceTempView("t")
      checkNotCollapsed("SELECT k, sum(v) as s FROM t GROUP BY k")
    }
  }

  test("distinct aggregate: correctness preserved") {
    withTempView("t1", "t2") {
      // COUNT(DISTINCT v) plan is not directly targeted by this rule (no adjacent
      // Partial->Final pair without an Exchange in the standard SMJ setup), but verify
      // correctness to guard against accidental interference.
      createFactAndLookup()
      val query =
        """SELECT /*+ MERGE(t1, t2) */ t1.a, count(distinct t1.v) as c
          |FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b
          |GROUP BY t1.a
          |""".stripMargin
      val expected = withSQLConf(SQLConf.REMOVE_REDUNDANT_AGGREGATES_ENABLED.key -> "false") {
        sql(query).collect()
      }
      checkAnswer(sql(query), expected)
    }
  }

  test("different grouping keys: Final(a) over Partial(a, b) not collapsed") {
    withTempView("t1", "t2") {
      spark.range(0, 50, 1, 4)
        .selectExpr("id as a", "id as b")
        .createOrReplaceTempView("t1")
      spark.range(0, 50, 1, 4)
        .selectExpr("id as a", "id as b")
        .createOrReplaceTempView("t2")

      // Join on (a, b) but GROUP BY only a: join output is ClusteredDistribution(a, b),
      // which does not satisfy ClusteredDistribution(a), so EnsureRequirements inserts
      // an Exchange before the Final agg and the rule does NOT fire.
      checkNotCollapsed(
        """SELECT /*+ MERGE(t1, t2) */ t1.a, count(*) as cnt
          |FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b
          |GROUP BY t1.a
          |""".stripMargin)
    }
  }

  test("disabled by config: rule does not collapse") {
    withSQLConf(SQLConf.REMOVE_REDUNDANT_AGGREGATES_ENABLED.key -> "false") {
      withTempView("t1", "t2") {
        createFactAndLookup()
        // With the rule disabled, checkNotCollapsed's enabled run also has it disabled
        // (outer withSQLConf wins), so we just verify the plan shape directly.
        val df = sql(
          """SELECT /*+ MERGE(t1, t2) */ t1.a, t1.b, sum(t1.v) as s
            |FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b
            |GROUP BY t1.a, t1.b
            |""".stripMargin)
        val aggs = aggNodes(df)
        assert(aggs.length == 2, s"Expected 2 agg nodes but got ${aggs.length}: $aggs")
      }
    }
  }
}

class RemoveRedundantAggregatesSuite extends RemoveRedundantAggregatesSuiteBase
  with DisableAdaptiveExecutionSuite

class RemoveRedundantAggregatesSuiteAE extends RemoveRedundantAggregatesSuiteBase
  with EnableAdaptiveExecutionSuite
