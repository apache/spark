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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.{functions => F, QueryTest}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Final, Partial}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{collect_list, count, session_window, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class PartialAggregationBypassSuite
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  private def aggNodes(plan: SparkPlan): Seq[BaseAggregateExec] =
    collectWithSubqueries(plan) { case a: BaseAggregateExec => a }

  private def withAndWithoutAQE(f: => Unit): Unit = {
    Seq("true", "false").foreach { aqe =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe)(f)
    }
  }

  test("bypassPartialAggregation=true produces no Partial-mode node and one Complete-mode node") {
    withAndWithoutAQE {
      withSQLConf(SQLConf.BYPASS_PARTIAL_AGGREGATION.key -> "true") {
        val df = spark.range(100).toDF("v")
          .groupBy((F.col("v") % 10).as("k"))
          .agg(F.sum("v"), F.count("v"))
        val aggs = aggNodes(df.queryExecution.executedPlan)
        assert(aggs.forall(_.aggregateExpressions.forall(_.mode != Partial)),
          "expected no Partial-mode aggregation nodes")
        assert(aggs.exists(_.aggregateExpressions.exists(_.mode == Complete)),
          "expected at least one Complete-mode aggregation node")
        assert(!aggs.exists(_.aggregateExpressions.exists(_.mode == Final)),
          "expected no Final-mode aggregation nodes")
      }
    }
  }

  test("bypassPartialAggregation=true does not bypass global aggregation " +
      "(groupingExpressions.isEmpty)") {
    // Global aggregations (no GROUP BY) always produce a single output row, so the
    // pre-shuffle partial aggregation achieves the maximum possible reduction ratio.
    // Bypassing it would shuffle all raw rows to a single partition with no benefit,
    // which is strictly worse than the normal Partial+Final path. The bypass is
    // therefore skipped for global aggregations.
    withAndWithoutAQE {
      withSQLConf(SQLConf.BYPASS_PARTIAL_AGGREGATION.key -> "true") {
        val df = spark.range(100).agg(F.sum("id"), F.count("id"))
        val aggs = aggNodes(df.queryExecution.executedPlan)
        assert(aggs.exists(_.aggregateExpressions.exists(_.mode == Partial)),
          "expected a Partial-mode node for global aggregation")
        assert(aggs.exists(_.aggregateExpressions.exists(_.mode == Final)),
          "expected a Final-mode node for global aggregation")
        assert(!aggs.exists(_.aggregateExpressions.exists(_.mode == Complete)),
          "expected no Complete-mode nodes for global aggregation")
      }
    }
  }

  test("bypassPartialAggregation=false (default) produces Partial+Final plan") {
    withAndWithoutAQE {
      val df = spark.range(100).toDF("v")
        .groupBy((F.col("v") % 10).as("k"))
        .agg(F.sum("v"))
      val aggs = aggNodes(df.queryExecution.executedPlan)
      assert(aggs.exists(_.aggregateExpressions.exists(_.mode == Partial)),
        "expected a Partial-mode aggregation node")
      assert(aggs.exists(_.aggregateExpressions.exists(_.mode == Final)),
        "expected a Final-mode aggregation node")
    }
  }

  test("results are identical with and without partial aggregation - SUM") {
    withAndWithoutAQE {
      val data = spark.range(1000).selectExpr("id % 7 as k", "id as v")
      val expected = data.groupBy("k").sum("v").orderBy("k").collect()
      withSQLConf(SQLConf.BYPASS_PARTIAL_AGGREGATION.key -> "true") {
        val actual = data.groupBy("k").sum("v").orderBy("k")
        checkAnswer(actual, expected)
      }
    }
  }

  test("results are identical with and without partial aggregation - COUNT") {
    withAndWithoutAQE {
      val data = spark.range(1000).selectExpr("id % 13 as k")
      val expected = data.groupBy("k").count().orderBy("k").collect()
      withSQLConf(SQLConf.BYPASS_PARTIAL_AGGREGATION.key -> "true") {
        val actual = data.groupBy("k").count().orderBy("k")
        checkAnswer(actual, expected)
      }
    }
  }

  test("results are identical with and without partial aggregation - AVG") {
    withAndWithoutAQE {
      val data = spark.range(1000).selectExpr("id % 5 as k", "id as v")
      val expected = data.groupBy("k").avg("v").orderBy("k").collect()
      withSQLConf(SQLConf.BYPASS_PARTIAL_AGGREGATION.key -> "true") {
        val actual = data.groupBy("k").avg("v").orderBy("k")
        checkAnswer(actual, expected)
      }
    }
  }

  test("results are identical with and without partial aggregation - collect_list " +
      "(TypedImperativeAggregate via ObjectHashAggregateExec)") {
    // collect_list is a TypedImperativeAggregate whose buffer cannot be expressed as fixed-width
    // slots, so createAggregate routes it through ObjectHashAggregateExec rather than
    // HashAggregateExec. This test verifies that the bypass correctly produces a single
    // Complete-mode ObjectHashAggregateExec and that its results match the normal Partial+Final
    // path.
    withAndWithoutAQE {
      val data = spark.range(20).selectExpr("id % 4 as k", "id as v")
      val expected = data.groupBy("k").agg(collect_list("v"))
        .orderBy("k").collect()
      withSQLConf(SQLConf.BYPASS_PARTIAL_AGGREGATION.key -> "true") {
        val df = data.groupBy("k").agg(collect_list("v")).orderBy("k")
        val aggs = aggNodes(df.queryExecution.executedPlan)
        assert(aggs.exists(_.isInstanceOf[ObjectHashAggregateExec]),
          "expected ObjectHashAggregateExec for TypedImperativeAggregate")
        assert(aggs.forall(_.aggregateExpressions.forall(_.mode != Partial)),
          "expected no Partial-mode aggregation nodes")
        // checkAnswer is not used here because it does not sort nested arrays, and
        // collect_list output order within each group is non-deterministic: it depends
        // on row processing order which can differ between Partial+Final and Complete
        // aggregation paths. Sort the arrays before comparing.
        val actual = df.collect()
        assert(actual.length == expected.length)
        actual.zip(expected).foreach { case (a, e) =>
          assert(a.getLong(0) == e.getLong(0))
          assert(a.getSeq[Long](1).sorted == e.getSeq[Long](1).sorted)
        }
      }
    }
  }

  test("session_window with bypassPartialAggregation=true merges overlapping sessions correctly") {
    // Regression test: when bypassPartialAggregation=true, the early-return path in
    // planAggregateWithoutDistinct skipped mayAppendMergingSessionExec, so overlapping
    // sessions were never merged and the aggregation produced wrong row counts / sums.
    import testImplicits._
    // Two events for key "a" fall within 10s of each other and must merge into one session.
    // One event for key "b" stands alone.
    val df = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:39", 2, "a"), // within 10s of the first "a" -- same session
      ("2016-03-27 19:39:56", 3, "a"), // > 10s gap -- separate session
      ("2016-03-27 19:39:27", 4, "b")
    ).toDF("time", "value", "id")

    val expected = df
      .groupBy(session_window($"time", "10 seconds"), $"id")
      .agg(count("*").as("cnt"), sum("value").as("total"))
      .orderBy($"session_window.start".asc)
      .selectExpr(
        "CAST(session_window.start AS STRING)",
        "CAST(session_window.end AS STRING)",
        "id", "cnt", "total")
      .collect()

    // With the bug, bypassPartialAggregation=true skips MergingSessionsExec and the two
    // "a" events that belong to the same session appear as separate rows.
    withAndWithoutAQE {
      withSQLConf(SQLConf.BYPASS_PARTIAL_AGGREGATION.key -> "true") {
        val actual = df
          .groupBy(session_window($"time", "10 seconds"), $"id")
          .agg(count("*").as("cnt"), sum("value").as("total"))
          .orderBy($"session_window.start".asc)
          .selectExpr(
            "CAST(session_window.start AS STRING)",
            "CAST(session_window.end AS STRING)",
            "id", "cnt", "total")
        checkAnswer(actual, expected)
      }
    }
  }
}
