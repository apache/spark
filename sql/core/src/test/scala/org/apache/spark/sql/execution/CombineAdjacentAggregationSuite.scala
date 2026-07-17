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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Final}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class CombineAdjacentAggregationSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private def numAggregates(query: String): Int = {
    val df = sql(query)
    df.collect()
    collect(df.queryExecution.executedPlan) {
      case agg: BaseAggregateExec => agg
    }.size
  }

  private def checkNumAggregation(
      query: String,
      numAggWithDisabled: Int,
      numAggWithEnabled: Int): Unit = {
    var expectedResult: Array[Row] = null
    withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "false") {
      val df = sql(query)
      expectedResult = df.collect()
      assert(collect(df.queryExecution.executedPlan) {
        case agg: BaseAggregateExec => agg
      }.size == numAggWithDisabled)
    }

    withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "true") {
      val df = sql(query)
      checkAnswer(df, expectedResult)
      assert(collect(df.queryExecution.executedPlan) {
        case agg: BaseAggregateExec => agg
      }.size == numAggWithEnabled)
    }
  }

  test("Test combine adjacent aggregation") {
    withTempView("t") {
      spark.range(20).selectExpr(s"id % 3 as k", "id % 7 as v")
        .createOrReplaceTempView("t")

      // do not combine if no adjacent aggregation
      checkNumAggregation(
        "SELECT k, count(*) FROM t GROUP BY k",
        numAggWithDisabled = 2,
        numAggWithEnabled = 2)

      // combine adjacent hash aggregation
      checkNumAggregation(
        "SELECT k, count(*) FROM (SELECT /*+ repartition(k) */ * FROM t) GROUP BY k",
        numAggWithDisabled = 2,
        numAggWithEnabled = 1)

      // combine adjacent object hash aggregate
      checkNumAggregation(
        "SELECT k, collect_set(v) FROM (SELECT /*+ repartition(k) */ * FROM t) GROUP BY k",
        numAggWithDisabled = 2,
        numAggWithEnabled = 1)

      // do not combine adjacent hash aggregation
      checkNumAggregation(
        "SELECT k, count(distinct v) FROM t GROUP BY k",
        numAggWithDisabled = 4,
        numAggWithEnabled = 4)

      // combine adjacent hash aggregation
      checkNumAggregation(
        "SELECT k, count(distinct v) FROM (SELECT /*+ repartition(k) */ * FROM t) GROUP BY k",
        numAggWithDisabled = 4,
        numAggWithEnabled = 2)
    }
  }

  test("Combine adjacent sort aggregate") {
    // `max`/`min` over a string column produces a non-mutable aggregation buffer, so it cannot be
    // planned as a hash or object-hash aggregate and Spark falls back to `SortAggregateExec`. This
    // exercises the sort-aggregate rewrite branch specifically (a numeric `max` would be planned as
    // a `HashAggregateExec`, leaving that branch untested).
    withTempView("t") {
      spark.range(20).selectExpr("id % 3 as k", "cast(id % 7 as string) as v")
        .createOrReplaceTempView("t")
      val query = "SELECT k, max(v) FROM (SELECT /*+ repartition(k) */ * FROM t) GROUP BY k"

      val expected = withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "false") {
        val df = sql(query)
        val aggs = collect(df.queryExecution.executedPlan) { case agg: BaseAggregateExec => agg }
        // Two sort aggregates (partial + final), confirming the sort-aggregate branch is exercised.
        assert(aggs.size == 2)
        assert(aggs.forall(_.isInstanceOf[SortAggregateExec]))
        df.collect()
      }

      withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "true") {
        val df = sql(query)
        checkAnswer(df, expected)
        val aggs = collect(df.queryExecution.executedPlan) { case agg: BaseAggregateExec => agg }
        // Combined into a single `Complete` mode sort aggregate.
        assert(aggs.size == 1)
        assert(aggs.head.isInstanceOf[SortAggregateExec])
        assert(aggs.head.aggregateExpressions.forall(_.mode == Complete))
      }
    }
  }

  test("Do not combine adjacent aggregates with mismatched grouping expressions") {
    // A grouping-expression mismatch normally forces `EnsureRequirements` to insert an Exchange
    // between the partial and final aggregate, so the planner never produces an adjacent pair with
    // differing grouping keys. To exercise the canonicalized grouping-equality guard in
    // `isPartialAgg` directly, we take a genuinely adjacent partial/final pair (the input is
    // already partitioned by `k`, so no Exchange is inserted between them) and rewrite the final
    // aggregate's grouping expressions to mismatch. The rule must then refuse to combine the pair.
    withTempView("t") {
      spark.range(20).selectExpr("id % 3 as k", "id % 7 as v").createOrReplaceTempView("t")
      val queryText = "SELECT k, count(*) FROM (SELECT /*+ repartition(k) */ * FROM t) GROUP BY k"
      // Build the physical plan with the rule disabled so the raw adjacent partial/final pair
      // survives, and with AQE disabled so `executedPlan` is a plain tree we can rewrite.
      val finalAgg = withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "false") {
        collect(sql(queryText).queryExecution.executedPlan) {
          case f: HashAggregateExec if f.child.isInstanceOf[HashAggregateExec] => f
        }.head
      }

      withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "true") {
        // Sanity check: with matching grouping expressions, the adjacent pair is combined into one.
        assert(collect(CombineAdjacentAggregation(finalAgg)) {
          case agg: BaseAggregateExec => agg
        }.size == 1)

        // Mismatched grouping expressions: the same adjacent pair must not be combined. We copy the
        // tags (including the `logicalLink`) from the original node so that `isPartialAgg` reaches
        // the grouping-equality check rather than short-circuiting on a missing `logicalLink`.
        val mismatched = finalAgg.copy(groupingExpressions = Nil)
        mismatched.copyTagsFrom(finalAgg)
        val result = CombineAdjacentAggregation(mismatched)
        val aggs = collect(result) { case agg: BaseAggregateExec => agg }
        assert(aggs.size == 2)
        assert(aggs.head.aggregateExpressions.forall(_.mode == Final))
      }
    }
  }

  test("Combined aggregate reads original input with a zero buffer offset") {
    // When adjacent aggregates are combined into a single `Complete` mode aggregate, its child
    // becomes the partial aggregate's child, so it reads the original input rather than a row of
    // `[groupingKeys, aggregationBuffers]`. `initialInputBufferOffset` must therefore be reset to
    // 0. Use multiple grouping keys and multiple aggregate functions so that a stale, non-zero
    // offset would bind the aggregate functions against the wrong input columns.
    withTempView("t") {
      spark.range(60).selectExpr("id % 3 as k1", "id % 5 as k2", "id % 7 as v")
        .createOrReplaceTempView("t")

      // hash aggregate with declarative aggregate functions
      checkNumAggregation(
        """SELECT k1, k2, sum(v), count(v), avg(v), max(v), min(v)
          |FROM (SELECT /*+ repartition(k1, k2) */ * FROM t) GROUP BY k1, k2""".stripMargin,
        numAggWithDisabled = 2,
        numAggWithEnabled = 1)

      // object hash aggregate with imperative aggregate functions
      checkNumAggregation(
        """SELECT k1, k2, sort_array(collect_list(v)), count(v)
          |FROM (SELECT /*+ repartition(k1, k2) */ * FROM t) GROUP BY k1, k2""".stripMargin,
        numAggWithDisabled = 2,
        numAggWithEnabled = 1)
    }
  }

  test("Do not combine when a shuffle sits between the partial and final aggregate") {
    withTempView("t") {
      spark.range(20).selectExpr("id % 3 as k", "id % 7 as v")
        .createOrReplaceTempView("t")

      // The input is repartitioned by `k`, but the query groups by `k + 1`, so the partial
      // aggregate's output is not partitioned the way the final aggregate requires.
      // `EnsureRequirements` therefore inserts an Exchange between the two aggregates, leaving them
      // non-adjacent, and the rule (which only matches a final aggregate whose child is the partial
      // aggregate) must not combine them.
      checkNumAggregation(
        "SELECT k + 1, count(*) FROM (SELECT /*+ repartition(k) */ * FROM t) GROUP BY k + 1",
        numAggWithDisabled = 2,
        numAggWithEnabled = 2)

      // Same non-adjacency guard for an object-hash aggregate (`collect_set` is imperative).
      checkNumAggregation(
        "SELECT k + 1, collect_set(v) FROM (SELECT /*+ repartition(k) */ * FROM t) GROUP BY k + 1",
        numAggWithDisabled = 2,
        numAggWithEnabled = 2)
    }
  }

  test("Combine with a string grouping key and a HAVING clause") {
    withTempView("t") {
      spark.range(20).selectExpr("cast(id % 4 as string) as s", "id % 7 as v")
        .createOrReplaceTempView("t")

      // string grouping key
      checkNumAggregation(
        "SELECT s, count(*) FROM (SELECT /*+ repartition(s) */ * FROM t) GROUP BY s",
        numAggWithDisabled = 2,
        numAggWithEnabled = 1)

      // aggregate with a FILTER clause and a HAVING predicate
      checkNumAggregation(
        """SELECT s, count(*) FILTER (WHERE v > 2), sum(v)
          |FROM (SELECT /*+ repartition(s) */ * FROM t) GROUP BY s HAVING sum(v) > 5""".stripMargin,
        numAggWithDisabled = 2,
        numAggWithEnabled = 1)
    }
  }

  test("SPARK-43317: Combined aggregate keeps the FILTER clause of aggregate functions") {
    // The FILTER (WHERE ...) clause only lives on the partial aggregate expressions; the final
    // aggregate merely merges the partial aggregation buffers. Combining must therefore take the
    // aggregate functions (and their filters) from the partial aggregate, otherwise the filter is
    // silently dropped and the result is wrong.
    withTempView("t") {
      spark.range(20).selectExpr("id % 3 as k", "id as v")
        .createOrReplaceTempView("t")
      val query =
        """SELECT k, count(*) FILTER (WHERE v > 5), sum(v) FILTER (WHERE v < 15), count(*)
          |FROM (SELECT /*+ repartition(k) */ * FROM t) GROUP BY k""".stripMargin

      val expected = withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "false") {
        sql(query).collect()
      }
      withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "true") {
        val df = sql(query)
        checkAnswer(df, expected)
        assert(collect(df.queryExecution.executedPlan) {
          case agg: BaseAggregateExec => agg
        }.size == 1)
      }

      // A `count(distinct)` with a FILTER clause. The distinct-with-filter rewrite expands the
      // input (adding a group id) and shuffles on the distinct key and again on the grouping key,
      // so an Exchange sits between the partial and final aggregate. They are therefore not
      // adjacent and the rule must not combine them; the FILTER must still produce correct results.
      val distinctQuery =
        """SELECT k, count(distinct v) FILTER (WHERE v > 5)
          |FROM (SELECT /*+ repartition(k) */ * FROM t) GROUP BY k""".stripMargin
      val distinctExpected =
        withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "false") {
          val df = sql(distinctQuery)
          assert(collect(df.queryExecution.executedPlan) {
            case agg: BaseAggregateExec => agg
          }.size == 4)
          df.collect()
        }
      withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "true") {
        val df = sql(distinctQuery)
        checkAnswer(df, distinctExpected)
        assert(collect(df.queryExecution.executedPlan) {
          case agg: BaseAggregateExec => agg
        }.size == 4)
      }
    }
  }

  test("Default value falls back to spark.sql.execution.replaceHashWithSortAgg") {
    withTempView("t") {
      spark.range(20).selectExpr("id % 3 as k", "id % 7 as v")
        .createOrReplaceTempView("t")
      val query = "SELECT k, count(*) FROM (SELECT /*+ repartition(k) */ * FROM t) GROUP BY k"

      // `spark.sql.execution.combineAdjacentAggregation` is unset here, so it falls back to
      // `spark.sql.execution.replaceHashWithSortAgg`.
      withSQLConf(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "false") {
        assert(numAggregates(query) == 2)
      }
      withSQLConf(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "true") {
        assert(numAggregates(query) == 1)
      }

      // An explicit value overrides the fallback.
      withSQLConf(
          SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "true",
          SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "false") {
        assert(numAggregates(query) == 2)
      }
    }
  }
}
