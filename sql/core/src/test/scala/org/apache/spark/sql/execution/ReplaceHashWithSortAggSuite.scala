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
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class ReplaceHashWithSortAggSuiteBase
    extends SharedSparkSession
    with AdaptiveSparkPlanHelper {

  private def checkNumAggs(df: DataFrame, hashAggCount: Int, sortAggCount: Int): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(collectWithSubqueries(plan) {
      case s @ (_: HashAggregateExec | _: ObjectHashAggregateExec) => s
    }.length == hashAggCount)
    assert(collectWithSubqueries(plan) { case s: SortAggregateExec => s }.length == sortAggCount)
  }

  private def checkAggs(
      query: String,
      enabledHashAggCount: Int,
      enabledSortAggCount: Int,
      disabledHashAggCount: Int,
      disabledSortAggCount: Int): Unit = {
    withSQLConf(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "true") {
      val df = sql(query)
      checkNumAggs(df, enabledHashAggCount, enabledSortAggCount)
      val result = df.collect()
      withSQLConf(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "false") {
        val df = sql(query)
        checkNumAggs(df, disabledHashAggCount, disabledSortAggCount)
        checkAnswer(df, result)
      }
    }
  }

  test("replace partial hash aggregate with sort aggregate") {
    withTempView("t") {
      spark.range(100).selectExpr("id as key").repartition(10).createOrReplaceTempView("t")
      Seq("FIRST", "COLLECT_LIST").foreach { aggExpr =>
        val query =
          s"""
             |SELECT key, $aggExpr(key)
             |FROM
             |(
             |   SELECT key
             |   FROM t
             |   WHERE key > 10
             |   SORT BY key
             |)
             |GROUP BY key
           """.stripMargin
        checkAggs(query, 1, 1, 2, 0)
      }
    }
  }

  test("replace partial and final hash aggregate together with sort aggregate") {
    withTempView("t1", "t2") {
      spark.range(100).selectExpr("id as key").createOrReplaceTempView("t1")
      spark.range(50).selectExpr("id as key").createOrReplaceTempView("t2")
      Seq("COUNT", "COLLECT_LIST").foreach { aggExpr =>
        val query =
          s"""
             |SELECT key, $aggExpr(key)
             |FROM
             |(
             |   SELECT /*+ SHUFFLE_MERGE(t1) */ t1.key AS key
             |   FROM t1
             |   JOIN t2
             |   ON t1.key = t2.key
             |)
             |GROUP BY key
           """.stripMargin
        checkAggs(query, 0, 1, 2, 0)
      }
    }
  }

  test("do not replace hash aggregate if child does not have sort order") {
    withTempView("t1", "t2") {
      spark.range(100).selectExpr("id as key").createOrReplaceTempView("t1")
      spark.range(50).selectExpr("id as key").createOrReplaceTempView("t2")
      Seq("COUNT", "COLLECT_LIST").foreach { aggExpr =>
        val query =
          s"""
             |SELECT key, $aggExpr(key)
             |FROM
             |(
             |   SELECT /*+ BROADCAST(t1) */ t1.key AS key
             |   FROM t1
             |   JOIN t2
             |   ON t1.key = t2.key
             |)
             |GROUP BY key
           """.stripMargin
        checkAggs(query, 1, 0, 2, 0)
      }
    }
  }

  test("do not replace hash aggregate if there is no group-by column") {
    withTempView("t1") {
      spark.range(100).selectExpr("id as key").createOrReplaceTempView("t1")
      Seq("COUNT", "COLLECT_LIST").foreach { aggExpr =>
        val query =
          s"""
             |SELECT $aggExpr(key)
             |FROM t1
           """.stripMargin
        checkAggs(query, 2, 0, 2, 0)
      }
    }
  }

  test("combine adjacent aggregate then replace it with sort aggregate") {
    withTempView("t1", "t2") {
      spark.range(100).selectExpr("id as key").createOrReplaceTempView("t1")
      spark.range(50).selectExpr("id as key").createOrReplaceTempView("t2")
      // The partial and final aggregate sit on top of a sort merge join, so the child of the
      // combined `Complete` aggregate is already ordered on the grouping key.
      val query =
        """SELECT key, count(key) FROM (
          |  SELECT /*+ SHUFFLE_MERGE(t1) */ t1.key AS key FROM t1 JOIN t2 ON t1.key = t2.key
          |) GROUP BY key""".stripMargin

      val expected = withSQLConf(
          SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "false",
          SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "false") {
        sql(query).collect()
      }

      // Combine only: the partial and final hash aggregate are merged into a single hash aggregate.
      withSQLConf(
          SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "true",
          SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "false") {
        val df = sql(query)
        checkNumAggs(df, hashAggCount = 1, sortAggCount = 0)
        checkAnswer(df, expected)
      }

      // Combine + replace: the combined hash aggregate is further replaced with a sort aggregate.
      withSQLConf(
          SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "true",
          SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "true") {
        val df = sql(query)
        checkNumAggs(df, hashAggCount = 0, sortAggCount = 1)
        checkAnswer(df, expected)
      }
    }
  }

  test("SPARK-43317: Combined sort aggregate keeps the FILTER clause") {
    // Regression test for the wrong-results bug this fix addresses: when the partial and final
    // aggregate are merged into a single `Complete` mode aggregate that is then replaced with a
    // `SortAggregateExec` (`replaceHashWithSortAgg=true`), the `FILTER (WHERE ...)` clause must be
    // preserved. The filter only lives on the partial aggregate expressions, so combining must take
    // the aggregate functions from the partial aggregate; otherwise the filter is silently dropped
    // and the result is wrong. This drives the PR's own repro query end-to-end through the sort
    // aggregate path.
    withTempView("t1", "t2") {
      spark.range(100).selectExpr("id as key").createOrReplaceTempView("t1")
      spark.range(50).selectExpr("id as key").createOrReplaceTempView("t2")
      val query =
        """SELECT key, count(*) FILTER (WHERE key > 10) FROM (
          |  SELECT /*+ SHUFFLE_MERGE(t1) */ t1.key AS key FROM t1 JOIN t2 ON t1.key = t2.key
          |) GROUP BY key""".stripMargin

      val expected = withSQLConf(
          SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "false",
          SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "false") {
        sql(query).collect()
      }

      withSQLConf(
          SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "true",
          SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "true") {
        val df = sql(query)
        // Combined into a single `Complete` mode sort aggregate.
        checkNumAggs(df, hashAggCount = 0, sortAggCount = 1)
        checkAnswer(df, expected)
      }
    }
  }
}

class ReplaceHashWithSortAggSuite extends ReplaceHashWithSortAggSuiteBase
  with DisableAdaptiveExecutionSuite

class ReplaceHashWithSortAggSuiteAE extends ReplaceHashWithSortAggSuiteBase
  with EnableAdaptiveExecutionSuite
