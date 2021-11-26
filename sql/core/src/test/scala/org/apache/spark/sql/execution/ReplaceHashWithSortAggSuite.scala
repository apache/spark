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

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class ReplaceHashWithSortAggSuiteBase
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  private def checkNumAggs(df: DataFrame, hashAggCount: Int, sortAggCount: Int): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(collectWithSubqueries(plan) { case s: HashAggregateExec => s }.length == hashAggCount)
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
      val query =
        """
          |SELECT key, FIRST(key)
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

  test("replace partial and final hash aggregate together with sort aggregate") {
    withTempView("t1", "t2") {
      spark.range(100).selectExpr("id as key").createOrReplaceTempView("t1")
      spark.range(50).selectExpr("id as key").createOrReplaceTempView("t2")
      val query =
        """
          |SELECT key, COUNT(key)
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

  test("do not replace hash aggregate if child does not have sort order") {
    withTempView("t1", "t2") {
      spark.range(100).selectExpr("id as key").createOrReplaceTempView("t1")
      spark.range(50).selectExpr("id as key").createOrReplaceTempView("t2")
      val query =
        """
          |SELECT key, COUNT(key)
          |FROM
          |(
          |   SELECT /*+ BROADCAST(t1) */ t1.key AS key
          |   FROM t1
          |   JOIN t2
          |   ON t1.key = t2.key
          |)
          |GROUP BY key
        """.stripMargin
      checkAggs(query, 2, 0, 2, 0)
    }
  }

  test("do not replace hash aggregate if there is no group-by column") {
    withTempView("t1") {
      spark.range(100).selectExpr("id as key").createOrReplaceTempView("t1")
      val query =
        """
          |SELECT COUNT(key)
          |FROM t1
        """.stripMargin
      checkAggs(query, 2, 0, 2, 0)
    }
  }
}

class ReplaceHashWithSortAggSuite extends ReplaceHashWithSortAggSuiteBase
  with DisableAdaptiveExecutionSuite

class ReplaceHashWithSortAggSuiteAE extends ReplaceHashWithSortAggSuiteBase
  with EnableAdaptiveExecutionSuite
