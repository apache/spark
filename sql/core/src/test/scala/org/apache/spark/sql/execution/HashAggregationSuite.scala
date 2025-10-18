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
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.test.SharedSparkSession

abstract class HashAggregationSuiteBase
    extends QueryTest
    with SharedSparkSession
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
      enabledSortAggCount: Int): Unit = {
    val df = sql(query)
    checkNumAggs(df, enabledHashAggCount, enabledSortAggCount)
  }

  test("should use SortAgg to preserve order") {
    withTempView("t") {
      spark.range(100).selectExpr("id as key").createOrReplaceTempView("t")
      Seq("FIRST", "LAST", "FIRST_VALUE", "LAST_VALUE").foreach { aggExpr =>
        val query =
          s"""
             |SELECT $aggExpr(key)
             |FROM
             |(
             |   SELECT key
             |   FROM t
             |   ORDER BY key
             |)
           """.stripMargin
        checkAggs(query, 0, 2)
      }
    }
  }
}

class HashAggregationSuite extends HashAggregationSuiteBase
  with DisableAdaptiveExecutionSuite

class HashAggregationSuiteAE extends HashAggregationSuiteBase
  with EnableAdaptiveExecutionSuite
