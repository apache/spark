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
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class CombineAdjacentAggregationSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

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

  test("test combine adjacent aggregation") {
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

      // combine adjacent sort aggregate
      checkNumAggregation(
        "SELECT v, max(k) FROM (SELECT /*+ repartition(v) */ * FROM t) GROUP BY v",
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
}
