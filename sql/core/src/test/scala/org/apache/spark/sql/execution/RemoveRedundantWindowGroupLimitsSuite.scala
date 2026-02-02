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
import org.apache.spark.sql.execution.window.WindowGroupLimitExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession

abstract class RemoveRedundantWindowGroupLimitsSuiteBase
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  private def checkNumWindowGroupLimits(df: DataFrame, count: Int): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(collectWithSubqueries(plan) { case exec: WindowGroupLimitExec => exec }.length == count)
  }

  private def checkWindowGroupLimits(query: String, count: Int): Unit = {
    val df = sql(query)
    checkNumWindowGroupLimits(df, count)
    val result = df.collect()
    checkAnswer(df, result)
  }

  test("remove redundant WindowGroupLimits") {
    withTempView("t") {
      spark.range(0, 100).withColumn("value", lit(1)).createOrReplaceTempView("t")
      val query1 =
        """
          |SELECT *
          |FROM (
          |    SELECT id, rank() OVER w AS rn
          |    FROM t
          |    GROUP BY id
          |    WINDOW w AS (PARTITION BY id ORDER BY max(value))
          |)
          |WHERE rn < 3
          |""".stripMargin
      checkWindowGroupLimits(query1, 1)

      val query2 =
        """
          |SELECT *
          |FROM (
          |    SELECT id, rank() OVER w AS rn
          |    FROM t
          |    GROUP BY id
          |    WINDOW w AS (ORDER BY max(value))
          |)
          |WHERE rn < 3
          |""".stripMargin
      checkWindowGroupLimits(query2, 2)
    }
  }
}

class RemoveRedundantWindowGroupLimitsSuite extends RemoveRedundantWindowGroupLimitsSuiteBase
  with DisableAdaptiveExecutionSuite

class RemoveRedundantWindowGroupLimitsSuiteAE extends RemoveRedundantWindowGroupLimitsSuiteBase
  with EnableAdaptiveExecutionSuite
