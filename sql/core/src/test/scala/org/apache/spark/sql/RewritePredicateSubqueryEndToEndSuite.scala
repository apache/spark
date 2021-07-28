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

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.test.SharedSparkSession

class RewritePredicateSubqueryEndToEndSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  test("SPARK-36280: Remove redundant aliases after RewritePredicateSubquery") {
    sql("CREATE TABLE t1 USING parquet AS SELECT id AS a, id AS b, id AS c FROM range(10)")
    sql("CREATE TABLE t2 USING parquet AS SELECT id AS x, id AS y FROM range(8)")
    val df = sql(
      """
        |SELECT *
        |FROM   t1
        |WHERE  a IN (SELECT x
        |             FROM   (SELECT x                         AS x,
        |                            Rank()
        |                              OVER (
        |                                PARTITION BY x
        |                                ORDER BY Sum(y) DESC) AS ranking
        |                     FROM   t2
        |                     GROUP  BY x) tmp1
        |             WHERE  ranking <= 5)
        |""".stripMargin)

    df.collect()
    val exchanges = collect(df.queryExecution.executedPlan) {
      case s: ShuffleExchangeExec => s
    }
    assert(exchanges.size === 1)
  }
}
