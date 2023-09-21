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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class AdjustSortersSuite
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  test("xx") {
    withTempView("t1", "t2") {
      spark.range(100).selectExpr("id as a", "id as b", "id as c")
        .createOrReplaceTempView("t1")
      spark.range(100).selectExpr("id as x", "id as y", "id as z")
        .createOrReplaceTempView("t2")
      val query =
        s"""
           |SELECT /*+ SHUFFLE_MERGE(t1) */ a, b, count(c)
           |FROM t1
           |JOIN t2
           |ON a = x
           |GROUP BY a, b
         """.stripMargin

      withSQLConf(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "true") {
        val df = sql(query)
        val plan = df.queryExecution.executedPlan
        val sorters = collectWithSubqueries(plan) {
          case s: SortExec => s
        }
        assert(sorters.length == 2)
        assert(sorters(0).sortOrder.map(_.sql) ==
          Seq("t1.a ASC NULLS FIRST", "t1.b ASC NULLS FIRST"))
        assert(sorters(1).sortOrder.map(_.sql) == Seq("t2.x ASC NULLS FIRST"))
      }
    }
  }
}
