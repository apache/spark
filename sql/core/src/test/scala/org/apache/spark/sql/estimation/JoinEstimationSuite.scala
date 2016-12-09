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

package org.apache.spark.sql.estimation

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Join, Statistics}
import org.apache.spark.sql.test.SharedSQLContext


class JoinEstimationSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private val data1 = Seq[Long](1, 2, 2)
  private val data2 = Seq[Long](1, 2, 3, 4)
  private val colStatAfterJoin = ColumnStat(2, Some(1L), Some(2L), 0, 8, 8)
  private val expectedJoinStats = Statistics(
    sizeInBytes = 3 * (8 + 8),
    rowCount = Some(3),
    colStats = Map("key1" -> colStatAfterJoin, "key2" -> colStatAfterJoin),
    isBroadcastable = false)

  test("join estimation using basic formula") {
    val table1 = "join_estimation_test1"
    val table2 = "join_estimation_test2"
    val df1 = data1.toDF("key1")
    val df2 = data2.toDF("key2")
    withTable(table1, table2) {
      df1.write.saveAsTable(table1)
      df2.write.saveAsTable(table2)

      // Collect statistics
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")
      sql(s"analyze table $table2 compute STATISTICS FOR COLUMNS key2")

      // Validate statistics
      val logicalPlan =
        sql(s"select * from $table1 join $table2 on key1=key2").queryExecution.optimizedPlan
      val joinNodes = logicalPlan.collect {
        case join: Join =>
          assert(join.statistics == expectedJoinStats)
          join
      }
      assert(joinNodes.size == 1)
    }
  }
}
