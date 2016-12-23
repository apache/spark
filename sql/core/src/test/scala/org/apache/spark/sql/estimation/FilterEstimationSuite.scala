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
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.test.SharedSQLContext


class FilterEstimationSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private val data1 = Seq[Long](1, 2, 3, 4)
  private val colStatAfterFilter = ColumnStat(1, Some(2L), Some(2L), 0, 8, 8)
  private val expectedFilterStats = Statistics(
    sizeInBytes = 1 * 8,
    rowCount = Some(1),
    colStats = Map("key1" -> colStatAfterFilter),
    isBroadcastable = false)

  test("filter estimation with equality comparison using basic formula") {
    val table1 = "filter_estimation_test1"
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val logicalPlan =
        sql(s"select * from $table1 where key1=2").queryExecution.optimizedPlan
      val filterNodes = logicalPlan.collect {
        case filter: Filter =>
          val filterStats = filter.statistics
          assert(filterStats == expectedFilterStats)
          filter
      }
      assert(filterNodes.size == 1)
    }
  }
}
