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

package org.apache.spark.sql.statsEstimation

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.IntegerType


class StatsEstimationSuite extends SharedSQLContext {
  test("statistics for a plan based on the cbo switch") {
    val expectedDefaultStats =
      Statistics(
        sizeInBytes = 40,
        rowCount = Some(10),
        attributeStats = AttributeMap(Seq(
          AttributeReference("c1", IntegerType)() -> ColumnStat(10, Some(1), Some(10), 0, 4, 4))),
        isBroadcastable = false)
    val expectedCboStats =
      Statistics(
        sizeInBytes = 4,
        rowCount = Some(1),
        attributeStats = AttributeMap(Seq(
          AttributeReference("c1", IntegerType)() -> ColumnStat(1, Some(5), Some(5), 0, 4, 4))),
        isBroadcastable = false)

    val plan = DummyLogicalPlan(defaultStats = expectedDefaultStats, cboStats = expectedCboStats)
    withSQLConf("spark.sql.cbo.enabled" -> "true") {
      // Use the statistics estimated by cbo
      assert(plan.planStats(spark.sessionState.conf) == expectedCboStats)
    }
    withSQLConf("spark.sql.cbo.enabled" -> "false") {
      // Use the default statistics
      assert(plan.planStats(spark.sessionState.conf) == expectedDefaultStats)
    }
  }
}

/**
 * This class is used for unit-testing the cbo switch, it mimics a logical plan which has both
 * default statistics and cbo estimated statistics.
 */
private case class DummyLogicalPlan(
    defaultStats: Statistics,
    cboStats: Statistics) extends LogicalPlan {
  override lazy val statistics = defaultStats
  override def cboStatistics(conf: CatalystConf): Statistics = cboStats
  override def output: Seq[Attribute] = Nil
  override def children: Seq[LogicalPlan] = Nil
}
