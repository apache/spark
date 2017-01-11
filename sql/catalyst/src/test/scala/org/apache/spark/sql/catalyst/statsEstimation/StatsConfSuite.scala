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

package org.apache.spark.sql.catalyst.statsEstimation

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.types.IntegerType


class StatsConfSuite extends StatsEstimationTestBase {
  test("estimate statistics when the conf changes") {
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
    // Return the statistics estimated by cbo
    assert(plan.stats(conf.copy(cboEnabled = true)) == expectedCboStats)
    // Invalidate statistics
    plan.invalidateStatsCache()
    // Return the simple statistics
    assert(plan.stats(conf.copy(cboEnabled = false)) == expectedDefaultStats)
  }
}

/**
 * This class is used for unit-testing the cbo switch, it mimics a logical plan which computes
 * a simple statistics or a cbo estimated statistics based on the conf.
 */
private case class DummyLogicalPlan(
    defaultStats: Statistics,
    cboStats: Statistics) extends LogicalPlan {
  override def output: Seq[Attribute] = Nil
  override def children: Seq[LogicalPlan] = Nil
  override def computeStats(conf: CatalystConf): Statistics =
    if (conf.cboEnabled) cboStats else defaultStats
}
