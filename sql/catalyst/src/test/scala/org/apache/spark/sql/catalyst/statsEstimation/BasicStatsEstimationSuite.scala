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

import org.mockito.Mockito.mock

import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.SupportsNamespaces
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType


class BasicStatsEstimationSuite extends PlanTest with StatsEstimationTestBase {
  val attribute = attr("key")
  val colStat = ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

  val plan = StatsTestPlan(
    outputList = Seq(attribute),
    attributeStats = AttributeMap(Seq(attribute -> colStat)),
    rowCount = 10,
    // row count * (overhead + column size)
    size = Some(10 * (8 + 4)))

  test("range") {
    val range = Range(1, 5, 1, None)
    val rangeStats = Statistics(sizeInBytes = 4 * 8)
    checkStats(
      range,
      expectedStatsCboOn = rangeStats,
      expectedStatsCboOff = rangeStats)
  }

  test("windows") {
    val windows = plan.window(Seq(min(attribute).as('sum_attr)), Seq(attribute), Nil)
    val windowsStats = Statistics(sizeInBytes = plan.size.get * (4 + 4 + 8) / (4 + 8))
    checkStats(
      windows,
      expectedStatsCboOn = windowsStats,
      expectedStatsCboOff = windowsStats)
  }

  test("limit estimation: limit < child's rowCount") {
    val localLimit = LocalLimit(Literal(2), plan)
    val globalLimit = GlobalLimit(Literal(2), plan)
    // LocalLimit's stats is just its child's stats except column stats
    checkStats(localLimit, plan.stats.copy(attributeStats = AttributeMap(Nil)))
    checkStats(globalLimit, Statistics(sizeInBytes = 24, rowCount = Some(2)))
  }

  test("limit estimation: limit > child's rowCount") {
    val localLimit = LocalLimit(Literal(20), plan)
    val globalLimit = GlobalLimit(Literal(20), plan)
    checkStats(localLimit, plan.stats.copy(attributeStats = AttributeMap(Nil)))
    // Limit is larger than child's rowCount, so GlobalLimit's stats is equal to its child's stats.
    checkStats(globalLimit, plan.stats.copy(attributeStats = AttributeMap(Nil)))
  }

  test("limit estimation: limit = 0") {
    val localLimit = LocalLimit(Literal(0), plan)
    val globalLimit = GlobalLimit(Literal(0), plan)
    val stats = Statistics(sizeInBytes = 1, rowCount = Some(0))
    checkStats(localLimit, stats)
    checkStats(globalLimit, stats)
  }

  test("sample estimation") {
    val sample = Sample(0.0, 0.5, withReplacement = false, (math.random * 1000).toLong, plan)
    checkStats(sample, Statistics(sizeInBytes = 60, rowCount = Some(5)))

    // Child doesn't have rowCount in stats
    val childStats = Statistics(sizeInBytes = 120)
    val childPlan = DummyLogicalPlan(childStats, childStats)
    val sample2 =
      Sample(0.0, 0.11, withReplacement = false, (math.random * 1000).toLong, childPlan)
    checkStats(sample2, Statistics(sizeInBytes = 14))
  }

  test("estimate statistics when the conf changes") {
    val expectedDefaultStats =
      Statistics(
        sizeInBytes = 40,
        rowCount = Some(10),
        attributeStats = AttributeMap(Seq(
          AttributeReference("c1", IntegerType)() -> ColumnStat(distinctCount = Some(10),
            min = Some(1), max = Some(10),
            nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))))
    val expectedCboStats =
      Statistics(
        sizeInBytes = 4,
        rowCount = Some(1),
        attributeStats = AttributeMap(Seq(
          AttributeReference("c1", IntegerType)() -> ColumnStat(distinctCount = Some(10),
            min = Some(5), max = Some(5),
            nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))))

    val plan = DummyLogicalPlan(defaultStats = expectedDefaultStats, cboStats = expectedCboStats)
    checkStats(
      plan, expectedStatsCboOn = expectedCboStats, expectedStatsCboOff = expectedDefaultStats)
  }

  test("command should report a dummy stats") {
    val plan = CommentOnNamespace(
      ResolvedNamespace(mock(classOf[SupportsNamespaces]), Array("ns")), "comment")
    checkStats(
      plan,
      expectedStatsCboOn = Statistics.DUMMY,
      expectedStatsCboOff = Statistics.DUMMY)
  }

  /** Check estimated stats when cbo is turned on/off. */
  private def checkStats(
      plan: LogicalPlan,
      expectedStatsCboOn: Statistics,
      expectedStatsCboOff: Statistics): Unit = {
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true") {
      // Invalidate statistics
      plan.invalidateStatsCache()
      assert(plan.stats == expectedStatsCboOn)
    }

    withSQLConf(SQLConf.CBO_ENABLED.key -> "false") {
      plan.invalidateStatsCache()
      assert(plan.stats == expectedStatsCboOff)
    }
  }

  /** Check estimated stats when it's the same whether cbo is turned on or off. */
  private def checkStats(plan: LogicalPlan, expectedStats: Statistics): Unit =
    checkStats(plan, expectedStats, expectedStats)
}

/**
 * This class is used for unit-testing the cbo switch, it mimics a logical plan which computes
 * a simple statistics or a cbo estimated statistics based on the conf.
 */
private case class DummyLogicalPlan(
    defaultStats: Statistics,
    cboStats: Statistics)
  extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override def computeStats(): Statistics = if (conf.cboEnabled) cboStats else defaultStats
}
