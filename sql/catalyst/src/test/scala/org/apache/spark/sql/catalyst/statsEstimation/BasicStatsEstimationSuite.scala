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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType


class BasicStatsEstimationSuite extends StatsEstimationTestBase {
  val attribute = attr("key")
  val colStat = ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
    nullCount = 0, avgLen = 4, maxLen = 4)

  val plan = StatsTestPlan(
    outputList = Seq(attribute),
    attributeStats = AttributeMap(Seq(attribute -> colStat)),
    rowCount = 10,
    // row count * (overhead + column size)
    size = Some(10 * (8 + 4)))

  test("BroadcastHint estimation") {
    val filter = Filter(Literal(true), plan)
    val filterStatsCboOn = Statistics(sizeInBytes = 10 * (8 +4),
      rowCount = Some(10), attributeStats = AttributeMap(Seq(attribute -> colStat)))
    val filterStatsCboOff = Statistics(sizeInBytes = 10 * (8 +4))
    checkStats(
      filter,
      expectedStatsCboOn = filterStatsCboOn,
      expectedStatsCboOff = filterStatsCboOff)

    val broadcastHint = ResolvedHint(filter, HintInfo(isBroadcastable = Option(true)))
    checkStats(
      broadcastHint,
      expectedStatsCboOn = filterStatsCboOn.copy(hints = HintInfo(isBroadcastable = Option(true))),
      expectedStatsCboOff = filterStatsCboOff.copy(hints = HintInfo(isBroadcastable = Option(true)))
    )
  }

  test("limit estimation: limit < child's rowCount") {
    val localLimit = LocalLimit(Literal(2), plan)
    val globalLimit = GlobalLimit(Literal(2), plan)
    // LocalLimit's stats is just its child's stats except column stats
    checkStats(localLimit, plan.stats(conf).copy(attributeStats = AttributeMap(Nil)))
    checkStats(globalLimit, Statistics(sizeInBytes = 24, rowCount = Some(2)))
  }

  test("limit estimation: limit > child's rowCount") {
    val localLimit = LocalLimit(Literal(20), plan)
    val globalLimit = GlobalLimit(Literal(20), plan)
    checkStats(localLimit, plan.stats(conf).copy(attributeStats = AttributeMap(Nil)))
    // Limit is larger than child's rowCount, so GlobalLimit's stats is equal to its child's stats.
    checkStats(globalLimit, plan.stats(conf).copy(attributeStats = AttributeMap(Nil)))
  }

  test("limit estimation: limit = 0") {
    val localLimit = LocalLimit(Literal(0), plan)
    val globalLimit = GlobalLimit(Literal(0), plan)
    val stats = Statistics(sizeInBytes = 1, rowCount = Some(0))
    checkStats(localLimit, stats)
    checkStats(globalLimit, stats)
  }

  test("sample estimation") {
    val sample = Sample(0.0, 0.5, withReplacement = false, (math.random * 1000).toLong, plan)()
    checkStats(sample, Statistics(sizeInBytes = 60, rowCount = Some(5)))

    // Child doesn't have rowCount in stats
    val childStats = Statistics(sizeInBytes = 120)
    val childPlan = DummyLogicalPlan(childStats, childStats)
    val sample2 =
      Sample(0.0, 0.11, withReplacement = false, (math.random * 1000).toLong, childPlan)()
    checkStats(sample2, Statistics(sizeInBytes = 14))
  }

  test("estimate statistics when the conf changes") {
    val expectedDefaultStats =
      Statistics(
        sizeInBytes = 40,
        rowCount = Some(10),
        attributeStats = AttributeMap(Seq(
          AttributeReference("c1", IntegerType)() -> ColumnStat(10, Some(1), Some(10), 0, 4, 4))))
    val expectedCboStats =
      Statistics(
        sizeInBytes = 4,
        rowCount = Some(1),
        attributeStats = AttributeMap(Seq(
          AttributeReference("c1", IntegerType)() -> ColumnStat(1, Some(5), Some(5), 0, 4, 4))))

    val plan = DummyLogicalPlan(defaultStats = expectedDefaultStats, cboStats = expectedCboStats)
    checkStats(
      plan, expectedStatsCboOn = expectedCboStats, expectedStatsCboOff = expectedDefaultStats)
  }

  /** Check estimated stats when cbo is turned on/off. */
  private def checkStats(
      plan: LogicalPlan,
      expectedStatsCboOn: Statistics,
      expectedStatsCboOff: Statistics): Unit = {
    // Invalidate statistics
    plan.invalidateStatsCache()
    assert(plan.stats(conf.copy(SQLConf.CBO_ENABLED -> true)) == expectedStatsCboOn)

    plan.invalidateStatsCache()
    assert(plan.stats(conf.copy(SQLConf.CBO_ENABLED -> false)) == expectedStatsCboOff)
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
    cboStats: Statistics) extends LogicalPlan {
  override def output: Seq[Attribute] = Nil
  override def children: Seq[LogicalPlan] = Nil
  override def computeStats(conf: SQLConf): Statistics =
    if (conf.cboEnabled) cboStats else defaultStats
}
