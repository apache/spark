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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeMap, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._


class AggEstimationSuite extends StatsEstimationTestBase {

  /** Column info: names and column stats for group-by columns */
  val (key11, colStat11) = (attr("key11"), ColumnStat(2, Some(1), Some(2), 0, 4, 4))
  val (key12, colStat12) = (attr("key12"), ColumnStat(1, Some(10), Some(10), 0, 4, 4))
  val (key21, colStat21) = (attr("key21"), colStat11)
  val (key22, colStat22) = (attr("key22"), ColumnStat(4, Some(10), Some(40), 0, 4, 4))
  val (key31, colStat31) = (attr("key31"), colStat11)
  val (key32, colStat32) = (attr("key32"), ColumnStat(2, Some(10), Some(20), 0, 4, 4))

  /** Tables for testing */
  /** Data for table1: (1, 10), (2, 10) */
  val table1 = StatsTestPlan(
    outputList = Seq(key11, key12),
    stats = Statistics(
      sizeInBytes = 2 * (4 + 4),
      rowCount = Some(2),
      attributeStats = AttributeMap(Seq(key11 -> colStat11, key12 -> colStat12))))

  /** Data for table2: (1, 10), (1, 20), (2, 30), (2, 40) */
  val table2 = StatsTestPlan(
    outputList = Seq(key21, key22),
    stats = Statistics(
      sizeInBytes = 4 * (4 + 4),
      rowCount = Some(4),
      attributeStats = AttributeMap(Seq(key21 -> colStat21, key22 -> colStat22))))

  /** Data for table3: (1, 10), (1, 10), (1, 20), (2, 20), (2, 10), (2, 10) */
  val table3 = StatsTestPlan(
    outputList = Seq(key31, key32),
    stats = Statistics(
      sizeInBytes = 6 * (4 + 4),
      rowCount = Some(6),
      attributeStats = AttributeMap(Seq(key31 -> colStat31, key32 -> colStat32))))

  test("empty group-by column") {
    checkAggStats(
      testAgg = Aggregate(
        groupingExpressions = Nil,
        aggregateExpressions = Alias(Count(Literal(1)), "cnt")() :: Nil,
        child = table1),
      expectedRowCount = 1,
      expectedAttrStats = AttributeMap(Nil))
  }

  test("there's a primary key in group-by columns") {
    checkAggStats(
      testAgg = Aggregate(
        groupingExpressions = Seq(key11, key12),
        aggregateExpressions = Seq(key11, key12),
        child = table1),
      // Column key11 a primary key, so row count = ndv of key11 = child's row count
      expectedRowCount = table1.stats.rowCount.get,
      expectedAttrStats = AttributeMap(Seq(key11 -> colStat11, key12 -> colStat12)))
  }

  test("the product of ndv's of group-by columns is too large") {
    checkAggStats(
      testAgg = Aggregate(
        groupingExpressions = Seq(key21, key22),
        aggregateExpressions = Seq(key21, key22),
        child = table2),
      // Use child's row count as an upper bound
      expectedRowCount = table2.stats.rowCount.get,
      expectedAttrStats = AttributeMap(Seq(key21 -> colStat21, key22 -> colStat22)))
  }

  test("data contains all combinations of distinct values of group-by columns.") {
    checkAggStats(
      testAgg = Aggregate(
        groupingExpressions = Seq(key31, key32),
        aggregateExpressions = Seq(key31, key32),
        child = table3),
      expectedRowCount = colStat31.distinctCount * colStat32.distinctCount,
      expectedAttrStats = AttributeMap(Seq(key31 -> colStat31, key32 -> colStat32)))
  }

  private def checkAggStats(
      testAgg: Aggregate,
      expectedRowCount: BigInt,
      expectedAttrStats: AttributeMap[ColumnStat]): Unit = {
    val expectedStats = Statistics(
      sizeInBytes = expectedRowCount * getRowSize(testAgg.output, expectedAttrStats),
      rowCount = Some(expectedRowCount),
      attributeStats = expectedAttrStats)
    assert(testAgg.statistics == expectedStats)
  }
}
