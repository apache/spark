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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.internal.SQLConf


class AggregateEstimationSuite extends StatsEstimationTestBase {

  /** Columns for testing */
  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    attr("key11") -> ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key12") -> ColumnStat(distinctCount = 4, min = Some(10), max = Some(40), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key21") -> ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key22") -> ColumnStat(distinctCount = 2, min = Some(10), max = Some(20), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key31") -> ColumnStat(distinctCount = 0, min = None, max = None, nullCount = 0,
      avgLen = 4, maxLen = 4)
  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  test("set an upper bound if the product of ndv's of group-by columns is too large") {
    // Suppose table1 (key11 int, key12 int) has 4 records: (1, 10), (1, 20), (2, 30), (2, 40)
    checkAggStats(
      tableColumns = Seq("key11", "key12"),
      tableRowCount = 4,
      groupByColumns = Seq("key11", "key12"),
      // Use child's row count as an upper bound
      expectedOutputRowCount = 4)
  }

  test("data contains all combinations of distinct values of group-by columns.") {
    // Suppose table2 (key21 int, key22 int) has 6 records:
    // (1, 10), (1, 10), (1, 20), (2, 20), (2, 10), (2, 10)
    checkAggStats(
      tableColumns = Seq("key21", "key22"),
      tableRowCount = 6,
      groupByColumns = Seq("key21", "key22"),
      // Row count = product of ndv
      expectedOutputRowCount = nameToColInfo("key21")._2.distinctCount * nameToColInfo("key22")._2
        .distinctCount)
  }

  test("empty group-by column") {
    // Suppose table1 (key11 int, key12 int) has 4 records: (1, 10), (1, 20), (2, 30), (2, 40)
    checkAggStats(
      tableColumns = Seq("key11", "key12"),
      tableRowCount = 4,
      groupByColumns = Nil,
      expectedOutputRowCount = 1)
  }

  test("aggregate on empty table - with or without group-by column") {
    // Suppose table3 (key31 int) is an empty table
    // Return a single row without group-by column
    checkAggStats(
      tableColumns = Seq("key31"),
      tableRowCount = 0,
      groupByColumns = Nil,
      expectedOutputRowCount = 1)
    // Return empty result with group-by column
    checkAggStats(
      tableColumns = Seq("key31"),
      tableRowCount = 0,
      groupByColumns = Seq("key31"),
      expectedOutputRowCount = 0)
  }

  test("non-cbo estimation") {
    val attributes = Seq("key12").map(nameToAttr)
    val child = StatsTestPlan(
      outputList = attributes,
      rowCount = 4,
      // rowCount * (overhead + column size)
      size = Some(4 * (8 + 4)),
      attributeStats = AttributeMap(Seq("key12").map(nameToColInfo)))

    val noGroupAgg = Aggregate(groupingExpressions = Nil,
      aggregateExpressions = Seq(Alias(Count(Literal(1)), "cnt")()), child)
    assert(noGroupAgg.stats(conf.copy(SQLConf.CBO_ENABLED -> false)) ==
      // overhead + count result size
      Statistics(sizeInBytes = 8 + 8, rowCount = Some(1)))

    val hasGroupAgg = Aggregate(groupingExpressions = attributes,
      aggregateExpressions = attributes :+ Alias(Count(Literal(1)), "cnt")(), child)
    assert(hasGroupAgg.stats(conf.copy(SQLConf.CBO_ENABLED -> false)) ==
      // From UnaryNode.computeStats, childSize * outputRowSize / childRowSize
      Statistics(sizeInBytes = 48 * (8 + 4 + 8) / (8 + 4)))
  }

  private def checkAggStats(
      tableColumns: Seq[String],
      tableRowCount: BigInt,
      groupByColumns: Seq[String],
      expectedOutputRowCount: BigInt): Unit = {
    val attributes = groupByColumns.map(nameToAttr)
    // Construct an Aggregate for testing
    val testAgg = Aggregate(
      groupingExpressions = attributes,
      aggregateExpressions = attributes :+ Alias(Count(Literal(1)), "cnt")(),
      child = StatsTestPlan(
        outputList = tableColumns.map(nameToAttr),
        rowCount = tableRowCount,
        attributeStats = AttributeMap(tableColumns.map(nameToColInfo))))

    val expectedAttrStats = AttributeMap(groupByColumns.map(nameToColInfo))
    val expectedStats = Statistics(
      sizeInBytes = getOutputSize(testAgg.output, expectedOutputRowCount, expectedAttrStats),
      rowCount = Some(expectedOutputRowCount),
      attributeStats = expectedAttrStats)

    assert(testAgg.stats(conf) == expectedStats)
  }
}
