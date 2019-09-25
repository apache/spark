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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.internal.SQLConf


class AggregateEstimationSuite extends StatsEstimationTestBase with PlanTest {

  /** Columns for testing */
  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    attr("key11") -> ColumnStat(distinctCount = Some(2), min = Some(1), max = Some(2),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("key12") -> ColumnStat(distinctCount = Some(4), min = Some(10), max = Some(40),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("key21") -> ColumnStat(distinctCount = Some(2), min = Some(1), max = Some(2),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("key22") -> ColumnStat(distinctCount = Some(2), min = Some(10), max = Some(20),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("key31") -> ColumnStat(distinctCount = Some(0), min = None, max = None,
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("key32") -> ColumnStat(distinctCount = Some(0), min = None, max = None,
      nullCount = Some(4), avgLen = Some(4), maxLen = Some(4)),
    attr("key33") -> ColumnStat(distinctCount = Some(2), min = None, max = None,
      nullCount = Some(2), avgLen = Some(4), maxLen = Some(4))
  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  test("SPARK-26894: propagate child stats for aliases in Aggregate") {
    val tableColumns = Seq("key11", "key12")
    val groupByColumns = Seq("key11")
    val attributes = groupByColumns.map(nameToAttr)

    val rowCount = 2
    val child = StatsTestPlan(
      outputList = tableColumns.map(nameToAttr),
      rowCount,
      // rowCount * (overhead + column size)
      size = Some(4 * (8 + 4)),
      attributeStats = AttributeMap(tableColumns.map(nameToColInfo)))

    val testAgg = Aggregate(
      groupingExpressions = attributes,
      aggregateExpressions = Seq(Alias(nameToAttr("key12"), "abc")()),
      child)

    val expectedColStats = Seq("abc" -> nameToColInfo("key12")._2)
    val expectedAttrStats = toAttributeMap(expectedColStats, testAgg)

    assert(testAgg.stats.attributeStats == expectedAttrStats)
  }

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
      expectedOutputRowCount = nameToColInfo("key21")._2.distinctCount.get *
        nameToColInfo("key22")._2.distinctCount.get)
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

  test("group-by column with only null value") {
    checkAggStats(
      tableColumns = Seq("key22", "key32"),
      tableRowCount = 6,
      groupByColumns = Seq("key22", "key32"),
      expectedOutputRowCount = nameToColInfo("key22")._2.distinctCount.get)
  }

  test("group-by column with null value") {
    checkAggStats(
      tableColumns = Seq("key21", "key33"),
      tableRowCount = 6,
      groupByColumns = Seq("key21", "key33"),
      expectedOutputRowCount = nameToColInfo("key21")._2.distinctCount.get *
        (nameToColInfo("key33")._2.distinctCount.get + 1))
  }

  test("non-cbo estimation") {
    val attributes = Seq("key12").map(nameToAttr)
    val child = StatsTestPlan(
      outputList = attributes,
      rowCount = 4,
      // rowCount * (overhead + column size)
      size = Some(4 * (8 + 4)),
      attributeStats = AttributeMap(Seq("key12").map(nameToColInfo)))

    withSQLConf(SQLConf.CBO_ENABLED.key -> "false") {
      val noGroupAgg = Aggregate(groupingExpressions = Nil,
        aggregateExpressions = Seq(Alias(Count(Literal(1)), "cnt")()), child)
      assert(noGroupAgg.stats ==
        // overhead + count result size
        Statistics(sizeInBytes = 8 + 8, rowCount = Some(1)))

      val hasGroupAgg = Aggregate(groupingExpressions = attributes,
        aggregateExpressions = attributes :+ Alias(Count(Literal(1)), "cnt")(), child)
      assert(hasGroupAgg.stats ==
        // From UnaryNode.computeStats, childSize * outputRowSize / childRowSize
        Statistics(sizeInBytes = 48 * (8 + 4 + 8) / (8 + 4)))
    }
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

    assert(testAgg.stats == expectedStats)
  }
}
