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


class AggEstimationSuite extends StatsEstimationTestBase {

  /** Columns for testing */
  private val columnInfo: Map[Attribute, ColumnStat] =
    Map(
      attr("key11") -> ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
        avgLen = 4, maxLen = 4),
      attr("key12") -> ColumnStat(distinctCount = 1, min = Some(10), max = Some(10), nullCount = 0,
        avgLen = 4, maxLen = 4),
      attr("key21") -> ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
        avgLen = 4, maxLen = 4),
      attr("key22") -> ColumnStat(distinctCount = 4, min = Some(10), max = Some(40), nullCount = 0,
        avgLen = 4, maxLen = 4),
      attr("key31") -> ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
        avgLen = 4, maxLen = 4),
      attr("key32") -> ColumnStat(distinctCount = 2, min = Some(10), max = Some(20), nullCount = 0,
        avgLen = 4, maxLen = 4))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  test("empty group-by column") {
    val colNames = Seq("key11", "key12")
    // Suppose table1 has 2 records: (1, 10), (2, 10)
    val table1 = StatsTestPlan(
      outputList = colNames.map(nameToAttr),
      stats = Statistics(
        sizeInBytes = 2 * (4 + 4),
        rowCount = Some(2),
        attributeStats = AttributeMap(colNames.map(nameToColInfo))))

    checkAggStats(
      child = table1,
      colNames = Nil,
      expectedRowCount = 1)
  }

  test("there's a primary key in group-by columns") {
    val colNames = Seq("key11", "key12")
    // Suppose table1 has 2 records: (1, 10), (2, 10)
    val table1 = StatsTestPlan(
      outputList = colNames.map(nameToAttr),
      stats = Statistics(
        sizeInBytes = 2 * (4 + 4),
        rowCount = Some(2),
        attributeStats = AttributeMap(colNames.map(nameToColInfo))))

    checkAggStats(
      child = table1,
      colNames = colNames,
      // Column key11 a primary key, so row count = ndv of key11 = child's row count
      expectedRowCount = table1.stats.rowCount.get)
  }

  test("the product of ndv's of group-by columns is too large") {
    val colNames = Seq("key21", "key22")
    // Suppose table2 has 4 records: (1, 10), (1, 20), (2, 30), (2, 40)
    val table2 = StatsTestPlan(
      outputList = colNames.map(nameToAttr),
      stats = Statistics(
        sizeInBytes = 4 * (4 + 4),
        rowCount = Some(4),
        attributeStats = AttributeMap(colNames.map(nameToColInfo))))

    checkAggStats(
      child = table2,
      colNames = colNames,
      // Use child's row count as an upper bound
      expectedRowCount = table2.stats.rowCount.get)
  }

  test("data contains all combinations of distinct values of group-by columns.") {
    val colNames = Seq("key31", "key32")
    // Suppose table3 has 6 records: (1, 10), (1, 10), (1, 20), (2, 20), (2, 10), (2, 10)
    val table3 = StatsTestPlan(
      outputList = colNames.map(nameToAttr),
      stats = Statistics(
        sizeInBytes = 6 * (4 + 4),
        rowCount = Some(6),
        attributeStats = AttributeMap(colNames.map(nameToColInfo))))

    checkAggStats(
      child = table3,
      colNames = colNames,
      // Row count = product of ndv
      expectedRowCount = nameToColInfo("key31")._2.distinctCount * nameToColInfo("key32")._2
        .distinctCount)
  }

  private def checkAggStats(
      child: LogicalPlan,
      colNames: Seq[String],
      expectedRowCount: BigInt): Unit = {

    val columns = colNames.map(nameToAttr)
    val testAgg = Aggregate(
      groupingExpressions = columns,
      aggregateExpressions = columns :+ Alias(Count(Literal(1)), "cnt")(),
      child = child)

    val expectedAttrStats = AttributeMap(colNames.map(nameToColInfo))
    val expectedStats = Statistics(
      sizeInBytes = expectedRowCount * getRowSize(testAgg.output, expectedAttrStats),
      rowCount = Some(expectedRowCount),
      attributeStats = expectedAttrStats)

    assert(testAgg.statistics == expectedStats)
  }
}
