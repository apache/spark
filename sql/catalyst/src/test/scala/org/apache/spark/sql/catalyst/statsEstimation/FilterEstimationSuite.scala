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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.types.IntegerType

/**
 * In this test suite, we test the proedicates containing the following operators:
 * =, <, <=, >, >=, AND, OR, IS NULL, IS NOT NULL, IN, NOT IN
 */

class FilterEstimationSuite extends StatsEstimationTestBase {

  // Suppose our test table has one column called "key1".
  // It has 10 rows with values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  // Hence, distinctCount:10, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  val ar = AttributeReference("key1", IntegerType)()
  val childColStat = ColumnStat(10, Some(1), Some(10), 0, 4, 4)
  val child = StatsTestPlan(
    outputList = Seq(ar),
    stats = Statistics(
      sizeInBytes = 10 * 4,
      rowCount = Some(10),
      attributeStats = AttributeMap(Seq(ar -> childColStat))
    )
  )

  test("filter estimation with equality comparison") {
    // the predicate is "WHERE key1 = 2"
    val intValue = Literal(2, IntegerType)
    val condition = EqualTo(ar, intValue)
    val filterNode = Filter(condition, child)
    val filteredColStats = ColumnStat(1, Some(2), Some(2), 0, 4, 4)

    validateEstimatedStats(filterNode, filteredColStats, Some(1L))
  }

  test("filter estimation with less than comparison") {
    // the predicate is "WHERE key1 < 3"
    val intValue = Literal(3, IntegerType)
    val condition = LessThan(ar, intValue)
    val filterNode = Filter(condition, child)
    val filteredColStats = ColumnStat(2, Some(1), Some(3), 0, 4, 4)

    validateEstimatedStats(filterNode, filteredColStats, Some(3L))
  }

  test("filter estimation with less than or equal to comparison") {
    // the predicate is "WHERE key1 <= 3"
    val intValue = Literal(3, IntegerType)
    val condition = LessThanOrEqual(ar, intValue)
    val filterNode = Filter(condition, child)
    val filteredColStats = ColumnStat(2, Some(1), Some(3), 0, 4, 4)

    validateEstimatedStats(filterNode, filteredColStats, Some(3L))

  }

  test("filter estimation with greater than comparison") {
    // the predicate is "WHERE key1 > 6"
    val intValue = Literal(6, IntegerType)
    val condition = GreaterThan(ar, intValue)
    val filterNode = Filter(condition, child)
    val filteredColStats = ColumnStat(4, Some(6), Some(10), 0, 4, 4)

    validateEstimatedStats(filterNode, filteredColStats, Some(5L))
  }

  test("filter estimation with greater than or equal to comparison") {
    // the predicate is "WHERE key1 >= 6"
    val intValue = Literal(6, IntegerType)
    val condition = GreaterThanOrEqual(ar, intValue)
    val filterNode = Filter(condition, child)
    val filteredColStats = ColumnStat(4, Some(6), Some(10), 0, 4, 4)

    validateEstimatedStats(filterNode, filteredColStats, Some(5L))

  }

  test("filter estimation with IS NULL comparison") {
    // the predicate is "WHERE key1 IS NULL"
    val condition = IsNull(ar)
    val filterNode = Filter(condition, child)
    val filteredColStats = ColumnStat(0, None, None, 0, 4, 4)

    validateEstimatedStats(filterNode, filteredColStats, Some(0L))
  }

  test("filter estimation with IS NOT NULL comparison") {
    // the predicate is "WHERE key1 IS NOT NULL"
    val condition = IsNotNull(ar)
    val filterNode = Filter(condition, child)
    val filteredColStats = ColumnStat(10, Some(1), Some(10), 0, 4, 4)

    validateEstimatedStats(filterNode, filteredColStats, Some(10L))
  }

  test("filter estimation with logical AND operator") {
    // the predicate is "WHERE key1 > 3 AND key1 <= 6"
    val condition1 = GreaterThan(ar, Literal(3, IntegerType))
    val condition2 = LessThanOrEqual(ar, Literal(6, IntegerType))
    val condition = And(condition1, condition2)
    val filterNode = Filter(condition, child)
    val filteredColStats = ColumnStat(3, Some(3), Some(6), 0, 4, 4)

    validateEstimatedStats(filterNode, filteredColStats, Some(4L))
  }

  test("filter estimation with logical OR operator") {
    // the predicate is "WHERE key1 = 3 OR key1 = 6"
    val condition1 = EqualTo(ar, Literal(3, IntegerType))
    val condition2 = EqualTo(ar, Literal(6, IntegerType))
    val condition = Or(condition1, condition2)
    val filterNode = Filter(condition, child)
    val filteredColStats = ColumnStat(10, Some(1), Some(10), 0, 4, 4)

    validateEstimatedStats(filterNode, filteredColStats, Some(2L))
  }

  test("filter estimation with logical IN operator") {
    // the predicate is "WHERE key1 IN (3, 4, 5)"
    val condition = InSet(ar, Set(3, 4, 5))
    val filterNode = Filter(condition, child)
    val filteredColStats = ColumnStat(3, Some(3), Some(5), 0, 4, 4)

    validateEstimatedStats(filterNode, filteredColStats, Some(3L))
  }

  test("filter estimation with logical NOT operator") {
    // the predicate is "WHERE key1 NOT IN (3, 4, 5)"
    val condition = InSet(ar, Set(3, 4, 5))
    val notCondition = Not(condition)
    val filterNode = Filter(notCondition, child)
    val filteredColStats = ColumnStat(10, Some(1), Some(10), 0, 4, 4)

    validateEstimatedStats(filterNode, filteredColStats, Some(7L))
  }

  private def validateEstimatedStats(
      filterNode: Filter,
      filteredColStats: ColumnStat,
      rowCount: Option[Long] = None)
  : Unit = {

    val expectedRowCount = rowCount.getOrElse(0L)
    val expectedAttrStats = toAttributeMap(Seq("key1" -> filteredColStats), filterNode)
    val expectedStats = Statistics(
      sizeInBytes = expectedRowCount * getRowSize(filterNode.output, expectedAttrStats),
      rowCount = Some(expectedRowCount),
      attributeStats = expectedAttrStats)

    assert(filterNode.statistics == expectedStats)
  }

}
