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
 * In this test suite, we test predicates containing the following operators:
 * =, <, <=, >, >=, AND, OR, IS NULL, IS NOT NULL, IN, NOT IN
 */

class FilterEstimationSuite extends StatsEstimationTestBase {

  // Suppose our test table has one column called "key".
  // It has 10 rows with values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  // Hence, distinctCount:10, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  val ar = AttributeReference("key", IntegerType)()
  val childColStat = ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
    nullCount = 0, avgLen = 4, maxLen = 4)
  val child = StatsTestPlan(
    outputList = Seq(ar),
    rowCount = 10L,
    attributeStats = AttributeMap(Seq(ar -> childColStat))
  )

  test("key = 2") {
    // the predicate is "WHERE key = 2"
    validateEstimatedStats(
      Filter(EqualTo(ar, Literal(2)), child),
      ColumnStat(distinctCount = 1, min = Some(2), max = Some(2),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(1L)
    )
  }

  test("key = 0") {
    // the predicate is "WHERE key = 0"
    // This is an out-of-range case since 0 is outside the range [min, max]
    validateEstimatedStats(
      Filter(EqualTo(ar, Literal(0)), child),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(0L)
    )
  }

  test("key < 3") {
    // the predicate is "WHERE key < 3"
    validateEstimatedStats(
      Filter(LessThan(ar, Literal(3)), child),
      ColumnStat(distinctCount = 2, min = Some(1), max = Some(3),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("key < 0") {
    // the predicate is "WHERE key < 0"
    // This is a corner case since literal 0 is smaller than min.
    validateEstimatedStats(
      Filter(LessThan(ar, Literal(0)), child),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(0L)
    )
  }

  test("key <= 3") {
    // the predicate is "WHERE key <= 3"
    validateEstimatedStats(
      Filter(LessThanOrEqual(ar, Literal(3)), child),
      ColumnStat(distinctCount = 2, min = Some(1), max = Some(3),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("key > 6") {
    // the predicate is "WHERE key > 6"
    validateEstimatedStats(
      Filter(GreaterThan(ar, Literal(6)), child),
      ColumnStat(distinctCount = 4, min = Some(6), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(5L)
    )
  }

  test("key > 10") {
    // the predicate is "WHERE key > 10"
    // This is a corner case since max value is 10.
    validateEstimatedStats(
      Filter(GreaterThan(ar, Literal(10)), child),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(0L)
    )
  }

  test("key >= 6") {
    // the predicate is "WHERE key >= 6"
    validateEstimatedStats(
      Filter(GreaterThanOrEqual(ar, Literal(6)), child),
      ColumnStat(distinctCount = 4, min = Some(6), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(5L)
    )
  }

  test("key IS NULL") {
    // the predicate is "WHERE key IS NULL"
    validateEstimatedStats(
      Filter(IsNull(ar), child),
      ColumnStat(distinctCount = 0, min = None, max = None,
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(0L)
    )
  }

  test("key IS NOT NULL") {
    // the predicate is "WHERE key IS NOT NULL"
    validateEstimatedStats(
      Filter(IsNotNull(ar), child),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(10L)
    )
  }

  test("key > 3 AND key <= 6") {
    // the predicate is "WHERE key > 3 AND key <= 6"
    val condition = And(GreaterThan(ar, Literal(3)), LessThanOrEqual(ar, Literal(6)))
    validateEstimatedStats(
      Filter(condition, child),
      ColumnStat(distinctCount = 3, min = Some(3), max = Some(6),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(4L)
    )
  }

  test("key = 3 OR key = 6") {
    // the predicate is "WHERE key = 3 OR key = 6"
    val condition = Or(EqualTo(ar, Literal(3)), EqualTo(ar, Literal(6)))
    validateEstimatedStats(
      Filter(condition, child),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(2L)
    )
  }

  test("key IN (3, 4, 5)") {
    // the predicate is "WHERE key IN (3, 4, 5)"
    validateEstimatedStats(
      Filter(InSet(ar, Set(3, 4, 5)), child),
      ColumnStat(distinctCount = 3, min = Some(3), max = Some(5),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("key NOT IN (3, 4, 5)") {
    // the predicate is "WHERE key NOT IN (3, 4, 5)"
    validateEstimatedStats(
      Filter(Not(InSet(ar, Set(3, 4, 5))), child),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(7L)
    )
  }

  private def validateEstimatedStats(
      filterNode: Filter,
      expectedColStats: ColumnStat,
      rowCount: Option[Long] = None)
  : Unit = {

    val expectedRowCount = rowCount.getOrElse(0L)
    val expectedAttrStats = toAttributeMap(Seq("key" -> expectedColStats), filterNode)
    val expectedStats = Statistics(
      sizeInBytes = getOutputSize(filterNode.output, expectedAttrStats, expectedRowCount),
      rowCount = Some(expectedRowCount),
      attributeStats = expectedAttrStats)

    assert(filterNode.stats(conf) == expectedStats)
  }

}
