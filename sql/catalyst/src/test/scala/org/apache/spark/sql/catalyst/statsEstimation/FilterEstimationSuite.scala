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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.types.{DateType, IntegerType, TimestampType}

/**
 * In this test suite, we test predicates containing the following operators:
 * =, <, <=, >, >=, AND, OR, IS NULL, IS NOT NULL, IN, NOT IN
 */

class FilterEstimationSuite extends StatsEstimationTestBase {

  // Suppose our test table has 10 rows and 3 columns.
  // First column cint has values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  // Hence, distinctCount:10, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  val arInt = AttributeReference("cint", IntegerType)()
  val childColStatInt = ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
    nullCount = 0, avgLen = 4, maxLen = 4)

  // Second column cdate has values, from 2017-01-01 through 2017-01-10 for 10 values.
  val dMin = Date.valueOf("2017-01-01")
  val dMax = Date.valueOf("2017-01-10")
  val arDate = AttributeReference("cdate", DateType)()
  val childColStatDate = ColumnStat(distinctCount = 10, min = Some(dMin), max = Some(dMax),
    nullCount = 0, avgLen = 4, maxLen = 4)

  // Third column ctimestamp has values from "2017-01-01 01:00:00" through
  // "2017-01-01 10:00:00" for 10 distinct timestamps (or hours).
  val tsMin = Timestamp.valueOf("2017-01-01 01:00:00")
  val tsMax = Timestamp.valueOf("2017-01-01 10:00:00")
  val arTimestamp = AttributeReference("ctimestamp", TimestampType)()
  val childColStatTimestamp = ColumnStat(distinctCount = 10, min = Some(tsMin), max = Some(tsMax),
    nullCount = 0, avgLen = 8, maxLen = 8)

  val child = StatsTestPlan(
    outputList = Seq(arInt),
    rowCount = 10L,
    attributeStats = AttributeMap(Seq(
      arInt -> childColStatInt,
      arDate -> childColStatDate,
      arTimestamp -> childColStatTimestamp
    ))
  )

  test("cint = 2") {
    // the predicate is "WHERE cint = 2"
    validateEstimatedStats(
      arInt,
      Filter(EqualTo(arInt, Literal(2)), child),
      ColumnStat(distinctCount = 1, min = Some(2), max = Some(2),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(1L)
    )
  }

  test("cint = 0") {
    // the predicate is "WHERE cint = 0"
    // This is an out-of-range case since 0 is outside the range [min, max]
    validateEstimatedStats(
      arInt,
      Filter(EqualTo(arInt, Literal(0)), child),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(0L)
    )
  }

  test("cint < 3") {
    // the predicate is "WHERE cint < 3"
    validateEstimatedStats(
      arInt,
      Filter(LessThan(arInt, Literal(3)), child),
      ColumnStat(distinctCount = 2, min = Some(1), max = Some(3),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("cint < 0") {
    // the predicate is "WHERE cint < 0"
    // This is a corner case since literal 0 is smaller than min.
    validateEstimatedStats(
      arInt,
      Filter(LessThan(arInt, Literal(0)), child),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(0L)
    )
  }

  test("cint <= 3") {
    // the predicate is "WHERE cint <= 3"
    validateEstimatedStats(
      arInt,
      Filter(LessThanOrEqual(arInt, Literal(3)), child),
      ColumnStat(distinctCount = 2, min = Some(1), max = Some(3),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("cint > 6") {
    // the predicate is "WHERE cint > 6"
    validateEstimatedStats(
      arInt,
      Filter(GreaterThan(arInt, Literal(6)), child),
      ColumnStat(distinctCount = 4, min = Some(6), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(5L)
    )
  }

  test("cint > 10") {
    // the predicate is "WHERE cint > 10"
    // This is a corner case since max value is 10.
    validateEstimatedStats(
      arInt,
      Filter(GreaterThan(arInt, Literal(10)), child),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(0L)
    )
  }

  test("cint >= 6") {
    // the predicate is "WHERE cint >= 6"
    validateEstimatedStats(
      arInt,
      Filter(GreaterThanOrEqual(arInt, Literal(6)), child),
      ColumnStat(distinctCount = 4, min = Some(6), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(5L)
    )
  }

  test("cint IS NULL") {
    // the predicate is "WHERE cint IS NULL"
    validateEstimatedStats(
      arInt,
      Filter(IsNull(arInt), child),
      ColumnStat(distinctCount = 0, min = None, max = None,
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(0L)
    )
  }

  test("cint IS NOT NULL") {
    // the predicate is "WHERE cint IS NOT NULL"
    validateEstimatedStats(
      arInt,
      Filter(IsNotNull(arInt), child),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(10L)
    )
  }

  test("cint > 3 AND cint <= 6") {
    // the predicate is "WHERE cint > 3 AND cint <= 6"
    val condition = And(GreaterThan(arInt, Literal(3)), LessThanOrEqual(arInt, Literal(6)))
    validateEstimatedStats(
      arInt,
      Filter(condition, child),
      ColumnStat(distinctCount = 3, min = Some(3), max = Some(6),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(4L)
    )
  }

  test("cint = 3 OR cint = 6") {
    // the predicate is "WHERE cint = 3 OR cint = 6"
    val condition = Or(EqualTo(arInt, Literal(3)), EqualTo(arInt, Literal(6)))
    validateEstimatedStats(
      arInt,
      Filter(condition, child),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(2L)
    )
  }

  test("cint IN (3, 4, 5)") {
    // the predicate is "WHERE cint IN (3, 4, 5)"
    validateEstimatedStats(
      arInt,
      Filter(InSet(arInt, Set(3, 4, 5)), child),
      ColumnStat(distinctCount = 3, min = Some(3), max = Some(5),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("cint NOT IN (3, 4, 5)") {
    // the predicate is "WHERE cint NOT IN (3, 4, 5)"
    validateEstimatedStats(
      arInt,
      Filter(Not(InSet(arInt, Set(3, 4, 5))), child),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(7L)
    )
  }

  test("cdate = 2017-01-02") {
    // the predicate is: WHERE cdate = "2017-01-02"
    val d20170102 = Date.valueOf("2017-01-02")
    validateEstimatedStats(
      arDate,
      Filter(EqualTo(arDate, Literal(d20170102, DateType)),
        child.copy(outputList = Seq(arDate))),
      ColumnStat(distinctCount = 1, min = Some(d20170102), max = Some(d20170102),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(1L)
    )
  }

  private def validateEstimatedStats(
      ar: AttributeReference,
      filterNode: Filter,
      expectedColStats: ColumnStat,
      rowCount: Option[Long] = None)
    : Unit = {

    val expectedRowCount = rowCount.getOrElse(0L)
    val expectedAttrStats = toAttributeMap(Seq(ar.name -> expectedColStats), filterNode)
    val expectedSizeInBytes = getOutputSize(filterNode.output, expectedAttrStats, expectedRowCount)

    val filteredStats = filterNode.stats(conf)
    assert(filteredStats.sizeInBytes == expectedSizeInBytes)
    assert(filteredStats.rowCount == rowCount)
    assert(filteredStats.attributeStats(ar) == expectedColStats)
  }

}
