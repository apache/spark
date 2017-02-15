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
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

/**
 * In this test suite, we test predicates containing the following operators:
 * =, <, <=, >, >=, AND, OR, IS NULL, IS NOT NULL, IN, NOT IN
 */
class FilterEstimationSuite extends StatsEstimationTestBase {

  // Suppose our test table has 10 rows and 6 columns.
  // First column cint has values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  // Hence, distinctCount:10, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  val arInt = AttributeReference("cint", IntegerType)()
  val childColStatInt = ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
    nullCount = 0, avgLen = 4, maxLen = 4)

  // Second column cdate has 10 values from 2017-01-01 through 2017-01-10.
  val dMin = Date.valueOf("2017-01-01")
  val dMax = Date.valueOf("2017-01-10")
  val arDate = AttributeReference("cdate", DateType)()
  val childColStatDate = ColumnStat(distinctCount = 10, min = Some(dMin), max = Some(dMax),
    nullCount = 0, avgLen = 4, maxLen = 4)

  // Third column ctimestamp has 10 values from "2017-01-01 01:00:00" through
  // "2017-01-01 10:00:00" for 10 distinct timestamps (or hours).
  val tsMin = Timestamp.valueOf("2017-01-01 01:00:00")
  val tsMax = Timestamp.valueOf("2017-01-01 10:00:00")
  val arTimestamp = AttributeReference("ctimestamp", TimestampType)()
  val childColStatTimestamp = ColumnStat(distinctCount = 10, min = Some(tsMin), max = Some(tsMax),
    nullCount = 0, avgLen = 8, maxLen = 8)

  // Fourth column cdate has 10 values from 0.20 through 2.00 at increment of 0.2.
  val decMin = new java.math.BigDecimal("0.200000000000000000")
  val decMax = new java.math.BigDecimal("2.000000000000000000")
  val arDecimal = AttributeReference("cdecimal", DecimalType(12, 2))()
  val childColStatDecimal = ColumnStat(distinctCount = 10, min = Some(decMin), max = Some(decMax),
    nullCount = 0, avgLen = 8, maxLen = 8)

  // Fifth column cfloat has 10 float values: 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0
  val arFloat = AttributeReference("cfloat", FloatType)()
  val childColStatFloat = ColumnStat(distinctCount = 10, min = Some(1.0), max = Some(10.0),
    nullCount = 0, avgLen = 4, maxLen = 4)

  // Sixth column cdouble has 10 double values: 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0
  val arDouble = AttributeReference("cdouble", FloatType)()
  val childColStatDouble = ColumnStat(distinctCount = 10, min = Some(1.0), max = Some(10.0),
    nullCount = 0, avgLen = 8, maxLen = 8)

  test("cint = 2") {
    validateEstimatedStats(
      arInt,
      Filter(EqualTo(arInt, Literal(2)), childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 1, min = Some(2), max = Some(2),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(1L)
    )
  }

  test("cint = 0") {
    // This is an out-of-range case since 0 is outside the range [min, max]
    validateEstimatedStats(
      arInt,
      Filter(EqualTo(arInt, Literal(0)), childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(0L)
    )
  }

  test("cint < 3") {
    validateEstimatedStats(
      arInt,
      Filter(LessThan(arInt, Literal(3)), childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 2, min = Some(1), max = Some(3),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("cint < 0") {
    // This is a corner case since literal 0 is smaller than min.
    validateEstimatedStats(
      arInt,
      Filter(LessThan(arInt, Literal(0)), childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(0L)
    )
  }

  test("cint <= 3") {
    validateEstimatedStats(
      arInt,
      Filter(LessThanOrEqual(arInt, Literal(3)), childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 2, min = Some(1), max = Some(3),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("cint > 6") {
    validateEstimatedStats(
      arInt,
      Filter(GreaterThan(arInt, Literal(6)), childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 4, min = Some(6), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(5L)
    )
  }

  test("cint > 10") {
    // This is a corner case since max value is 10.
    validateEstimatedStats(
      arInt,
      Filter(GreaterThan(arInt, Literal(10)), childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(0L)
    )
  }

  test("cint >= 6") {
    validateEstimatedStats(
      arInt,
      Filter(GreaterThanOrEqual(arInt, Literal(6)), childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 4, min = Some(6), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(5L)
    )
  }

  test("cint IS NULL") {
    validateEstimatedStats(
      arInt,
      Filter(IsNull(arInt), childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 0, min = None, max = None,
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(0L)
    )
  }

  test("cint IS NOT NULL") {
    validateEstimatedStats(
      arInt,
      Filter(IsNotNull(arInt), childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(10L)
    )
  }

  test("cint > 3 AND cint <= 6") {
    val condition = And(GreaterThan(arInt, Literal(3)), LessThanOrEqual(arInt, Literal(6)))
    validateEstimatedStats(
      arInt,
      Filter(condition, childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 3, min = Some(3), max = Some(6),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(4L)
    )
  }

  test("cint = 3 OR cint = 6") {
    val condition = Or(EqualTo(arInt, Literal(3)), EqualTo(arInt, Literal(6)))
    validateEstimatedStats(
      arInt,
      Filter(condition, childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(2L)
    )
  }

  test("cint IN (3, 4, 5)") {
    validateEstimatedStats(
      arInt,
      Filter(InSet(arInt, Set(3, 4, 5)), childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 3, min = Some(3), max = Some(5),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("cint NOT IN (3, 4, 5)") {
    validateEstimatedStats(
      arInt,
      Filter(Not(InSet(arInt, Set(3, 4, 5))), childStatsTestPlan(Seq(arInt))),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(7L)
    )
  }

  test("cdate = cast('2017-01-02' AS DATE)") {
    val d20170102 = Date.valueOf("2017-01-02")
    val d20170102_SQLDate = DateTimeUtils.fromJavaDate(d20170102)
    validateEstimatedStats(
      arDate,
      Filter(EqualTo(arDate, Literal(d20170102_SQLDate, IntegerType)),
        childStatsTestPlan(Seq(arDate))),
      ColumnStat(distinctCount = 1, min = Some(d20170102), max = Some(d20170102),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(1L)
    )
  }

  test("cdate < '2017-01-03' ") {
    val d20170103 = Date.valueOf("2017-01-03")
    validateEstimatedStats(
      arDate,
      Filter(LessThan(arDate, Literal("2017-01-03")), childStatsTestPlan(Seq(arDate))),
      ColumnStat(distinctCount = 2, min = Some(dMin), max = Some(d20170103),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("cdate < cast('2017-01-03' AS DATE)") {
    val d20170103 = Date.valueOf("2017-01-03")
    val d20170103_SQLDate = DateTimeUtils.fromJavaDate(d20170103)
    validateEstimatedStats(
      arDate,
      Filter(LessThan(arDate, Literal(d20170103_SQLDate, IntegerType)),
        childStatsTestPlan(Seq(arDate))),
      ColumnStat(distinctCount = 2, min = Some(dMin), max = Some(d20170103),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("cdate IN ('2017-01-03', '2017-01-04', '2017-01-05')") {
    val d20170103 = Date.valueOf("2017-01-03")
    val d20170105 = Date.valueOf("2017-01-05")
    validateEstimatedStats(
      arDate,
      Filter(InSet(arDate, Set("2017-01-03", "2017-01-04", "2017-01-05")),
        childStatsTestPlan(Seq(arDate))),
      ColumnStat(distinctCount = 3, min = Some(d20170103), max = Some(d20170105),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("ctimestamp = cast('2017-01-01 02:00:00' AS TIMESTAMP)") {
    val ts2017010102 = Timestamp.valueOf("2017-01-01 02:00:00")
    val ts2017010102_SQLTS = DateTimeUtils.fromJavaTimestamp(ts2017010102)
    validateEstimatedStats(
      arTimestamp,
      Filter(EqualTo(arTimestamp, Literal(ts2017010102_SQLTS, LongType)),
        childStatsTestPlan(Seq(arTimestamp))),
      ColumnStat(distinctCount = 1, min = Some(ts2017010102), max = Some(ts2017010102),
        nullCount = 0, avgLen = 8, maxLen = 8),
      Some(1L)
    )
  }

  test("ctimestamp < '2017-01-01 03:00:00' ") {
    val ts2017010103 = Timestamp.valueOf("2017-01-01 03:00:00")
    validateEstimatedStats(
      arTimestamp,
      Filter(LessThan(arTimestamp, Literal("2017-01-01 03:00:00")),
        childStatsTestPlan(Seq(arTimestamp))),
      ColumnStat(distinctCount = 2, min = Some(tsMin), max = Some(ts2017010103),
        nullCount = 0, avgLen = 8, maxLen = 8),
      Some(3L)
    )
  }

  test("ctimestamp < cast('2017-01-01 03:00:00' AS TIMESTAMP)") {
    val ts2017010103 = Timestamp.valueOf("2017-01-01 03:00:00")
    val ts2017010102_SQLTS = DateTimeUtils.fromJavaTimestamp(ts2017010103)
    validateEstimatedStats(
      arTimestamp,
      Filter(LessThan(arTimestamp, Literal(ts2017010102_SQLTS, LongType)),
        childStatsTestPlan(Seq(arTimestamp))),
      ColumnStat(distinctCount = 2, min = Some(tsMin), max = Some(ts2017010103),
        nullCount = 0, avgLen = 8, maxLen = 8),
      Some(3L)
    )
  }

  test("cdecimal = 0.40") {
    val dec_0_40 = new java.math.BigDecimal("0.400000000000000000")
    validateEstimatedStats(
      arDecimal,
      Filter(EqualTo(arDecimal, Literal(dec_0_40, DecimalType(12, 2))),
        childStatsTestPlan(Seq(arDecimal))),
      ColumnStat(distinctCount = 1, min = Some(dec_0_40), max = Some(dec_0_40),
        nullCount = 0, avgLen = 8, maxLen = 8),
      Some(1L)
    )
  }

  test("cdecimal < 0.60 ") {
    val dec_0_60 = new java.math.BigDecimal("0.600000000000000000")
    validateEstimatedStats(
      arDecimal,
      Filter(LessThan(arDecimal, Literal(dec_0_60, DecimalType(12, 2))),
        childStatsTestPlan(Seq(arDecimal))),
      ColumnStat(distinctCount = 2, min = Some(decMin), max = Some(dec_0_60),
        nullCount = 0, avgLen = 8, maxLen = 8),
      Some(3L)
    )
  }

  test("cfloat < 3.0") {
    validateEstimatedStats(
      arFloat,
      Filter(LessThan(arFloat, Literal(3.0)), childStatsTestPlan(Seq(arFloat))),
      ColumnStat(distinctCount = 2, min = Some(1.0), max = Some(3.0),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(3L)
    )
  }

  test("cdouble < 3.0") {
    validateEstimatedStats(
      arDouble,
      Filter(LessThan(arDouble, Literal(3.0)), childStatsTestPlan(Seq(arDouble))),
      ColumnStat(distinctCount = 2, min = Some(1.0), max = Some(3.0),
        nullCount = 0, avgLen = 8, maxLen = 8),
      Some(3L)
    )
  }

  // This is a corner test case.  We want to test if we can handle the case when the number of
  // valid values in IN clause is greater than the number of distinct values for a given column.
  // For example, column has only 2 distinct values 1 and 6.
  // The predicate is: column IN (1, 2, 3, 4, 5).
  test("cint IN (1, 2, 3, 4, 5)") {
    val cornerChildColStatInt = ColumnStat(distinctCount = 2, min = Some(1), max = Some(6),
      nullCount = 0, avgLen = 4, maxLen = 4)
    val cornerChildStatsTestplan = StatsTestPlan(
      outputList = Seq(arInt),
      rowCount = 2L,
      attributeStats = AttributeMap(Seq(arInt -> cornerChildColStatInt))
    )
    validateEstimatedStats(
      arInt,
      Filter(InSet(arInt, Set(1, 2, 3, 4, 5)), cornerChildStatsTestplan),
      ColumnStat(distinctCount = 2, min = Some(1), max = Some(5),
        nullCount = 0, avgLen = 4, maxLen = 4),
      Some(2L)
    )
  }

  private def childStatsTestPlan(outList: Seq[Attribute]): StatsTestPlan = {
    StatsTestPlan(
      outputList = outList,
      rowCount = 10L,
      attributeStats = AttributeMap(Seq(
        arInt -> childColStatInt,
        arDate -> childColStatDate,
        arTimestamp -> childColStatTimestamp,
        arDecimal -> childColStatDecimal,
        arFloat -> childColStatFloat,
        arDouble -> childColStatDouble
      ))
    )
  }

  private def validateEstimatedStats(
      ar: AttributeReference,
      filterNode: Filter,
      expectedColStats: ColumnStat,
      rowCount: Option[BigInt] = None)
    : Unit = {

    val expectedRowCount: BigInt = rowCount.getOrElse(0L)
    val expectedAttrStats = toAttributeMap(Seq(ar.name -> expectedColStats), filterNode)
    val expectedSizeInBytes = getOutputSize(filterNode.output, expectedRowCount, expectedAttrStats)

    val filteredStats = filterNode.stats(conf)
    assert(filteredStats.sizeInBytes == expectedSizeInBytes)
    assert(filteredStats.rowCount == rowCount)
    assert(filteredStats.attributeStats(ar) == expectedColStats)
  }

}
