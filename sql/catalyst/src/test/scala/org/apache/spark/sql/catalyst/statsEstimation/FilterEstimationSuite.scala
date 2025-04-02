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

import java.sql.Date

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.ColumnStatsMap
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

/**
 * In this test suite, we test predicates containing the following operators:
 * =, <, <=, >, >=, AND, OR, IS NULL, IS NOT NULL, IN, NOT IN
 */
class FilterEstimationSuite extends StatsEstimationTestBase {

  // Suppose our test table has 10 rows and 10 columns.
  // column cint has values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  // Hence, distinctCount:10, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  val attrInt = AttributeReference("cint", IntegerType)()
  val colStatInt = ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

  // column cbool has only 2 distinct values
  val attrBool = AttributeReference("cbool", BooleanType)()
  val colStatBool = ColumnStat(distinctCount = Some(2), min = Some(false), max = Some(true),
    nullCount = Some(0), avgLen = Some(1), maxLen = Some(1))

  // column cdate has 10 values from 2017-01-01 through 2017-01-10.
  val dMin = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-01"))
  val dMax = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-10"))
  val attrDate = AttributeReference("cdate", DateType)()
  val colStatDate = ColumnStat(distinctCount = Some(10),
    min = Some(dMin), max = Some(dMax),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

  // column cdecimal has 4 values from 0.20 through 0.80 at increment of 0.20.
  val decMin = Decimal("0.200000000000000000")
  val decMax = Decimal("0.800000000000000000")
  val attrDecimal = AttributeReference("cdecimal", DecimalType(18, 18))()
  val colStatDecimal = ColumnStat(distinctCount = Some(4),
    min = Some(decMin), max = Some(decMax),
    nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))

  // column cdouble has 10 double values: 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0
  val attrDouble = AttributeReference("cdouble", DoubleType)()
  val colStatDouble = ColumnStat(distinctCount = Some(10), min = Some(1.0), max = Some(10.0),
    nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))

  // column cstring has 10 String values:
  // "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9"
  val attrString = AttributeReference("cstring", StringType)()
  val colStatString = ColumnStat(distinctCount = Some(10), min = None, max = None,
    nullCount = Some(0), avgLen = Some(2), maxLen = Some(2))

  // column cint2 has values: 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
  // Hence, distinctCount:10, min:7, max:16, nullCount:0, avgLen:4, maxLen:4
  // This column is created to test "cint < cint2
  val attrInt2 = AttributeReference("cint2", IntegerType)()
  val colStatInt2 = ColumnStat(distinctCount = Some(10), min = Some(7), max = Some(16),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

  // column cint3 has values: 30, 31, 32, 33, 34, 35, 36, 37, 38, 39
  // Hence, distinctCount:10, min:30, max:39, nullCount:0, avgLen:4, maxLen:4
  // This column is created to test "cint = cint3 without overlap at all.
  val attrInt3 = AttributeReference("cint3", IntegerType)()
  val colStatInt3 = ColumnStat(distinctCount = Some(10), min = Some(30), max = Some(39),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

  // column cint4 has values in the range from 1 to 10
  // distinctCount:10, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  // This column is created to test complete overlap
  val attrInt4 = AttributeReference("cint4", IntegerType)()
  val colStatInt4 = ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

  // column cintHgm has values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 with histogram.
  // Note that cintHgm has an even distribution with histogram information built.
  // Hence, distinctCount:10, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  val attrIntHgm = AttributeReference("cintHgm", IntegerType)()
  val hgmInt = Histogram(2.0, Array(HistogramBin(1.0, 2.0, 2),
    HistogramBin(2.0, 4.0, 2), HistogramBin(4.0, 6.0, 2),
    HistogramBin(6.0, 8.0, 2), HistogramBin(8.0, 10.0, 2)))
  val colStatIntHgm = ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))

  // column cintSkewHgm has values: 1, 4, 4, 5, 5, 5, 5, 6, 6, 10 with histogram.
  // Note that cintSkewHgm has a skewed distribution with histogram information built.
  // distinctCount:5, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  val attrIntSkewHgm = AttributeReference("cintSkewHgm", IntegerType)()
  val hgmIntSkew = Histogram(2.0, Array(HistogramBin(1.0, 4.0, 2),
    HistogramBin(4.0, 5.0, 2), HistogramBin(5.0, 5.0, 1),
    HistogramBin(5.0, 6.0, 2), HistogramBin(6.0, 10.0, 2)))
  val colStatIntSkewHgm = ColumnStat(distinctCount = Some(5), min = Some(1), max = Some(10),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))

  val attributeMap = AttributeMap(Seq(
    attrInt -> colStatInt,
    attrBool -> colStatBool,
    attrDate -> colStatDate,
    attrDecimal -> colStatDecimal,
    attrDouble -> colStatDouble,
    attrString -> colStatString,
    attrInt2 -> colStatInt2,
    attrInt3 -> colStatInt3,
    attrInt4 -> colStatInt4,
    attrIntHgm -> colStatIntHgm,
    attrIntSkewHgm -> colStatIntSkewHgm
  ))

  test("true") {
    validateEstimatedStats(
      Filter(TrueLiteral, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt),
      expectedRowCount = 10)
  }

  test("false") {
    validateEstimatedStats(
      Filter(FalseLiteral, childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("null") {
    validateEstimatedStats(
      Filter(Literal(null, IntegerType), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("Not(null)") {
    validateEstimatedStats(
      Filter(Not(Literal(null, IntegerType)), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("Not(Not(null))") {
    validateEstimatedStats(
      Filter(Not(Not(Literal(null, IntegerType))), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint < 3 AND null") {
    val condition = And(LessThan(attrInt, Literal(3)), Literal(null, IntegerType))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint < 3 OR null") {
    val condition = Or(LessThan(attrInt, Literal(3)), Literal(null, IntegerType))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(3))),
      expectedRowCount = 3)
  }

  test("Not(cint < 3 AND null)") {
    val condition = Not(And(LessThan(attrInt, Literal(3)), Literal(null, IntegerType)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(8))),
      expectedRowCount = 8)
  }

  test("Not(cint < 3 OR null)") {
    val condition = Not(Or(LessThan(attrInt, Literal(3)), Literal(null, IntegerType)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("Not(cint < 3 AND Not(null))") {
    val condition = Not(And(LessThan(attrInt, Literal(3)), Not(Literal(null, IntegerType))))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(8))),
      expectedRowCount = 8)
  }

  test("cint = 2") {
    validateEstimatedStats(
      Filter(EqualTo(attrInt, Literal(2)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(1), min = Some(2), max = Some(2),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 1)
  }

  test("cint <=> 2") {
    validateEstimatedStats(
      Filter(EqualNullSafe(attrInt, Literal(2)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(1), min = Some(2), max = Some(2),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 1)
  }

  test("cint = 0") {
    // This is an out-of-range case since 0 is outside the range [min, max]
    validateEstimatedStats(
      Filter(EqualTo(attrInt, Literal(0)), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint < 3") {
    validateEstimatedStats(
      Filter(LessThan(attrInt, Literal(3)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(3), min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 3)
  }

  test("cint < 0") {
    // This is a corner case since literal 0 is smaller than min.
    validateEstimatedStats(
      Filter(LessThan(attrInt, Literal(0)), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint <= 3") {
    validateEstimatedStats(
      Filter(LessThanOrEqual(attrInt, Literal(3)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(3), min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 3)
  }

  test("cint > 6") {
    validateEstimatedStats(
      Filter(GreaterThan(attrInt, Literal(6)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(5), min = Some(6), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 5)
  }

  test("cint > 10") {
    // This is a corner case since max value is 10.
    validateEstimatedStats(
      Filter(GreaterThan(attrInt, Literal(10)), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint >= 6") {
    validateEstimatedStats(
      Filter(GreaterThanOrEqual(attrInt, Literal(6)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(5), min = Some(6), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 5)
  }

  test("cint IS NULL") {
    validateEstimatedStats(
      Filter(IsNull(attrInt), childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint IS NOT NULL") {
    validateEstimatedStats(
      Filter(IsNotNull(attrInt), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 10)
  }

  test("cint IS NOT NULL && null") {
    // 'cint < null' will be optimized to 'cint IS NOT NULL && null'.
    // More similar cases can be found in the Optimizer NullPropagation.
    val condition = And(IsNotNull(attrInt), Literal(null, IntegerType))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cint > 3 AND cint <= 6") {
    val condition = And(GreaterThan(attrInt, Literal(3)), LessThanOrEqual(attrInt, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(4), min = Some(3), max = Some(6),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 4)
  }

  test("cint = 3 OR cint = 6") {
    val condition = Or(EqualTo(attrInt, Literal(3)), EqualTo(attrInt, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(2))),
      expectedRowCount = 2)
  }

  test("Not(cint > 3 AND cint <= 6)") {
    val condition = Not(And(GreaterThan(attrInt, Literal(3)), LessThanOrEqual(attrInt, Literal(6))))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(6))),
      expectedRowCount = 6)
  }

  test("Not(cint <= 3 OR cint > 6)") {
    val condition = Not(Or(LessThanOrEqual(attrInt, Literal(3)), GreaterThan(attrInt, Literal(6))))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(5))),
      expectedRowCount = 5)
  }

  test("Not(cint = 3 AND cstring < 'A8')") {
    val condition = Not(And(EqualTo(attrInt, Literal(3)), LessThan(attrString, Literal("A8"))))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt, attrString), 10L)),
      Seq(attrInt -> colStatInt, attrString -> colStatString),
      expectedRowCount = 10)
  }

  test("Not(cint = 3 OR cstring < 'A8')") {
    val condition = Not(Or(EqualTo(attrInt, Literal(3)), LessThan(attrString, Literal("A8"))))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt, attrString), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(9)),
        attrString -> colStatString.copy(distinctCount = Some(9))),
      expectedRowCount = 9)
  }

  test("cint IN (3, 4, 5)") {
    validateEstimatedStats(
      Filter(InSet(attrInt, Set(3, 4, 5)), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(3), min = Some(3), max = Some(5),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 3)
  }

  test("evaluateInSet with all zeros") {
    validateEstimatedStats(
      Filter(InSet(attrString, Set(3, 4, 5)),
        StatsTestPlan(Seq(attrString), 0,
          AttributeMap(Seq(attrString ->
            ColumnStat(distinctCount = Some(0), min = None, max = None,
              nullCount = Some(0), avgLen = Some(0), maxLen = Some(0)))))),
      Seq(attrString -> ColumnStat(distinctCount = Some(0))),
      expectedRowCount = 0)
  }

  test("evaluateInSet with string") {
    validateEstimatedStats(
      Filter(InSet(attrString, Set("A0")),
        StatsTestPlan(Seq(attrString), 10,
          AttributeMap(Seq(attrString ->
            ColumnStat(distinctCount = Some(10), min = None, max = None,
              nullCount = Some(0), avgLen = Some(2), maxLen = Some(2)))))),
      Seq(attrString -> ColumnStat(distinctCount = Some(1), min = None, max = None,
        nullCount = Some(0), avgLen = Some(2), maxLen = Some(2))),
      expectedRowCount = 1)
  }

  test("cint NOT IN (3, 4, 5)") {
    validateEstimatedStats(
      Filter(Not(InSet(attrInt, Set(3, 4, 5))), childStatsTestPlan(Seq(attrInt), 10L)),
      Seq(attrInt -> colStatInt.copy(distinctCount = Some(7))),
      expectedRowCount = 7)
  }

  test("cbool IN (true)") {
    validateEstimatedStats(
      Filter(InSet(attrBool, Set(true)), childStatsTestPlan(Seq(attrBool), 10L)),
      Seq(attrBool -> ColumnStat(distinctCount = Some(1), min = Some(true), max = Some(true),
        nullCount = Some(0), avgLen = Some(1), maxLen = Some(1))),
      expectedRowCount = 5)
  }

  test("cbool = true") {
    validateEstimatedStats(
      Filter(EqualTo(attrBool, Literal(true)), childStatsTestPlan(Seq(attrBool), 10L)),
      Seq(attrBool -> ColumnStat(distinctCount = Some(1), min = Some(true), max = Some(true),
        nullCount = Some(0), avgLen = Some(1), maxLen = Some(1))),
      expectedRowCount = 5)
  }

  test("cbool > false") {
    validateEstimatedStats(
      Filter(GreaterThan(attrBool, Literal(false)), childStatsTestPlan(Seq(attrBool), 10L)),
      Seq(attrBool -> ColumnStat(distinctCount = Some(1), min = Some(false), max = Some(true),
        nullCount = Some(0), avgLen = Some(1), maxLen = Some(1))),
      expectedRowCount = 5)
  }

  test("cdate = cast('2017-01-02' AS DATE)") {
    val d20170102 = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-02"))
    validateEstimatedStats(
      Filter(EqualTo(attrDate, Literal(d20170102, DateType)),
        childStatsTestPlan(Seq(attrDate), 10L)),
      Seq(attrDate -> ColumnStat(distinctCount = Some(1),
        min = Some(d20170102), max = Some(d20170102),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 1)
  }

  test("cdate < cast('2017-01-03' AS DATE)") {
    val d20170101 = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-01"))
    val d20170103 = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-03"))
    validateEstimatedStats(
      Filter(LessThan(attrDate, Literal(d20170103, DateType)),
        childStatsTestPlan(Seq(attrDate), 10L)),
      Seq(attrDate -> ColumnStat(distinctCount = Some(3),
        min = Some(d20170101), max = Some(d20170103),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 3)
  }

  test("""cdate IN ( cast('2017-01-03' AS DATE),
      cast('2017-01-04' AS DATE), cast('2017-01-05' AS DATE) )""") {
    val d20170103 = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-03"))
    val d20170104 = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-04"))
    val d20170105 = DateTimeUtils.fromJavaDate(Date.valueOf("2017-01-05"))
    validateEstimatedStats(
      Filter(In(attrDate, Seq(Literal(d20170103, DateType), Literal(d20170104, DateType),
        Literal(d20170105, DateType))), childStatsTestPlan(Seq(attrDate), 10L)),
      Seq(attrDate -> ColumnStat(distinctCount = Some(3),
        min = Some(d20170103), max = Some(d20170105),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 3)
  }

  test("cdecimal = 0.400000000000000000") {
    val dec_0_40 = Decimal("0.400000000000000000")
    validateEstimatedStats(
      Filter(EqualTo(attrDecimal, Literal(dec_0_40)),
        childStatsTestPlan(Seq(attrDecimal), 4L)),
      Seq(attrDecimal -> ColumnStat(distinctCount = Some(1),
        min = Some(dec_0_40), max = Some(dec_0_40),
        nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))),
      expectedRowCount = 1)
  }

  test("cdecimal < 0.60 ") {
    val dec_0_20 = Decimal("0.200000000000000000")
    val dec_0_60 = Decimal("0.600000000000000000")
    validateEstimatedStats(
      Filter(LessThan(attrDecimal, Literal(dec_0_60)),
        childStatsTestPlan(Seq(attrDecimal), 4L)),
      Seq(attrDecimal -> ColumnStat(distinctCount = Some(3),
        min = Some(dec_0_20), max = Some(dec_0_60),
        nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))),
      expectedRowCount = 3)
  }

  test("cdouble < 3.0") {
    validateEstimatedStats(
      Filter(LessThan(attrDouble, Literal(3.0)), childStatsTestPlan(Seq(attrDouble), 10L)),
      Seq(attrDouble -> ColumnStat(distinctCount = Some(3), min = Some(1.0), max = Some(3.0),
        nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))),
      expectedRowCount = 3)
  }

  test("cstring = 'A2'") {
    validateEstimatedStats(
      Filter(EqualTo(attrString, Literal("A2")), childStatsTestPlan(Seq(attrString), 10L)),
      Seq(attrString -> ColumnStat(distinctCount = Some(1), min = None, max = None,
        nullCount = Some(0), avgLen = Some(2), maxLen = Some(2))),
      expectedRowCount = 1)
  }

  test("cstring < 'A2' - unsupported condition") {
    validateEstimatedStats(
      Filter(LessThan(attrString, Literal("A2")), childStatsTestPlan(Seq(attrString), 10L)),
      Seq(attrString -> ColumnStat(distinctCount = Some(10), min = None, max = None,
        nullCount = Some(0), avgLen = Some(2), maxLen = Some(2))),
      expectedRowCount = 10)
  }

  test("cint IN (1, 2, 3, 4, 5)") {
    // This is a corner test case.  We want to test if we can handle the case when the number of
    // valid values in IN clause is greater than the number of distinct values for a given column.
    // For example, column has only 2 distinct values 1 and 6.
    // The predicate is: column IN (1, 2, 3, 4, 5).
    val cornerChildColStatInt = ColumnStat(distinctCount = Some(2),
      min = Some(1), max = Some(6),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val cornerChildStatsTestplan = StatsTestPlan(
      outputList = Seq(attrInt),
      rowCount = 2L,
      attributeStats = AttributeMap(Seq(attrInt -> cornerChildColStatInt))
    )
    validateEstimatedStats(
      Filter(InSet(attrInt, Set(1, 2, 3, 4, 5)), cornerChildStatsTestplan),
      Seq(attrInt -> ColumnStat(distinctCount = Some(2), min = Some(1), max = Some(5),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 2)
  }

  // This is a limitation test. We should remove it after the limitation is removed.
  test("don't estimate IsNull or IsNotNull if the child is a non-leaf node") {
    val attrIntLargerRange = AttributeReference("c1", IntegerType)()
    val colStatIntLargerRange = ColumnStat(distinctCount = Some(20),
      min = Some(1), max = Some(20),
      nullCount = Some(10), avgLen = Some(4), maxLen = Some(4))
    val smallerTable = childStatsTestPlan(Seq(attrInt), 10L)
    val largerTable = StatsTestPlan(
      outputList = Seq(attrIntLargerRange),
      rowCount = 30,
      attributeStats = AttributeMap(Seq(attrIntLargerRange -> colStatIntLargerRange)))
    val nonLeafChild = Join(largerTable, smallerTable, LeftOuter,
      Some(EqualTo(attrIntLargerRange, attrInt)), JoinHint.NONE)

    Seq(IsNull(attrIntLargerRange), IsNotNull(attrIntLargerRange)).foreach { predicate =>
      validateEstimatedStats(
        Filter(predicate, nonLeafChild),
        // column stats don't change
        Seq(attrInt -> colStatInt, attrIntLargerRange -> colStatIntLargerRange),
        expectedRowCount = 30)
    }
  }

  test("cint = cint2") {
    // partial overlap case
    validateEstimatedStats(
      Filter(EqualTo(attrInt, attrInt2), childStatsTestPlan(Seq(attrInt, attrInt2), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(4), min = Some(7), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt2 -> ColumnStat(distinctCount = Some(4), min = Some(7), max = Some(10),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 4)
  }

  test("cint > cint2") {
    // partial overlap case
    validateEstimatedStats(
      Filter(GreaterThan(attrInt, attrInt2), childStatsTestPlan(Seq(attrInt, attrInt2), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(4), min = Some(7), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt2 -> ColumnStat(distinctCount = Some(4), min = Some(7), max = Some(10),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 4)
  }

  test("cint < cint2") {
    // partial overlap case
    validateEstimatedStats(
      Filter(LessThan(attrInt, attrInt2), childStatsTestPlan(Seq(attrInt, attrInt2), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(4), min = Some(1), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt2 -> ColumnStat(distinctCount = Some(4), min = Some(7), max = Some(16),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 4)
  }

  test("cint = cint4") {
    // complete overlap case
    validateEstimatedStats(
      Filter(EqualTo(attrInt, attrInt4), childStatsTestPlan(Seq(attrInt, attrInt4), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt4 -> ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 10)
  }

  test("cint < cint4") {
    // partial overlap case
    validateEstimatedStats(
      Filter(LessThan(attrInt, attrInt4), childStatsTestPlan(Seq(attrInt, attrInt4), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(4), min = Some(1), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt4 -> ColumnStat(distinctCount = Some(4), min = Some(1), max = Some(10),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 4)
  }

  test("cint = cint3") {
    // no records qualify due to no overlap
    validateEstimatedStats(
      Filter(EqualTo(attrInt, attrInt3), childStatsTestPlan(Seq(attrInt, attrInt3), 10L)),
      Nil, // set to empty
      expectedRowCount = 0)
  }

  test("cint < cint3") {
    // all table records qualify.
    validateEstimatedStats(
      Filter(LessThan(attrInt, attrInt3), childStatsTestPlan(Seq(attrInt, attrInt3), 10L)),
      Seq(attrInt -> ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt3 -> ColumnStat(distinctCount = Some(10), min = Some(30), max = Some(39),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))),
      expectedRowCount = 10)
  }

  test("cint > cint3") {
    // no records qualify due to no overlap
    validateEstimatedStats(
      Filter(GreaterThan(attrInt, attrInt3), childStatsTestPlan(Seq(attrInt, attrInt3), 10L)),
      Nil, // set to empty
      expectedRowCount = 0)
  }

  test("update ndv for columns based on overall selectivity") {
    // filter condition: cint > 3 AND cint4 <= 6
    val condition = And(GreaterThan(attrInt, Literal(3)), LessThanOrEqual(attrInt4, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrInt, attrInt4, attrString), 10L)),
      Seq(
        attrInt -> ColumnStat(distinctCount = Some(5), min = Some(3), max = Some(10),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrInt4 -> ColumnStat(distinctCount = Some(5), min = Some(1), max = Some(6),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        attrString -> colStatString.copy(distinctCount = Some(5))),
      expectedRowCount = 5)
  }

  // The following test cases have histogram information collected for the test column with
  // an even distribution
  test("Not(cintHgm < 3 AND null)") {
    val condition = Not(And(LessThan(attrIntHgm, Literal(3)), Literal(null, IntegerType)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> colStatIntHgm.copy(distinctCount = Some(7))),
      expectedRowCount = 7)
  }

  test("cintHgm = 5") {
    validateEstimatedStats(
      Filter(EqualTo(attrIntHgm, Literal(5)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> ColumnStat(distinctCount = Some(1), min = Some(5), max = Some(5),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))),
      expectedRowCount = 1)
  }

  test("cintHgm = 0") {
    // This is an out-of-range case since 0 is outside the range [min, max]
    validateEstimatedStats(
      Filter(EqualTo(attrIntHgm, Literal(0)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cintHgm < 3") {
    validateEstimatedStats(
      Filter(LessThan(attrIntHgm, Literal(3)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> ColumnStat(distinctCount = Some(3), min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))),
      expectedRowCount = 3)
  }

  test("cintHgm < 0") {
    // This is a corner case since literal 0 is smaller than min.
    validateEstimatedStats(
      Filter(LessThan(attrIntHgm, Literal(0)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cintHgm <= 3") {
    validateEstimatedStats(
      Filter(LessThanOrEqual(attrIntHgm, Literal(3)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> ColumnStat(distinctCount = Some(3), min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))),
      expectedRowCount = 3)
  }

  test("cintHgm > 6") {
    validateEstimatedStats(
      Filter(GreaterThan(attrIntHgm, Literal(6)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> ColumnStat(distinctCount = Some(4), min = Some(6), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))),
      expectedRowCount = 4)
  }

  test("cintHgm > 10") {
    // This is a corner case since max value is 10.
    validateEstimatedStats(
      Filter(GreaterThan(attrIntHgm, Literal(10)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cintHgm >= 6") {
    validateEstimatedStats(
      Filter(GreaterThanOrEqual(attrIntHgm, Literal(6)), childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> ColumnStat(distinctCount = Some(5), min = Some(6), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))),
      expectedRowCount = 5)
  }

  test("cintHgm > 3 AND cintHgm <= 6") {
    val condition = And(GreaterThan(attrIntHgm,
      Literal(3)), LessThanOrEqual(attrIntHgm, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> ColumnStat(distinctCount = Some(4), min = Some(3), max = Some(6),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmInt))),
      expectedRowCount = 4)
  }

  test("cintHgm = 3 OR cintHgm = 6") {
    val condition = Or(EqualTo(attrIntHgm, Literal(3)), EqualTo(attrIntHgm, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrIntHgm), 10L)),
      Seq(attrIntHgm -> colStatIntHgm.copy(distinctCount = Some(3))),
      expectedRowCount = 3)
  }

  // The following test cases have histogram information collected for the test column with
  // a skewed distribution.
  test("Not(cintSkewHgm < 3 AND null)") {
    val condition = Not(And(LessThan(attrIntSkewHgm, Literal(3)), Literal(null, IntegerType)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> colStatIntSkewHgm.copy(distinctCount = Some(5))),
      expectedRowCount = 9)
  }

  test("cintSkewHgm = 5") {
    validateEstimatedStats(
      Filter(EqualTo(attrIntSkewHgm, Literal(5)), childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> ColumnStat(distinctCount = Some(1), min = Some(5), max = Some(5),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))),
      expectedRowCount = 4)
  }

  test("cintSkewHgm = 0") {
    // This is an out-of-range case since 0 is outside the range [min, max]
    validateEstimatedStats(
      Filter(EqualTo(attrIntSkewHgm, Literal(0)), childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cintSkewHgm < 3") {
    validateEstimatedStats(
      Filter(LessThan(attrIntSkewHgm, Literal(3)), childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> ColumnStat(distinctCount = Some(1), min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))),
      expectedRowCount = 2)
  }

  test("cintSkewHgm < 0") {
    // This is a corner case since literal 0 is smaller than min.
    validateEstimatedStats(
      Filter(LessThan(attrIntSkewHgm, Literal(0)), childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cintSkewHgm <= 3") {
    validateEstimatedStats(
      Filter(LessThanOrEqual(attrIntSkewHgm, Literal(3)),
        childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> ColumnStat(distinctCount = Some(1), min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))),
      expectedRowCount = 2)
  }

  test("cintSkewHgm > 6") {
    validateEstimatedStats(
      Filter(GreaterThan(attrIntSkewHgm, Literal(6)), childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> ColumnStat(distinctCount = Some(1), min = Some(6), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))),
      expectedRowCount = 2)
  }

  test("cintSkewHgm > 10") {
    // This is a corner case since max value is 10.
    validateEstimatedStats(
      Filter(GreaterThan(attrIntSkewHgm, Literal(10)),
        childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Nil,
      expectedRowCount = 0)
  }

  test("cintSkewHgm >= 6") {
    validateEstimatedStats(
      Filter(GreaterThanOrEqual(attrIntSkewHgm, Literal(6)),
        childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> ColumnStat(distinctCount = Some(2), min = Some(6), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))),
      expectedRowCount = 3)
  }

  test("cintSkewHgm > 3 AND cintSkewHgm <= 6") {
    val condition = And(GreaterThan(attrIntSkewHgm,
      Literal(3)), LessThanOrEqual(attrIntSkewHgm, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> ColumnStat(distinctCount = Some(4), min = Some(3), max = Some(6),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = Some(hgmIntSkew))),
      expectedRowCount = 8)
  }

  test("cintSkewHgm = 3 OR cintSkewHgm = 6") {
    val condition = Or(EqualTo(attrIntSkewHgm, Literal(3)), EqualTo(attrIntSkewHgm, Literal(6)))
    validateEstimatedStats(
      Filter(condition, childStatsTestPlan(Seq(attrIntSkewHgm), 10L)),
      Seq(attrIntSkewHgm -> colStatIntSkewHgm.copy(distinctCount = Some(2))),
      expectedRowCount = 3)
  }

  test("SPARK-36079: Null count should be no higher than row count after filter") {
    val colStatNullableString = colStatString.copy(nullCount = Some(10))
    val condition = Filter(EqualTo(attrBool, Literal(true)),
      childStatsTestPlan(Seq(attrBool, attrString), tableRowCount = 10L,
        attributeMap = AttributeMap(Seq(
          attrBool -> colStatBool, attrString -> colStatNullableString))))
    validateEstimatedStats(
      condition,
      Seq(attrBool -> colStatBool.copy(distinctCount = Some(1), min = Some(true)),
        attrString -> colStatNullableString.copy(distinctCount = Some(5), nullCount = Some(5))),
      expectedRowCount = 5)
  }

  test("SPARK-36079: Null count higher than row count") {
    val colStatNullableString = colStatString.copy(nullCount = Some(15))
    val condition = Filter(IsNotNull(attrString),
      childStatsTestPlan(Seq(attrString), tableRowCount = 10L,
        attributeMap = AttributeMap(Seq(attrString -> colStatNullableString))))
    validateEstimatedStats(
      condition,
      Seq(attrString -> colStatNullableString),
      expectedRowCount = 0)
  }

  test("SPARK-36079: Bound selectivity >= 0") {
    val colStatNullableString = colStatString.copy(nullCount = Some(-1))
    val condition = Filter(IsNotNull(attrString),
      childStatsTestPlan(Seq(attrString), tableRowCount = 10L,
        attributeMap = AttributeMap(Seq(attrString -> colStatNullableString))))
    validateEstimatedStats(
      condition,
      Seq(attrString -> colStatString),
      expectedRowCount = 10)
  }

  test("ColumnStatsMap tests") {
    val attrNoDistinct = AttributeReference("att_without_distinct", IntegerType)()
    val attrNoCount = AttributeReference("att_without_count", BooleanType)()
    val attrNoMinMax = AttributeReference("att_without_min_max", DateType)()
    val colStatNoDistinct = ColumnStat(distinctCount = None, min = Some(1), max = Some(10),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val colStatNoCount = ColumnStat(distinctCount = Some(2), min = Some(false), max = Some(true),
      nullCount = None, avgLen = Some(1), maxLen = Some(1))
    val colStatNoMinMax = ColumnStat(distinctCount = Some(1), min = None, max = None,
      nullCount = Some(1), avgLen = None, maxLen = None)
    val columnStatsMap = ColumnStatsMap(AttributeMap(Seq(
      attrNoDistinct -> colStatNoDistinct,
      attrNoCount -> colStatNoCount,
      attrNoMinMax -> colStatNoMinMax
    )))
    assert(!columnStatsMap.hasDistinctCount(attrNoDistinct))
    assert(columnStatsMap.hasDistinctCount(attrNoCount))
    assert(columnStatsMap.hasDistinctCount(attrNoMinMax))
    assert(!columnStatsMap.hasCountStats(attrNoDistinct))
    assert(!columnStatsMap.hasCountStats(attrNoCount))
    assert(columnStatsMap.hasCountStats(attrNoMinMax))
    assert(columnStatsMap.hasMinMaxStats(attrNoDistinct))
    assert(columnStatsMap.hasMinMaxStats(attrNoCount))
    assert(!columnStatsMap.hasMinMaxStats(attrNoMinMax))
  }

  private def childStatsTestPlan(
      outList: Seq[Attribute],
      tableRowCount: BigInt,
      attributeMap: AttributeMap[ColumnStat] = attributeMap): StatsTestPlan = {
    StatsTestPlan(
      outputList = outList,
      rowCount = tableRowCount,
      attributeStats = AttributeMap(outList.map(a => a -> attributeMap(a))))
  }

  private def validateEstimatedStats(
      filterNode: Filter,
      expectedColStats: Seq[(Attribute, ColumnStat)],
      expectedRowCount: Int): Unit = {

    // If the filter has a binary operator (including those nested inside AND/OR/NOT), swap the
    // sides of the attribute and the literal, reverse the operator, and then check again.
    val swappedFilter = filterNode transformExpressionsDown {
      case EqualTo(attr: Attribute, l: Literal) =>
        EqualTo(l, attr)

      case LessThan(attr: Attribute, l: Literal) =>
        GreaterThan(l, attr)
      case LessThanOrEqual(attr: Attribute, l: Literal) =>
        GreaterThanOrEqual(l, attr)

      case GreaterThan(attr: Attribute, l: Literal) =>
        LessThan(l, attr)
      case GreaterThanOrEqual(attr: Attribute, l: Literal) =>
        LessThanOrEqual(l, attr)
    }

    val testFilters = if (swappedFilter != filterNode) {
      Seq(swappedFilter, filterNode)
    } else {
      Seq(filterNode)
    }

    testFilters.foreach { filter =>
      val expectedAttributeMap = AttributeMap(expectedColStats)
      val expectedStats = Statistics(
        sizeInBytes = getOutputSize(filter.output, expectedRowCount, expectedAttributeMap),
        rowCount = Some(expectedRowCount),
        attributeStats = expectedAttributeMap)

      val filterStats = filter.stats
      assert(filterStats.sizeInBytes == expectedStats.sizeInBytes)
      assert(filterStats.rowCount == expectedStats.rowCount)
      val rowCountValue = filterStats.rowCount.getOrElse(0)
      // check the output column stats if the row count is > 0.
      // When row count is 0, the output is set to empty.
      if (rowCountValue != 0) {
        // Need to check attributeStats one by one because we may have multiple output columns.
        // Due to update operation, the output columns may be in different order.
        assert(expectedColStats.size == filterStats.attributeStats.size)
        expectedColStats.foreach { kv =>
          val filterColumnStat = filterStats.attributeStats.get(kv._1).get
          assert(filterColumnStat == kv._2)
        }
      }
    }
  }

}
