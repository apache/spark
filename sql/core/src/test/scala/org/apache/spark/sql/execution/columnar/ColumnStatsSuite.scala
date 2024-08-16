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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types.StringType

class ColumnStatsSuite extends SparkFunSuite {
  testColumnStats(classOf[BooleanColumnStats], BOOLEAN, Array(true, false, 0))
  testColumnStats(classOf[ByteColumnStats], BYTE, Array(Byte.MaxValue, Byte.MinValue, 0))
  testColumnStats(classOf[ShortColumnStats], SHORT, Array(Short.MaxValue, Short.MinValue, 0))
  testColumnStats(classOf[IntColumnStats], INT, Array(Int.MaxValue, Int.MinValue, 0))
  testColumnStats(classOf[LongColumnStats], LONG, Array(Long.MaxValue, Long.MinValue, 0))
  testColumnStats(classOf[FloatColumnStats], FLOAT, Array(Float.MaxValue, Float.MinValue, 0))
  testColumnStats(classOf[DoubleColumnStats], DOUBLE, Array(Double.MaxValue, Double.MinValue, 0))
  testDecimalColumnStats(Array(null, null, 0))
  testIntervalColumnStats(Array(null, null, 0))
  testStringColumnStats(Array(null, null, 0))

  def testColumnStats[T <: PhysicalDataType, U <: ColumnStats](
      columnStatsClass: Class[U],
      columnType: NativeColumnType[T],
      initialStatistics: Array[Any]): Unit = {

    val columnStatsName = columnStatsClass.getSimpleName

    test(s"$columnStatsName: empty") {
      val columnStats = columnStatsClass.getConstructor().newInstance()
      columnStats.collectedStatistics.zip(initialStatistics).foreach {
        case (actual, expected) => assert(actual === expected)
      }
    }

    test(s"$columnStatsName: non-empty") {
      import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._

      val columnStats = columnStatsClass.getConstructor().newInstance()
      val rows = Seq.fill(10)(makeRandomRow(columnType)) ++ Seq.fill(10)(makeNullRow(1))
      rows.foreach(columnStats.gatherStats(_, 0))

      val values = rows.take(10).map(_.get(
        0, ColumnarDataTypeUtils.toLogicalDataType(columnType.dataType))
        .asInstanceOf[T#InternalType])
      val ordering = PhysicalDataType.ordering(
        ColumnarDataTypeUtils.toLogicalDataType(columnType.dataType))
      val stats = columnStats.collectedStatistics

      assertResult(values.min(ordering), "Wrong lower bound")(stats(0))
      assertResult(values.max(ordering), "Wrong upper bound")(stats(1))
      assertResult(10, "Wrong null count")(stats(2))
      assertResult(20, "Wrong row count")(stats(3))
      assertResult(stats(4), "Wrong size in bytes") {
        rows.map { row =>
          if (row.isNullAt(0)) 4 else columnType.actualSize(row, 0)
        }.sum
      }
    }
  }

  def testDecimalColumnStats[T <: PhysicalDataType, U <: ColumnStats](
      initialStatistics: Array[Any]): Unit = {

    val columnStatsName = classOf[DecimalColumnStats].getSimpleName
    val columnType = COMPACT_DECIMAL(15, 10)

    test(s"$columnStatsName: empty") {
      val columnStats = new DecimalColumnStats(15, 10)
      columnStats.collectedStatistics.zip(initialStatistics).foreach {
        case (actual, expected) => assert(actual === expected)
      }
    }

    test(s"$columnStatsName: non-empty") {
      import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._

      val columnStats = new DecimalColumnStats(15, 10)
      val rows = Seq.fill(10)(makeRandomRow(columnType)) ++ Seq.fill(10)(makeNullRow(1))
      rows.foreach(columnStats.gatherStats(_, 0))

      val values = rows.take(10).map(_.get(0,
        ColumnarDataTypeUtils.toLogicalDataType(columnType.dataType)))
      val ordering = PhysicalDataType.ordering(
        ColumnarDataTypeUtils.toLogicalDataType(columnType.dataType))
      val stats = columnStats.collectedStatistics

      assertResult(values.min(ordering), "Wrong lower bound")(stats(0))
      assertResult(values.max(ordering), "Wrong upper bound")(stats(1))
      assertResult(10, "Wrong null count")(stats(2))
      assertResult(20, "Wrong row count")(stats(3))
      assertResult(stats(4), "Wrong size in bytes") {
        rows.map { row =>
          if (row.isNullAt(0)) 4 else columnType.actualSize(row, 0)
        }.sum
      }
    }
  }

  def testIntervalColumnStats[T <: PhysicalDataType, U <: ColumnStats](
      initialStatistics: Array[Any]): Unit = {

    val columnStatsName = classOf[IntervalColumnStats].getSimpleName
    val columnType = CALENDAR_INTERVAL

    test(s"$columnStatsName: empty") {
      val columnStats = new IntervalColumnStats
      columnStats.collectedStatistics.zip(initialStatistics).foreach {
        case (actual, expected) => assert(actual === expected)
      }
    }

    test(s"$columnStatsName: non-empty") {
      import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._

      val columnStats = new IntervalColumnStats
      val rows = Seq.fill(10)(makeRandomRow(columnType)) ++ Seq.fill(10)(makeNullRow(1))
      rows.foreach(columnStats.gatherStats(_, 0))

      val stats = columnStats.collectedStatistics

      assertResult(10, "Wrong null count")(stats(2))
      assertResult(20, "Wrong row count")(stats(3))
      assertResult(stats(4), "Wrong size in bytes") {
        rows.map { row =>
          if (row.isNullAt(0)) 4 else columnType.actualSize(row, 0)
        }.sum
      }
    }
  }

  private def testSupportedCollations: Seq[String] =
    Seq("UTF8_BINARY", "UTF8_BINARY_TRIM", "UTF8_LCASE", "UTF8_LCASE_TRIM",
      "UNICODE", "UNICODE_TRIM", "UNICODE_CI", "UNICODE_CI_TRIM")

  def testStringColumnStats[T <: PhysicalDataType, U <: ColumnStats](
      initialStatistics: Array[Any]): Unit = {

    testSupportedCollations.foreach(collation => {
      val columnType = STRING(StringType(collation))

      test(s"STRING($collation): empty") {
        val columnStats = new StringColumnStats(StringType(collation).collationId)
        columnStats.collectedStatistics.zip(initialStatistics).foreach {
          case (actual, expected) => assert(actual === expected)
        }
      }

      test(s"STRING($collation): non-empty") {
        import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._

        val columnStats = new StringColumnStats(StringType(collation).collationId)
        val rows = Seq.fill(10)(makeRandomRow(columnType)) ++ Seq.fill(10)(makeNullRow(1))
        rows.foreach(columnStats.gatherStats(_, 0))

        val values = rows.take(10).map(_.get(0,
          ColumnarDataTypeUtils.toLogicalDataType(columnType.dataType)))
        val ordering = PhysicalDataType.ordering(
          ColumnarDataTypeUtils.toLogicalDataType(columnType.dataType))
        val stats = columnStats.collectedStatistics

        assertResult(values.min(ordering), "Wrong lower bound")(stats(0))
        assertResult(values.max(ordering), "Wrong upper bound")(stats(1))
        assertResult(10, "Wrong null count")(stats(2))
        assertResult(20, "Wrong row count")(stats(3))
        assertResult(stats(4), "Wrong size in bytes") {
          rows.map { row =>
            if (row.isNullAt(0)) 4 else columnType.actualSize(row, 0)
          }.sum
        }
      }
    })

    test("STRING(UTF8_LCASE): collation-defined ordering with case insensitivity") {
      import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
      import org.apache.spark.unsafe.types.UTF8String

      val columnStats = new StringColumnStats(StringType("UTF8_LCASE").collationId)
      val rows = Seq("b", "a", "C", "A").map(str => {
        val row = new GenericInternalRow(1)
        row(0) = UTF8String.fromString(str)
        row
      })
      rows.foreach(columnStats.gatherStats(_, 0))

      val stats = columnStats.collectedStatistics
      assertResult(UTF8String.fromString("a"), "Wrong lower bound")(stats(0))
      assertResult(UTF8String.fromString("C"), "Wrong upper bound")(stats(1))
    }

    test("STRING(UTF8_BINARY_TRIM): collation-defined ordering with trim insensitivity") {
      import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
      import org.apache.spark.unsafe.types.UTF8String

      val columnStats = new StringColumnStats(StringType("UTF8_BINARY_TRIM").collationId)
      val rows = Seq("b", "a", "C", " a ").map(str => {
        val row = new GenericInternalRow(1)
        row(0) = UTF8String.fromString(str)
        row
      })
      rows.foreach(columnStats.gatherStats(_, 0))

      val stats = columnStats.collectedStatistics
      assertResult(UTF8String.fromString("C"), "Wrong lower bound")(stats(0))
      assertResult(UTF8String.fromString("b"), "Wrong upper bound")(stats(1))
    }
  }
}
