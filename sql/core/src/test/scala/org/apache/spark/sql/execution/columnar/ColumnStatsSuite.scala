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
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._

class ColumnStatsSuite extends SparkFunSuite {
  testColumnStats(classOf[BooleanColumnStats], BOOLEAN, createRow(true, false, 0))
  testColumnStats(classOf[ByteColumnStats], BYTE, createRow(Byte.MaxValue, Byte.MinValue, 0))
  testColumnStats(classOf[ShortColumnStats], SHORT, createRow(Short.MaxValue, Short.MinValue, 0))
  testColumnStats(classOf[IntColumnStats], INT, createRow(Int.MaxValue, Int.MinValue, 0))
  testColumnStats(classOf[LongColumnStats], LONG, createRow(Long.MaxValue, Long.MinValue, 0))
  testColumnStats(classOf[FloatColumnStats], FLOAT, createRow(Float.MaxValue, Float.MinValue, 0))
  testColumnStats(classOf[DoubleColumnStats], DOUBLE,
    createRow(Double.MaxValue, Double.MinValue, 0))
  testColumnStats(classOf[StringColumnStats], STRING, createRow(null, null, 0))
  testDecimalColumnStats(createRow(null, null, 0))

  def createRow(values: Any*): GenericInternalRow = new GenericInternalRow(values.toArray)

  def testColumnStats[T <: AtomicType, U <: ColumnStats](
      columnStatsClass: Class[U],
      columnType: NativeColumnType[T],
      initialStatistics: GenericInternalRow): Unit = {

    val columnStatsName = columnStatsClass.getSimpleName

    test(s"$columnStatsName: empty") {
      val columnStats = columnStatsClass.newInstance()
      columnStats.collectedStatistics.values.zip(initialStatistics.values).foreach {
        case (actual, expected) => assert(actual === expected)
      }
    }

    test(s"$columnStatsName: non-empty") {
      import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._

      val columnStats = columnStatsClass.newInstance()
      val rows = Seq.fill(10)(makeRandomRow(columnType)) ++ Seq.fill(10)(makeNullRow(1))
      rows.foreach(columnStats.gatherStats(_, 0))

      val values = rows.take(10).map(_.get(0, columnType.dataType).asInstanceOf[T#InternalType])
      val ordering = columnType.dataType.ordering.asInstanceOf[Ordering[T#InternalType]]
      val stats = columnStats.collectedStatistics

      assertResult(values.min(ordering), "Wrong lower bound")(stats.values(0))
      assertResult(values.max(ordering), "Wrong upper bound")(stats.values(1))
      assertResult(10, "Wrong null count")(stats.values(2))
      assertResult(20, "Wrong row count")(stats.values(3))
      assertResult(stats.values(4), "Wrong size in bytes") {
        rows.map { row =>
          if (row.isNullAt(0)) 4 else columnType.actualSize(row, 0)
        }.sum
      }
    }
  }

  def testDecimalColumnStats[T <: AtomicType, U <: ColumnStats](
      initialStatistics: GenericInternalRow): Unit = {

    val columnStatsName = classOf[DecimalColumnStats].getSimpleName
    val columnType = COMPACT_DECIMAL(15, 10)

    test(s"$columnStatsName: empty") {
      val columnStats = new DecimalColumnStats(15, 10)
      columnStats.collectedStatistics.values.zip(initialStatistics.values).foreach {
        case (actual, expected) => assert(actual === expected)
      }
    }

    test(s"$columnStatsName: non-empty") {
      import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._

      val columnStats = new DecimalColumnStats(15, 10)
      val rows = Seq.fill(10)(makeRandomRow(columnType)) ++ Seq.fill(10)(makeNullRow(1))
      rows.foreach(columnStats.gatherStats(_, 0))

      val values = rows.take(10).map(_.get(0, columnType.dataType).asInstanceOf[T#InternalType])
      val ordering = columnType.dataType.ordering.asInstanceOf[Ordering[T#InternalType]]
      val stats = columnStats.collectedStatistics

      assertResult(values.min(ordering), "Wrong lower bound")(stats.values(0))
      assertResult(values.max(ordering), "Wrong upper bound")(stats.values(1))
      assertResult(10, "Wrong null count")(stats.values(2))
      assertResult(20, "Wrong row count")(stats.values(3))
      assertResult(stats.values(4), "Wrong size in bytes") {
        rows.map { row =>
          if (row.isNullAt(0)) 4 else columnType.actualSize(row, 0)
        }.sum
      }
    }
  }
}
