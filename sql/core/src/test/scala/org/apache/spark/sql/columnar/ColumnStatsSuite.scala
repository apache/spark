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

package org.apache.spark.sql.columnar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.InternalRow
import org.apache.spark.sql.types._

class ColumnStatsSuite extends SparkFunSuite {
  testColumnStats(classOf[ByteColumnStats], BYTE, InternalRow(Byte.MaxValue, Byte.MinValue, 0))
  testColumnStats(classOf[ShortColumnStats], SHORT, InternalRow(Short.MaxValue, Short.MinValue, 0))
  testColumnStats(classOf[IntColumnStats], INT, InternalRow(Int.MaxValue, Int.MinValue, 0))
  testColumnStats(classOf[LongColumnStats], LONG, InternalRow(Long.MaxValue, Long.MinValue, 0))
  testColumnStats(classOf[FloatColumnStats], FLOAT, InternalRow(Float.MaxValue, Float.MinValue, 0))
  testColumnStats(classOf[DoubleColumnStats], DOUBLE,
    InternalRow(Double.MaxValue, Double.MinValue, 0))
  testColumnStats(classOf[FixedDecimalColumnStats], FIXED_DECIMAL(15, 10),
    InternalRow(null, null, 0))
  testColumnStats(classOf[StringColumnStats], STRING, InternalRow(null, null, 0))
  testColumnStats(classOf[DateColumnStats], DATE, InternalRow(Int.MaxValue, Int.MinValue, 0))
  testColumnStats(classOf[TimestampColumnStats], TIMESTAMP, InternalRow(null, null, 0))

  def testColumnStats[T <: AtomicType, U <: ColumnStats](
      columnStatsClass: Class[U],
      columnType: NativeColumnType[T],
      initialStatistics: InternalRow): Unit = {

    val columnStatsName = columnStatsClass.getSimpleName

    test(s"$columnStatsName: empty") {
      val columnStats = columnStatsClass.newInstance()
      columnStats.collectedStatistics.toSeq.zip(initialStatistics.toSeq).foreach {
        case (actual, expected) => assert(actual === expected)
      }
    }

    test(s"$columnStatsName: non-empty") {
      import ColumnarTestUtils._

      val columnStats = columnStatsClass.newInstance()
      val rows = Seq.fill(10)(makeRandomRow(columnType)) ++ Seq.fill(10)(makeNullRow(1))
      rows.foreach(columnStats.gatherStats(_, 0))

      val values = rows.take(10).map(_(0).asInstanceOf[T#InternalType])
      val ordering = columnType.dataType.ordering.asInstanceOf[Ordering[T#InternalType]]
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
}
