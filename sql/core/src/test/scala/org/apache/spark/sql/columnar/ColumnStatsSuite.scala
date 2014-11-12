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

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types._

class ColumnStatsSuite extends FunSuite {
  testColumnStats(classOf[ByteColumnStats], BYTE, Row(Byte.MaxValue, Byte.MinValue, 0))
  testColumnStats(classOf[ShortColumnStats], SHORT, Row(Short.MaxValue, Short.MinValue, 0))
  testColumnStats(classOf[IntColumnStats], INT, Row(Int.MaxValue, Int.MinValue, 0))
  testColumnStats(classOf[LongColumnStats], LONG, Row(Long.MaxValue, Long.MinValue, 0))
  testColumnStats(classOf[FloatColumnStats], FLOAT, Row(Float.MaxValue, Float.MinValue, 0))
  testColumnStats(classOf[DoubleColumnStats], DOUBLE, Row(Double.MaxValue, Double.MinValue, 0))
  testColumnStats(classOf[StringColumnStats], STRING, Row(null, null, 0))
  testColumnStats(classOf[DateColumnStats], DATE, Row(null, null, 0))
  testColumnStats(classOf[TimestampColumnStats], TIMESTAMP, Row(null, null, 0))

  def testColumnStats[T <: NativeType, U <: ColumnStats](
      columnStatsClass: Class[U],
      columnType: NativeColumnType[T],
      initialStatistics: Row): Unit = {

    val columnStatsName = columnStatsClass.getSimpleName

    test(s"$columnStatsName: empty") {
      val columnStats = columnStatsClass.newInstance()
      columnStats.collectedStatistics.zip(initialStatistics).foreach { case (actual, expected) =>
        assert(actual === expected)
      }
    }

    test(s"$columnStatsName: non-empty") {
      import ColumnarTestUtils._

      val columnStats = columnStatsClass.newInstance()
      val rows = Seq.fill(10)(makeRandomRow(columnType)) ++ Seq.fill(10)(makeNullRow(1))
      rows.foreach(columnStats.gatherStats(_, 0))

      val values = rows.take(10).map(_.head.asInstanceOf[T#JvmType])
      val ordering = columnType.dataType.ordering.asInstanceOf[Ordering[T#JvmType]]
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
