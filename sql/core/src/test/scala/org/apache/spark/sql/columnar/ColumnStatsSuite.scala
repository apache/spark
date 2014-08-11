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

import org.apache.spark.sql.catalyst.types._

class ColumnStatsSuite extends FunSuite {
  testColumnStats(classOf[BooleanColumnStats],   BOOLEAN)
  testColumnStats(classOf[ByteColumnStats],      BYTE)
  testColumnStats(classOf[ShortColumnStats],     SHORT)
  testColumnStats(classOf[IntColumnStats],       INT)
  testColumnStats(classOf[LongColumnStats],      LONG)
  testColumnStats(classOf[FloatColumnStats],     FLOAT)
  testColumnStats(classOf[DoubleColumnStats],    DOUBLE)
  testColumnStats(classOf[StringColumnStats],    STRING)
  testColumnStats(classOf[TimestampColumnStats], TIMESTAMP)

  def testColumnStats[T <: NativeType, U <: NativeColumnStats[T]](
      columnStatsClass: Class[U],
      columnType: NativeColumnType[T]) {

    val columnStatsName = columnStatsClass.getSimpleName

    test(s"$columnStatsName: empty") {
      val columnStats = columnStatsClass.newInstance()
      assertResult(columnStats.initialBounds, "Wrong initial bounds") {
        (columnStats.lowerBound, columnStats.upperBound)
      }
    }

    test(s"$columnStatsName: non-empty") {
      import ColumnarTestUtils._

      val columnStats = columnStatsClass.newInstance()
      val rows = Seq.fill(10)(makeRandomRow(columnType))
      rows.foreach(columnStats.gatherStats(_, 0))

      val values = rows.map(_.head.asInstanceOf[T#JvmType])
      val ordering = columnType.dataType.ordering.asInstanceOf[Ordering[T#JvmType]]

      assertResult(values.min(ordering), "Wrong lower bound")(columnStats.lowerBound)
      assertResult(values.max(ordering), "Wrong upper bound")(columnStats.upperBound)
    }
  }
}
