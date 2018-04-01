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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

class ColumnStatsSuite extends SparkFunSuite {
  testColumnStats(classOf[BooleanColumnStats], BOOLEAN, Array(true, false, 0, 0, 0))
  testColumnStats(classOf[ByteColumnStats], BYTE, Array(Byte.MaxValue, Byte.MinValue, 0, 0, 0))
  testColumnStats(classOf[ShortColumnStats], SHORT, Array(Short.MaxValue, Short.MinValue, 0, 0, 0))
  testColumnStats(classOf[IntColumnStats], INT, Array(Int.MaxValue, Int.MinValue, 0, 0, 0))
  testColumnStats(classOf[LongColumnStats], LONG, Array(Long.MaxValue, Long.MinValue, 0, 0, 0))
  testColumnStats(classOf[FloatColumnStats], FLOAT, Array(Float.MaxValue, Float.MinValue, 0, 0, 0))
  testColumnStats(
    classOf[DoubleColumnStats], DOUBLE,
    Array(Double.MaxValue, Double.MinValue, 0, 0, 0)
  )
  testColumnStats(classOf[StringColumnStats], STRING, Array(null, null, 0, 0, 0))
  testDecimalColumnStats(Array(null, null, 0, 0, 0))

  private val orderableArrayDataType = ArrayType(IntegerType)
  testOrderableColumnStats(
    orderableArrayDataType,
    () => new ArrayColumnStats(orderableArrayDataType),
    ARRAY(orderableArrayDataType),
    orderable = true,
    Array(null, null, 0, 0, 0)
  )

  private val unorderableArrayDataType = ArrayType(MapType(IntegerType, StringType))
  testOrderableColumnStats(
    unorderableArrayDataType,
    () => new ArrayColumnStats(unorderableArrayDataType),
    ARRAY(unorderableArrayDataType),
    orderable = false,
    Array(null, null, 0, 0, 0)
  )

  private val structDataType = StructType(Array(StructField("test", DataTypes.StringType)))
  testOrderableColumnStats(
    structDataType,
    () => new StructColumnStats(structDataType),
    STRUCT(structDataType),
    orderable = true,
    Array(null, null, 0, 0, 0)
  )
  testMapColumnStats(
    MapType(IntegerType, StringType),
    Array(null, null, 0, 0, 0)
  )


  def testColumnStats[T <: AtomicType, U <: ColumnStats](
      columnStatsClass: Class[U],
      columnType: NativeColumnType[T],
      initialStatistics: Array[Any]): Unit = {

    val columnStatsName = columnStatsClass.getSimpleName

    test(s"$columnStatsName: empty") {
      val columnStats = columnStatsClass.newInstance()
      columnStats.collectedStatistics.zip(initialStatistics).foreach {
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

  def testDecimalColumnStats[T <: AtomicType, U <: ColumnStats](
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

      val values = rows.take(10).map(_.get(0, columnType.dataType).asInstanceOf[T#InternalType])
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

  def testOrderableColumnStats[T](
      dataType: DataType,
      statsSupplier: () => OrderableSafeColumnStats[T],
      columnType: ColumnType[T],
      orderable: Boolean,
      initialStatistics: Array[Any]): Unit = {

    test(s"${dataType.typeName}, $orderable: empty") {
      val objectStats = statsSupplier()
      objectStats.collectedStatistics.zip(initialStatistics).foreach {
        case (actual, expected) => assert(actual === expected)
      }
    }

    test(s"${dataType.typeName}, $orderable: non-empty") {
      import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._
      val objectStats = statsSupplier()
      val rows = Seq.fill(10)(makeRandomRow(columnType)) ++ Seq.fill(10)(makeNullRow(1))
      rows.foreach(objectStats.gatherStats(_, 0))

      val stats = objectStats.collectedStatistics
      if (orderable) {
        val values = rows.take(10).map(_.get(0, columnType.dataType))
        val ordering = TypeUtils.getInterpretedOrdering(dataType)

        assertResult(values.min(ordering), "Wrong lower bound")(stats(0))
        assertResult(values.max(ordering), "Wrong upper bound")(stats(1))
      } else {
        assertResult(null, "Wrong lower bound")(stats(0))
        assertResult(null, "Wrong upper bound")(stats(1))
      }
      assertResult(10, "Wrong null count")(stats(2))
      assertResult(20, "Wrong row count")(stats(3))
      assertResult(stats(4), "Wrong size in bytes") {
        rows.map { row =>
          if (row.isNullAt(0)) 4 else columnType.actualSize(row, 0)
        }.sum
      }
    }
  }

  def testMapColumnStats(dataType: MapType, initialStatistics: Array[Any]): Unit = {
    val columnType = ColumnType(dataType)

    test(s"${dataType.typeName}: empty") {
      val objectStats = new MapColumnStats(dataType)
      objectStats.collectedStatistics.zip(initialStatistics).foreach {
        case (actual, expected) => assert(actual === expected)
      }
    }

    test(s"${dataType.typeName}: non-empty") {
      import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._
      val objectStats = new MapColumnStats(dataType)
      val rows = Seq.fill(10)(makeRandomRow(columnType)) ++ Seq.fill(10)(makeNullRow(1))
      rows.foreach(objectStats.gatherStats(_, 0))

      val stats = objectStats.collectedStatistics
      assertResult(null, "Wrong lower bound")(stats(0))
      assertResult(null, "Wrong upper bound")(stats(1))
      assertResult(10, "Wrong null count")(stats(2))
      assertResult(20, "Wrong row count")(stats(3))
      assertResult(stats(4), "Wrong size in bytes") {
        rows.map { row =>
          if (row.isNullAt(0)) 4 else columnType.actualSize(row, 0)
        }.sum
      }
    }
  }

  test("Reuse UnsafeArrayData for stats") {
    val stats = new ArrayColumnStats(ArrayType(IntegerType))
    val unsafeData = UnsafeArrayData.fromPrimitiveArray(Array(1))
    (1 to 10).foreach { value =>
      val row = new GenericInternalRow(Array[Any](unsafeData))
      unsafeData.setInt(0, value)
      stats.gatherStats(row, 0)
    }
    val collected = stats.collectedStatistics
    assertResult(UnsafeArrayData.fromPrimitiveArray(Array(1)))(collected(0))
    assertResult(UnsafeArrayData.fromPrimitiveArray(Array(10)))(collected(1))
    assertResult(0)(collected(2))
    assertResult(10)(collected(3))
    assertResult(10 * (4 + unsafeData.getSizeInBytes))(collected(4))
  }

  test("Reuse UnsafeRow for stats") {
    val structType = StructType(Array(StructField("int", IntegerType)))
    val stats = new StructColumnStats(structType)
    val converter = UnsafeProjection.create(structType)
    val unsafeData = converter(InternalRow(1))
    (1 to 10).foreach { value =>
      val row = new GenericInternalRow(Array[Any](unsafeData))
      unsafeData.setInt(0, value)
      stats.gatherStats(row, 0)
    }
    val collected = stats.collectedStatistics
    assertResult(converter(InternalRow(1)))(collected(0))
    assertResult(converter(InternalRow(10)))(collected(1))
    assertResult(0)(collected(2))
    assertResult(10)(collected(3))
    assertResult(10 * (4 + unsafeData.getSizeInBytes))(collected(4))
  }
}
