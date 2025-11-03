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

import scala.collection.immutable.HashSet
import scala.util.Random

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

object ColumnarTestUtils {
  def makeNullRow(length: Int): GenericInternalRow = {
    val row = new GenericInternalRow(length)
    (0 until length).foreach(row.setNullAt)
    row
  }

  def makeRandomValue[JvmType](columnType: ColumnType[JvmType]): JvmType = {
    def randomBytes(length: Int) = {
      val bytes = new Array[Byte](length)
      Random.nextBytes(bytes)
      bytes
    }

    (columnType match {
      case NULL => null
      case BOOLEAN => Random.nextBoolean()
      case BYTE => (Random.nextInt(Byte.MaxValue * 2) - Byte.MaxValue).toByte
      case SHORT => (Random.nextInt(Short.MaxValue * 2) - Short.MaxValue).toShort
      case INT => Random.nextInt()
      case LONG => Random.nextLong()
      case FLOAT => Random.nextFloat()
      case DOUBLE => Random.nextDouble()
      case _: STRING => UTF8String.fromString(Random.nextString(Random.nextInt(32)))
      case BINARY => randomBytes(Random.nextInt(32))
      case CALENDAR_INTERVAL =>
        new CalendarInterval(Random.nextInt(), Random.nextInt(), Random.nextLong())
      case COMPACT_DECIMAL(precision, scale) => Decimal(Random.nextLong() % 100, precision, scale)
      case LARGE_DECIMAL(precision, scale) => Decimal(Random.nextLong(), precision, scale)
      case STRUCT(_) =>
        new GenericInternalRow(Array[Any](UTF8String.fromString(Random.nextString(10))))
      case ARRAY(_) =>
        new GenericArrayData(Array[Any](Random.nextInt(), Random.nextInt()))
      case MAP(_) =>
        ArrayBasedMapData(
          Map(Random.nextInt() -> UTF8String.fromString(Random.nextString(Random.nextInt(32)))))
      case _ => throw new IllegalArgumentException(s"Unknown column type $columnType")
    }).asInstanceOf[JvmType]
  }

  def makeRandomValues(
      head: ColumnType[_],
      tail: ColumnType[_]*): Seq[Any] = makeRandomValues(Seq(head) ++ tail)

  def makeRandomValues(columnTypes: Seq[ColumnType[_]]): Seq[Any] = {
    columnTypes.map(makeRandomValue(_))
  }

  def makeUniqueRandomValues[JvmType](
      columnType: ColumnType[JvmType],
      count: Int): Seq[JvmType] = {

    Iterator.iterate(HashSet.empty[JvmType]) { set =>
      set + Iterator.continually(makeRandomValue(columnType)).filterNot(set.contains).next()
    }.drop(count).next().toSeq
  }

  def makeRandomRow(
      head: ColumnType[_],
      tail: ColumnType[_]*): InternalRow = makeRandomRow(Seq(head) ++ tail)

  def makeRandomRow(columnTypes: Seq[ColumnType[_]]): InternalRow = {
    val row = new GenericInternalRow(columnTypes.length)
    makeRandomValues(columnTypes).zipWithIndex.foreach { case (value, index) =>
      row(index) = value
    }
    row
  }

  def makeUniqueValuesAndSingleValueRows[T <: PhysicalDataType](
      columnType: NativeColumnType[T],
      count: Int): (Seq[T#InternalType], Seq[GenericInternalRow]) = {

    val values = makeUniqueRandomValues(columnType, count)
    val rows = values.map { value =>
      val row = new GenericInternalRow(1)
      row(0) = value
      row
    }

    (values, rows)
  }
}
