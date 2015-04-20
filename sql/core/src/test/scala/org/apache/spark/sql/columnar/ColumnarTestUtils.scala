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

import java.sql.Timestamp

import scala.collection.immutable.HashSet
import scala.util.Random

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.{UTF8String, DataType, Decimal, NativeType}

object ColumnarTestUtils {
  def makeNullRow(length: Int): GenericMutableRow = {
    val row = new GenericMutableRow(length)
    (0 until length).foreach(row.setNullAt)
    row
  }

  def makeRandomValue[T <: DataType, JvmType](columnType: ColumnType[T, JvmType]): JvmType = {
    def randomBytes(length: Int) = {
      val bytes = new Array[Byte](length)
      Random.nextBytes(bytes)
      bytes
    }

    (columnType match {
      case BYTE => (Random.nextInt(Byte.MaxValue * 2) - Byte.MaxValue).toByte
      case SHORT => (Random.nextInt(Short.MaxValue * 2) - Short.MaxValue).toShort
      case INT => Random.nextInt()
      case LONG => Random.nextLong()
      case FLOAT => Random.nextFloat()
      case DOUBLE => Random.nextDouble()
      case FIXED_DECIMAL(precision, scale) => Decimal(Random.nextLong() % 100, precision, scale)
      case STRING => UTF8String(Random.nextString(Random.nextInt(32)))
      case BOOLEAN => Random.nextBoolean()
      case BINARY => randomBytes(Random.nextInt(32))
      case DATE => Random.nextInt()
      case TIMESTAMP =>
        val timestamp = new Timestamp(Random.nextLong())
        timestamp.setNanos(Random.nextInt(999999999))
        timestamp
      case _ =>
        // Using a random one-element map instead of an arbitrary object
        Map(Random.nextInt() -> Random.nextString(Random.nextInt(32)))
    }).asInstanceOf[JvmType]
  }

  def makeRandomValues(
      head: ColumnType[_ <: DataType, _],
      tail: ColumnType[_ <: DataType, _]*): Seq[Any] = makeRandomValues(Seq(head) ++ tail)

  def makeRandomValues(columnTypes: Seq[ColumnType[_ <: DataType, _]]): Seq[Any] = {
    columnTypes.map(makeRandomValue(_))
  }

  def makeUniqueRandomValues[T <: DataType, JvmType](
      columnType: ColumnType[T, JvmType],
      count: Int): Seq[JvmType] = {

    Iterator.iterate(HashSet.empty[JvmType]) { set =>
      set + Iterator.continually(makeRandomValue(columnType)).filterNot(set.contains).next()
    }.drop(count).next().toSeq
  }

  def makeRandomRow(
      head: ColumnType[_ <: DataType, _],
      tail: ColumnType[_ <: DataType, _]*): Row = makeRandomRow(Seq(head) ++ tail)

  def makeRandomRow(columnTypes: Seq[ColumnType[_ <: DataType, _]]): Row = {
    val row = new GenericMutableRow(columnTypes.length)
    makeRandomValues(columnTypes).zipWithIndex.foreach { case (value, index) =>
      row(index) = value
    }
    row
  }

  def makeUniqueValuesAndSingleValueRows[T <: NativeType](
      columnType: NativeColumnType[T],
      count: Int): (Seq[T#JvmType], Seq[GenericMutableRow]) = {

    val values = makeUniqueRandomValues(columnType, count)
    val rows = values.map { value =>
      val row = new GenericMutableRow(1)
      row(0) = value
      row
    }

    (values, rows)
  }
}
