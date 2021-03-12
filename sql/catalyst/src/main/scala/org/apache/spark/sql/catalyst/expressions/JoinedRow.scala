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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * A mutable wrapper that makes two rows appear as a single concatenated row.  Designed to
 * be instantiated once per thread and reused.
 */
class JoinedRow extends InternalRow {
  private[this] var row1: InternalRow = _
  private[this] var row2: InternalRow = _

  def this(left: InternalRow, right: InternalRow) = {
    this()
    row1 = left
    row2 = right
  }

  /** Updates this JoinedRow to used point at two new base rows.  Returns itself. */
  def apply(r1: InternalRow, r2: InternalRow): JoinedRow = {
    row1 = r1
    row2 = r2
    this
  }

  /** Updates this JoinedRow by updating its left base row.  Returns itself. */
  def withLeft(newLeft: InternalRow): JoinedRow = {
    row1 = newLeft
    this
  }

  /** Updates this JoinedRow by updating its right base row.  Returns itself. */
  def withRight(newRight: InternalRow): JoinedRow = {
    row2 = newRight
    this
  }

  override def toSeq(fieldTypes: Seq[DataType]): Seq[Any] = {
    assert(fieldTypes.length == row1.numFields + row2.numFields)
    val (left, right) = fieldTypes.splitAt(row1.numFields)
    row1.toSeq(left) ++ row2.toSeq(right)
  }

  override def numFields: Int = row1.numFields + row2.numFields

  override def get(i: Int, dt: DataType): AnyRef =
    if (i < row1.numFields) row1.get(i, dt) else row2.get(i - row1.numFields, dt)

  override def isNullAt(i: Int): Boolean =
    if (i < row1.numFields) row1.isNullAt(i) else row2.isNullAt(i - row1.numFields)

  override def getBoolean(i: Int): Boolean =
    if (i < row1.numFields) row1.getBoolean(i) else row2.getBoolean(i - row1.numFields)

  override def getByte(i: Int): Byte =
    if (i < row1.numFields) row1.getByte(i) else row2.getByte(i - row1.numFields)

  override def getShort(i: Int): Short =
    if (i < row1.numFields) row1.getShort(i) else row2.getShort(i - row1.numFields)

  override def getInt(i: Int): Int =
    if (i < row1.numFields) row1.getInt(i) else row2.getInt(i - row1.numFields)

  override def getLong(i: Int): Long =
    if (i < row1.numFields) row1.getLong(i) else row2.getLong(i - row1.numFields)

  override def getFloat(i: Int): Float =
    if (i < row1.numFields) row1.getFloat(i) else row2.getFloat(i - row1.numFields)

  override def getDouble(i: Int): Double =
    if (i < row1.numFields) row1.getDouble(i) else row2.getDouble(i - row1.numFields)

  override def getDecimal(i: Int, precision: Int, scale: Int): Decimal = {
    if (i < row1.numFields) {
      row1.getDecimal(i, precision, scale)
    } else {
      row2.getDecimal(i - row1.numFields, precision, scale)
    }
  }

  override def getUTF8String(i: Int): UTF8String =
    if (i < row1.numFields) row1.getUTF8String(i) else row2.getUTF8String(i - row1.numFields)

  override def getBinary(i: Int): Array[Byte] =
    if (i < row1.numFields) row1.getBinary(i) else row2.getBinary(i - row1.numFields)

  override def getArray(i: Int): ArrayData =
    if (i < row1.numFields) row1.getArray(i) else row2.getArray(i - row1.numFields)

  override def getInterval(i: Int): CalendarInterval =
    if (i < row1.numFields) row1.getInterval(i) else row2.getInterval(i - row1.numFields)

  override def getMap(i: Int): MapData =
    if (i < row1.numFields) row1.getMap(i) else row2.getMap(i - row1.numFields)

  override def getStruct(i: Int, numFields: Int): InternalRow = {
    if (i < row1.numFields) {
      row1.getStruct(i, numFields)
    } else {
      row2.getStruct(i - row1.numFields, numFields)
    }
  }

  override def anyNull: Boolean = row1.anyNull || row2.anyNull

  override def setNullAt(i: Int): Unit = {
    if (i < row1.numFields) {
      row1.setNullAt(i)
    } else {
      row2.setNullAt(i - row1.numFields)
    }
  }

  override def update(i: Int, value: Any): Unit = {
    if (i < row1.numFields) {
      row1.update(i, value)
    } else {
      row2.update(i - row1.numFields, value)
    }
  }

  override def copy(): InternalRow = {
    val copy1 = row1.copy()
    val copy2 = row2.copy()
    new JoinedRow(copy1, copy2)
  }

  override def toString: String = {
    // Make sure toString never throws NullPointerException.
    if ((row1 eq null) && (row2 eq null)) {
      "[ empty row ]"
    } else if (row1 eq null) {
      row2.toString
    } else if (row2 eq null) {
      row1.toString
    } else {
      s"{${row1.toString} + ${row2.toString}}"
    }
  }
}
