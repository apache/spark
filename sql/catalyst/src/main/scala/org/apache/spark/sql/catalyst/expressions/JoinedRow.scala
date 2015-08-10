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
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}


/**
 * A mutable wrapper that makes two rows appear as a single concatenated row.  Designed to
 * be instantiated once per thread and reused.
 */
class JoinedRow extends InternalRow {
  private[this] var numLeftFields = 0
  private[this] val rows = new Array[InternalRow](2)

  /**
   * Determine the index of the row for for a given ordinal:
   * 1. Subtract the number of fields in the left row from the ordinal; if this result is negative
   *    the ordinal maps the left row, otherwise it maps to the right row.
   * 2. Isolate the sign bit of this result by means of a bitwise 'and' and a right shift:
   *    1 = left row, 0 = right row.
   * 3. Flip the sign bit in order to get the proper index.
   *
   * @param i ordinal to determine the row index for.
   * @return the row index.
   */
  @inline
  private[this] def row(i: Int) = ((i - numLeftFields) >>> 31) ^ 1

  @inline
  private[this] def ordinal(i: Int, row: Int) = i - row * numLeftFields

  def this(left: InternalRow, right: InternalRow) = {
    this()
    rows(0) = left
    rows(1) = right
    numLeftFields = left.numFields
  }

  /** Updates this JoinedRow to used point at two new base rows.  Returns itself. */
  def apply(r1: InternalRow, r2: InternalRow): InternalRow = {
    rows(0) = r1
    rows(1) = r2
    numLeftFields = r1.numFields
    this
  }

  /** Updates this JoinedRow by updating its left base row.  Returns itself. */
  def withLeft(newLeft: InternalRow): InternalRow = {
    rows(0) = newLeft
    numLeftFields = newLeft.numFields
    this
  }

  /** Updates this JoinedRow by updating its right base row.  Returns itself. */
  def withRight(newRight: InternalRow): InternalRow = {
    rows(1) = newRight
    this
  }

  // TODO move left & right to Internal row?
  def left: InternalRow = rows(0)

  def right: InternalRow = rows(1)

  override def toSeq(fieldTypes: Seq[DataType]): Seq[Any] = {
    val row1 = rows(0)
    val row2 = rows(1)
    assert(fieldTypes.length == row1.numFields + row2.numFields)
    val (left, right) = fieldTypes.splitAt(row1.numFields)
    row1.toSeq(left) ++ row2.toSeq(right)
  }

  override def numFields: Int = rows(0).numFields + rows(1).numFields

  override def get(i: Int, dt: DataType): AnyRef = {
    val r = row(i)
    rows(r).get(ordinal(i, r), dt)
  }

  override def isNullAt(i: Int): Boolean = {
    val r = row(i)
    rows(r).isNullAt(ordinal(i, r))
  }

  override def getBoolean(i: Int): Boolean = {
    val r = row(i)
    rows(r).getBoolean(ordinal(i, r))
  }

  override def getByte(i: Int): Byte = {
    val r = row(i)
    rows(r).getByte(ordinal(i, r))
  }

  override def getShort(i: Int): Short = {
    val r = row(i)
    rows(r).getShort(ordinal(i, r))
  }

  override def getInt(i: Int): Int = {
    val r = row(i)
    rows(r).getInt(ordinal(i, r))
  }

  override def getLong(i: Int): Long = {
    val r = row(i)
    rows(r).getLong(ordinal(i, r))
  }

  override def getFloat(i: Int): Float = {
    val r = row(i)
    rows(r).getFloat(ordinal(i, r))
  }

  override def getDouble(i: Int): Double = {
    val r = row(i)
    rows(r).getDouble(ordinal(i, r))
  }

  override def getDecimal(i: Int, precision: Int, scale: Int): Decimal = {
    val r = row(i)
    rows(r).getDecimal(ordinal(i, r), precision, scale)
  }

  override def getUTF8String(i: Int): UTF8String = {
    val r = row(i)
    rows(r).getUTF8String(ordinal(i, r))
  }

  override def getBinary(i: Int): Array[Byte] = {
    val r = row(i)
    rows(r).getBinary(ordinal(i, r))
  }

  override def getArray(i: Int): ArrayData = {
    val r = row(i)
    rows(r).getArray(ordinal(i, r))
  }

  override def getInterval(i: Int): CalendarInterval = {
    val r = row(i)
    rows(r).getInterval(ordinal(i, r))
  }

  override def getMap(i: Int): MapData = {
    val r = row(i)
    rows(r).getMap(ordinal(i, r))
  }

  override def getStruct(i: Int, numFields: Int): InternalRow = {
    val r = row(i)
    rows(r).getStruct(ordinal(i, r), numFields)
  }

  override def anyNull: Boolean = rows(0).anyNull || rows(1).anyNull

  override def copy(): InternalRow = {
    val copy1 = rows(0).copy()
    val copy2 = rows(1).copy()
    new JoinedRow(copy1, copy2)
  }

  override def toString: String = {
    val row1 = rows(0)
    val row2 = rows(1)
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
