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
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * A sliced row provides a view of the underlying row, and forwards all read/write requests to the
 * underlying row, after shifting the index by offset.
 *
 * Note that, the underlying row field in this class is mutable, so that we can reuse the same
 * instance of this class while processing many rows.
 */
trait BaseSlicedInternalRow extends InternalRow {
  protected def underlyingRow: InternalRow
  protected def offset: Int

  protected def underlyingIndexOf(index: Int): Int = {
    assert(index >= 0, "index (" + index + ") should >= 0")
    assert(index < numFields, "index (" + index + ") should < " + numFields)
    index + offset
  }

  override def isNullAt(i: Int): Boolean =
    underlyingRow.isNullAt(underlyingIndexOf(i))

  override def getBoolean(i: Int): Boolean =
    underlyingRow.getBoolean(underlyingIndexOf(i))

  override def getByte(i: Int): Byte =
    underlyingRow.getByte(underlyingIndexOf(i))

  override def getShort(i: Int): Short =
    underlyingRow.getShort(underlyingIndexOf(i))

  override def getInt(i: Int): Int =
    underlyingRow.getInt(underlyingIndexOf(i))

  override def getLong(i: Int): Long =
    underlyingRow.getLong(underlyingIndexOf(i))

  override def getFloat(i: Int): Float =
    underlyingRow.getFloat(underlyingIndexOf(i))

  override def getDouble(i: Int): Double =
    underlyingRow.getDouble(underlyingIndexOf((i)))

  override def getDecimal(i: Int, precision: Int, scale: Int): Decimal =
    underlyingRow.getDecimal(underlyingIndexOf(i), precision, scale)

  override def getUTF8String(i: Int): UTF8String =
    underlyingRow.getUTF8String(underlyingIndexOf(i))

  override def getBinary(i: Int): Array[Byte] =
    underlyingRow.getBinary(underlyingIndexOf(i))

  override def getInterval(i: Int): CalendarInterval =
    underlyingRow.getInterval(underlyingIndexOf(i))

  override def getStruct(i: Int, numFields: Int): InternalRow =
    underlyingRow.getStruct(underlyingIndexOf(i), numFields)

  override def getArray(i: Int): ArrayData =
    underlyingRow.getArray(underlyingIndexOf(i))

  override def getMap(i: Int): MapData =
    underlyingRow.getMap(underlyingIndexOf(i))

  override def get(i: Int, dataType: DataType): AnyRef =
    underlyingRow.get(underlyingIndexOf(i), dataType)

  override def anyNull: Boolean = {
    val len = offset + numFields
    var i = offset
    while (i < len) {
      if (isNullAt(i)) { return true }
      i += 1
    }
    false
  }

  override def copy(): InternalRow = {
    throw new UnsupportedOperationException("Cannot copy a SlicedInternalRow")
  }
}

class SlicedInternalRow(protected val offset: Int, val numFields: Int)
  extends BaseSlicedInternalRow {

  private var _baseRow: InternalRow = _
  def target(row: InternalRow): SlicedInternalRow = {
    _baseRow = row
    this
  }

  def underlyingRow: InternalRow = _baseRow
}

class SlicedMutableRow(protected val offset: Int, val numFields: Int)
  extends MutableRow with BaseSlicedInternalRow {

  private var _baseRow: MutableRow = _
  def target(row: MutableRow): SlicedMutableRow = {
    _baseRow = row
    this
  }

  def underlyingRow: InternalRow = _baseRow

  override def setNullAt(i: Int): Unit =
    _baseRow.setNullAt(underlyingIndexOf(i))

  override def update(i: Int, value: Any): Unit =
    _baseRow.update(underlyingIndexOf(i), value)

  override def setBoolean(i: Int, value: Boolean): Unit =
    _baseRow.setBoolean(underlyingIndexOf(i), value)

  override def setByte(i: Int, value: Byte): Unit =
    _baseRow.setByte(underlyingIndexOf(i), value)

  override def setShort(i: Int, value: Short): Unit =
    _baseRow.setShort(underlyingIndexOf(i), value)

  override def setInt(i: Int, value: Int): Unit =
    _baseRow.setInt(underlyingIndexOf(i), value)

  override def setLong(i: Int, value: Long): Unit =
    _baseRow.setLong(underlyingIndexOf(i), value)

  override def setFloat(i: Int, value: Float): Unit =
    _baseRow.setFloat(underlyingIndexOf(i), value)

  override def setDouble(i: Int, value: Double): Unit =
    _baseRow.setDouble(underlyingIndexOf(i), value)

  override def setDecimal(i: Int, value: Decimal, precision: Int): Unit =
    _baseRow.setDecimal(underlyingIndexOf(i), value, precision)
}
