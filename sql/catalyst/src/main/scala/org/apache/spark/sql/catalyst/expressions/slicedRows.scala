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
  def baseRow: InternalRow
  def offset: Int

  protected def toBaseIndex(index: Int): Int = {
    assert(index >= 0, "index (" + index + ") should >= 0")
    assert(index < numFields, "index (" + index + ") should < " + numFields)
    index + offset
  }

  override def isNullAt(i: Int): Boolean = baseRow.isNullAt(toBaseIndex(i))
  override def getBoolean(i: Int): Boolean = baseRow.getBoolean(toBaseIndex(i))
  override def getByte(i: Int): Byte = baseRow.getByte(toBaseIndex(i))
  override def getShort(i: Int): Short = baseRow.getShort(toBaseIndex(i))
  override def getInt(i: Int): Int = baseRow.getInt(toBaseIndex(i))
  override def getLong(i: Int): Long = baseRow.getLong(toBaseIndex(i))
  override def getFloat(i: Int): Float = baseRow.getFloat(toBaseIndex(i))
  override def getDouble(i: Int): Double = baseRow.getDouble(toBaseIndex((i)))
  override def getDecimal(i: Int, precision: Int, scale: Int): Decimal =
    baseRow.getDecimal(toBaseIndex(i), precision, scale)
  override def getUTF8String(i: Int): UTF8String = baseRow.getUTF8String(toBaseIndex(i))
  override def getBinary(i: Int): Array[Byte] = baseRow.getBinary(toBaseIndex(i))
  override def getInterval(i: Int): CalendarInterval = baseRow.getInterval(toBaseIndex(i))
  override def getStruct(i: Int, numFields: Int): InternalRow =
    baseRow.getStruct(toBaseIndex(i), numFields)
  override def getArray(i: Int): ArrayData = baseRow.getArray(toBaseIndex(i))
  override def getMap(i: Int): MapData = baseRow.getMap(toBaseIndex(i))
  override def get(i: Int, dataType: DataType): AnyRef = baseRow.get(toBaseIndex(i), dataType)

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
    throw new UnsupportedOperationException("Cannot copy a SlicedMutableRow")
  }
}

case class SlicedInternalRow(offset: Int, numFields: Int) extends BaseSlicedInternalRow {
  private var _baseRow: InternalRow = _
  def target(row: InternalRow): SlicedInternalRow = {
    _baseRow = row
    this
  }

  def baseRow: InternalRow = _baseRow
}

case class SlicedMutableRow(offset: Int, numFields: Int)
  extends MutableRow with BaseSlicedInternalRow {

  private var _baseRow: MutableRow = _
  def target(row: MutableRow): SlicedMutableRow = {
    _baseRow = row
    this
  }

  def baseRow: InternalRow = _baseRow

  override def setNullAt(i: Int): Unit = _baseRow.setNullAt(toBaseIndex(i))
  override def update(i: Int, value: Any): Unit = _baseRow.update(toBaseIndex(i), value)
  override def setBoolean(i: Int, value: Boolean): Unit = _baseRow.setBoolean(toBaseIndex(i), value)
  override def setByte(i: Int, value: Byte): Unit = _baseRow.setByte(toBaseIndex(i), value)
  override def setShort(i: Int, value: Short): Unit = _baseRow.setShort(toBaseIndex(i), value)
  override def setInt(i: Int, value: Int): Unit = _baseRow.setInt(toBaseIndex(i), value)
  override def setLong(i: Int, value: Long): Unit = _baseRow.setLong(toBaseIndex(i), value)
  override def setFloat(i: Int, value: Float): Unit = _baseRow.setFloat(toBaseIndex(i), value)
  override def setDouble(i: Int, value: Double): Unit = _baseRow.setDouble(toBaseIndex(i), value)
  override def setDecimal(i: Int, value: Decimal, precision: Int): Unit =
    _baseRow.setDecimal(toBaseIndex(i), value, precision)
}
