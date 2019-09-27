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

package org.apache.spark.sql.avro

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.Decimal


/**
* A base interface for updating values inside catalyst data structure like `InternalRow` and
* `ArrayData`.
*/
sealed trait CatalystDataUpdater {
  def set(ordinal: Int, value: Any): Unit

  def setNullAt(ordinal: Int): Unit = set(ordinal, null)
  def setBoolean(ordinal: Int, value: Boolean): Unit = set(ordinal, value)
  def setByte(ordinal: Int, value: Byte): Unit = set(ordinal, value)
  def setShort(ordinal: Int, value: Short): Unit = set(ordinal, value)
  def setInt(ordinal: Int, value: Int): Unit = set(ordinal, value)
  def setLong(ordinal: Int, value: Long): Unit = set(ordinal, value)
  def setDouble(ordinal: Int, value: Double): Unit = set(ordinal, value)
  def setFloat(ordinal: Int, value: Float): Unit = set(ordinal, value)
  def setDecimal(ordinal: Int, value: Decimal): Unit = set(ordinal, value)
}

final class RowUpdater(row: InternalRow) extends CatalystDataUpdater {
  override def set(ordinal: Int, value: Any): Unit = row.update(ordinal, value)

  override def setNullAt(ordinal: Int): Unit = row.setNullAt(ordinal)
  override def setBoolean(ordinal: Int, value: Boolean): Unit = row.setBoolean(ordinal, value)
  override def setByte(ordinal: Int, value: Byte): Unit = row.setByte(ordinal, value)
  override def setShort(ordinal: Int, value: Short): Unit = row.setShort(ordinal, value)
  override def setInt(ordinal: Int, value: Int): Unit = row.setInt(ordinal, value)
  override def setLong(ordinal: Int, value: Long): Unit = row.setLong(ordinal, value)
  override def setDouble(ordinal: Int, value: Double): Unit = row.setDouble(ordinal, value)
  override def setFloat(ordinal: Int, value: Float): Unit = row.setFloat(ordinal, value)
  override def setDecimal(ordinal: Int, value: Decimal): Unit =
    row.setDecimal(ordinal, value, value.precision)
}

final class ArrayDataUpdater(array: ArrayData) extends CatalystDataUpdater {
  override def set(ordinal: Int, value: Any): Unit = array.update(ordinal, value)

  override def setNullAt(ordinal: Int): Unit = array.setNullAt(ordinal)
  override def setBoolean(ordinal: Int, value: Boolean): Unit = array.setBoolean(ordinal, value)
  override def setByte(ordinal: Int, value: Byte): Unit = array.setByte(ordinal, value)
  override def setShort(ordinal: Int, value: Short): Unit = array.setShort(ordinal, value)
  override def setInt(ordinal: Int, value: Int): Unit = array.setInt(ordinal, value)
  override def setLong(ordinal: Int, value: Long): Unit = array.setLong(ordinal, value)
  override def setDouble(ordinal: Int, value: Double): Unit = array.setDouble(ordinal, value)
  override def setFloat(ordinal: Int, value: Float): Unit = array.setFloat(ordinal, value)
  override def setDecimal(ordinal: Int, value: Decimal): Unit = array.update(ordinal, value)
}
