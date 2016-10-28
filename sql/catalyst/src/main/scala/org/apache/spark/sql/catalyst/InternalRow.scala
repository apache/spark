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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, Decimal, StructType}

/**
 * An abstract class for row used internal in Spark SQL, which only contain the columns as
 * internal types.
 */
abstract class InternalRow extends SpecializedGetters with Serializable {

  def numFields: Int

  // This is only use for test and will throw a null pointer exception if the position is null.
  def getString(ordinal: Int): String = getUTF8String(ordinal).toString

  def setNullAt(i: Int): Unit

  def update(i: Int, value: Any): Unit

  // default implementation (slow)
  def setBoolean(i: Int, value: Boolean): Unit = update(i, value)
  def setByte(i: Int, value: Byte): Unit = update(i, value)
  def setShort(i: Int, value: Short): Unit = update(i, value)
  def setInt(i: Int, value: Int): Unit = update(i, value)
  def setLong(i: Int, value: Long): Unit = update(i, value)
  def setFloat(i: Int, value: Float): Unit = update(i, value)
  def setDouble(i: Int, value: Double): Unit = update(i, value)

  /**
   * Update the decimal column at `i`.
   *
   * Note: In order to support update decimal with precision > 18 in UnsafeRow,
   * CAN NOT call setNullAt() for decimal column on UnsafeRow, call setDecimal(i, null, precision).
   */
  def setDecimal(i: Int, value: Decimal, precision: Int) { update(i, value) }

  /**
   * Make a copy of the current [[InternalRow]] object.
   */
  def copy(): InternalRow

  /** Returns true if there are any NULL values in this row. */
  def anyNull: Boolean

  /* ---------------------- utility methods for Scala ---------------------- */

  /**
   * Return a Scala Seq representing the row. Elements are placed in the same order in the Seq.
   */
  def toSeq(fieldTypes: Seq[DataType]): Seq[Any] = {
    val len = numFields
    assert(len == fieldTypes.length)

    val values = new Array[Any](len)
    var i = 0
    while (i < len) {
      values(i) = get(i, fieldTypes(i))
      i += 1
    }
    values
  }

  def toSeq(schema: StructType): Seq[Any] = toSeq(schema.map(_.dataType))
}

object InternalRow {
  /**
   * This method can be used to construct a [[InternalRow]] with the given values.
   */
  def apply(values: Any*): InternalRow = new GenericInternalRow(values.toArray)

  /**
   * This method can be used to construct a [[InternalRow]] from a [[Seq]] of values.
   */
  def fromSeq(values: Seq[Any]): InternalRow = new GenericInternalRow(values.toArray)

  /** Returns an empty [[InternalRow]]. */
  val empty = apply()
}
