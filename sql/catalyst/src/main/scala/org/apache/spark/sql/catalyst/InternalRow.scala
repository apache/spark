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
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * An abstract class for row used internally in Spark SQL, which only contains the columns as
 * internal types.
 */
abstract class InternalRow extends SpecializedGetters with Serializable {

  def numFields: Int

  // This is only use for test and will throw a null pointer exception if the position is null.
  def getString(ordinal: Int): String = getUTF8String(ordinal).toString

  def setNullAt(i: Int): Unit

  /**
   * Updates the value at column `i`. Note that after updating, the given value will be kept in this
   * row, and the caller side should guarantee that this value won't be changed afterwards.
   */
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
  def setDecimal(i: Int, value: Decimal, precision: Int): Unit = update(i, value)

  def setInterval(i: Int, value: CalendarInterval): Unit = update(i, value)

  /**
   * Make a copy of the current [[InternalRow]] object.
   */
  def copy(): InternalRow

  /** Returns true if there are any NULL values in this row. */
  def anyNull: Boolean = {
    val len = numFields
    var i = 0
    while (i < len) {
      if (isNullAt(i)) { return true }
      i += 1
    }
    false
  }

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

  /**
   * Copies the given value if it's string/struct/array/map type.
   */
  def copyValue(value: Any): Any = value match {
    case v: UTF8String => v.copy()
    case v: InternalRow => v.copy()
    case v: ArrayData => v.copy()
    case v: MapData => v.copy()
    case _ => value
  }

  /**
   * Returns an accessor for an `InternalRow` with given data type. The returned accessor
   * actually takes a `SpecializedGetters` input because it can be generalized to other classes
   * that implements `SpecializedGetters` (e.g., `ArrayData`) too.
   */
  def getAccessor(dt: DataType, nullable: Boolean = true): (SpecializedGetters, Int) => Any = {
    val getValueNullSafe: (SpecializedGetters, Int) => Any = dt match {
      case BooleanType => (input, ordinal) => input.getBoolean(ordinal)
      case ByteType => (input, ordinal) => input.getByte(ordinal)
      case ShortType => (input, ordinal) => input.getShort(ordinal)
      case IntegerType | DateType => (input, ordinal) => input.getInt(ordinal)
      case LongType | TimestampType => (input, ordinal) => input.getLong(ordinal)
      case FloatType => (input, ordinal) => input.getFloat(ordinal)
      case DoubleType => (input, ordinal) => input.getDouble(ordinal)
      case StringType => (input, ordinal) => input.getUTF8String(ordinal)
      case BinaryType => (input, ordinal) => input.getBinary(ordinal)
      case CalendarIntervalType => (input, ordinal) => input.getInterval(ordinal)
      case t: DecimalType => (input, ordinal) => input.getDecimal(ordinal, t.precision, t.scale)
      case t: StructType => (input, ordinal) => input.getStruct(ordinal, t.size)
      case _: ArrayType => (input, ordinal) => input.getArray(ordinal)
      case _: MapType => (input, ordinal) => input.getMap(ordinal)
      case u: UserDefinedType[_] => getAccessor(u.sqlType, nullable)
      case _ => (input, ordinal) => input.get(ordinal, dt)
    }

    if (nullable) {
      (getter, index) => {
        if (getter.isNullAt(index)) {
          null
        } else {
          getValueNullSafe(getter, index)
        }
      }
    } else {
      getValueNullSafe
    }
  }

  /**
   * Returns a writer for an `InternalRow` with given data type.
   */
  def getWriter(ordinal: Int, dt: DataType): (InternalRow, Any) => Unit = dt match {
    case BooleanType => (input, v) => input.setBoolean(ordinal, v.asInstanceOf[Boolean])
    case ByteType => (input, v) => input.setByte(ordinal, v.asInstanceOf[Byte])
    case ShortType => (input, v) => input.setShort(ordinal, v.asInstanceOf[Short])
    case IntegerType | DateType => (input, v) => input.setInt(ordinal, v.asInstanceOf[Int])
    case LongType | TimestampType => (input, v) => input.setLong(ordinal, v.asInstanceOf[Long])
    case FloatType => (input, v) => input.setFloat(ordinal, v.asInstanceOf[Float])
    case DoubleType => (input, v) => input.setDouble(ordinal, v.asInstanceOf[Double])
    case CalendarIntervalType =>
      (input, v) => input.setInterval(ordinal, v.asInstanceOf[CalendarInterval])
    case DecimalType.Fixed(precision, _) =>
      (input, v) => input.setDecimal(ordinal, v.asInstanceOf[Decimal], precision)
    case udt: UserDefinedType[_] => getWriter(ordinal, udt.sqlType)
    case NullType => (input, _) => input.setNullAt(ordinal)
    case StringType => (input, v) => input.update(ordinal, v.asInstanceOf[UTF8String].copy())
    case _: StructType => (input, v) => input.update(ordinal, v.asInstanceOf[InternalRow].copy())
    case _: ArrayType => (input, v) => input.update(ordinal, v.asInstanceOf[ArrayData].copy())
    case _: MapType => (input, v) => input.update(ordinal, v.asInstanceOf[MapData].copy())
    case _ => (input, v) => input.update(ordinal, v)
  }
}
