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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * An abstract class for row used internal in Spark SQL, which only contain the columns as
 * internal types.
 */
abstract class InternalRow extends Serializable with SpecializedGetters {

  def numFields: Int

  def get(ordinal: Int): Any = get(ordinal, null)

  def genericGet(ordinal: Int): Any = get(ordinal, null)

  def get(ordinal: Int, dataType: DataType): Any

  def getAs[T](ordinal: Int, dataType: DataType): T = get(ordinal, dataType).asInstanceOf[T]

  override def isNullAt(ordinal: Int): Boolean = get(ordinal) == null

  override def getBoolean(ordinal: Int): Boolean = getAs[Boolean](ordinal, BooleanType)

  override def getByte(ordinal: Int): Byte = getAs[Byte](ordinal, ByteType)

  override def getShort(ordinal: Int): Short = getAs[Short](ordinal, ShortType)

  override def getInt(ordinal: Int): Int = getAs[Int](ordinal, IntegerType)

  override def getLong(ordinal: Int): Long = getAs[Long](ordinal, LongType)

  override def getFloat(ordinal: Int): Float = getAs[Float](ordinal, FloatType)

  override def getDouble(ordinal: Int): Double = getAs[Double](ordinal, DoubleType)

  override def getUTF8String(ordinal: Int): UTF8String = getAs[UTF8String](ordinal, StringType)

  override def getBinary(ordinal: Int): Array[Byte] = getAs[Array[Byte]](ordinal, BinaryType)

  override def getDecimal(ordinal: Int): Decimal =
    getAs[Decimal](ordinal, DecimalType.SYSTEM_DEFAULT)

  override def getInterval(ordinal: Int): CalendarInterval =
    getAs[CalendarInterval](ordinal, CalendarIntervalType)

  // This is only use for test and will throw a null pointer exception if the position is null.
  def getString(ordinal: Int): String = getUTF8String(ordinal).toString

  /**
   * Returns a struct from ordinal position.
   *
   * @param ordinal position to get the struct from.
   * @param numFields number of fields the struct type has
   */
  override def getStruct(ordinal: Int, numFields: Int): InternalRow =
    getAs[InternalRow](ordinal, null)

  override def getArray(ordinal: Int): ArrayData = getAs(ordinal, null)

  override def toString: String = s"[${this.mkString(",")}]"

  /**
   * Make a copy of the current [[InternalRow]] object.
   */
  def copy(): InternalRow = this

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

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[InternalRow]) {
      return false
    }

    val other = o.asInstanceOf[InternalRow]
    if (other eq null) {
      return false
    }

    val len = numFields
    if (len != other.numFields) {
      return false
    }

    var i = 0
    while (i < len) {
      if (isNullAt(i) != other.isNullAt(i)) {
        return false
      }
      if (!isNullAt(i)) {
        val o1 = get(i)
        val o2 = other.get(i)
        o1 match {
          case b1: Array[Byte] =>
            if (!o2.isInstanceOf[Array[Byte]] ||
              !java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
              return false
            }
          case f1: Float if java.lang.Float.isNaN(f1) =>
            if (!o2.isInstanceOf[Float] || ! java.lang.Float.isNaN(o2.asInstanceOf[Float])) {
              return false
            }
          case d1: Double if java.lang.Double.isNaN(d1) =>
            if (!o2.isInstanceOf[Double] || ! java.lang.Double.isNaN(o2.asInstanceOf[Double])) {
              return false
            }
          case _ => if (o1 != o2) {
            return false
          }
        }
      }
      i += 1
    }
    true
  }

  /* ---------------------- utility methods for Scala ---------------------- */

  /**
   * Return a Scala Seq representing the row. Elements are placed in the same order in the Seq.
   */
  def toSeq: Seq[Any] = {
    val n = numFields
    val values = new Array[Any](n)
    var i = 0
    while (i < n) {
      values.update(i, get(i))
      i += 1
    }
    values.toSeq
  }

  /** Displays all elements of this sequence in a string (without a separator). */
  def mkString: String = toSeq.mkString

  /** Displays all elements of this sequence in a string using a separator string. */
  def mkString(sep: String): String = toSeq.mkString(sep)

  /**
   * Displays all elements of this traversable or iterator in a string using
   * start, end, and separator strings.
   */
  def mkString(start: String, sep: String, end: String): String = toSeq.mkString(start, sep, end)

  // Custom hashCode function that matches the efficient code generated version.
  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    val len = numFields
    while (i < len) {
      val update: Int =
        if (isNullAt(i)) {
          0
        } else {
          get(i) match {
            case b: Boolean => if (b) 0 else 1
            case b: Byte => b.toInt
            case s: Short => s.toInt
            case i: Int => i
            case l: Long => (l ^ (l >>> 32)).toInt
            case f: Float => java.lang.Float.floatToIntBits(f)
            case d: Double =>
              val b = java.lang.Double.doubleToLongBits(d)
              (b ^ (b >>> 32)).toInt
            case a: Array[Byte] => java.util.Arrays.hashCode(a)
            case other => other.hashCode()
          }
        }
      result = 37 * result + update
      i += 1
    }
    result
  }
}

object InternalRow {
  /**
   * This method can be used to construct a [[Row]] with the given values.
   */
  def apply(values: Any*): InternalRow = new GenericInternalRow(values.toArray)

  /**
   * This method can be used to construct a [[Row]] from a [[Seq]] of values.
   */
  def fromSeq(values: Seq[Any]): InternalRow = new GenericInternalRow(values.toArray)

  /** Returns an empty row. */
  val empty = apply()
}
