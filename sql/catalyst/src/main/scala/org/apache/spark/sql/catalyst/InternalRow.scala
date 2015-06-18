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
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * An abstract class for row used internal in Spark SQL, which only contain the columns as
 * internal types.
 *
 * The following is a mapping between Spark SQL types and types of objects in row:
 *
 *   BooleanType -> java.lang.Boolean
 *   ByteType -> java.lang.Byte
 *   ShortType -> java.lang.Short
 *   IntegerType -> java.lang.Integer
 *   FloatType -> java.lang.Float
 *   DoubleType -> java.lang.Double
 *   StringType -> UTF8String
 *   DecimalType -> org.apache.spark.sql.types.Decimal
 *
 *   DateType -> java.lang.Int
 *   TimestampType -> java.lang.Long
 *
 *   BinaryType -> Array[Byte]
 *   ArrayType -> scala.collection.Seq
 *   MapType -> scala.collection.Map
 *   StructType -> InternalRow
 */
abstract class InternalRow extends Serializable {
  /** Number of elements in the Row. */
  def size: Int = length

  /** Number of elements in the Row. */
  def length: Int

  /**
   * Schema for the row.
   */
  def schema: StructType = null

  /**
   * Returns the value at position i. If the value is null, null is returned.
   */
  def apply(i: Int): Any

  /**
   * Returns the value at position i. If the value is null, null is returned.
   */
  def get(i: Int): Any = apply(i)

  /** Checks whether the value at position i is null. */
  def isNullAt(i: Int): Boolean

  /**
   * Returns the value at position i as a primitive boolean.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getBoolean(i: Int): Boolean

  /**
   * Returns the value at position i as a primitive byte.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getByte(i: Int): Byte

  /**
   * Returns the value at position i as a primitive short.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getShort(i: Int): Short

  /**
   * Returns the value at position i as a primitive int.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getInt(i: Int): Int

  /**
   * Returns the value at position i as a primitive long.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getLong(i: Int): Long

  /**
   * Returns the value at position i as a primitive float.
   * Throws an exception if the type mismatches or if the value is null.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getFloat(i: Int): Float

  /**
   * Returns the value at position i as a primitive double.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getDouble(i: Int): Double

  /**
   * Returns the value at position i as a String object.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getString(i: Int): String = getAs[UTF8String](i).toString

  /**
   * Returns the value at position i.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getAs[T](i: Int): T = apply(i).asInstanceOf[T]

  override def toString: String = s"[${this.mkString(",")}]"

  /** Returns true if there are any NULL values in this row. */
  def anyNull: Boolean = {
    val len = length
    var i = 0
    while (i < len) {
      if (isNullAt(i)) { return true }
      i += 1
    }
    false
  }

  override def equals(that: Any): Boolean = that match {
    case null => false
    case that: InternalRow =>
      if (this.length != that.length) {
        return false
      }
      var i = 0
      val len = this.length
      while (i < len) {
        if (apply(i) != that.apply(i)) {
          return false
        }
        i += 1
      }
      true
    case _ => false
  }

  def copy(): InternalRow

  /* ---------------------- utility methods for Scala ---------------------- */

  /**
   * Return a Scala Seq representing the row. ELements are placed in the same order in the Seq.
   */
  def toSeq: Seq[Any]

  /** Displays all elements of this sequence in a string (without a separator). */
  def mkString: String = toSeq.mkString

  /** Displays all elements of this sequence in a string using a separator string. */
  def mkString(sep: String): String = toSeq.mkString(sep)

  /**
   * Displays all elements of this traversable or iterator in a string using
   * start, end, and separator strings.
   */
  def mkString(start: String, sep: String, end: String): String = toSeq.mkString(start, sep, end)
}

object InternalRow {
  /**
   * This method can be used to construct a [[Row]] with the given values.
   */
  def apply(values: Any*): InternalRow = new GenericRow(values.toArray)

  /**
   * This method can be used to construct a [[Row]] from a [[Seq]] of values.
   */
  def fromSeq(values: Seq[Any]): InternalRow = new GenericRow(values.toArray)

  /** Returns an empty row. */
  val empty = apply()
}
