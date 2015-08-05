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

/**
 * An abstract class for row used internal in Spark SQL, which only contain the columns as
 * internal types.
 */
abstract class InternalRow extends SpecializedGetters with Serializable {

  def numFields: Int

  // This is only use for test and will throw a null pointer exception if the position is null.
  def getString(ordinal: Int): String = getUTF8String(ordinal).toString

  override def toString: String = mkString("[", ",", "]")

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
  // todo: remove this as it needs the generic getter
  def toSeq: Seq[Any] = {
    val n = numFields
    val values = new Array[Any](n)
    var i = 0
    while (i < n) {
      values.update(i, get(i, null))
      i += 1
    }
    values
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
