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

package org.apache.spark.sql
package catalyst
package expressions

import types._

/**
 * Represents one row of output from a relational operator.  Allows both generic access by ordinal,
 * which will incur boxing overhead for primitives, as well as native primitive access.
 *
 * It is invalid to use the native primitive interface to retrieve a value that is null, instead a
 * user must check [[isNullAt]] before attempting to retrieve a value that might be null.
 */
abstract class Row extends Seq[Any] with Serializable {
  def apply(i: Int): Any

  def isNullAt(i: Int): Boolean

  def getInt(i: Int): Int
  def getLong(i: Int): Long
  def getDouble(i: Int): Double
  def getBoolean(i: Int): Boolean
  def getShort(i: Int): Short
  def getByte(i: Int): Byte

  override def toString() =
    s"[${this.mkString(",")}]"
}

/**
 * A row with no data.  Calling any methods will result in an error.  Can be used as a placeholder.
 */
object EmptyRow extends Row {
  def apply(i: Int): Any = throw new UnsupportedOperationException

  def iterator = Iterator.empty
  def length = 0
  def isNullAt(i: Int): Boolean = throw new UnsupportedOperationException

  def getInt(i: Int): Int = throw new UnsupportedOperationException
  def getLong(i: Int): Long = throw new UnsupportedOperationException
  def getDouble(i: Int): Double = throw new UnsupportedOperationException
  def getBoolean(i: Int): Boolean = throw new UnsupportedOperationException
  def getShort(i: Int): Short = throw new UnsupportedOperationException
  def getByte(i: Int): Byte = throw new UnsupportedOperationException
}

/**
 * A row implementation that uses an array of objects as the underlying storage.
 */
class GenericRow(input: Seq[Any]) extends Row {
  val values = input.toIndexedSeq

  def iterator = values.iterator

  def length = values.length

  def apply(i: Int) = values(i)

  def isNullAt(i: Int) = values(i) == null

  def getInt(i: Int): Int = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive int value.")
    values(i).asInstanceOf[Int]
  }
  def getLong(i: Int): Long = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive long value.")
    values(i).asInstanceOf[Long]
  }
  def getDouble(i: Int): Double = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive double value.")
    values(i).asInstanceOf[Double]
  }
  def getBoolean(i: Int): Boolean = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive boolean value.")
    values(i).asInstanceOf[Boolean]
  }
  def getShort(i: Int): Short = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive short value.")
    values(i).asInstanceOf[Short]
  }
  def getByte(i: Int): Byte = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive byte value.")
    values(i).asInstanceOf[Byte]
  }
}

class RowOrdering(ordering: Seq[SortOrder]) extends Ordering[Row] {
  def compare(a: Row, b: Row): Int = {
    var i = 0
    while (i < ordering.size) {
      val order = ordering(i)
      val left = Evaluate(order.child, Vector(a))
      val right = Evaluate(order.child, Vector(b))

      if (left == null && right == null) {
        // Both null, continue looking.
      } else if (left == null) {
        return if (order.direction == Ascending) -1 else 1
      } else if (right == null) {
        return if (order.direction == Ascending) 1 else -1
      } else {
        val comparison = order.dataType match {
          case n: NativeType if order.direction == Ascending =>
            n.ordering.asInstanceOf[Ordering[Any]].compare(left, right)
          case n: NativeType if order.direction == Descending =>
            n.ordering.asInstanceOf[Ordering[Any]].reverse.compare(left, right)
        }
        if (comparison != 0) return comparison
      }
      i += 1
    }
    return 0
  }
}
