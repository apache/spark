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

import org.apache.spark.sql.catalyst.types.NativeType

object Row {
  /**
   * This method can be used to extract fields from a [[Row]] object in a pattern match. Example:
   * {{{
   * import org.apache.spark.sql._
   *
   * val pairs = sql("SELECT key, value FROM src").rdd.map {
   *   case Row(key: Int, value: String) =>
   *     key -> value
   * }
   * }}}
   */
  def unapplySeq(row: Row): Some[Seq[Any]] = Some(row)
}

/**
 * Represents one row of output from a relational operator.  Allows both generic access by ordinal,
 * which will incur boxing overhead for primitives, as well as native primitive access.
 *
 * It is invalid to use the native primitive interface to retrieve a value that is null, instead a
 * user must check [[isNullAt]] before attempting to retrieve a value that might be null.
 */
trait Row extends Seq[Any] with Serializable {
  def apply(i: Int): Any

  def isNullAt(i: Int): Boolean

  def getInt(i: Int): Int
  def getLong(i: Int): Long
  def getDouble(i: Int): Double
  def getFloat(i: Int): Float
  def getBoolean(i: Int): Boolean
  def getShort(i: Int): Short
  def getByte(i: Int): Byte
  def getString(i: Int): String

  override def toString() =
    s"[${this.mkString(",")}]"

  def copy(): Row

  /** Returns true if there are any NULL values in this row. */
  def anyNull: Boolean = {
    var i = 0
    while (i < length) {
      if (isNullAt(i)) { return true }
      i += 1
    }
    false
  }
}

/**
 * An extended interface to [[Row]] that allows the values for each column to be updated.  Setting
 * a value through a primitive function implicitly marks that column as not null.
 */
trait MutableRow extends Row {
  def setNullAt(i: Int): Unit

  def update(ordinal: Int, value: Any)

  def setInt(ordinal: Int, value: Int)
  def setLong(ordinal: Int, value: Long)
  def setDouble(ordinal: Int, value: Double)
  def setBoolean(ordinal: Int, value: Boolean)
  def setShort(ordinal: Int, value: Short)
  def setByte(ordinal: Int, value: Byte)
  def setFloat(ordinal: Int, value: Float)
  def setString(ordinal: Int, value: String)

  /**
   * Experimental
   *
   * Returns a mutable string builder for the specified column.  A given row should return the
   * result of any mutations made to the returned buffer next time getString is called for the same
   * column.
   */
  def getStringBuilder(ordinal: Int): StringBuilder
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
  def getFloat(i: Int): Float = throw new UnsupportedOperationException
  def getBoolean(i: Int): Boolean = throw new UnsupportedOperationException
  def getShort(i: Int): Short = throw new UnsupportedOperationException
  def getByte(i: Int): Byte = throw new UnsupportedOperationException
  def getString(i: Int): String = throw new UnsupportedOperationException

  def copy() = this
}

/**
 * A row implementation that uses an array of objects as the underlying storage.  Note that, while
 * the array is not copied, and thus could technically be mutated after creation, this is not
 * allowed.
 */
class GenericRow(protected[catalyst] val values: Array[Any]) extends Row {
  /** No-arg constructor for serialization. */
  def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

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

  def getFloat(i: Int): Float = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive float value.")
    values(i).asInstanceOf[Float]
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

  def getString(i: Int): String = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive String value.")
    values(i).asInstanceOf[String]
  }

  def copy() = this
}

class GenericMutableRow(size: Int) extends GenericRow(size) with MutableRow {
  /** No-arg constructor for serialization. */
  def this() = this(0)

  def getStringBuilder(ordinal: Int): StringBuilder = ???

  override def setBoolean(ordinal: Int,value: Boolean): Unit = { values(ordinal) = value }
  override def setByte(ordinal: Int,value: Byte): Unit = { values(ordinal) = value }
  override def setDouble(ordinal: Int,value: Double): Unit = { values(ordinal) = value }
  override def setFloat(ordinal: Int,value: Float): Unit = { values(ordinal) = value }
  override def setInt(ordinal: Int,value: Int): Unit = { values(ordinal) = value }
  override def setLong(ordinal: Int,value: Long): Unit = { values(ordinal) = value }
  override def setString(ordinal: Int,value: String): Unit = { values(ordinal) = value }

  override def setNullAt(i: Int): Unit = { values(i) = null }

  override def setShort(ordinal: Int,value: Short): Unit = { values(ordinal) = value }

  override def update(ordinal: Int,value: Any): Unit = { values(ordinal) = value }

  override def copy() = new GenericRow(values.clone())
}


class RowOrdering(ordering: Seq[SortOrder]) extends Ordering[Row] {
  def this(ordering: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(ordering.map(BindReferences.bindReference(_, inputSchema)))

  def compare(a: Row, b: Row): Int = {
    var i = 0
    while (i < ordering.size) {
      val order = ordering(i)
      val left = order.child.eval(a)
      val right = order.child.eval(b)

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
