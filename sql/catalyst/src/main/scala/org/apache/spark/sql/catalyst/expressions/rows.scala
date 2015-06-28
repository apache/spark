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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructType, AtomicType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * An extended interface to [[InternalRow]] that allows the values for each column to be updated.
 * Setting a value through a primitive function implicitly marks that column as not null.
 */
abstract class MutableRow extends InternalRow {
  def setNullAt(i: Int): Unit

  def update(ordinal: Int, value: Any): Unit

  // default implementation for codegen (for a Row which does not have those types)
  def setBoolean(ordinal: Int, value: Boolean): Unit = throw new UnsupportedOperationException
  def setByte(ordinal: Int, value: Byte): Unit = throw new UnsupportedOperationException
  def setShort(ordinal: Int, value: Short): Unit = throw new UnsupportedOperationException
  def setInt(ordinal: Int, value: Int): Unit = throw new UnsupportedOperationException
  def setLong(ordinal: Int, value: Long): Unit = throw new UnsupportedOperationException
  def setFloat(ordinal: Int, value: Float): Unit = throw new UnsupportedOperationException
  def setDouble(ordinal: Int, value: Double): Unit = throw new UnsupportedOperationException

  def setString(ordinal: Int, value: String): Unit = {
    update(ordinal, UTF8String.fromString(value))
  }
}

/**
 * A general row implementation that uses an array of objects as the underlying storage.
 * Note that, while the array is not copied, and thus could technically be mutated after
 * creation, this is not allowed.
 */
trait ArrayBackedRow {
  self: Row =>

  protected val values: Array[Any]

  override def toSeq: Seq[Any] = values.toSeq

  override def length: Int = values.length

  override def apply(i: Int): Any = values(i)

  override def isNullAt(i: Int): Boolean = values(i) == null

  override def getBoolean(i: Int): Boolean = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive boolean value.")
    getAs[Boolean](i)
  }

  override def getByte(i: Int): Byte = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive byte value.")
    getAs[Byte](i)
  }

  override def getShort(i: Int): Short = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive short value.")
    getAs[Short](i)
  }

  override def getInt(i: Int): Int = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive int value.")
    getAs[Int](i)
  }

  override def getLong(i: Int): Long = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive long value.")
    getAs[Long](i)
  }

  override def getFloat(i: Int): Float = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive float value.")
    getAs[Float](i)
  }

  override def getDouble(i: Int): Double = {
    if (values(i) == null) sys.error("Failed to check null bit for primitive double value.")
    getAs[Double](i)
  }
}


/**
 * A row with no data.  Calling any methods will result in an error.  Can be used as a placeholder.
 */
object EmptyRow extends InternalRow {
  override def apply(i: Int): Any = throw new UnsupportedOperationException
  override def toSeq: Seq[Any] = Seq.empty
  override def length: Int = 0
  override def isNullAt(i: Int): Boolean = throw new UnsupportedOperationException
  override def getBoolean(i: Int): Boolean = throw new UnsupportedOperationException
  override def getByte(i: Int): Byte = throw new UnsupportedOperationException
  override def getShort(i: Int): Short = throw new UnsupportedOperationException
  override def getInt(i: Int): Int = throw new UnsupportedOperationException
  override def getLong(i: Int): Long = throw new UnsupportedOperationException
  override def getFloat(i: Int): Float = throw new UnsupportedOperationException
  override def getDouble(i: Int): Double = throw new UnsupportedOperationException
  override def getString(i: Int): String = throw new UnsupportedOperationException
}

/**
 * A row implementation that uses an array of objects as the underlying storage.  Note that, while
 * the array is not copied, and thus could technically be mutated after creation, this is not
 * allowed.
 */
class GenericRow(protected[sql] val values: Array[Any]) extends InternalRow with ArrayBackedRow {
  /** No-arg constructor for serialization. */
  protected def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

  override def getString(i: Int): String = {
    values(i) match {
      case null => null
      case s: String => s
      case utf8: UTF8String => utf8.toString
    }
  }
}

class GenericRowWithSchema(values: Array[Any], override val schema: StructType)
  extends GenericRow(values) {

  /** No-arg constructor for serialization. */
  protected def this() = this(null, null)

  override def fieldIndex(name: String): Int = schema.fieldIndex(name)
}

class GenericMutableRow(protected[sql] val values: Array[Any])
  extends MutableRow with ArrayBackedRow {
  /** No-arg constructor for serialization. */
  protected def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

  override def setBoolean(ordinal: Int, value: Boolean): Unit = { values(ordinal) = value }
  override def setByte(ordinal: Int, value: Byte): Unit = { values(ordinal) = value }
  override def setShort(ordinal: Int, value: Short): Unit = { values(ordinal) = value }
  override def setInt(ordinal: Int, value: Int): Unit = { values(ordinal) = value }
  override def setLong(ordinal: Int, value: Long): Unit = { values(ordinal) = value }
  override def setFloat(ordinal: Int, value: Float): Unit = { values(ordinal) = value }
  override def setDouble(ordinal: Int, value: Double): Unit = { values(ordinal) = value }

  override def setNullAt(i: Int): Unit = { values(i) = null }

  override def update(ordinal: Int, value: Any): Unit = { values(ordinal) = value }

  override def copy(): InternalRow = new GenericRow(values.clone())

  override def getString(i: Int): String = {
    values(i) match {
      case null => null
      case s: String => s
      case utf8: UTF8String => utf8.toString
    }
  }
}


class RowOrdering(ordering: Seq[SortOrder]) extends Ordering[InternalRow] {
  def this(ordering: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(ordering.map(BindReferences.bindReference(_, inputSchema)))

  def compare(a: InternalRow, b: InternalRow): Int = {
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
          case n: AtomicType if order.direction == Ascending =>
            n.ordering.asInstanceOf[Ordering[Any]].compare(left, right)
          case n: AtomicType if order.direction == Descending =>
            n.ordering.asInstanceOf[Ordering[Any]].reverse.compare(left, right)
          case other => sys.error(s"Type $other does not support ordered operations")
        }
        if (comparison != 0) return comparison
      }
      i += 1
    }
    return 0
  }
}

object RowOrdering {
  def forSchema(dataTypes: Seq[DataType]): RowOrdering =
    new RowOrdering(dataTypes.zipWithIndex.map {
      case(dt, index) => new SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    })
}
