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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{Decimal, DataType, StructType, AtomicType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * An extended interface to [[InternalRow]] that allows the values for each column to be updated.
 * Setting a value through a primitive function implicitly marks that column as not null.
 */
abstract class MutableRow extends InternalRow {
  def setNullAt(i: Int): Unit

  def update(i: Int, value: Any)

  // default implementation (slow)
  def setBoolean(i: Int, value: Boolean): Unit = { update(i, value) }
  def setByte(i: Int, value: Byte): Unit = { update(i, value) }
  def setShort(i: Int, value: Short): Unit = { update(i, value) }
  def setInt(i: Int, value: Int): Unit = { update(i, value) }
  def setLong(i: Int, value: Long): Unit = { update(i, value) }
  def setFloat(i: Int, value: Float): Unit = { update(i, value) }
  def setDouble(i: Int, value: Double): Unit = { update(i, value) }
  def setDecimal(i: Int, value: Decimal, precision: Int) { update(i, value) }
}

/**
 * A row implementation that uses an array of objects as the underlying storage.  Note that, while
 * the array is not copied, and thus could technically be mutated after creation, this is not
 * allowed.
 */
class GenericRow(protected[sql] val values: Array[Any]) extends Row {
  /** No-arg constructor for serialization. */
  protected def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

  override def length: Int = values.length

  override def get(i: Int): Any = values(i)

  override def toSeq: Seq[Any] = values.toSeq

  override def copy(): Row = this
}

class GenericRowWithSchema(values: Array[Any], override val schema: StructType)
  extends GenericRow(values) {

  /** No-arg constructor for serialization. */
  protected def this() = this(null, null)

  override def fieldIndex(name: String): Int = schema.fieldIndex(name)
}

/**
 * A internal row implementation that uses an array of objects as the underlying storage.
 * Note that, while the array is not copied, and thus could technically be mutated after creation,
 * this is not allowed.
 */
class GenericInternalRow(protected[sql] val values: Array[Any]) extends InternalRow {
  /** No-arg constructor for serialization. */
  protected def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

  override def genericGet(ordinal: Int): Any = values(ordinal)

  override def toSeq: Seq[Any] = values

  override def numFields: Int = values.length

  override def copy(): InternalRow = new GenericInternalRow(values.clone())
}

/**
 * This is used for serialization of Python DataFrame
 */
class GenericInternalRowWithSchema(values: Array[Any], val schema: StructType)
  extends GenericInternalRow(values) {

  /** No-arg constructor for serialization. */
  protected def this() = this(null, null)

  def fieldIndex(name: String): Int = schema.fieldIndex(name)
}

class GenericMutableRow(val values: Array[Any]) extends MutableRow {
  /** No-arg constructor for serialization. */
  protected def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

  override def genericGet(ordinal: Int): Any = values(ordinal)

  override def toSeq: Seq[Any] = values

  override def numFields: Int = values.length

  override def setNullAt(i: Int): Unit = { values(i) = null}

  override def update(i: Int, value: Any): Unit = { values(i) = value }

  override def copy(): InternalRow = new GenericInternalRow(values.clone())
}
