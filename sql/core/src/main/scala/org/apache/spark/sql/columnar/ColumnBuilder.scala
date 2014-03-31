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

package org.apache.spark.sql.columnar

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.columnar.ColumnBuilder._
import org.apache.spark.sql.execution.SparkSqlSerializer

private[sql] trait ColumnBuilder {
  /**
   * Initializes with an approximate lower bound on the expected number of elements in this column.
   */
  def initialize(initialSize: Int, columnName: String = "")

  def appendFrom(row: Row, ordinal: Int)

  def build(): ByteBuffer
}

private[sql] abstract class BasicColumnBuilder[T <: DataType, JvmType] extends ColumnBuilder {

  private var columnName: String = _
  protected var buffer: ByteBuffer = _

  def columnType: ColumnType[T, JvmType]

  override def initialize(initialSize: Int, columnName: String = "") = {
    val size = if (initialSize == 0) DEFAULT_INITIAL_BUFFER_SIZE else initialSize
    this.columnName = columnName
    buffer = ByteBuffer.allocate(4 + 4 + size * columnType.defaultSize)
    buffer.order(ByteOrder.nativeOrder()).putInt(columnType.typeId)
  }

  // Have to give a concrete implementation to make mixin possible
  override def appendFrom(row: Row, ordinal: Int) {
    doAppendFrom(row, ordinal)
  }

  // Concrete `ColumnBuilder`s can override this method to append values
  protected def doAppendFrom(row: Row, ordinal: Int)

  // Helper method to append primitive values (to avoid boxing cost)
  protected def appendValue(v: JvmType) {
    buffer = ensureFreeSpace(buffer, columnType.actualSize(v))
    columnType.append(v, buffer)
  }

  override def build() = {
    buffer.limit(buffer.position()).rewind()
    buffer
  }
}

private[sql] abstract class NativeColumnBuilder[T <: NativeType](
    val columnType: NativeColumnType[T])
  extends BasicColumnBuilder[T, T#JvmType]
  with NullableColumnBuilder

private[sql] class BooleanColumnBuilder extends NativeColumnBuilder(BOOLEAN) {
  override def doAppendFrom(row: Row, ordinal: Int) {
    appendValue(row.getBoolean(ordinal))
  }
}

private[sql] class IntColumnBuilder extends NativeColumnBuilder(INT) {
  override def doAppendFrom(row: Row, ordinal: Int) {
    appendValue(row.getInt(ordinal))
  }
}

private[sql] class ShortColumnBuilder extends NativeColumnBuilder(SHORT) {
  override def doAppendFrom(row: Row, ordinal: Int) {
    appendValue(row.getShort(ordinal))
  }
}

private[sql] class LongColumnBuilder extends NativeColumnBuilder(LONG) {
  override def doAppendFrom(row: Row, ordinal: Int) {
    appendValue(row.getLong(ordinal))
  }
}

private[sql] class ByteColumnBuilder extends NativeColumnBuilder(BYTE) {
  override def doAppendFrom(row: Row, ordinal: Int) {
    appendValue(row.getByte(ordinal))
  }
}

private[sql] class DoubleColumnBuilder extends NativeColumnBuilder(DOUBLE) {
  override def doAppendFrom(row: Row, ordinal: Int) {
    appendValue(row.getDouble(ordinal))
  }
}

private[sql] class FloatColumnBuilder extends NativeColumnBuilder(FLOAT) {
  override def doAppendFrom(row: Row, ordinal: Int) {
    appendValue(row.getFloat(ordinal))
  }
}

private[sql] class StringColumnBuilder extends NativeColumnBuilder(STRING) {
  override def doAppendFrom(row: Row, ordinal: Int) {
    appendValue(row.getString(ordinal))
  }
}

private[sql] class BinaryColumnBuilder
  extends BasicColumnBuilder[BinaryType.type, Array[Byte]]
  with NullableColumnBuilder {

  def columnType = BINARY

  override def doAppendFrom(row: Row, ordinal: Int) {
    appendValue(row(ordinal).asInstanceOf[Array[Byte]])
  }
}

// TODO (lian) Add support for array, struct and map
private[sql] class GenericColumnBuilder
  extends BasicColumnBuilder[DataType, Array[Byte]]
  with NullableColumnBuilder {

  def columnType = GENERIC

  override def doAppendFrom(row: Row, ordinal: Int) {
    val serialized = SparkSqlSerializer.serialize(row(ordinal))
    buffer = ColumnBuilder.ensureFreeSpace(buffer, columnType.actualSize(serialized))
    columnType.append(serialized, buffer)
  }
}

private[sql] object ColumnBuilder {
  val DEFAULT_INITIAL_BUFFER_SIZE = 10 * 1024 * 104

  private[columnar] def ensureFreeSpace(orig: ByteBuffer, size: Int) = {
    if (orig.remaining >= size) {
      orig
    } else {
      // grow in steps of initial size
      val capacity = orig.capacity()
      val newSize = capacity + size.max(capacity / 8 + 1)
      val pos = orig.position()

      orig.clear()
      ByteBuffer
        .allocate(newSize)
        .order(ByteOrder.nativeOrder())
        .put(orig.array(), 0, pos)
    }
  }

  def apply(typeId: Int, initialSize: Int = 0, columnName: String = ""): ColumnBuilder = {
    val builder = (typeId match {
      case INT.typeId     => new IntColumnBuilder
      case LONG.typeId    => new LongColumnBuilder
      case FLOAT.typeId   => new FloatColumnBuilder
      case DOUBLE.typeId  => new DoubleColumnBuilder
      case BOOLEAN.typeId => new BooleanColumnBuilder
      case BYTE.typeId    => new ByteColumnBuilder
      case SHORT.typeId   => new ShortColumnBuilder
      case STRING.typeId  => new StringColumnBuilder
      case BINARY.typeId  => new BinaryColumnBuilder
      case GENERIC.typeId => new GenericColumnBuilder
    }).asInstanceOf[ColumnBuilder]

    builder.initialize(initialSize, columnName)
    builder
  }
}
