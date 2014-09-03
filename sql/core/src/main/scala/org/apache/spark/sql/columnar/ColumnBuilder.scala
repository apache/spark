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
import org.apache.spark.sql.columnar.compression.{AllCompressionSchemes, CompressibleColumnBuilder}

private[sql] trait ColumnBuilder {
  /**
   * Initializes with an approximate lower bound on the expected number of elements in this column.
   */
  def initialize(initialSize: Int, columnName: String = "", useCompression: Boolean = false)

  /**
   * Appends `row(ordinal)` to the column builder.
   */
  def appendFrom(row: Row, ordinal: Int)

  /**
   * Column statistics information
   */
  def columnStats: ColumnStats[_, _]

  /**
   * Returns the final columnar byte buffer.
   */
  def build(): ByteBuffer
}

private[sql] class BasicColumnBuilder[T <: DataType, JvmType](
    val columnStats: ColumnStats[T, JvmType],
    val columnType: ColumnType[T, JvmType])
  extends ColumnBuilder {

  protected var columnName: String = _

  protected var buffer: ByteBuffer = _

  override def initialize(
      initialSize: Int,
      columnName: String = "",
      useCompression: Boolean = false) = {

    val size = if (initialSize == 0) DEFAULT_INITIAL_BUFFER_SIZE else initialSize
    this.columnName = columnName

    // Reserves 4 bytes for column type ID
    buffer = ByteBuffer.allocate(4 + size * columnType.defaultSize)
    buffer.order(ByteOrder.nativeOrder()).putInt(columnType.typeId)
  }

  override def appendFrom(row: Row, ordinal: Int) {
    val field = columnType.getField(row, ordinal)
    buffer = ensureFreeSpace(buffer, columnType.actualSize(field))
    columnType.append(field, buffer)
  }

  override def build() = {
    buffer.flip().asInstanceOf[ByteBuffer]
  }
}

private[sql] abstract class ComplexColumnBuilder[T <: DataType, JvmType](
    columnType: ColumnType[T, JvmType])
  extends BasicColumnBuilder[T, JvmType](new NoopColumnStats[T, JvmType], columnType)
  with NullableColumnBuilder

private[sql] abstract class NativeColumnBuilder[T <: NativeType](
    override val columnStats: NativeColumnStats[T],
    override val columnType: NativeColumnType[T])
  extends BasicColumnBuilder[T, T#JvmType](columnStats, columnType)
  with NullableColumnBuilder
  with AllCompressionSchemes
  with CompressibleColumnBuilder[T]

private[sql] class BooleanColumnBuilder extends NativeColumnBuilder(new BooleanColumnStats, BOOLEAN)

private[sql] class IntColumnBuilder extends NativeColumnBuilder(new IntColumnStats, INT)

private[sql] class ShortColumnBuilder extends NativeColumnBuilder(new ShortColumnStats, SHORT)

private[sql] class LongColumnBuilder extends NativeColumnBuilder(new LongColumnStats, LONG)

private[sql] class ByteColumnBuilder extends NativeColumnBuilder(new ByteColumnStats, BYTE)

private[sql] class DoubleColumnBuilder extends NativeColumnBuilder(new DoubleColumnStats, DOUBLE)

private[sql] class FloatColumnBuilder extends NativeColumnBuilder(new FloatColumnStats, FLOAT)

private[sql] class StringColumnBuilder extends NativeColumnBuilder(new StringColumnStats, STRING)

private[sql] class TimestampColumnBuilder
  extends NativeColumnBuilder(new TimestampColumnStats, TIMESTAMP)

private[sql] class BinaryColumnBuilder extends ComplexColumnBuilder(BINARY)

// TODO (lian) Add support for array, struct and map
private[sql] class GenericColumnBuilder extends ComplexColumnBuilder(GENERIC)

private[sql] object ColumnBuilder {
  val DEFAULT_INITIAL_BUFFER_SIZE = 1024 * 1024

  private[columnar] def ensureFreeSpace(orig: ByteBuffer, size: Int) = {
    if (orig.remaining >= size) {
      orig
    } else {
      // grow in steps of initial size
      val capacity = orig.capacity()
      val newSize = capacity + size.max(capacity / 8 + 1)
      val pos = orig.position()

      ByteBuffer
        .allocate(newSize)
        .order(ByteOrder.nativeOrder())
        .put(orig.array(), 0, pos)
    }
  }

  def apply(
      typeId: Int,
      initialSize: Int = 0,
      columnName: String = "",
      useCompression: Boolean = false): ColumnBuilder = {

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
      case TIMESTAMP.typeId => new TimestampColumnBuilder
    }).asInstanceOf[ColumnBuilder]

    builder.initialize(initialSize, columnName, useCompression)
    builder
  }
}
