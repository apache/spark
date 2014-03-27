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

private[sql] trait ColumnBuilder {
  /**
   * Initializes with an approximate lower bound on the expected number of elements in this column.
   */
  def initialize(initialSize: Int, columnName: String = "")

  /**
   * Gathers statistics information from `row(ordinal)`.
   */
  def gatherStats(row: Row, ordinal: Int) {}

  /**
   * Appends `row(ordinal)` to the column builder.
   */
  def appendFrom(row: Row, ordinal: Int)

  /**
   * Returns the final columnar byte buffer.
   */
  def build(): ByteBuffer
}

private[sql] abstract class BasicColumnBuilder[T <: DataType, JvmType](
    val columnType: ColumnType[T, JvmType])
  extends ColumnBuilder {

  protected var columnName: String = _

  protected var buffer: ByteBuffer = _

  override def initialize(initialSize: Int, columnName: String = "") = {
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
    buffer.limit(buffer.position()).rewind()
    buffer
  }
}

private[sql] abstract class NativeColumnBuilder[T <: NativeType](
    protected val columnStats: ColumnStats[T],
    columnType: NativeColumnType[T])
  extends BasicColumnBuilder(columnType)
  with NullableColumnBuilder
  with CompressedColumnBuilder[T] {

  override def gatherStats(row: Row, ordinal: Int) {
    columnStats.gatherStats(row, ordinal)
  }
}

private[sql] class BooleanColumnBuilder
  extends NativeColumnBuilder(new BooleanColumnStats, BOOLEAN)

private[sql] class IntColumnBuilder extends NativeColumnBuilder(new IntColumnStats, INT)

private[sql] class ShortColumnBuilder extends NativeColumnBuilder(new ShortColumnStats, SHORT)

private[sql] class LongColumnBuilder extends NativeColumnBuilder(new LongColumnStats, LONG)

private[sql] class ByteColumnBuilder extends NativeColumnBuilder(new ByteColumnStats, BYTE)

private[sql] class DoubleColumnBuilder extends NativeColumnBuilder(new DoubleColumnStats, DOUBLE)

private[sql] class FloatColumnBuilder extends NativeColumnBuilder(new FloatColumnStats, FLOAT)

private[sql] class StringColumnBuilder extends NativeColumnBuilder(new StringColumnStates, STRING)

private[sql] class BinaryColumnBuilder
  extends BasicColumnBuilder[BinaryType.type, Array[Byte]](BINARY)
  with NullableColumnBuilder

// TODO (lian) Add support for array, struct and map
private[sql] class GenericColumnBuilder
  extends BasicColumnBuilder[DataType, Array[Byte]](GENERIC)
  with NullableColumnBuilder

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
