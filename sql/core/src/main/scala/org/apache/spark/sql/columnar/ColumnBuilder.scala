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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.columnar.ColumnBuilder._
import org.apache.spark.sql.columnar.compression.{AllCompressionSchemes, CompressibleColumnBuilder}
import org.apache.spark.sql.types._

private[sql] trait ColumnBuilder {
  /**
   * Initializes with an approximate lower bound on the expected number of elements in this column.
   */
  def initialize(initialSize: Int, columnName: String = "", useCompression: Boolean = false)

  /**
   * Appends `row(ordinal)` to the column builder.
   */
  def appendFrom(row: InternalRow, ordinal: Int)

  /**
   * Column statistics information
   */
  def columnStats: ColumnStats

  /**
   * Returns the final columnar byte buffer.
   */
  def build(): ByteBuffer
}

private[sql] class BasicColumnBuilder[JvmType](
    val columnStats: ColumnStats,
    val columnType: ColumnType[JvmType])
  extends ColumnBuilder {

  protected var columnName: String = _

  protected var buffer: ByteBuffer = _

  override def initialize(
      initialSize: Int,
      columnName: String = "",
      useCompression: Boolean = false): Unit = {

    val size = if (initialSize == 0) DEFAULT_INITIAL_BUFFER_SIZE else initialSize
    this.columnName = columnName

    // Reserves 4 bytes for column type ID
    buffer = ByteBuffer.allocate(4 + size * columnType.defaultSize)
    buffer.order(ByteOrder.nativeOrder()).putInt(columnType.typeId)
  }

  override def appendFrom(row: InternalRow, ordinal: Int): Unit = {
    buffer = ensureFreeSpace(buffer, columnType.actualSize(row, ordinal))
    columnType.append(row, ordinal, buffer)
  }

  override def build(): ByteBuffer = {
    buffer.flip().asInstanceOf[ByteBuffer]
  }
}

private[sql] abstract class ComplexColumnBuilder[JvmType](
    columnStats: ColumnStats,
    columnType: ColumnType[JvmType])
  extends BasicColumnBuilder[JvmType](columnStats, columnType)
  with NullableColumnBuilder

private[sql] abstract class NativeColumnBuilder[T <: AtomicType](
    override val columnStats: ColumnStats,
    override val columnType: NativeColumnType[T])
  extends BasicColumnBuilder[T#InternalType](columnStats, columnType)
  with NullableColumnBuilder
  with AllCompressionSchemes
  with CompressibleColumnBuilder[T]

private[sql] class BooleanColumnBuilder extends NativeColumnBuilder(new BooleanColumnStats, BOOLEAN)

private[sql] class ByteColumnBuilder extends NativeColumnBuilder(new ByteColumnStats, BYTE)

private[sql] class ShortColumnBuilder extends NativeColumnBuilder(new ShortColumnStats, SHORT)

private[sql] class IntColumnBuilder extends NativeColumnBuilder(new IntColumnStats, INT)

private[sql] class LongColumnBuilder extends NativeColumnBuilder(new LongColumnStats, LONG)

private[sql] class FloatColumnBuilder extends NativeColumnBuilder(new FloatColumnStats, FLOAT)

private[sql] class DoubleColumnBuilder extends NativeColumnBuilder(new DoubleColumnStats, DOUBLE)

private[sql] class StringColumnBuilder extends NativeColumnBuilder(new StringColumnStats, STRING)

private[sql] class BinaryColumnBuilder extends ComplexColumnBuilder(new BinaryColumnStats, BINARY)

private[sql] class FixedDecimalColumnBuilder(
    precision: Int,
    scale: Int)
  extends NativeColumnBuilder(
    new FixedDecimalColumnStats(precision, scale),
    FIXED_DECIMAL(precision, scale))

// TODO (lian) Add support for array, struct and map
private[sql] class GenericColumnBuilder(dataType: DataType)
  extends ComplexColumnBuilder(new GenericColumnStats(dataType), GENERIC(dataType))

private[sql] class DateColumnBuilder extends NativeColumnBuilder(new DateColumnStats, DATE)

private[sql] class TimestampColumnBuilder
  extends NativeColumnBuilder(new TimestampColumnStats, TIMESTAMP)

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
      dataType: DataType,
      initialSize: Int = 0,
      columnName: String = "",
      useCompression: Boolean = false): ColumnBuilder = {
    val builder: ColumnBuilder = dataType match {
      case BooleanType => new BooleanColumnBuilder
      case ByteType => new ByteColumnBuilder
      case ShortType => new ShortColumnBuilder
      case IntegerType => new IntColumnBuilder
      case DateType => new DateColumnBuilder
      case LongType => new LongColumnBuilder
      case TimestampType => new TimestampColumnBuilder
      case FloatType => new FloatColumnBuilder
      case DoubleType => new DoubleColumnBuilder
      case StringType => new StringColumnBuilder
      case BinaryType => new BinaryColumnBuilder
      case DecimalType.Fixed(precision, scale) if precision < 19 =>
        new FixedDecimalColumnBuilder(precision, scale)
      case other => new GenericColumnBuilder(other)
    }

    builder.initialize(initialSize, columnName, useCompression)
    builder
  }
}
