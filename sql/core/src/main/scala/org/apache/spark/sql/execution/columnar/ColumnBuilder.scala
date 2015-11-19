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

package org.apache.spark.sql.execution.columnar

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.columnar.ColumnBuilder._
import org.apache.spark.sql.execution.columnar.compression.{AllCompressionSchemes, CompressibleColumnBuilder}
import org.apache.spark.sql.types._

private[columnar] trait ColumnBuilder {
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

private[columnar] class BasicColumnBuilder[JvmType](
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

    buffer = ByteBuffer.allocate(size * columnType.defaultSize)
    buffer.order(ByteOrder.nativeOrder())
  }

  override def appendFrom(row: InternalRow, ordinal: Int): Unit = {
    buffer = ensureFreeSpace(buffer, columnType.actualSize(row, ordinal))
    columnType.append(row, ordinal, buffer)
  }

  override def build(): ByteBuffer = {
    if (buffer.capacity() > buffer.position() * 1.1) {
      // trim the buffer
      buffer = ByteBuffer
        .allocate(buffer.position())
        .order(ByteOrder.nativeOrder())
        .put(buffer.array(), 0, buffer.position())
    }
    buffer.flip().asInstanceOf[ByteBuffer]
  }
}

private[columnar] class NullColumnBuilder
  extends BasicColumnBuilder[Any](new ObjectColumnStats(NullType), NULL)
  with NullableColumnBuilder

private[columnar] abstract class ComplexColumnBuilder[JvmType](
    columnStats: ColumnStats,
    columnType: ColumnType[JvmType])
  extends BasicColumnBuilder[JvmType](columnStats, columnType)
  with NullableColumnBuilder

private[columnar] abstract class NativeColumnBuilder[T <: AtomicType](
    override val columnStats: ColumnStats,
    override val columnType: NativeColumnType[T])
  extends BasicColumnBuilder[T#InternalType](columnStats, columnType)
  with NullableColumnBuilder
  with AllCompressionSchemes
  with CompressibleColumnBuilder[T]

private[columnar]
class BooleanColumnBuilder extends NativeColumnBuilder(new BooleanColumnStats, BOOLEAN)

private[columnar]
class ByteColumnBuilder extends NativeColumnBuilder(new ByteColumnStats, BYTE)

private[columnar] class ShortColumnBuilder extends NativeColumnBuilder(new ShortColumnStats, SHORT)

private[columnar] class IntColumnBuilder extends NativeColumnBuilder(new IntColumnStats, INT)

private[columnar] class LongColumnBuilder extends NativeColumnBuilder(new LongColumnStats, LONG)

private[columnar] class FloatColumnBuilder extends NativeColumnBuilder(new FloatColumnStats, FLOAT)

private[columnar]
class DoubleColumnBuilder extends NativeColumnBuilder(new DoubleColumnStats, DOUBLE)

private[columnar]
class StringColumnBuilder extends NativeColumnBuilder(new StringColumnStats, STRING)

private[columnar]
class BinaryColumnBuilder extends ComplexColumnBuilder(new BinaryColumnStats, BINARY)

private[columnar] class CompactDecimalColumnBuilder(dataType: DecimalType)
  extends NativeColumnBuilder(new DecimalColumnStats(dataType), COMPACT_DECIMAL(dataType))

private[columnar] class DecimalColumnBuilder(dataType: DecimalType)
  extends ComplexColumnBuilder(new DecimalColumnStats(dataType), LARGE_DECIMAL(dataType))

private[columnar] class StructColumnBuilder(dataType: StructType)
  extends ComplexColumnBuilder(new ObjectColumnStats(dataType), STRUCT(dataType))

private[columnar] class ArrayColumnBuilder(dataType: ArrayType)
  extends ComplexColumnBuilder(new ObjectColumnStats(dataType), ARRAY(dataType))

private[columnar] class MapColumnBuilder(dataType: MapType)
  extends ComplexColumnBuilder(new ObjectColumnStats(dataType), MAP(dataType))

private[columnar] object ColumnBuilder {
  val DEFAULT_INITIAL_BUFFER_SIZE = 128 * 1024
  val MAX_BATCH_SIZE_IN_BYTE = 4 * 1024 * 1024L

  private[columnar] def ensureFreeSpace(orig: ByteBuffer, size: Int) = {
    if (orig.remaining >= size) {
      orig
    } else {
      // grow in steps of initial size
      val capacity = orig.capacity()
      val newSize = capacity + size.max(capacity)
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
      case NullType => new NullColumnBuilder
      case BooleanType => new BooleanColumnBuilder
      case ByteType => new ByteColumnBuilder
      case ShortType => new ShortColumnBuilder
      case IntegerType | DateType => new IntColumnBuilder
      case LongType | TimestampType => new LongColumnBuilder
      case FloatType => new FloatColumnBuilder
      case DoubleType => new DoubleColumnBuilder
      case StringType => new StringColumnBuilder
      case BinaryType => new BinaryColumnBuilder
      case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS =>
        new CompactDecimalColumnBuilder(dt)
      case dt: DecimalType => new DecimalColumnBuilder(dt)
      case struct: StructType => new StructColumnBuilder(struct)
      case array: ArrayType => new ArrayColumnBuilder(array)
      case map: MapType => new MapColumnBuilder(map)
      case udt: UserDefinedType[_] =>
        return apply(udt.sqlType, initialSize, columnName, useCompression)
      case other =>
        throw new Exception(s"not suppported type: $other")
    }

    builder.initialize(initialSize, columnName, useCompression)
    builder
  }
}
