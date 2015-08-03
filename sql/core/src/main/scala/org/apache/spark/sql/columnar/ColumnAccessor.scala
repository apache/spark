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

import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.columnar.compression.CompressibleColumnAccessor
import org.apache.spark.sql.types._

/**
 * An `Iterator` like trait used to extract values from columnar byte buffer. When a value is
 * extracted from the buffer, instead of directly returning it, the value is set into some field of
 * a [[MutableRow]]. In this way, boxing cost can be avoided by leveraging the setter methods
 * for primitive values provided by [[MutableRow]].
 */
private[sql] trait ColumnAccessor {
  initialize()

  protected def initialize()

  def hasNext: Boolean

  def extractTo(row: MutableRow, ordinal: Int)

  protected def underlyingBuffer: ByteBuffer
}

private[sql] abstract class BasicColumnAccessor[JvmType](
    protected val buffer: ByteBuffer,
    protected val columnType: ColumnType[JvmType])
  extends ColumnAccessor {

  protected def initialize() {}

  override def hasNext: Boolean = buffer.hasRemaining

  override def extractTo(row: MutableRow, ordinal: Int): Unit = {
    extractSingle(row, ordinal)
  }

  def extractSingle(row: MutableRow, ordinal: Int): Unit = {
    columnType.extract(buffer, row, ordinal)
  }

  protected def underlyingBuffer = buffer
}

private[sql] abstract class NativeColumnAccessor[T <: AtomicType](
    override protected val buffer: ByteBuffer,
    override protected val columnType: NativeColumnType[T])
  extends BasicColumnAccessor(buffer, columnType)
  with NullableColumnAccessor
  with CompressibleColumnAccessor[T]

private[sql] class BooleanColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, BOOLEAN)

private[sql] class ByteColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, BYTE)

private[sql] class ShortColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, SHORT)

private[sql] class IntColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, INT)

private[sql] class LongColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, LONG)

private[sql] class FloatColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, FLOAT)

private[sql] class DoubleColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, DOUBLE)

private[sql] class StringColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, STRING)

private[sql] class BinaryColumnAccessor(buffer: ByteBuffer)
  extends BasicColumnAccessor[Array[Byte]](buffer, BINARY)
  with NullableColumnAccessor

private[sql] class FixedDecimalColumnAccessor(buffer: ByteBuffer, precision: Int, scale: Int)
  extends NativeColumnAccessor(buffer, FIXED_DECIMAL(precision, scale))

private[sql] class GenericColumnAccessor(buffer: ByteBuffer, dataType: DataType)
  extends BasicColumnAccessor[Array[Byte]](buffer, GENERIC(dataType))
  with NullableColumnAccessor

private[sql] class DateColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, DATE)

private[sql] class TimestampColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, TIMESTAMP)

private[sql] object ColumnAccessor {
  def apply(dataType: DataType, buffer: ByteBuffer): ColumnAccessor = {
    val dup = buffer.duplicate().order(ByteOrder.nativeOrder)

    // The first 4 bytes in the buffer indicate the column type.  This field is not used now,
    // because we always know the data type of the column ahead of time.
    dup.getInt()

    dataType match {
      case BooleanType => new BooleanColumnAccessor(dup)
      case ByteType => new ByteColumnAccessor(dup)
      case ShortType => new ShortColumnAccessor(dup)
      case IntegerType => new IntColumnAccessor(dup)
      case DateType => new DateColumnAccessor(dup)
      case LongType => new LongColumnAccessor(dup)
      case TimestampType => new TimestampColumnAccessor(dup)
      case FloatType => new FloatColumnAccessor(dup)
      case DoubleType => new DoubleColumnAccessor(dup)
      case StringType => new StringColumnAccessor(dup)
      case BinaryType => new BinaryColumnAccessor(dup)
      case DecimalType.Fixed(precision, scale) if precision < 19 =>
        new FixedDecimalColumnAccessor(dup, precision, scale)
      case other => new GenericColumnAccessor(dup, other)
    }
  }
}
