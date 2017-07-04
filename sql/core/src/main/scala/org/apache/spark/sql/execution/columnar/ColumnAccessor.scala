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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.execution.columnar.compression.CompressibleColumnAccessor
import org.apache.spark.sql.types._

/**
 * An `Iterator` like trait used to extract values from columnar byte buffer. When a value is
 * extracted from the buffer, instead of directly returning it, the value is set into some field of
 * a [[InternalRow]]. In this way, boxing cost can be avoided by leveraging the setter methods
 * for primitive values provided by [[InternalRow]].
 */
private[columnar] trait ColumnAccessor {
  initialize()

  protected def initialize()

  def hasNext: Boolean

  def extractTo(row: InternalRow, ordinal: Int): Unit

  protected def underlyingBuffer: ByteBuffer
}

private[columnar] abstract class BasicColumnAccessor[JvmType](
    protected val buffer: ByteBuffer,
    protected val columnType: ColumnType[JvmType])
  extends ColumnAccessor {

  protected def initialize() {}

  override def hasNext: Boolean = buffer.hasRemaining

  override def extractTo(row: InternalRow, ordinal: Int): Unit = {
    extractSingle(row, ordinal)
  }

  def extractSingle(row: InternalRow, ordinal: Int): Unit = {
    columnType.extract(buffer, row, ordinal)
  }

  protected def underlyingBuffer = buffer
}

private[columnar] class NullColumnAccessor(buffer: ByteBuffer)
  extends BasicColumnAccessor[Any](buffer, NULL)
  with NullableColumnAccessor

private[columnar] abstract class NativeColumnAccessor[T <: AtomicType](
    override protected val buffer: ByteBuffer,
    override protected val columnType: NativeColumnType[T])
  extends BasicColumnAccessor(buffer, columnType)
  with NullableColumnAccessor
  with CompressibleColumnAccessor[T]

private[columnar] class BooleanColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, BOOLEAN)

private[columnar] class ByteColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, BYTE)

private[columnar] class ShortColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, SHORT)

private[columnar] class IntColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, INT)

private[columnar] class LongColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, LONG)

private[columnar] class FloatColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, FLOAT)

private[columnar] class DoubleColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, DOUBLE)

private[columnar] class StringColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, STRING)

private[columnar] class BinaryColumnAccessor(buffer: ByteBuffer)
  extends BasicColumnAccessor[Array[Byte]](buffer, BINARY)
  with NullableColumnAccessor

private[columnar] class CompactDecimalColumnAccessor(buffer: ByteBuffer, dataType: DecimalType)
  extends NativeColumnAccessor(buffer, COMPACT_DECIMAL(dataType))

private[columnar] class DecimalColumnAccessor(buffer: ByteBuffer, dataType: DecimalType)
  extends BasicColumnAccessor[Decimal](buffer, LARGE_DECIMAL(dataType))
  with NullableColumnAccessor

private[columnar] class StructColumnAccessor(buffer: ByteBuffer, dataType: StructType)
  extends BasicColumnAccessor[UnsafeRow](buffer, STRUCT(dataType))
  with NullableColumnAccessor

private[columnar] class ArrayColumnAccessor(buffer: ByteBuffer, dataType: ArrayType)
  extends BasicColumnAccessor[UnsafeArrayData](buffer, ARRAY(dataType))
  with NullableColumnAccessor

private[columnar] class MapColumnAccessor(buffer: ByteBuffer, dataType: MapType)
  extends BasicColumnAccessor[UnsafeMapData](buffer, MAP(dataType))
  with NullableColumnAccessor

private[columnar] object ColumnAccessor {
  @tailrec
  def apply(dataType: DataType, buffer: ByteBuffer): ColumnAccessor = {
    val buf = buffer.order(ByteOrder.nativeOrder)

    dataType match {
      case NullType => new NullColumnAccessor(buf)
      case BooleanType => new BooleanColumnAccessor(buf)
      case ByteType => new ByteColumnAccessor(buf)
      case ShortType => new ShortColumnAccessor(buf)
      case IntegerType | DateType => new IntColumnAccessor(buf)
      case LongType | TimestampType => new LongColumnAccessor(buf)
      case FloatType => new FloatColumnAccessor(buf)
      case DoubleType => new DoubleColumnAccessor(buf)
      case StringType => new StringColumnAccessor(buf)
      case BinaryType => new BinaryColumnAccessor(buf)
      case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS =>
        new CompactDecimalColumnAccessor(buf, dt)
      case dt: DecimalType => new DecimalColumnAccessor(buf, dt)
      case struct: StructType => new StructColumnAccessor(buf, struct)
      case array: ArrayType => new ArrayColumnAccessor(buf, array)
      case map: MapType => new MapColumnAccessor(buf, map)
      case udt: UserDefinedType[_] => ColumnAccessor(udt.sqlType, buffer)
      case other =>
        throw new Exception(s"not support type: $other")
    }
  }
}
