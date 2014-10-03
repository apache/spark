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

import java.nio.{ByteOrder, ByteBuffer}

import org.apache.spark.sql.catalyst.types.{BinaryType, NativeType, DataType}
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.columnar.compression.CompressibleColumnAccessor

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

private[sql] abstract class BasicColumnAccessor[T <: DataType, JvmType](
    protected val buffer: ByteBuffer,
    protected val columnType: ColumnType[T, JvmType])
  extends ColumnAccessor {

  protected def initialize() {}

  def hasNext = buffer.hasRemaining

  def extractTo(row: MutableRow, ordinal: Int): Unit = {
    extractSingle(row, ordinal)
  }

  def extractSingle(row: MutableRow, ordinal: Int): Unit = {
    columnType.extract(buffer, row, ordinal)
  }

  protected def underlyingBuffer = buffer
}

private[sql] abstract class NativeColumnAccessor[T <: NativeType](
    override protected val buffer: ByteBuffer,
    override protected val columnType: NativeColumnType[T])
  extends BasicColumnAccessor(buffer, columnType)
  with NullableColumnAccessor
  with CompressibleColumnAccessor[T]

private[sql] class BooleanColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, BOOLEAN)

private[sql] class IntColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, INT)

private[sql] class ShortColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, SHORT)

private[sql] class LongColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, LONG)

private[sql] class ByteColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, BYTE)

private[sql] class DoubleColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, DOUBLE)

private[sql] class FloatColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, FLOAT)

private[sql] class StringColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, STRING)

private[sql] class TimestampColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, TIMESTAMP)

private[sql] class BinaryColumnAccessor(buffer: ByteBuffer)
  extends BasicColumnAccessor[BinaryType.type, Array[Byte]](buffer, BINARY)
  with NullableColumnAccessor

private[sql] class GenericColumnAccessor(buffer: ByteBuffer)
  extends BasicColumnAccessor[DataType, Array[Byte]](buffer, GENERIC)
  with NullableColumnAccessor

private[sql] object ColumnAccessor {
  def apply(buffer: ByteBuffer): ColumnAccessor = {
    val dup = buffer.duplicate().order(ByteOrder.nativeOrder)
    // The first 4 bytes in the buffer indicate the column type.
    val columnTypeId = dup.getInt()

    columnTypeId match {
      case INT.typeId       => new IntColumnAccessor(dup)
      case LONG.typeId      => new LongColumnAccessor(dup)
      case FLOAT.typeId     => new FloatColumnAccessor(dup)
      case DOUBLE.typeId    => new DoubleColumnAccessor(dup)
      case BOOLEAN.typeId   => new BooleanColumnAccessor(dup)
      case BYTE.typeId      => new ByteColumnAccessor(dup)
      case SHORT.typeId     => new ShortColumnAccessor(dup)
      case STRING.typeId    => new StringColumnAccessor(dup)
      case TIMESTAMP.typeId => new TimestampColumnAccessor(dup)
      case BINARY.typeId    => new BinaryColumnAccessor(dup)
      case GENERIC.typeId   => new GenericColumnAccessor(dup)
    }
  }
}
