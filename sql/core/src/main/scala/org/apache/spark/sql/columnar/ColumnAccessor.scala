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
import org.apache.spark.sql.execution.SparkSqlSerializer

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

private[sql] abstract class BasicColumnAccessor[T <: DataType, JvmType](buffer: ByteBuffer)
  extends ColumnAccessor {

  protected def initialize() {}

  def columnType: ColumnType[T, JvmType]

  def hasNext = buffer.hasRemaining

  def extractTo(row: MutableRow, ordinal: Int) {
    doExtractTo(row, ordinal)
  }

  protected def doExtractTo(row: MutableRow, ordinal: Int)

  protected def underlyingBuffer = buffer
}

private[sql] abstract class NativeColumnAccessor[T <: NativeType](
    buffer: ByteBuffer,
    val columnType: NativeColumnType[T])
  extends BasicColumnAccessor[T, T#JvmType](buffer)
  with NullableColumnAccessor

private[sql] class BooleanColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, BOOLEAN) {

  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setBoolean(ordinal, columnType.extract(buffer))
  }
}

private[sql] class IntColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, INT) {

  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setInt(ordinal, columnType.extract(buffer))
  }
}

private[sql] class ShortColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, SHORT) {

  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setShort(ordinal, columnType.extract(buffer))
  }
}

private[sql] class LongColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, LONG) {

  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setLong(ordinal, columnType.extract(buffer))
  }
}

private[sql] class ByteColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, BYTE) {

  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setByte(ordinal, columnType.extract(buffer))
  }
}

private[sql] class DoubleColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, DOUBLE) {

  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setDouble(ordinal, columnType.extract(buffer))
  }
}

private[sql] class FloatColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, FLOAT) {

  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setFloat(ordinal, columnType.extract(buffer))
  }
}

private[sql] class StringColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, STRING) {

  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setString(ordinal, columnType.extract(buffer))
  }
}

private[sql] class BinaryColumnAccessor(buffer: ByteBuffer)
  extends BasicColumnAccessor[BinaryType.type, Array[Byte]](buffer)
  with NullableColumnAccessor {

  def columnType = BINARY

  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row(ordinal) = columnType.extract(buffer)
  }
}

private[sql] class GenericColumnAccessor(buffer: ByteBuffer)
  extends BasicColumnAccessor[DataType, Array[Byte]](buffer)
  with NullableColumnAccessor {

  def columnType = GENERIC

  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    val serialized = columnType.extract(buffer)
    row(ordinal) = SparkSqlSerializer.deserialize[Any](serialized)
  }
}

private[sql] object ColumnAccessor {
  def apply(b: ByteBuffer): ColumnAccessor = {
    // The first 4 bytes in the buffer indicates the column type.
    val buffer = b.duplicate().order(ByteOrder.nativeOrder())
    val columnTypeId = buffer.getInt()

    columnTypeId match {
      case INT.typeId     => new IntColumnAccessor(buffer)
      case LONG.typeId    => new LongColumnAccessor(buffer)
      case FLOAT.typeId   => new FloatColumnAccessor(buffer)
      case DOUBLE.typeId  => new DoubleColumnAccessor(buffer)
      case BOOLEAN.typeId => new BooleanColumnAccessor(buffer)
      case BYTE.typeId    => new ByteColumnAccessor(buffer)
      case SHORT.typeId   => new ShortColumnAccessor(buffer)
      case STRING.typeId  => new StringColumnAccessor(buffer)
      case BINARY.typeId  => new BinaryColumnAccessor(buffer)
      case GENERIC.typeId => new GenericColumnAccessor(buffer)
    }
  }
}
