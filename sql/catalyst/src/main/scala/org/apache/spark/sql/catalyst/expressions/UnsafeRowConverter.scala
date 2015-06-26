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

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.PlatformDependent
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

/**
 * Converts Rows into UnsafeRow format. This class is NOT thread-safe.
 *
 * @param fieldTypes the data types of the row's columns.
 */
class UnsafeRowConverter(fieldTypes: Array[DataType]) {

  def this(schema: StructType) {
    this(schema.fields.map(_.dataType))
  }

  /** Re-used pointer to the unsafe row being written */
  private[this] val unsafeRow = new UnsafeRow()

  /** Functions for encoding each column */
  private[this] val writers: Array[UnsafeColumnWriter] = {
    fieldTypes.map(t => UnsafeColumnWriter.forType(t))
  }

  /** The size, in bytes, of the fixed-length portion of the row, including the null bitmap */
  private[this] val fixedLengthSize: Int =
    (8 * fieldTypes.length) + UnsafeRow.calculateBitSetWidthInBytes(fieldTypes.length)

  /**
   * Compute the amount of space, in bytes, required to encode the given row.
   */
  def getSizeRequirement(row: InternalRow): Int = {
    var fieldNumber = 0
    var variableLengthFieldSize: Int = 0
    while (fieldNumber < writers.length) {
      if (!row.isNullAt(fieldNumber)) {
        variableLengthFieldSize += writers(fieldNumber).getSize(row, fieldNumber)
      }
      fieldNumber += 1
    }
    fixedLengthSize + variableLengthFieldSize
  }

  /**
   * Convert the given row into UnsafeRow format.
   *
   * @param row the row to convert
   * @param baseObject the base object of the destination address
   * @param baseOffset the base offset of the destination address
   * @return the number of bytes written. This should be equal to `getSizeRequirement(row)`.
   */
  def writeRow(row: InternalRow, baseObject: Object, baseOffset: Long): Int = {
    unsafeRow.pointTo(baseObject, baseOffset, writers.length, null)

    if (writers.length > 0) {
      // zero-out the bitset
      var n = writers.length / 64
      while (n >= 0) {
        PlatformDependent.UNSAFE.putLong(
          unsafeRow.getBaseObject,
          unsafeRow.getBaseOffset + n * 8,
          0L)
        n -= 1
      }
    }

    var fieldNumber = 0
    var appendCursor: Int = fixedLengthSize
    while (fieldNumber < writers.length) {
      if (row.isNullAt(fieldNumber)) {
        unsafeRow.setNullAt(fieldNumber)
      } else {
        appendCursor += writers(fieldNumber).write(row, unsafeRow, fieldNumber, appendCursor)
      }
      fieldNumber += 1
    }
    appendCursor
  }

}

/**
 * Function for writing a column into an UnsafeRow.
 */
private abstract class UnsafeColumnWriter {
  /**
   * Write a value into an UnsafeRow.
   *
   * @param source the row being converted
   * @param target a pointer to the converted unsafe row
   * @param column the column to write
   * @param appendCursor the offset from the start of the unsafe row to the end of the row;
   *                     used for calculating where variable-length data should be written
   * @return the number of variable-length bytes written
   */
  def write(source: InternalRow, target: UnsafeRow, column: Int, appendCursor: Int): Int

  /**
   * Return the number of bytes that are needed to write this variable-length value.
   */
  def getSize(source: InternalRow, column: Int): Int
}

private object UnsafeColumnWriter {

  def forType(dataType: DataType): UnsafeColumnWriter = {
    dataType match {
      case NullType => NullUnsafeColumnWriter
      case BooleanType => BooleanUnsafeColumnWriter
      case ByteType => ByteUnsafeColumnWriter
      case ShortType => ShortUnsafeColumnWriter
      case IntegerType => IntUnsafeColumnWriter
      case LongType => LongUnsafeColumnWriter
      case FloatType => FloatUnsafeColumnWriter
      case DoubleType => DoubleUnsafeColumnWriter
      case StringType => StringUnsafeColumnWriter
      case BinaryType => BinaryUnsafeColumnWriter
      case DateType => IntUnsafeColumnWriter
      case TimestampType => LongUnsafeColumnWriter
      case t =>
        throw new UnsupportedOperationException(s"Do not know how to write columns of type $t")
    }
  }
}

// ------------------------------------------------------------------------------------------------

private object NullUnsafeColumnWriter extends NullUnsafeColumnWriter
private object BooleanUnsafeColumnWriter extends BooleanUnsafeColumnWriter
private object ByteUnsafeColumnWriter extends ByteUnsafeColumnWriter
private object ShortUnsafeColumnWriter extends ShortUnsafeColumnWriter
private object IntUnsafeColumnWriter extends IntUnsafeColumnWriter
private object LongUnsafeColumnWriter extends LongUnsafeColumnWriter
private object FloatUnsafeColumnWriter extends FloatUnsafeColumnWriter
private object DoubleUnsafeColumnWriter extends DoubleUnsafeColumnWriter
private object StringUnsafeColumnWriter extends StringUnsafeColumnWriter
private object BinaryUnsafeColumnWriter extends BinaryUnsafeColumnWriter

private abstract class PrimitiveUnsafeColumnWriter extends UnsafeColumnWriter {
  // Primitives don't write to the variable-length region:
  def getSize(sourceRow: InternalRow, column: Int): Int = 0
}

private class NullUnsafeColumnWriter private() extends PrimitiveUnsafeColumnWriter {
  override def write(
      source: InternalRow,
      target: UnsafeRow,
      column: Int,
      appendCursor: Int): Int = {
    target.setNullAt(column)
    0
  }
}

private class BooleanUnsafeColumnWriter private() extends PrimitiveUnsafeColumnWriter {
  override def write(
      source: InternalRow,
      target: UnsafeRow,
      column: Int,
      appendCursor: Int): Int = {
    target.setBoolean(column, source.getBoolean(column))
    0
  }
}

private class ByteUnsafeColumnWriter private() extends PrimitiveUnsafeColumnWriter {
  override def write(
      source: InternalRow,
      target: UnsafeRow,
      column: Int,
      appendCursor: Int): Int = {
    target.setByte(column, source.getByte(column))
    0
  }
}

private class ShortUnsafeColumnWriter private() extends PrimitiveUnsafeColumnWriter {
  override def write(
      source: InternalRow,
      target: UnsafeRow,
      column: Int,
      appendCursor: Int): Int = {
    target.setShort(column, source.getShort(column))
    0
  }
}

private class IntUnsafeColumnWriter private() extends PrimitiveUnsafeColumnWriter {
  override def write(
      source: InternalRow,
      target: UnsafeRow,
      column: Int,
      appendCursor: Int): Int = {
    target.setInt(column, source.getInt(column))
    0
  }
}

private class LongUnsafeColumnWriter private() extends PrimitiveUnsafeColumnWriter {
  override def write(
      source: InternalRow,
      target: UnsafeRow,
      column: Int,
      appendCursor: Int): Int = {
    target.setLong(column, source.getLong(column))
    0
  }
}

private class FloatUnsafeColumnWriter private() extends PrimitiveUnsafeColumnWriter {
  override def write(
      source: InternalRow,
      target: UnsafeRow,
      column: Int,
      appendCursor: Int): Int = {
    target.setFloat(column, source.getFloat(column))
    0
  }
}

private class DoubleUnsafeColumnWriter private() extends PrimitiveUnsafeColumnWriter {
  override def write(
      source: InternalRow,
      target: UnsafeRow,
      column: Int,
      appendCursor: Int): Int = {
    target.setDouble(column, source.getDouble(column))
    0
  }
}

private abstract class BytesUnsafeColumnWriter extends UnsafeColumnWriter {

  def getBytes(source: InternalRow, column: Int): Array[Byte]

  def getSize(source: InternalRow, column: Int): Int = {
    val numBytes = getBytes(source, column).length
    ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes)
  }

  override def write(
      source: InternalRow,
      target: UnsafeRow,
      column: Int,
      appendCursor: Int): Int = {
    val offset = target.getBaseOffset + appendCursor
    val bytes = getBytes(source, column)
    val numBytes = bytes.length
    if ((numBytes & 0x07) > 0) {
      // zero-out the padding bytes
      PlatformDependent.UNSAFE.putLong(target.getBaseObject, offset + ((numBytes >> 3) << 3), 0L)
    }
    PlatformDependent.copyMemory(
      bytes,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      target.getBaseObject,
      offset,
      numBytes
    )
    target.setLong(column, (appendCursor.toLong << 32L) | numBytes.toLong)
    ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes)
  }
}

private class StringUnsafeColumnWriter private() extends BytesUnsafeColumnWriter {
  def getBytes(source: InternalRow, column: Int): Array[Byte] = {
    source.getAs[UTF8String](column).getBytes
  }
}

private class BinaryUnsafeColumnWriter private() extends BytesUnsafeColumnWriter {
  def getBytes(source: InternalRow, column: Int): Array[Byte] = {
    source.getAs[Array[Byte]](column)
  }
}
