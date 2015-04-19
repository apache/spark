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

/** Write a column into an UnsafeRow */
private abstract class UnsafeColumnWriter[T] {
  /**
   * Write a value into an UnsafeRow.
   *
   * @param value the value to write
   * @param columnNumber what column to write it to
   * @param row a pointer to the unsafe row
   * @param baseObject
   * @param baseOffset
   * @param appendCursor the offset from the start of the unsafe row to the end of the row;
   *                     used for calculating where variable-length data should be written
   * @return the number of variable-length bytes written
   */
  def write(
      value: T,
      columnNumber: Int,
      row: UnsafeRow,
      baseObject: Object,
      baseOffset: Long,
      appendCursor: Int): Int

  /**
   * Return the number of bytes that are needed to write this variable-length value.
   */
  def getSize(value: T): Int
}

private object UnsafeColumnWriter {
  def forType(dataType: DataType): UnsafeColumnWriter[_] = {
    dataType match {
      case IntegerType => IntUnsafeColumnWriter
      case LongType => LongUnsafeColumnWriter
      case StringType => StringUnsafeColumnWriter
      case _ => throw new UnsupportedOperationException()
    }
  }
}

private class StringUnsafeColumnWriter private() extends UnsafeColumnWriter[UTF8String] {
  def getSize(value: UTF8String): Int = {
    // round to nearest word
    val numBytes = value.getBytes.length
    8 + ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes)
  }

  override def write(
      value: UTF8String,
      columnNumber: Int,
      row: UnsafeRow,
      baseObject: Object,
      baseOffset: Long,
      appendCursor: Int): Int = {
    val numBytes = value.getBytes.length
    PlatformDependent.UNSAFE.putLong(baseObject, baseOffset + appendCursor, numBytes)
    PlatformDependent.copyMemory(
      value.getBytes,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      baseObject,
      baseOffset + appendCursor + 8,
      numBytes
    )
    row.setLong(columnNumber, appendCursor)
    8 + ((numBytes / 8) + (if (numBytes % 8 == 0) 0 else 1)) * 8
  }
}
private object StringUnsafeColumnWriter extends StringUnsafeColumnWriter

private abstract class PrimitiveUnsafeColumnWriter[T] extends UnsafeColumnWriter[T] {
  def getSize(value: T): Int = 0
}

private class IntUnsafeColumnWriter private() extends PrimitiveUnsafeColumnWriter[Int] {
  override def write(
      value: Int,
      columnNumber: Int,
      row: UnsafeRow,
      baseObject: Object,
      baseOffset: Long,
      appendCursor: Int): Int = {
    row.setInt(columnNumber, value)
    0
  }
}
private object IntUnsafeColumnWriter extends IntUnsafeColumnWriter

private class LongUnsafeColumnWriter private() extends PrimitiveUnsafeColumnWriter[Long] {
  override def write(
      value: Long,
      columnNumber: Int,
      row: UnsafeRow,
      baseObject: Object,
      baseOffset: Long,
      appendCursor: Int): Int = {
    row.setLong(columnNumber, value)
    0
  }
}
private case object LongUnsafeColumnWriter extends LongUnsafeColumnWriter


class UnsafeRowConverter(fieldTypes: Array[DataType]) {

  private[this] val writers: Array[UnsafeColumnWriter[Any]] = {
    fieldTypes.map(t => UnsafeColumnWriter.forType(t).asInstanceOf[UnsafeColumnWriter[Any]])
  }

  def getSizeRequirement(row: Row): Int = {
    var fieldNumber = 0
    var variableLengthFieldSize: Int = 0
    while (fieldNumber < writers.length) {
      if (!row.isNullAt(fieldNumber)) {
        variableLengthFieldSize += writers(fieldNumber).getSize(row.get(fieldNumber))

      }
      fieldNumber += 1
    }
    (8 * fieldTypes.length) + UnsafeRow.calculateBitSetWidthInBytes(fieldTypes.length) + variableLengthFieldSize
  }

  def writeRow(row: Row, baseObject: Object, baseOffset: Long): Long = {
    val unsafeRow = new UnsafeRow()
    unsafeRow.set(baseObject, baseOffset, writers.length, null) // TODO: schema?
    var fieldNumber = 0
    var appendCursor: Int =
      (8 * fieldTypes.length) + UnsafeRow.calculateBitSetWidthInBytes(fieldTypes.length)
    while (fieldNumber < writers.length) {
      if (row.isNullAt(fieldNumber)) {
        unsafeRow.setNullAt(fieldNumber)
        // TODO: type-specific null value writing?
      } else {
        appendCursor += writers(fieldNumber).write(
          row.get(fieldNumber),
          fieldNumber,
          unsafeRow,
          baseObject,
          baseOffset,
          appendCursor)
      }
      fieldNumber += 1
    }
    appendCursor
  }

}