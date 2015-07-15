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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowConverter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.PlatformDependent
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

/**
 * Converts Rows into UnsafeRow format.
 */
abstract class UnsafeRowConverter {

  def numFields: Int

  /**
   * Compute the amount of space, in bytes, required to encode the given row.
   */
  def getSizeRequirement(row: InternalRow): Int

  /**
   * Convert the given row into UnsafeRow format.
   *
   * @param row the row to convert
   * @param unsafeRow the target UnsafeRow to write
   * @return the number of bytes written. This should be equal to `getSizeRequirement(row)`.
   */
  def writeRow(row: InternalRow, unsafeRow: UnsafeRow): Int
}

object UnsafeRowConverter {
  def apply(schema: StructType): UnsafeRowConverter = {
    apply(schema.fields.map(_.dataType))
  }
  def apply(fields: Array[DataType]): UnsafeRowConverter = {
    GenerateUnsafeRowConverter.generate(fields)
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
   * @param cursor the offset from the start of the unsafe row to the end of the row;
   *                     used for calculating where variable-length data should be written
   * @return the number of variable-length bytes written
   */
  def write(source: InternalRow, target: UnsafeRow, column: Int, cursor: Int): Int

  /**
   * Return the number of bytes that are needed to write this variable-length value.
   */
  def getSize(source: InternalRow, column: Int): Int
}


// ------------------------------------------------------------------------------------------------

private abstract class BytesUnsafeColumnWriter extends UnsafeColumnWriter {

  def getBytes(source: InternalRow, column: Int): Array[Byte]

  def getSize(source: InternalRow, column: Int): Int = {
    val numBytes = getBytes(source, column).length
    ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes)
  }

  protected[this] def isString: Boolean

  override def write(source: InternalRow, target: UnsafeRow, column: Int, cursor: Int): Int = {
    val offset = target.getBaseOffset + cursor
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
    val flag = if (isString) 1L << (UnsafeRow.OFFSET_BITS * 2) else 0
    target.setLong(column, flag | (cursor.toLong << UnsafeRow.OFFSET_BITS) | numBytes.toLong)
    ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes)
  }
}

private class StringUnsafeColumnWriter() extends BytesUnsafeColumnWriter {
  protected[this] def isString: Boolean = true
  def getBytes(source: InternalRow, column: Int): Array[Byte] = {
    source.getAs[UTF8String](column).getBytes
  }
}

private class BinaryUnsafeColumnWriter() extends BytesUnsafeColumnWriter {
  protected[this] def isString: Boolean = false
  def getBytes(source: InternalRow, column: Int): Array[Byte] = {
    source.getAs[Array[Byte]](column)
  }
}

private class ObjectUnsafeColumnWriter() extends UnsafeColumnWriter {
  def getSize(sourceRow: InternalRow, column: Int): Int = 0
  override def write(source: InternalRow, target: UnsafeRow, column: Int, cursor: Int): Int = {
    val obj = source.get(column)
    val idx = target.getPool.put(obj)
    target.setLong(column, - idx)
    0
  }
}
