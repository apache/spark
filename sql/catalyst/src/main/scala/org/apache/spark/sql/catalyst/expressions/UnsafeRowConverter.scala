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

import org.apache.spark.unsafe.PlatformDependent
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

/**
 * Function for writing a column into an UnsafeRow.
 */
private abstract class UnsafeColumnWriter[T] {
  /**
   * Write a value into an UnsafeRow.
   *
   * @param target a pointer to the converted unsafe row
   * @param value the value to write
   * @param cursor the offset from the start of the unsafe row to the end of the row;
   *                     used for calculating where variable-length data should be written
   * @return the number of variable-length bytes written
   */
  def write(target: UnsafeRow, value: T, column: Int, cursor: Int): Int

  def getSize(value: T): Int = 0
}


private abstract class BytesUnsafeColumnWriter[T] extends UnsafeColumnWriter[T] {

  def getBytes(value: T): Array[Byte]

  override def getSize(value: T): Int = {
    val numBytes = getBytes(value).length
    ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes)
  }

  protected[this] def isString: Boolean

  override def write(target: UnsafeRow, value: T, column: Int, cursor: Int): Int = {
    val offset = target.getBaseOffset + cursor
    val bytes = getBytes(value)
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

private class StringUnsafeColumnWriter() extends BytesUnsafeColumnWriter[UTF8String] {
  protected[this] def isString: Boolean = true
  def getBytes(value: UTF8String): Array[Byte] = {
    value.getBytes
  }
}

private class BinaryUnsafeColumnWriter() extends BytesUnsafeColumnWriter[Array[Byte]] {
  protected[this] def isString: Boolean = false
  def getBytes(value: Array[Byte]): Array[Byte] = {
    value
  }
}

private class ObjectUnsafeColumnWriter() extends UnsafeColumnWriter[Any] {
  override def write(target: UnsafeRow, value: Any, column: Int, cursor: Int): Int = {
    val idx = target.getPool.put(value)
    target.setLong(column, - idx)
    0
  }
}
