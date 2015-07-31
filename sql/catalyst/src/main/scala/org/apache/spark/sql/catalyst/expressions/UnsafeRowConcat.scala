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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.PlatformDependent
import org.apache.spark.unsafe.array.ByteArrayMethods

/**
 * Concatenate an UnsafeRow of leftSchema with another UnsafeRow of rightSchema.
 *
 * leftSchema and rightSchema are used to determine each field's dataType,
 * thus controls the copy behaviour for each field.
 */
abstract class UnsafeRowConcat {
  def leftSchema: StructType
  def rightSchema: StructType
  def concat(left: UnsafeRow, right: UnsafeRow): UnsafeRow
}

class InterpretedUnsafeRowConcat(
    val leftSchema: StructType,
    val rightSchema: StructType) extends UnsafeRowConcat {

  private[this] val finalSchema = leftSchema.merge(rightSchema)
  private[this] val numFields = finalSchema.length
  private[this] val bitSetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(numFields)
  private[this] val leftVarFields = UnsafeRowConcat.getVarLengthFields(leftSchema)
  private[this] val rightVarFields = UnsafeRowConcat.getVarLengthFields(rightSchema)

  def concat(left: UnsafeRow, right: UnsafeRow): UnsafeRow = {
    val lBaseObject = left.getBaseObject
    val lBaseOffset = left.getBaseOffset
    val lNumFields = left.numFields
    val lSizeInBytes = left.getSizeInBytes
    val lBitSetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(lNumFields)

    val rBaseObject = right.getBaseObject
    val rBaseOffset = right.getBaseOffset
    val rNumFields = right.numFields
    val rSizeInBytes = right.getSizeInBytes
    val rBitSetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(rNumFields)

    val size = bitSetWidthInBytes +
      lSizeInBytes - lBitSetWidthInBytes + rSizeInBytes - rBitSetWidthInBytes
    val data = new Array[Byte](size)
    val baseOffset = PlatformDependent.BYTE_ARRAY_OFFSET

    var cursor = baseOffset
    var varFieldsOffset = bitSetWidthInBytes + (lNumFields + rNumFields) * 8
    var lCursor = lBaseOffset
    var rCursor = rBaseOffset

    // concat null bit set
    val remainingBitCnt = (lNumFields & 0x3f)
    val emptyBitCnt = 64 - remainingBitCnt
    if (remainingBitCnt == 0) {
      // no remaining bits in left, we could insert the null bit sets one after another
      PlatformDependent.copyMemory(lBaseObject, lCursor, data, cursor, lBitSetWidthInBytes)
      cursor += lBitSetWidthInBytes
      lCursor += lBitSetWidthInBytes
      PlatformDependent.copyMemory(rBaseObject, rCursor, data, cursor, rBitSetWidthInBytes)
      cursor += rBitSetWidthInBytes
      rCursor += rBitSetWidthInBytes
    } else {
      // left remains `emptyBitCnt` bit slot in its last word, therefore, we should shift right
      // words one by one to pad the empty slots.

      // copy left n-1 words first
      if (lNumFields > 64) {
        PlatformDependent.copyMemory(lBaseObject, lCursor, data, cursor, lBitSetWidthInBytes - 8)
        cursor += lBitSetWidthInBytes - 8
        lCursor += lBitSetWidthInBytes - 8
      }

      // shift the right word one by one to pad the previous empty null bit slot.
      var remainingBits =
        PlatformDependent.UNSAFE.getLong(lBaseObject, lCursor) & ((1L << remainingBitCnt) - 1)
      lCursor += 8
      val lowerBitsMask = (1L << emptyBitCnt) - 1
      val higherBitsMask = -1L ^ lowerBitsMask

      var i = 0
      while (i < rBitSetWidthInBytes / 8) {
        val word = PlatformDependent.UNSAFE.getLong(rBaseObject, rCursor)
        val lowerBits = word & lowerBitsMask
        val higherBits = word & higherBitsMask
        val newWord = lowerBits << remainingBitCnt | remainingBits
        remainingBits = higherBits >> emptyBitCnt
        PlatformDependent.UNSAFE.putLong(data, cursor, newWord)
        cursor += 8
        rCursor += 8
        i += 1
      }

      // if last right word has remaining null bits, just append it at last.
      if (rNumFields > emptyBitCnt && (numFields & 0x3f) != 0) {
        PlatformDependent.UNSAFE.putLong(data, cursor, remainingBits)
        cursor += 8
      }
    }

    // append the left data first
    PlatformDependent.copyMemory(
      lBaseObject, lBaseOffset + lBitSetWidthInBytes, data, cursor, lNumFields * 8)
    var curIter = leftVarFields.iterator
    // append var-length fields and fix its index
    while (curIter.hasNext) {
      val fieldIdx = curIter.next
      val offsetAndSize = left.getLong(fieldIdx)
      val preOffset = (offsetAndSize >> 32).toInt
      val fieldSize = (offsetAndSize & ((1L << 32) - 1)).toInt
      PlatformDependent.UNSAFE.putLong(
        data, cursor + fieldIdx * 8, (varFieldsOffset.toLong << 32) | (fieldSize.toLong))
      PlatformDependent.copyMemory(
        lBaseObject, lBaseOffset + preOffset, data, baseOffset + varFieldsOffset, fieldSize)
      varFieldsOffset += ByteArrayMethods.roundNumberOfBytesToNearestWord(fieldSize)
    }
    cursor += lNumFields * 8

    // now comes the right data
    PlatformDependent.copyMemory(
      rBaseObject, rBaseOffset + rBitSetWidthInBytes, data, cursor, rNumFields * 8)
    curIter = rightVarFields.iterator
    // append var-length fields and fix its index
    while (curIter.hasNext) {
      val fieldIdx = curIter.next
      val offsetAndSize = right.getLong(fieldIdx)
      val preOffset = (offsetAndSize >> 32).toInt
      val fieldSize = (offsetAndSize & ((1L << 32) - 1)).toInt
      PlatformDependent.UNSAFE.putLong(
        data, cursor + fieldIdx * 8, (varFieldsOffset.toLong << 32) | (fieldSize.toLong))
      PlatformDependent.copyMemory(
        rBaseObject, rBaseOffset + preOffset, data, baseOffset + varFieldsOffset, fieldSize)
      varFieldsOffset += ByteArrayMethods.roundNumberOfBytesToNearestWord(fieldSize)
    }
    cursor += rNumFields * 8

    val result: UnsafeRow = new UnsafeRow
    result.pointTo(data, baseOffset, numFields, size)
    result
  }
}

object UnsafeRowConcat {

  /**
   * Get all var-length field's index from the given schema
   */
  def getVarLengthFields(schema: StructType): Seq[Int] = {
    val varLengthFields = ArrayBuffer.empty[Int]
    schema.zipWithIndex.foreach { case (field, i) =>
      if (!UnsafeRow.settableFieldTypes.contains(field.dataType)) {
        varLengthFields += i
      }
    }
    varLengthFields.toSeq
  }
}
