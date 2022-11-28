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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types._

object UnsafeRowUtils {

  /**
   * Use the following rules to check the integrity of the UnsafeRow:
   * - schema.fields.length == row.numFields should always be true
   * - UnsafeRow.calculateBitSetWidthInBytes(row.numFields) < row.getSizeInBytes should always be
   *   true if the expectedSchema contains at least one field.
   * - For variable-length fields:
   *   - if null bit says it's null, then
   *     - in general the offset-and-size should be zero
   *     - special case: variable-length DecimalType is considered mutable in UnsafeRow, and to
   *       support that, the offset is set to point to the variable-length part like a non-null
   *       value, while the size is set to zero to signal that it's a null value. The offset
   *       may also be set to zero, in which case this variable-length Decimal no longer supports
   *       being mutable in the UnsafeRow.
   *   - otherwise the field is not null, then extract offset and size:
   *   1) 0 <= size < row.getSizeInBytes should always be true. We can be even more precise than
   *      this, where the upper bound of size can only be as big as the variable length part of
   *      the row.
   *   2) offset should be >= fixed sized part of the row.
   *   3) offset + size should be within the row bounds.
   * - For fixed-length fields that are narrower than 8 bytes (boolean/byte/short/int/float), if
   *   null bit says it's null then don't do anything, else:
   *     check if the unused bits in the field are all zeros. The UnsafeRowWriter's write() methods
   *     make this guarantee.
   * - Check the total length of the row.
   */
  def validateStructuralIntegrity(row: UnsafeRow, expectedSchema: StructType): Boolean = {
    if (expectedSchema.fields.length != row.numFields) {
      return false
    }
    val bitSetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(row.numFields)
    val rowSizeInBytes = row.getSizeInBytes
    if (expectedSchema.fields.length > 0 && bitSetWidthInBytes >= rowSizeInBytes) {
      return false
    }
    var varLenFieldsSizeInBytes = 0
    expectedSchema.fields.zipWithIndex.foreach {
      case (field, index) if !UnsafeRow.isFixedLength(field.dataType) && !row.isNullAt(index) =>
        val (offset, size) = getOffsetAndSize(row, index)
        if (size < 0 ||
            offset < bitSetWidthInBytes + 8 * row.numFields || offset + size > rowSizeInBytes) {
          return false
        }
        varLenFieldsSizeInBytes += size
      case (field, index) if UnsafeRow.isFixedLength(field.dataType) && !row.isNullAt(index) =>
        field.dataType match {
          case BooleanType =>
            if ((row.getLong(index) >> 1) != 0L) return false
          case ByteType =>
            if ((row.getLong(index) >> 8) != 0L) return false
          case ShortType =>
            if ((row.getLong(index) >> 16) != 0L) return false
          case IntegerType =>
            if ((row.getLong(index) >> 32) != 0L) return false
          case FloatType =>
            if ((row.getLong(index) >> 32) != 0L) return false
          case _ =>
        }
      case (field, index) if row.isNullAt(index) =>
        field.dataType match {
          case dt: DecimalType if !UnsafeRow.isFixedLength(dt) =>
            // See special case in UnsafeRowWriter.write(int, Decimal, int, int) and
            // UnsafeRow.setDecimal(int, Decimal, int).
            // A variable-length Decimal may be marked as null while having non-zero offset and
            // zero length. This allows the field to be updated (i.e. mutable variable-length data)

            // Check the integrity of null value of variable-length DecimalType in UnsafeRow:
            // 1. size must be zero
            // 2. offset may be zero, in which case this variable-length field is no longer mutable
            // 3. otherwise offset is non-zero, range check it the same way as a non-null value
            val (offset, size) = getOffsetAndSize(row, index)
            if (size != 0 || offset != 0 &&
                (offset < bitSetWidthInBytes + 8 * row.numFields || offset > rowSizeInBytes)) {
              return false
            }
          case _ =>
            if (row.getLong(index) != 0L) return false
        }
      case _ =>
    }
    if (bitSetWidthInBytes + 8 * row.numFields + varLenFieldsSizeInBytes > rowSizeInBytes) {
      return false
    }
    true
  }

  def getOffsetAndSize(row: UnsafeRow, index: Int): (Int, Int) = {
    val offsetAndSize = row.getLong(index)
    val offset = (offsetAndSize >> 32).toInt
    val size = offsetAndSize.toInt
    (offset, size)
  }
}
