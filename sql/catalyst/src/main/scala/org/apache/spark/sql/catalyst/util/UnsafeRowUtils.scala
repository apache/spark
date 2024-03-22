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
   * @param row The input UnsafeRow to be validated
   * @param expectedSchema The expected schema that should match with the UnsafeRow
   * @return None if all the checks pass. An error message if the row is not matched with the schema
   */
  private def validateStructuralIntegrityWithReasonImpl(
      row: UnsafeRow, expectedSchema: StructType): Option[String] = {
    if (expectedSchema.fields.length != row.numFields) {
      return Some(s"Field length mismatch: " +
        s"expected: ${expectedSchema.fields.length}, actual: ${row.numFields}")
    }
    val bitSetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(row.numFields)
    val rowSizeInBytes = row.getSizeInBytes
    if (expectedSchema.fields.length > 0 && bitSetWidthInBytes >= rowSizeInBytes) {
      return Some(s"rowSizeInBytes should not exceed bitSetWidthInBytes, " +
        s"bitSetWidthInBytes: $bitSetWidthInBytes, rowSizeInBytes: $rowSizeInBytes")
    }
    var varLenFieldsSizeInBytes = 0
    expectedSchema.fields.zipWithIndex.foreach {
      case (field, index) if !UnsafeRow.isFixedLength(field.dataType) && !row.isNullAt(index) =>
        val (offset, size) = getOffsetAndSize(row, index)
        if (size < 0 ||
            offset < bitSetWidthInBytes + 8 * row.numFields || offset + size > rowSizeInBytes) {
          return Some(s"Variable-length field validation error: field: $field, index: $index")
        }
        varLenFieldsSizeInBytes += size
      case (field, index) if UnsafeRow.isFixedLength(field.dataType) && !row.isNullAt(index) =>
        field.dataType match {
          case BooleanType =>
            if ((row.getLong(index) >> 1) != 0L) {
              return Some(s"Fixed-length field validation error: field: $field, index: $index")
            }
          case ByteType =>
            if ((row.getLong(index) >> 8) != 0L) {
              return Some(s"Fixed-length field validation error: field: $field, index: $index")
            }
          case ShortType =>
            if ((row.getLong(index) >> 16) != 0L) {
              return Some(s"Fixed-length field validation error: field: $field, index: $index")
            }
          case IntegerType =>
            if ((row.getLong(index) >> 32) != 0L) {
              return Some(s"Fixed-length field validation error: field: $field, index: $index")
            }
          case FloatType =>
            if ((row.getLong(index) >> 32) != 0L) {
              return Some(s"Fixed-length field validation error: field: $field, index: $index")
            }
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
              return Some(s"Variable-length decimal field special case validation error: " +
                s"field: $field, index: $index")
            }
          case _ =>
            if (row.getLong(index) != 0L) {
              return Some(s"Variable-length offset-size validation error: " +
                s"field: $field, index: $index")
            }
        }
      case _ =>
    }
    if (bitSetWidthInBytes + 8 * row.numFields + varLenFieldsSizeInBytes > rowSizeInBytes) {
      return Some(s"Row total length invalid: " +
        s"calculated: ${bitSetWidthInBytes + 8 * row.numFields + varLenFieldsSizeInBytes} " +
        s"rowSizeInBytes: $rowSizeInBytes")
    }
    None
  }

  /**
   * Wrapper of validateStructuralIntegrityWithReasonImpl, add more information for debugging
   * @param row The input UnsafeRow to be validated
   * @param expectedSchema The expected schema that should match with the UnsafeRow
   * @return None if all the checks pass. An error message if the row is not matched with the schema
   */
  def validateStructuralIntegrityWithReason(
      row: UnsafeRow, expectedSchema: StructType): Option[String] = {
    validateStructuralIntegrityWithReasonImpl(row, expectedSchema).map {
      errorMessage => s"Error message is: $errorMessage, " +
          s"UnsafeRow status: ${getStructuralIntegrityStatus(row, expectedSchema)}"
    }
  }

  def getOffsetAndSize(row: UnsafeRow, index: Int): (Int, Int) = {
    val offsetAndSize = row.getLong(index)
    val offset = (offsetAndSize >> 32).toInt
    val size = offsetAndSize.toInt
    (offset, size)
  }

  /**
   * Returns a Boolean indicating whether one should avoid calling
   * UnsafeRow.setNullAt for a field of the given data type.
   * Fields of type DecimalType (with precision
   * greater than Decimal.MAX_LONG_DIGITS) and CalendarIntervalType use
   * pointers into the variable length region, and those pointers should
   * never get zeroed out (setNullAt will zero out those pointers) because UnsafeRow
   * may do in-place update for these 2 types even though they are not primitive.
   *
   * When avoidSetNullAt returns true, callers should not use
   * UnsafeRow#setNullAt for fields of that data type, but instead pass
   * a null value to the appropriate set method, e.g.:
   *
   *   row.setDecimal(ordinal, null, precision)
   *
   * Even though only UnsafeRow has this limitation, it's safe to extend this rule
   * to all subclasses of InternalRow, since you don't always know the concrete type
   * of the row you are dealing with, and all subclasses of InternalRow will
   * handle a null value appropriately.
   */
  def avoidSetNullAt(dt: DataType): Boolean = dt match {
    case t: DecimalType if t.precision > Decimal.MAX_LONG_DIGITS => true
    case CalendarIntervalType => true
    case _ => false
  }

  def getStructuralIntegrityStatus(row: UnsafeRow, expectedSchema: StructType): String = {
    val minLength = Math.min(row.numFields(), expectedSchema.fields.length)
    val fieldStatusArr = expectedSchema.fields.take(minLength).zipWithIndex.map {
      case (field, index) =>
        val offsetAndSizeStr = if (!UnsafeRow.isFixedLength(field.dataType)) {
          val (offset, size) = getOffsetAndSize(row, index)
          s"offset: $offset, size: $size"
        } else {
          "" // offset and size doesn't make sense for fixed length field
        }
        s"[UnsafeRowFieldStatus] index: $index, " +
          s"expectedFieldType: ${field.dataType}, isNull: ${row.isNullAt(index)}, " +
          s"isFixedLength: ${UnsafeRow.isFixedLength(field.dataType)}. $offsetAndSizeStr"
    }

    s"[UnsafeRowStatus] expectedSchema: $expectedSchema, " +
      s"expectedSchemaNumFields: ${expectedSchema.fields.length}, numFields: ${row.numFields}, " +
      s"bitSetWidthInBytes: ${UnsafeRow.calculateBitSetWidthInBytes(row.numFields)}, " +
      s"rowSizeInBytes: ${row.getSizeInBytes}\nfieldStatus:\n" +
      fieldStatusArr.mkString("\n")
  }

  /**
   * True if comparisons, equality and hashing can be done purely on binary representation of
   * Unsafe row. i.e. binary(e1) = binary(e2) <=> e1 = e2.
   * e.g. this is not true for non-binary collations (any case/accent insensitive collation
   * can lead to rows being semantically equal even though their binary representations differ).
   */
  def isBinaryStable(dataType: DataType): Boolean = !dataType.existsRecursively {
    case st: StringType => !CollationFactory.fetchCollation(st.collationId).isBinaryCollation
    case _ => false
  }
}
