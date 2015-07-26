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

package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.array.ByteArrayMethods;

/**
 * Concatenate an UnsafeRow of leftSchema with another UnsafeRow of rightSchema.
 *
 * leftSchema and rightSchema are used to determine each field's dataType,
 * thus controls the copy behaviour for each field.
 */
public class UnsafeRowConcat {

  private StructType leftSchema;
  private StructType rightSchema;
  private StructType finalSchema;
  private int numFields;
  private int bitSetWidthInBytes;

  public UnsafeRowConcat(StructType leftSchema, StructType rightSchema) {
    this.leftSchema = leftSchema;
    this.rightSchema = rightSchema;
    this.finalSchema = leftSchema.merge(rightSchema);
    this.numFields = finalSchema.length();
    this.bitSetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(numFields);
  }

  public UnsafeRow concat(UnsafeRow left, UnsafeRow right) {
    final Object lBaseObject = left.getBaseObject();
    final long lBaseOffset = left.getBaseOffset();
    final int lNumFields = left.numFields();
    final int lSizeInBytes = left.getSizeInBytes();
    final int lBitSetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(lNumFields);

    final Object rBaseObject = right.getBaseObject();
    final long rBaseOffset = right.getBaseOffset();
    final int rNumFields = right.numFields();
    final int rSizeInBytes = right.getSizeInBytes();
    final int rBitSetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(rNumFields);

    int size = bitSetWidthInBytes + lSizeInBytes - lBitSetWidthInBytes +
        rSizeInBytes - rBitSetWidthInBytes;
    final byte[] data = new byte[size];
    long baseOffset = PlatformDependent.BYTE_ARRAY_OFFSET;

    long cursor = baseOffset;
    long varFieldsOffset = bitSetWidthInBytes + (lNumFields + rNumFields) * 8;
    long lCursor = lBaseOffset;
    long rCursor = rBaseOffset;

    // concat null bit set
    int remainingBitCnt = (lNumFields & 0x3f);
    int emptyBitCnt = 64 - remainingBitCnt;
    if (remainingBitCnt == 0) {
      // no remaining bits in left, we could insert the null bit sets one after another
      PlatformDependent.copyMemory(lBaseObject, lCursor, data, cursor, lBitSetWidthInBytes);
      cursor += lBitSetWidthInBytes;
      lCursor += lBitSetWidthInBytes;
      PlatformDependent.copyMemory(rBaseObject, rCursor, data, cursor, rBitSetWidthInBytes);
      cursor += rBitSetWidthInBytes;
      rCursor += rBitSetWidthInBytes;
    } else {
      // left remains `emptyBitCnt` bit slot in its last word, therefore, we should shift right
      // words one by one to pad the empty slots.

      // copy left n-1 words first
      if (lNumFields > 64) {
        PlatformDependent.copyMemory(lBaseObject, lCursor, data, cursor, lBitSetWidthInBytes - 8);
        cursor += lBitSetWidthInBytes - 8;
        lCursor += lBitSetWidthInBytes - 8;
      }

      // shift the right word one by one to pad the previous empty null bit slot.
      long remainingBits =
        PlatformDependent.UNSAFE.getLong(lBaseObject, lCursor) & ((1L << remainingBitCnt) - 1);
      lCursor += 8;
      long lowerBitsMask = (1L << emptyBitCnt) - 1;
      long higherBitsMask = -1L ^ lowerBitsMask;
      for (int i = 0; i < rBitSetWidthInBytes / 8; i ++) {
        long word = PlatformDependent.UNSAFE.getLong(rBaseObject, rCursor);
        long lowerBits = word & lowerBitsMask;
        long higherBits = word & higherBitsMask;

        long newWord = lowerBits << remainingBitCnt | remainingBits;
        remainingBits = higherBits >> emptyBitCnt;
        PlatformDependent.UNSAFE.putLong(data, cursor, newWord);
        cursor += 8;
        rCursor += 8;
      }

      // if last right word has remaining null bits, just append it at last.
      if (rNumFields > emptyBitCnt && (numFields & 0x3f) != 0) {
        PlatformDependent.UNSAFE.putLong(data, cursor, remainingBits);
        cursor += 8;
      }
    }

    // append the left data first
    for (int i = 0; i < leftSchema.length(); i ++) {
      DataType dt = leftSchema.apply(i).dataType();
      if (UnsafeRow.settableFieldTypes.contains(dt)) {
        // settable fields have fixed length and fits in a word, it's safe to copy the whole word
        PlatformDependent.UNSAFE.putLong(data, cursor,
          PlatformDependent.UNSAFE.getLong(lBaseObject, lCursor));
      } else {
        long offsetAndSize = PlatformDependent.UNSAFE.getLong(lBaseObject, lCursor);
        final int preOffset = (int) (offsetAndSize >> 32);
        final int fieldSize = (int) (offsetAndSize & ((1L << 32) - 1));
        PlatformDependent.UNSAFE.putLong(
          data, cursor, (varFieldsOffset << 32) | ((long) fieldSize));
        PlatformDependent.copyMemory(
          lBaseObject, lBaseOffset + preOffset, data, baseOffset + varFieldsOffset, fieldSize);
        varFieldsOffset += ByteArrayMethods.roundNumberOfBytesToNearestWord(fieldSize);
      }
      lCursor += 8;
      cursor += 8;
    }

    // now comes the right data
    for (int i = 0; i < rightSchema.length(); i ++) {
      DataType dt = rightSchema.apply(i).dataType();
      if (UnsafeRow.settableFieldTypes.contains(dt)) {
        // settable fields have fixed length and fits in a word, it's safe to copy the whole word
        PlatformDependent.UNSAFE.putLong(data, cursor,
            PlatformDependent.UNSAFE.getLong(rBaseObject, rCursor));
      } else {
        long offsetAndSize = PlatformDependent.UNSAFE.getLong(rBaseObject, rCursor);
        final int preOffset = (int) (offsetAndSize >> 32);
        final int fieldSize = (int) (offsetAndSize & ((1L << 32) - 1));
        PlatformDependent.UNSAFE.putLong(
          data, cursor, (varFieldsOffset << 32) | ((long) fieldSize));
        PlatformDependent.copyMemory(
          rBaseObject, rBaseOffset + preOffset, data, baseOffset + varFieldsOffset, fieldSize);
        varFieldsOffset += ByteArrayMethods.roundNumberOfBytesToNearestWord(fieldSize);
      }
      rCursor += 8;
      cursor += 8;
    }

    UnsafeRow result = new UnsafeRow();
    result.pointTo(data, baseOffset, numFields, size);
    return result;
  }
}
