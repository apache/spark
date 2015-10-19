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

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A helper class to write data into global row buffer using `UnsafeRow` format,
 * used by {@link org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection}.
 */
public class UnsafeRowWriter {

  private BufferHolder holder;
  // The offset of the global buffer where we start to write this row.
  private int startingOffset;
  private int nullBitsSize;

  public void initialize(BufferHolder holder, int numFields) {
    this.holder = holder;
    this.startingOffset = holder.cursor;
    this.nullBitsSize = UnsafeRow.calculateBitSetWidthInBytes(numFields);

    // grow the global buffer to make sure it has enough space to write fixed-length data.
    final int fixedSize = nullBitsSize + 8 * numFields;
    holder.grow(fixedSize);
    holder.cursor += fixedSize;

    // zero-out the null bits region
    for (int i = 0; i < nullBitsSize; i += 8) {
      Platform.putLong(holder.buffer, startingOffset + i, 0L);
    }
  }

  private void zeroOutPaddingBytes(int numBytes) {
    if ((numBytes & 0x07) > 0) {
      Platform.putLong(holder.buffer, holder.cursor + ((numBytes >> 3) << 3), 0L);
    }
  }

  public void setNullAt(int ordinal) {
    BitSetMethods.set(holder.buffer, startingOffset, ordinal);
    Platform.putLong(holder.buffer, getFieldOffset(ordinal), 0L);
  }

  public long getFieldOffset(int ordinal) {
    return startingOffset + nullBitsSize + 8 * ordinal;
  }

  public void setOffsetAndSize(int ordinal, long size) {
    setOffsetAndSize(ordinal, holder.cursor, size);
  }

  public void setOffsetAndSize(int ordinal, long currentCursor, long size) {
    final long relativeOffset = currentCursor - startingOffset;
    final long fieldOffset = getFieldOffset(ordinal);
    final long offsetAndSize = (relativeOffset << 32) | size;

    Platform.putLong(holder.buffer, fieldOffset, offsetAndSize);
  }

  // Do word alignment for this row and grow the row buffer if needed.
  // todo: remove this after we make unsafe array data word align.
  public void alignToWords(int numBytes) {
    final int remainder = numBytes & 0x07;

    if (remainder > 0) {
      final int paddingBytes = 8 - remainder;
      holder.grow(paddingBytes);

      for (int i = 0; i < paddingBytes; i++) {
        Platform.putByte(holder.buffer, holder.cursor, (byte) 0);
        holder.cursor++;
      }
    }
  }

  public void writeCompactDecimal(int ordinal, Decimal input, int precision, int scale) {
    // make sure Decimal object has the same scale as DecimalType
    if (input.changePrecision(precision, scale)) {
      Platform.putLong(holder.buffer, getFieldOffset(ordinal), input.toUnscaledLong());
    } else {
      setNullAt(ordinal);
    }
  }

  public void write(int ordinal, Decimal input, int precision, int scale) {
    // grow the global buffer before writing data.
    holder.grow(16);

    // zero-out the bytes
    Platform.putLong(holder.buffer, holder.cursor, 0L);
    Platform.putLong(holder.buffer, holder.cursor + 8, 0L);

    // Make sure Decimal object has the same scale as DecimalType.
    // Note that we may pass in null Decimal object to set null for it.
    if (input == null || !input.changePrecision(precision, scale)) {
      BitSetMethods.set(holder.buffer, startingOffset, ordinal);
      // keep the offset for future update
      setOffsetAndSize(ordinal, 0L);
    } else {
      final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
      assert bytes.length <= 16;

      // Write the bytes to the variable length portion.
      Platform.copyMemory(
        bytes, Platform.BYTE_ARRAY_OFFSET, holder.buffer, holder.cursor, bytes.length);
      setOffsetAndSize(ordinal, bytes.length);
    }

    // move the cursor forward.
    holder.cursor += 16;
  }

  public void write(int ordinal, UTF8String input) {
    final int numBytes = input.numBytes();
    final int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);

    // grow the global buffer before writing data.
    holder.grow(roundedSize);

    zeroOutPaddingBytes(numBytes);

    // Write the bytes to the variable length portion.
    input.writeToMemory(holder.buffer, holder.cursor);

    setOffsetAndSize(ordinal, numBytes);

    // move the cursor forward.
    holder.cursor += roundedSize;
  }

  public void write(int ordinal, byte[] input) {
    final int numBytes = input.length;
    final int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);

    // grow the global buffer before writing data.
    holder.grow(roundedSize);

    zeroOutPaddingBytes(numBytes);

    // Write the bytes to the variable length portion.
    Platform.copyMemory(input, Platform.BYTE_ARRAY_OFFSET, holder.buffer, holder.cursor, numBytes);

    setOffsetAndSize(ordinal, numBytes);

    // move the cursor forward.
    holder.cursor += roundedSize;
  }

  public void write(int ordinal, CalendarInterval input) {
    // grow the global buffer before writing data.
    holder.grow(16);

    // Write the months and microseconds fields of Interval to the variable length portion.
    Platform.putLong(holder.buffer, holder.cursor, input.months);
    Platform.putLong(holder.buffer, holder.cursor + 8, input.microseconds);

    setOffsetAndSize(ordinal, 16);

    // move the cursor forward.
    holder.cursor += 16;
  }
}
