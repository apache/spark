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

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Arrays;

import static org.apache.spark.sql.catalyst.expressions.UnsafeArrayData.calculateHeaderPortionInBytes;

/**
 * A helper class to write data into global row buffer using `UnsafeArrayData` format,
 * used by {@link org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection}.
 */
public class UnsafeArrayWriter {

  private BufferHolder holder;

  // The offset of the global buffer where we start to write this array.
  private int startingOffset;

  // The number of elements in this array
  private int numElements;

  private int headerInBytes;

  private void assertIndexIsValid(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < numElements : "index (" + index + ") should < " + numElements;
  }

  public void initialize(BufferHolder holder, int numElements, int fixedElementSize) {
    this.numElements = numElements;
    this.headerInBytes = calculateHeaderPortionInBytes(numElements);

    this.holder = holder;
    this.startingOffset = holder.cursor;

    // Grows the global buffer ahead for header and fixed size data.
    holder.grow(headerInBytes + fixedElementSize * numElements);

    // Initialize information in header
    Platform.putInt(holder.buffer, startingOffset, numElements);
    Arrays.fill(holder.buffer, startingOffset + 4 - Platform.BYTE_ARRAY_OFFSET,
      startingOffset + headerInBytes - Platform.BYTE_ARRAY_OFFSET, (byte)0);

    holder.cursor += (headerInBytes + fixedElementSize * numElements);
  }

  private long getElementOffset(int ordinal, int scale) {
    return startingOffset + headerInBytes + ordinal * scale;
  }

  public void setOffsetAndSize(int ordinal, long currentCursor, long size) {
    final long relativeOffset = currentCursor - startingOffset;
    final long offsetAndSize = (relativeOffset << 32) | size;

    write(ordinal, offsetAndSize);
  }

  public void setNullAt(int ordinal) {
    throw new UnsupportedOperationException("setNullAt() is not supported");
  }

  private void setNullBit(int ordinal) {
    assertIndexIsValid(ordinal);
    BitSetMethods.set(holder.buffer, startingOffset + 4, ordinal);
  }

  public void setNullBoolean(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    Platform.putBoolean(holder.buffer, getElementOffset(ordinal, 1), false);
  }

  public void setNullByte(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    Platform.putByte(holder.buffer, getElementOffset(ordinal, 1), (byte)0);
  }

  public void setNullShort(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    Platform.putShort(holder.buffer, getElementOffset(ordinal, 2), (short)0);
  }

  public void setNullInt(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    Platform.putInt(holder.buffer, getElementOffset(ordinal, 4), (int)0);
  }

  public void setNullLong(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    Platform.putLong(holder.buffer, getElementOffset(ordinal, 8), (long)0);
  }

  public void setNullFloat(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    Platform.putFloat(holder.buffer, getElementOffset(ordinal, 4), (float)0);
  }

  public void setNullDouble(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    Platform.putDouble(holder.buffer, getElementOffset(ordinal, 8), (double)0);
  }

  public void setNull(int ordinal) {
    setNullLong(ordinal);
  }

  public void write(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    Platform.putBoolean(holder.buffer, getElementOffset(ordinal, 1), value);
  }

  public void write(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    Platform.putByte(holder.buffer, getElementOffset(ordinal, 1), value);
  }

  public void write(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    Platform.putShort(holder.buffer, getElementOffset(ordinal, 2), value);
  }

  public void write(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    Platform.putInt(holder.buffer, getElementOffset(ordinal, 4), value);
  }

  public void write(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    Platform.putLong(holder.buffer, getElementOffset(ordinal, 8), value);
  }

  public void write(int ordinal, float value) {
    if (Float.isNaN(value)) {
      value = Float.NaN;
    }
    assertIndexIsValid(ordinal);
    Platform.putFloat(holder.buffer, getElementOffset(ordinal, 4), value);
  }

  public void write(int ordinal, double value) {
    if (Double.isNaN(value)) {
      value = Double.NaN;
    }
    assertIndexIsValid(ordinal);
    Platform.putDouble(holder.buffer, getElementOffset(ordinal, 8), value);
  }

  public void write(int ordinal, Decimal input, int precision, int scale) {
    // make sure Decimal object has the same scale as DecimalType
    assertIndexIsValid(ordinal);
    if (input.changePrecision(precision, scale)) {
      if (precision <= Decimal.MAX_LONG_DIGITS()) {
        write(ordinal, input.toUnscaledLong());
      } else {
        final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
        assert bytes.length <= 16;
        holder.grow(bytes.length);

        // Write the bytes to the variable length portion.
        Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, holder.buffer, holder.cursor, bytes.length);
        write(ordinal, ((long)(holder.cursor - startingOffset) << 32) | ((long) bytes.length));
        holder.cursor += bytes.length;
      }
    } else {
      setNull(ordinal);
    }
  }

  public void write(int ordinal, UTF8String input) {
    final int numBytes = input.numBytes();

    // grow the global buffer before writing data.
    holder.grow(numBytes);

    // Write the bytes to the variable length portion.
    input.writeToMemory(holder.buffer, holder.cursor);

    write(ordinal, ((long)(holder.cursor - startingOffset) << 32) | ((long) numBytes));

    // move the cursor forward.
    holder.cursor += numBytes;
  }

  public void write(int ordinal, byte[] input) {
    // grow the global buffer before writing data.
    holder.grow(input.length);

    // Write the bytes to the variable length portion.
    Platform.copyMemory(
      input, Platform.BYTE_ARRAY_OFFSET, holder.buffer, holder.cursor, input.length);

    write(ordinal, ((long)(holder.cursor - startingOffset) << 32) | ((long) input.length));

    // move the cursor forward.
    holder.cursor += input.length;
  }

  public void write(int ordinal, CalendarInterval input) {
    // grow the global buffer before writing data.
    holder.grow(16);

    // Write the months and microseconds fields of Interval to the variable length portion.
    Platform.putLong(holder.buffer, holder.cursor, input.months);
    Platform.putLong(holder.buffer, holder.cursor + 8, input.microseconds);

    write(ordinal, ((long)(holder.cursor - startingOffset) << 32) | ((long) 16));

    // move the cursor forward.
    holder.cursor += 16;
  }
}
