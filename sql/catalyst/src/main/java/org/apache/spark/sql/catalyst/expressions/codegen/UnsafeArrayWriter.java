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

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

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

  public void initialize(BufferHolder holder, int numElements, int elementSize) {
    // We need 8 bytes to store numElements in header
    this.numElements = numElements;
    this.headerInBytes = calculateHeaderPortionInBytes(numElements);

    this.holder = holder;
    this.startingOffset = holder.cursor;

    // Grows the global buffer ahead for header and fixed size data.
    int fixedPartInBytes =
      ByteArrayMethods.roundNumberOfBytesToNearestWord(elementSize * numElements);
    holder.grow(headerInBytes + fixedPartInBytes);

    // Write numElements and clear out null bits to header
    Platform.putLong(holder.buffer, startingOffset, numElements);
    for (int i = 8; i < headerInBytes; i += 8) {
      Platform.putLong(holder.buffer, startingOffset + i, 0L);
    }

    // fill 0 into reminder part of 8-bytes alignment in unsafe array
    for (int i = elementSize * numElements; i < fixedPartInBytes; i++) {
      Platform.putByte(holder.buffer, startingOffset + headerInBytes + i, (byte) 0);
    }
    holder.cursor += (headerInBytes + fixedPartInBytes);
  }

  private void zeroOutPaddingBytes(int numBytes) {
    if ((numBytes & 0x07) > 0) {
      Platform.putLong(holder.buffer, holder.cursor + ((numBytes >> 3) << 3), 0L);
    }
  }

  private long getElementOffset(int ordinal, int elementSize) {
    return startingOffset + headerInBytes + ordinal * elementSize;
  }

  public void setOffsetAndSize(int ordinal, long currentCursor, int size) {
    assertIndexIsValid(ordinal);
    final long relativeOffset = currentCursor - startingOffset;
    final long offsetAndSize = (relativeOffset << 32) | (long)size;

    write(ordinal, offsetAndSize);
  }

  private void setNullBit(int ordinal) {
    assertIndexIsValid(ordinal);
    BitSetMethods.set(holder.buffer, startingOffset + 8, ordinal);
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
    Platform.putInt(holder.buffer, getElementOffset(ordinal, 4), 0);
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

  public void setNull(int ordinal) { setNullLong(ordinal); }

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
        final int numBytes = bytes.length;
        assert numBytes <= 16;
        int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
        holder.grow(roundedSize);

        zeroOutPaddingBytes(numBytes);

        // Write the bytes to the variable length portion.
        Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, holder.buffer, holder.cursor, numBytes);
        setOffsetAndSize(ordinal, holder.cursor, numBytes);

        // move the cursor forward with 8-bytes boundary
        holder.cursor += roundedSize;
      }
    } else {
      setNull(ordinal);
    }
  }

  public void write(int ordinal, UTF8String input) {
    final int numBytes = input.numBytes();
    final int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);

    // grow the global buffer before writing data.
    holder.grow(roundedSize);

    zeroOutPaddingBytes(numBytes);

    // Write the bytes to the variable length portion.
    input.writeToMemory(holder.buffer, holder.cursor);

    setOffsetAndSize(ordinal, holder.cursor, numBytes);

    // move the cursor forward.
    holder.cursor += roundedSize;
  }

  public void write(int ordinal, byte[] input) {
    final int numBytes = input.length;
    final int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(input.length);

    // grow the global buffer before writing data.
    holder.grow(roundedSize);

    zeroOutPaddingBytes(numBytes);

    // Write the bytes to the variable length portion.
    Platform.copyMemory(
      input, Platform.BYTE_ARRAY_OFFSET, holder.buffer, holder.cursor, numBytes);

    setOffsetAndSize(ordinal, holder.cursor, numBytes);

    // move the cursor forward.
    holder.cursor += roundedSize;
  }

  public void write(int ordinal, CalendarInterval input) {
    // grow the global buffer before writing data.
    holder.grow(16);

    // Write the months and microseconds fields of Interval to the variable length portion.
    Platform.putLong(holder.buffer, holder.cursor, input.months);
    Platform.putLong(holder.buffer, holder.cursor + 8, input.microseconds);

    setOffsetAndSize(ordinal, holder.cursor, 16);

    // move the cursor forward.
    holder.cursor += 16;
  }

  private void writePrimitiveArray(Object input, int offset, int elementSize, int length)  {
    Platform.copyMemory(input, offset, holder.buffer, startingOffset + headerInBytes, elementSize * length);
  }

  public void writePrimitiveBooleanArray(ArrayData arrayData) {
    boolean[] input = arrayData.toBooleanArray();
    int length = input.length;
    int offset = Platform.BYTE_ARRAY_OFFSET;
    writePrimitiveArray(input, offset, 1, length);
  }

  public void writePrimitiveByteArray(ArrayData arrayData) {
    byte[] input = arrayData.toByteArray();
    int length = input.length;
    int offset = Platform.BYTE_ARRAY_OFFSET;
    writePrimitiveArray(input, offset, 1, length);
  }

  public void writePrimitiveShortArray(ArrayData arrayData) {
    short[] input = arrayData.toShortArray();
    int length = input.length;
    int offset = Platform.SHORT_ARRAY_OFFSET;
    writePrimitiveArray(input, offset, 2, length);
  }

  public void writePrimitiveIntArray(ArrayData arrayData) {
    int[] input = arrayData.toIntArray();
    int length = input.length;
    int offset = Platform.INT_ARRAY_OFFSET;
    writePrimitiveArray(input, offset, 4, length);
  }

  public void writePrimitiveLongArray(ArrayData arrayData) {
    long[] input = arrayData.toLongArray();
    int length = input.length;
    int offset = Platform.LONG_ARRAY_OFFSET;
    writePrimitiveArray(input, offset, 8, length);
  }

  public void writePrimitiveFloatArray(ArrayData arrayData) {
    float[] input = arrayData.toFloatArray();
    int length = input.length;
    int offset = Platform.FLOAT_ARRAY_OFFSET;
    writePrimitiveArray(input, offset, 4, length);
  }

  public void writePrimitiveDoubleArray(ArrayData arrayData) {
    double[] input = arrayData.toDoubleArray();
    int length = input.length;
    int offset = Platform.DOUBLE_ARRAY_OFFSET;
    writePrimitiveArray(input, offset, 8, length);
  }

/** uncomment this if SPARK-16043 is merged

  public void writePrimitiveBooleanArray(ArrayData arrayData) {
    if (arrayData instanceof GenericBooleanArrayData) {
      boolean[] input = ((GenericBooleanArrayData)arrayData).primitiveArray();
      int length = input.length;
      Platform.copyMemory(input, Platform.BOOLEAN_ARRAY_OFFSET,
              holder.buffer, startingOffset + headerInBytes, length);
    } else {
      int length = arrayData.numElements();
      for (int i = 0; i < length; i++) {
        Platform.putBoolean(holder.buffer, holder.cursor + i, arrayData.getBoolean(i));
      }
    }
  }

  public void writePrimitiveByteArray(ArrayData arrayData) {
    if (arrayData instanceof GenericByteArrayData) {
      byte[] input = ((GenericByteArrayData)arrayData).primitiveArray();
      int length = input.length;
      Platform.copyMemory(input, Platform.BYTE_ARRAY_OFFSET,
              holder.buffer, startingOffset + headerInBytes, length);
    } else {
      int length = arrayData.numElements();
      for (int i = 0; i < length; i++) {
        Platform.putByte(holder.buffer, holder.cursor + i, arrayData.getByte(i));
      }
    }
  }

  public void writePrimitiveShortArray(ArrayData arrayData) {
    if (arrayData instanceof GenericShortArrayData) {
      short[] input = ((GenericShortArrayData)arrayData).primitiveArray();
      int length = input.length;
      Platform.copyMemory(input, Platform.SHORT_ARRAY_OFFSET,
              holder.buffer, startingOffset + headerInBytes, length);
    } else {
      int length = arrayData.numElements();
      for (int i = 0; i < length; i++) {
        Platform.putShort(holder.buffer, holder.cursor + i, arrayData.getShort(i));
      }
    }
  }

  public void writePrimitiveIntArray(ArrayData arrayData) {
    if (arrayData instanceof GenericIntArrayData) {
      int[] input = ((GenericIntArrayData)arrayData).primitiveArray();
      int length = input.length;
      Platform.copyMemory(input, Platform.INT_ARRAY_OFFSET,
              holder.buffer, startingOffset + headerInBytes, length);
    } else {
      int length = arrayData.numElements();
      for (int i = 0; i < length; i++) {
        Platform.putInt(holder.buffer, holder.cursor + i, arrayData.getInt(i));
      }
    }
  }

  public void writePrimitiveLongArray(ArrayData arrayData) {
    if (arrayData instanceof GenericLongArrayData) {
      long[] input = ((GenericLongArrayData)arrayData).primitiveArray();
      int length = input.length;
      Platform.copyMemory(input, Platform.LONG_ARRAY_OFFSET,
              holder.buffer, startingOffset + headerInBytes, length);
    } else {
      int length = arrayData.numElements();
      for (int i = 0; i < length; i++) {
        Platform.putLong(holder.buffer, holder.cursor + i, arrayData.getLong(i));
      }
    }
  }

  public void writePrimitiveFloatArray(ArrayData arrayData) {
    if (arrayData instanceof GenericFloatArrayData) {
      float[] input = ((GenericFloatArrayData)arrayData).primitiveArray();
      int length = input.length;
      Platform.copyMemory(input, Platform.FLOAT_ARRAY_OFFSET,
              holder.buffer, startingOffset + headerInBytes, length);
    } else {
      int length = arrayData.numElements();
      for (int i = 0; i < length; i++) {
        Platform.putFloat(holder.buffer, holder.cursor + i, arrayData.getFloat(i));
      }
    }
  }

  public void writePrimitiveDoubleArray(ArrayData arrayData) {
    if (arrayData instanceof GenericDoubleArrayData) {
      double[] input = ((GenericDoubleArrayData)arrayData).primitiveArray();
      int length = input.length;
      Platform.copyMemory(input, Platform.DOUBLE_ARRAY_OFFSET,
              holder.buffer, startingOffset + headerInBytes, length);
    } else {
      int length = arrayData.numElements();
      for (int i = 0; i < length; i++) {
        Platform.putFloat(holder.buffer, holder.cursor + i, arrayData.getFloat(i));
      }
    }
  }
*/
}
