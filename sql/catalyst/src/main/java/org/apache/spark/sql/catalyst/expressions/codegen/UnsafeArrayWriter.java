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
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;

import static org.apache.spark.sql.catalyst.expressions.UnsafeArrayData.calculateHeaderPortionInBytes;

/**
 * A helper class to write data into global row buffer using `UnsafeArrayData` format,
 * used by {@link org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection}.
 */
public final class UnsafeArrayWriter extends UnsafeWriter {

  // The number of elements in this array
  private int numElements;

  // The element size in this array
  private int elementSize;

  private int headerInBytes;

  private void assertIndexIsValid(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < numElements : "index (" + index + ") should < " + numElements;
  }

  public UnsafeArrayWriter(UnsafeWriter writer, int elementSize) {
    super(writer.getBufferHolder());
    this.elementSize = elementSize;
  }

  public void initialize(int numElements) {
    // We need 8 bytes to store numElements in header
    this.numElements = numElements;
    this.headerInBytes = calculateHeaderPortionInBytes(numElements);

    this.startingOffset = cursor();

    // Grows the global buffer ahead for header and fixed size data.
    int fixedPartInBytes =
      ByteArrayMethods.roundNumberOfBytesToNearestWord(elementSize * numElements);
    holder.grow(headerInBytes + fixedPartInBytes);

    // Write numElements and clear out null bits to header
    Platform.putLong(buffer(), startingOffset, numElements);
    for (int i = 8; i < headerInBytes; i += 8) {
      Platform.putLong(buffer(), startingOffset + i, 0L);
    }

    // fill 0 into reminder part of 8-bytes alignment in unsafe array
    for (int i = elementSize * numElements; i < fixedPartInBytes; i++) {
      Platform.putByte(buffer(), startingOffset + headerInBytes + i, (byte) 0);
    }
    incrementCursor(headerInBytes + fixedPartInBytes);
  }

  protected long getOffset(int ordinal, int elementSize) {
    return getElementOffset(ordinal, elementSize);
  }

  private long getElementOffset(int ordinal, int elementSize) {
    return startingOffset + headerInBytes + ordinal * elementSize;
  }

  @Override
  public void setOffsetAndSizeFromMark(int ordinal, int mark) {
    assertIndexIsValid(ordinal);
    _setOffsetAndSizeFromMark(ordinal, mark);
  }

  private void setNullBit(int ordinal) {
    assertIndexIsValid(ordinal);
    BitSetMethods.set(buffer(), startingOffset + 8, ordinal);
  }

  public void setNull1Bytes(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    Platform.putByte(buffer(), getElementOffset(ordinal, 1), (byte)0);
  }

  public void setNull2Bytes(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    Platform.putShort(buffer(), getElementOffset(ordinal, 2), (short)0);
  }

  public void setNull4Bytes(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    Platform.putInt(buffer(), getElementOffset(ordinal, 4), 0);
  }

  public void setNull8Bytes(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    Platform.putLong(buffer(), getElementOffset(ordinal, 8), (long)0);
  }

  public void setNull(int ordinal) { setNull8Bytes(ordinal); }

  public void write(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    writeBoolean(getElementOffset(ordinal, 1), value);
  }

  public void write(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    writeByte(getElementOffset(ordinal, 1), value);
  }

  public void write(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    writeShort(getElementOffset(ordinal, 2), value);
  }

  public void write(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    writeInt(getElementOffset(ordinal, 4), value);
  }

  public void write(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    writeLong(getElementOffset(ordinal, 8), value);
  }

  public void write(int ordinal, float value) {
    assertIndexIsValid(ordinal);
    writeFloat(getElementOffset(ordinal, 4), value);
  }

  public void write(int ordinal, double value) {
    assertIndexIsValid(ordinal);
    writeDouble(getElementOffset(ordinal, 8), value);
  }

  public void write(int ordinal, Decimal input, int precision, int scale) {
    // make sure Decimal object has the same scale as DecimalType
    assertIndexIsValid(ordinal);
    if (input != null && input.changePrecision(precision, scale)) {
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
          bytes, Platform.BYTE_ARRAY_OFFSET, buffer(), cursor(), numBytes);
        setOffsetAndSize(ordinal, numBytes);

        // move the cursor forward with 8-bytes boundary
        incrementCursor(roundedSize);
      }
    } else {
      setNull(ordinal);
    }
  }
}
