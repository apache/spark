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
    Platform.putLong(getBuffer(), startingOffset, numElements);
    for (int i = 8; i < headerInBytes; i += 8) {
      Platform.putLong(getBuffer(), startingOffset + i, 0L);
    }

    // fill 0 into reminder part of 8-bytes alignment in unsafe array
    for (int i = elementSize * numElements; i < fixedPartInBytes; i++) {
      Platform.putByte(getBuffer(), startingOffset + headerInBytes + i, (byte) 0);
    }
    increaseCursor(headerInBytes + fixedPartInBytes);
  }

  private long getElementOffset(int ordinal) {
    return startingOffset + headerInBytes + ordinal * (long) elementSize;
  }

  private void setNullBit(int ordinal) {
    assertIndexIsValid(ordinal);
    BitSetMethods.set(getBuffer(), startingOffset + 8, ordinal);
  }

  @Override
  public void setNull1Bytes(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    writeByte(getElementOffset(ordinal), (byte)0);
  }

  @Override
  public void setNull2Bytes(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    writeShort(getElementOffset(ordinal), (short)0);
  }

  @Override
  public void setNull4Bytes(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    writeInt(getElementOffset(ordinal), 0);
  }

  @Override
  public void setNull8Bytes(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    writeLong(getElementOffset(ordinal), 0);
  }

  public void setNull(int ordinal) { setNull8Bytes(ordinal); }

  @Override
  public void write(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    writeBoolean(getElementOffset(ordinal), value);
  }

  @Override
  public void write(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    writeByte(getElementOffset(ordinal), value);
  }

  @Override
  public void write(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    writeShort(getElementOffset(ordinal), value);
  }

  @Override
  public void write(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    writeInt(getElementOffset(ordinal), value);
  }

  @Override
  public void write(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    writeLong(getElementOffset(ordinal), value);
  }

  @Override
  public void write(int ordinal, float value) {
    assertIndexIsValid(ordinal);
    writeFloat(getElementOffset(ordinal), value);
  }

  @Override
  public void write(int ordinal, double value) {
    assertIndexIsValid(ordinal);
    writeDouble(getElementOffset(ordinal), value);
  }

  @Override
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
          bytes, Platform.BYTE_ARRAY_OFFSET, getBuffer(), cursor(), numBytes);
        setOffsetAndSize(ordinal, numBytes);

        // move the cursor forward with 8-bytes boundary
        increaseCursor(roundedSize);
      }
    } else {
      setNull(ordinal);
    }
  }
}
