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
import org.apache.spark.unsafe.bitset.BitSetMethods;

/**
 * A helper class to write data into global row buffer using `UnsafeRow` format.
 *
 * It will remember the offset of row buffer which it starts to write, and move the cursor of row
 * buffer while writing.  If new data(can be the input record if this is the outermost writer, or
 * nested struct if this is an inner writer) comes, the starting cursor of row buffer may be
 * changed, so we need to call `UnsafeRowWriter.resetRowWriter` before writing, to update the
 * `startingOffset` and clear out null bits.
 *
 * Note that if this is the outermost writer, which means we will always write from the very
 * beginning of the global row buffer, we don't need to update `startingOffset` and can just call
 * `zeroOutNullBytes` before writing new data.
 */
public final class UnsafeRowWriter extends UnsafeWriter {

  private final UnsafeRow row;

  private final int nullBitsSize;
  private final int fixedSize;

  public UnsafeRowWriter(int numFields) {
    this(new UnsafeRow(numFields));
  }

  public UnsafeRowWriter(int numFields, int initialBufferSize) {
    this(new UnsafeRow(numFields), initialBufferSize);
  }

  public UnsafeRowWriter(UnsafeWriter writer, int numFields) {
    this(null, writer.getBufferHolder(), numFields);
  }

  private UnsafeRowWriter(UnsafeRow row) {
    this(row, new BufferHolder(row), row.numFields());
  }

  private UnsafeRowWriter(UnsafeRow row, int initialBufferSize) {
    this(row, new BufferHolder(row, initialBufferSize), row.numFields());
  }

  private UnsafeRowWriter(UnsafeRow row, BufferHolder holder, int numFields) {
    super(holder);
    this.row = row;
    this.nullBitsSize = UnsafeRow.calculateBitSetWidthInBytes(numFields);
    this.fixedSize = nullBitsSize + 8 * numFields;
    this.startingOffset = cursor();
  }

  /**
   * Updates total size of the UnsafeRow using the size collected by BufferHolder, and returns
   * the UnsafeRow created at a constructor
   */
  public UnsafeRow getRow() {
    row.setTotalSize(totalSize());
    return row;
  }

  /**
   * Resets the `startingOffset` according to the current cursor of row buffer, and clear out null
   * bits.  This should be called before we write a new nested struct to the row buffer.
   */
  public void resetRowWriter() {
    this.startingOffset = cursor();

    // grow the global buffer to make sure it has enough space to write fixed-length data.
    grow(fixedSize);
    increaseCursor(fixedSize);

    zeroOutNullBytes();
  }

  /**
   * Clears out null bits.  This should be called before we write a new row to row buffer.
   */
  public void zeroOutNullBytes() {
    for (int i = 0; i < nullBitsSize; i += 8) {
      Platform.putLong(getBuffer(), startingOffset + i, 0L);
    }
  }

  public boolean isNullAt(int ordinal) {
    return BitSetMethods.isSet(getBuffer(), startingOffset, ordinal);
  }

  public void setNullAt(int ordinal) {
    BitSetMethods.set(getBuffer(), startingOffset, ordinal);
    write(ordinal, 0L);
  }

  @Override
  public void setNull1Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  public void setNull2Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  public void setNull4Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  public void setNull8Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  public long getFieldOffset(int ordinal) {
    return startingOffset + nullBitsSize + 8 * ordinal;
  }

  public void write(int ordinal, boolean value) {
    final long offset = getFieldOffset(ordinal);
    writeLong(offset, 0L);
    writeBoolean(offset, value);
  }

  public void write(int ordinal, byte value) {
    final long offset = getFieldOffset(ordinal);
    writeLong(offset, 0L);
    writeByte(offset, value);
  }

  public void write(int ordinal, short value) {
    final long offset = getFieldOffset(ordinal);
    writeLong(offset, 0L);
    writeShort(offset, value);
  }

  public void write(int ordinal, int value) {
    final long offset = getFieldOffset(ordinal);
    writeLong(offset, 0L);
    writeInt(offset, value);
  }

  public void write(int ordinal, long value) {
    writeLong(getFieldOffset(ordinal), value);
  }

  public void write(int ordinal, float value) {
    final long offset = getFieldOffset(ordinal);
    writeLong(offset, 0);
    writeFloat(offset, value);
  }

  public void write(int ordinal, double value) {
    writeDouble(getFieldOffset(ordinal), value);
  }

  public void write(int ordinal, Decimal input, int precision, int scale) {
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      // make sure Decimal object has the same scale as DecimalType
      if (input != null && input.changePrecision(precision, scale)) {
        write(ordinal, input.toUnscaledLong());
      } else {
        setNullAt(ordinal);
      }
    } else {
      // grow the global buffer before writing data.
      holder.grow(16);

      // always zero-out the 16-byte buffer
      Platform.putLong(getBuffer(), cursor(), 0L);
      Platform.putLong(getBuffer(), cursor() + 8, 0L);

      // Make sure Decimal object has the same scale as DecimalType.
      // Note that we may pass in null Decimal object to set null for it.
      if (input == null || !input.changePrecision(precision, scale)) {
        BitSetMethods.set(getBuffer(), startingOffset, ordinal);
        // keep the offset for future update
        setOffsetAndSize(ordinal, 0);
      } else {
        final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
        final int numBytes = bytes.length;
        assert numBytes <= 16;

        // Write the bytes to the variable length portion.
        Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, getBuffer(), cursor(), numBytes);
        setOffsetAndSize(ordinal, bytes.length);
      }

      // move the cursor forward.
      increaseCursor(16);
    }
  }
}
