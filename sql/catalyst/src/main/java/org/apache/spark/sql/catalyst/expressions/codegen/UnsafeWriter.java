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

import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

/**
 * Base class for writing Unsafe* structures.
 */
public abstract class UnsafeWriter {
  // Keep internal buffer holder
  protected final BufferHolder holder;

  // The offset of the global buffer where we start to write this structure.
  protected int startingOffset;

  protected UnsafeWriter(BufferHolder holder) {
    this.holder = holder;
  }

  /**
   * Accessor methods are delegated from BufferHolder class
   */
  public final BufferHolder getBufferHolder() {
    return holder;
  }

  public final byte[] getBuffer() {
    return holder.getBuffer();
  }

  public final void reset() {
    holder.reset();
  }

  public final int totalSize() {
    return holder.totalSize();
  }

  public final void grow(int neededSize) {
    holder.grow(neededSize);
  }

  public final int cursor() {
    return holder.getCursor();
  }

  public final void increaseCursor(int val) {
    holder.increaseCursor(val);
  }

  public final void setOffsetAndSizeFromPreviousCursor(int ordinal, int previousCursor) {
    setOffsetAndSize(ordinal, previousCursor, cursor() - previousCursor);
  }

  protected void setOffsetAndSize(int ordinal, int size) {
    setOffsetAndSize(ordinal, cursor(), size);
  }

  protected void setOffsetAndSize(int ordinal, int currentCursor, int size) {
    final long relativeOffset = currentCursor - startingOffset;
    final long offsetAndSize = (relativeOffset << 32) | (long)size;

    write(ordinal, offsetAndSize);
  }

  protected final void zeroOutPaddingBytes(int numBytes) {
    if ((numBytes & 0x07) > 0) {
      Platform.putLong(getBuffer(), cursor() + ((numBytes >> 3) << 3), 0L);
    }
  }

  public abstract void setNull1Bytes(int ordinal);
  public abstract void setNull2Bytes(int ordinal);
  public abstract void setNull4Bytes(int ordinal);
  public abstract void setNull8Bytes(int ordinal);

  public abstract void write(int ordinal, boolean value);
  public abstract void write(int ordinal, byte value);
  public abstract void write(int ordinal, short value);
  public abstract void write(int ordinal, int value);
  public abstract void write(int ordinal, long value);
  public abstract void write(int ordinal, float value);
  public abstract void write(int ordinal, double value);
  public abstract void write(int ordinal, Decimal input, int precision, int scale);

  public final void write(int ordinal, UTF8String input) {
    writeUnalignedBytes(ordinal, input.getBaseObject(), input.getBaseOffset(), input.numBytes());
  }

  public final void write(int ordinal, byte[] input) {
    write(ordinal, input, 0, input.length);
  }

  public final void write(int ordinal, byte[] input, int offset, int numBytes) {
    writeUnalignedBytes(ordinal, input, Platform.BYTE_ARRAY_OFFSET + offset, numBytes);
  }

  private void writeUnalignedBytes(
      int ordinal,
      Object baseObject,
      long baseOffset,
      int numBytes) {
    final int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
    grow(roundedSize);
    zeroOutPaddingBytes(numBytes);
    Platform.copyMemory(baseObject, baseOffset, getBuffer(), cursor(), numBytes);
    setOffsetAndSize(ordinal, numBytes);
    increaseCursor(roundedSize);
  }

  public void write(int ordinal, CalendarInterval input) {
    // grow the global buffer before writing data.
    grow(16);

    if (input == null) {
      BitSetMethods.set(getBuffer(), startingOffset, ordinal);
    } else {
      // Write the months, days and microseconds fields of interval to the variable length portion.
      Platform.putInt(getBuffer(), cursor(), input.months);
      Platform.putInt(getBuffer(), cursor() + 4, input.days);
      Platform.putLong(getBuffer(), cursor() + 8, input.microseconds);
    }
    // we need to reserve the space so that we can update it later.
    setOffsetAndSize(ordinal, 16);
    // move the cursor forward.
    increaseCursor(16);
  }

  public void write(int ordinal, VariantVal input) {
    // See the class comment of VariantVal for the format of the binary content.
    byte[] value = input.getValue();
    byte[] metadata = input.getMetadata();
    int totalSize = 4 + value.length + metadata.length;
    int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(totalSize);
    grow(roundedSize);
    zeroOutPaddingBytes(totalSize);
    Platform.putInt(getBuffer(), cursor(), value.length);
    Platform.copyMemory(value, Platform.BYTE_ARRAY_OFFSET, getBuffer(), cursor() + 4, value.length);
    Platform.copyMemory(
        metadata,
        Platform.BYTE_ARRAY_OFFSET,
        getBuffer(),
        cursor() + 4 + value.length,
        metadata.length
    );
    setOffsetAndSize(ordinal, totalSize);
    increaseCursor(roundedSize);
  }

  public final void write(int ordinal, UnsafeRow row) {
    writeAlignedBytes(ordinal, row.getBaseObject(), row.getBaseOffset(), row.getSizeInBytes());
  }

  public final void write(int ordinal, UnsafeMapData map) {
    writeAlignedBytes(ordinal, map.getBaseObject(), map.getBaseOffset(), map.getSizeInBytes());
  }

  public final void write(UnsafeArrayData array) {
    // Unsafe arrays both can be written as a regular array field or as part of a map. This makes
    // updating the offset and size dependent on the code path, this is why we currently do not
    // provide an method for writing unsafe arrays that also updates the size and offset.
    int numBytes = array.getSizeInBytes();
    grow(numBytes);
    Platform.copyMemory(
            array.getBaseObject(),
            array.getBaseOffset(),
            getBuffer(),
            cursor(),
            numBytes);
    increaseCursor(numBytes);
  }

  private void writeAlignedBytes(
      int ordinal,
      Object baseObject,
      long baseOffset,
      int numBytes) {
    grow(numBytes);
    Platform.copyMemory(baseObject, baseOffset, getBuffer(), cursor(), numBytes);
    setOffsetAndSize(ordinal, numBytes);
    increaseCursor(numBytes);
  }

  protected final void writeBoolean(long offset, boolean value) {
    Platform.putBoolean(getBuffer(), offset, value);
  }

  protected final void writeByte(long offset, byte value) {
    Platform.putByte(getBuffer(), offset, value);
  }

  protected final void writeShort(long offset, short value) {
    Platform.putShort(getBuffer(), offset, value);
  }

  protected final void writeInt(long offset, int value) {
    Platform.putInt(getBuffer(), offset, value);
  }

  protected final void writeLong(long offset, long value) {
    Platform.putLong(getBuffer(), offset, value);
  }

  protected final void writeFloat(long offset, float value) {
    Platform.putFloat(getBuffer(), offset, value);
  }

  protected final void writeDouble(long offset, double value) {
    Platform.putDouble(getBuffer(), offset, value);
  }
}
