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

package org.apache.spark.unsafe.memory;

import org.apache.spark.unsafe.Platform;

/**
 * A consecutive block of memory with a byte array on Java heap.
 */
public final class ByteArrayMemoryBlock extends MemoryBlock {

  private final byte[] array;

  public ByteArrayMemoryBlock(byte[] obj, long offset, long length) {
    super(obj, offset, length);
    this.array = obj;
  }

  @Override
  public MemoryBlock allocate(long offset, long size) {
    return new ByteArrayMemoryBlock(array, this.offset + offset, size);
  }

  public byte[] getByteArray() { return array; }

  /**
   * Creates a memory block pointing to the memory used by the byte array.
   */
  public static ByteArrayMemoryBlock fromArray(final byte[] array) {
    return new ByteArrayMemoryBlock(array, Platform.BYTE_ARRAY_OFFSET, array.length);
  }


  public final int getInt(long offset) {
    // UTF8String.getPrefix() assumes data is 4-byte aligned
    assert(offset + 4 - Platform.BYTE_ARRAY_OFFSET <= ((array.length + 3) / 4) * 4);
    return Platform.getInt(array, offset);
  }

  public final void putInt(long offset, int value) {
    assert(offset + 4 - Platform.BYTE_ARRAY_OFFSET <= array.length);
    Platform.putInt(array, offset, value);
  }

  public final boolean getBoolean(long offset) {
    assert(offset + 1 - Platform.BYTE_ARRAY_OFFSET <= array.length);
    return Platform.getBoolean(array, offset);
  }

  public final void putBoolean(long offset, boolean value) {
    assert(offset + 1 - Platform.BYTE_ARRAY_OFFSET <= array.length);
    Platform.putBoolean(array, offset, value);
  }

  public final byte getByte(long offset) {
    return array[(int)(offset - Platform.BYTE_ARRAY_OFFSET)];
  }

  public final void putByte(long offset, byte value) {
    array[(int)(offset - Platform.BYTE_ARRAY_OFFSET)] = value;
  }

  public final short getShort(long offset) {
    assert(offset + 2 - Platform.BYTE_ARRAY_OFFSET <= array.length);
    return Platform.getShort(array, offset);
  }

  public final void putShort(long offset, short value) {
    assert(offset + 2 - Platform.BYTE_ARRAY_OFFSET <= array.length);
    Platform.putShort(array, offset, value);
  }

  public final long getLong(long offset) {
    // UTF8String.getPrefix() assumes data is 8-byte aligned
    assert(offset + 8 - Platform.BYTE_ARRAY_OFFSET <= ((array.length + 7) / 8) * 8);
    return Platform.getLong(array, offset);
  }

  public final void putLong(long offset, long value) {
    assert(offset + 8 - Platform.BYTE_ARRAY_OFFSET <= array.length);
    Platform.putLong(array, offset, value);
  }

  public final float getFloat(long offset) {
    assert(offset + 4 - Platform.BYTE_ARRAY_OFFSET <= array.length);
    return Platform.getFloat(array, offset);
  }

  public final void putFloat(long offset, float value) {
    assert(offset + 4 - Platform.BYTE_ARRAY_OFFSET <= array.length);
    Platform.putFloat(array, offset, value);
  }

  public final double getDouble(long offset) {
    assert(offset + 8 - Platform.BYTE_ARRAY_OFFSET <= array.length);
    return Platform.getDouble(array, offset);
  }

  public final void putDouble(long offset, double value) {
    assert(offset + 8 - Platform.BYTE_ARRAY_OFFSET <= array.length);
    Platform.putDouble(array, offset, value);
  }

  public final void copyFrom(byte[] src, long srcOffset, long dstOffset, long length) {
    assert(srcOffset - Platform.BYTE_ARRAY_OFFSET + length <= src.length);
    assert(dstOffset - Platform.BYTE_ARRAY_OFFSET + length <= array.length);
    Platform.copyMemory(src, srcOffset, array, dstOffset, length);
  }

  public final void copyFrom(short[] src, long srcOffset, long dstOffset, long length) {
    assert(srcOffset - Platform.SHORT_ARRAY_OFFSET + length <= src.length * 2);
    assert(dstOffset - Platform.BYTE_ARRAY_OFFSET + length <= array.length);
    Platform.copyMemory(src, srcOffset, array, dstOffset, length);
  }

  public final void copyFrom(int[] src, long srcOffset, long dstOffset, long length) {
    assert(srcOffset - Platform.INT_ARRAY_OFFSET + length <= src.length * 4);
    assert(dstOffset - Platform.BYTE_ARRAY_OFFSET + length <= array.length);
    Platform.copyMemory(src, srcOffset, array, dstOffset, length);
  }

  public final void copyFrom(long[] src, long srcOffset, long dstOffset, long length) {
    assert(srcOffset - Platform.LONG_ARRAY_OFFSET + length <= src.length * 8);
    assert(dstOffset - Platform.BYTE_ARRAY_OFFSET + length <= array.length);
    Platform.copyMemory(src, srcOffset, array, dstOffset, length);
  }

  public final void copyFrom(float[] src, long srcOffset, long dstOffset, long length) {
    assert(srcOffset - Platform.FLOAT_ARRAY_OFFSET + length <= src.length * 4);
    assert(dstOffset - Platform.BYTE_ARRAY_OFFSET + length <= array.length);
    Platform.copyMemory(src, srcOffset, array, dstOffset, length);
  }

  public final void copyFrom(double[] src, long srcOffset, long dstOffset, long length) {
    assert(srcOffset - Platform.DOUBLE_ARRAY_OFFSET + length <= src.length * 8);
    assert(dstOffset - Platform.LONG_ARRAY_OFFSET + length <= array.length);
    Platform.copyMemory(src, srcOffset, array, dstOffset, length);
  }

  public final void writeTo(long srcOffset, byte[] dst, long dstOffset, long length) {
    assert(srcOffset - Platform.BYTE_ARRAY_OFFSET + length <= array.length);
    assert(dstOffset - Platform.BYTE_ARRAY_OFFSET + length <= dst.length);
    Platform.copyMemory(array, srcOffset, dst, dstOffset, length);
  }

  public final void writeTo(long srcOffset, short[] dst, long dstOffset, long length) {
    assert(srcOffset - Platform.BYTE_ARRAY_OFFSET + length <= array.length);
    assert(dstOffset - Platform.SHORT_ARRAY_OFFSET + length <= dst.length * 2);
    Platform.copyMemory(array, srcOffset, dst, dstOffset, length);
  }

  public final void writeTo(long srcOffset, int[] dst, long dstOffset, long length) {
    assert(srcOffset - Platform.BYTE_ARRAY_OFFSET + length <= array.length);
    assert(dstOffset - Platform.INT_ARRAY_OFFSET + length <= dst.length * 4);
    Platform.copyMemory(array, srcOffset, dst, dstOffset, length);
  }

  public final void writeTo(long srcOffset, long[] dst, long dstOffset, long length) {
    assert(srcOffset - Platform.BYTE_ARRAY_OFFSET + length <= array.length);
    assert(dstOffset - Platform.LONG_ARRAY_OFFSET + length <= dst.length * 8);
    Platform.copyMemory(array, srcOffset, dst, dstOffset, length);
  }

  public final void writeTo(long srcOffset, float[] dst, long dstOffset, long length) {
    assert(srcOffset - Platform.BYTE_ARRAY_OFFSET + length <= array.length);
    assert(dstOffset - Platform.FLOAT_ARRAY_OFFSET + length <= dst.length * 4);
    Platform.copyMemory(array, srcOffset, dst, dstOffset, length);
  }

  public final void writeTo(long srcOffset, double[] dst, long dstOffset, long length) {
    assert(srcOffset - Platform.BYTE_ARRAY_OFFSET + length <= array.length);
    assert(dstOffset - Platform.DOUBLE_ARRAY_OFFSET + length <= dst.length * 8);
    Platform.copyMemory(array, srcOffset, dst, dstOffset, length);
  }
}
