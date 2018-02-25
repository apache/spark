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

public class OffHeapMemoryBlock extends MemoryBlock {
  static public final OffHeapMemoryBlock NULL = new OffHeapMemoryBlock(0, 0);

  public OffHeapMemoryBlock(long address, long size) {
    super(null, address, size);
  }

  public void setAddressAndSize(long address, long size) {
    this.offset = address;
    this.length = size;
  }

  @Override
  public MemoryBlock allocate(long offset, long size) {
    return new OffHeapMemoryBlock(this.offset + offset, size);
  }

  public final int getInt(long offset) {
    assert(offset + 4 <= this.offset + this.length); 
    return Platform.getInt(null, offset);
  }

  public final void putInt(long offset, int value) {
    assert(offset + 4 <= this.offset + this.length);
    Platform.putInt(null, offset, value);
  }

  public final boolean getBoolean(long offset) {
    assert(offset + 1 <= this.offset + this.length);
    return Platform.getBoolean(null, offset);
  }

  public final void putBoolean(long offset, boolean value) {
    assert(offset + 1 <= this.offset + this.length);
    Platform.putBoolean(null, offset, value);
  }

  public final byte getByte(long offset) {
    assert(offset + 1 <= this.offset + this.length);
    return Platform.getByte(null, offset);
  }

  public final void putByte(long offset, byte value) {
    assert(offset + 1 <= this.offset + this.length);
    Platform.putByte(null, offset, value);
  }

  public final short getShort(long offset) {
    assert(offset + 2 <= this.offset + this.length);
    return Platform.getShort(null, offset);
  }

  public final void putShort(long offset, short value) {
    assert(offset + 2 <= this.offset + this.length);
    Platform.putShort(null, offset, value);
  }

  public final long getLong(long offset) {
    assert(offset + 8 <= this.offset + this.length);
    return Platform.getLong(null, offset);
  }

  public final void putLong(long offset, long value) {
    assert(offset + 8 <= this.offset + this.length);
    Platform.putLong(null, offset, value);
  }

  public final float getFloat(long offset) {
    assert(offset + 4 <= this.offset + this.length);
    return Platform.getFloat(null, offset);
  }

  public final void putFloat(long offset, float value) {
    assert(offset + 4 <= this.offset + this.length);
    Platform.putFloat(null, offset, value);
  }

  public final double getDouble(long offset) {
    assert(offset + 8 <= this.offset + this.length);
    return Platform.getDouble(null, offset);
  }

  public final void putDouble(long offset, double value) {
    assert(offset + 8 <= this.offset + this.length);
    Platform.putDouble(null, offset, value);
  }

  public final void copyFrom(byte[] src, long srcOffset, long dstOffset, long length) {
    assert(srcOffset - Platform.BYTE_ARRAY_OFFSET + length <= src.length);
    assert(dstOffset + length <= this.offset + this.length);
    Platform.copyMemory(src, srcOffset, null, dstOffset, length);
  }

  public final void copyFrom(short[] src, long srcOffset, long dstOffset, long length) {
    assert(srcOffset - Platform.SHORT_ARRAY_OFFSET + length <= src.length * 2);
    assert(dstOffset + length <= this.offset + this.length);
    Platform.copyMemory(src, srcOffset, null, dstOffset, length);
  }

  public final void copyFrom(int[] src, long srcOffset, long dstOffset, long length) {
    assert(srcOffset - Platform.INT_ARRAY_OFFSET + length <= src.length * 4);
    assert(dstOffset + length <= this.offset + this.length);
    Platform.copyMemory(src, srcOffset, null, dstOffset, length);
  }

  public final void copyFrom(long[] src, long srcOffset, long dstOffset, long length) {
    assert(srcOffset - Platform.LONG_ARRAY_OFFSET + length <= src.length * 8);
    assert(dstOffset + length <= this.offset + this.length);
    Platform.copyMemory(src, srcOffset, null, dstOffset, length);
  }

  public final void copyFrom(float[] src, long srcOffset, long dstOffset, long length) {
    assert(srcOffset - Platform.FLOAT_ARRAY_OFFSET + length <= src.length * 4);
    assert(dstOffset + length <= this.offset + this.length);
    Platform.copyMemory(src, srcOffset, null, dstOffset, length);
  }

  public final void copyFrom(double[] src, long srcOffset, long dstOffset, long length) {
    assert(srcOffset - Platform.DOUBLE_ARRAY_OFFSET + length <= src.length * 8);
    assert(dstOffset + length <= this.offset + this.length);
    Platform.copyMemory(src, srcOffset, null, dstOffset, length);
  }

  public final void writeTo(long srcOffset, byte[] dst, long dstOffset, long length) {
    assert(srcOffset + length <= this.offset + this.length);
    assert(dstOffset - Platform.BYTE_ARRAY_OFFSET + length <= dst.length);
    Platform.copyMemory(null, srcOffset, dst, dstOffset, length);
  }

  public final void writeTo(long srcOffset, short[] dst, long dstOffset, long length) {
    assert(srcOffset + length <= this.offset + this.length);
    assert(dstOffset - Platform.SHORT_ARRAY_OFFSET + length <= dst.length * 2);
    Platform.copyMemory(null, srcOffset, dst, dstOffset, length);
  }

  public final void writeTo(long srcOffset, int[] dst, long dstOffset, long length) {
    assert(srcOffset + length <= this.offset + this.length);
    assert(dstOffset - Platform.INT_ARRAY_OFFSET + length <= dst.length * 4);
    Platform.copyMemory(null, srcOffset, dst, dstOffset, length);
  }

  public final void writeTo(long srcOffset, long[] dst, long dstOffset, long length) {
    assert(srcOffset + length <= this.offset + this.length);
    assert(dstOffset - Platform.LONG_ARRAY_OFFSET + length <= dst.length * 8);
    Platform.copyMemory(null, srcOffset, dst, dstOffset, length);
  }

  public final void writeTo(long srcOffset, float[] dst, long dstOffset, long length) {
    assert(srcOffset + length <= this.offset + this.length);
    assert(dstOffset - Platform.FLOAT_ARRAY_OFFSET + length <= dst.length * 4);
    Platform.copyMemory(null, srcOffset, dst, dstOffset, length);
  }

  public final void writeTo(long srcOffset, double[] dst, long dstOffset, long length) {
    assert(srcOffset + length <= this.offset + this.length);
    assert(dstOffset - Platform.DOUBLE_ARRAY_OFFSET + length <= dst.length * 8);
    Platform.copyMemory(null, srcOffset, dst, dstOffset, length);
  }
}
