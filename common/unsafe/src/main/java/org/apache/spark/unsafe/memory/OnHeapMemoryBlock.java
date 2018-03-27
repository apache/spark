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

import com.google.common.primitives.Ints;

import org.apache.spark.unsafe.Platform;

/**
 * A consecutive block of memory with a long array on Java heap.
 */
public final class OnHeapMemoryBlock extends MemoryBlock {

  private final long[] array;

  public OnHeapMemoryBlock(long[] obj, long offset, long size) {
    super(obj, offset, size);
    this.array = obj;
    assert(offset + size <= obj.length * 8L + Platform.LONG_ARRAY_OFFSET) :
      "The sum of size " + size + " and offset " + offset + " should not be larger than " +
        "the array size " + (obj.length * 8L + Platform.LONG_ARRAY_OFFSET);
  }

  public OnHeapMemoryBlock(long size) {
    this(new long[Ints.checkedCast((size + 7) / 8)], Platform.LONG_ARRAY_OFFSET, size);
  }

  @Override
  public MemoryBlock subBlock(long offset, long size) {
    checkSubBlockRange(offset, size);
    return new OnHeapMemoryBlock(array, this.offset + offset, size);
  }

  public long[] getLongArray() { return array; }

  /**
   * Creates a memory block pointing to the memory used by the long array.
   */
  public static OnHeapMemoryBlock fromArray(final long[] array) {
    return new OnHeapMemoryBlock(array, Platform.LONG_ARRAY_OFFSET, array.length * 8L);
  }

  public static OnHeapMemoryBlock fromArray(final long[] array, long size) {
    return new OnHeapMemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
  }

  @Override
  public final int getInt(long offset) {
    return Platform.getInt(array, this.offset + offset);
  }

  @Override
  public final void putInt(long offset, int value) {
    Platform.putInt(array, this.offset + offset, value);
  }

  @Override
  public final boolean getBoolean(long offset) {
    return Platform.getBoolean(array, this.offset + offset);
  }

  @Override
  public final void putBoolean(long offset, boolean value) {
    Platform.putBoolean(array, this.offset + offset, value);
  }

  @Override
  public final byte getByte(long offset) {
    return Platform.getByte(array, this.offset + offset);
  }

  @Override
  public final void putByte(long offset, byte value) {
    Platform.putByte(array, this.offset + offset, value);
  }

  @Override
  public final short getShort(long offset) {
    return Platform.getShort(array, this.offset + offset);
  }

  @Override
  public final void putShort(long offset, short value) {
    Platform.putShort(array, this.offset + offset, value);
  }

  @Override
  public final long getLong(long offset) {
    return array[(int)((this.offset + offset - Platform.LONG_ARRAY_OFFSET) / 8)];
  }

  @Override
  public final void putLong(long offset, long value) {
    array[(int)((this.offset + offset - Platform.LONG_ARRAY_OFFSET) / 8)] = value;
  }

  @Override
  public final float getFloat(long offset) {
    return Platform.getFloat(array, this.offset + offset);
  }

  @Override
  public final void putFloat(long offset, float value) {
    Platform.putFloat(array, this.offset + offset, value);
  }

  @Override
  public final double getDouble(long offset) {
    return Platform.getDouble(array, this.offset + offset);
  }

  @Override
  public final void putDouble(long offset, double value) {
    Platform.putDouble(array, this.offset + offset, value);
  }
}
