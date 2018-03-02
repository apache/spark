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
 * A consecutive block of memory with a long array on Java heap.
 */
public final class OnHeapMemoryBlock extends MemoryBlock {

  private final long[] array;

  public OnHeapMemoryBlock(long[] obj, long offset, long size) {
    super(obj, offset, size);
    this.array = obj;
    assert(offset - Platform.LONG_ARRAY_OFFSET + size <= obj.length * 8L);
  }

  public OnHeapMemoryBlock(long size) {
    this(new long[(int)((size + 7) / 8)], Platform.LONG_ARRAY_OFFSET,
      ((size + 7) / 8) * 8L);
  }

  @Override
  public MemoryBlock subBlock(long offset, long size) {
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
    assert(offset + 4 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    return Platform.getInt(array, offset);
  }

  @Override
  public final void putInt(long offset, int value) {
    assert(offset + 4 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    Platform.putInt(array, offset, value);
  }

  @Override
  public final boolean getBoolean(long offset) {
    assert(offset + 1 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    return Platform.getBoolean(array, offset);
  }

  @Override
  public final void putBoolean(long offset, boolean value) {
    assert(offset + 1 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    Platform.putBoolean(array, offset, value);
  }

  @Override
  public final byte getByte(long offset) {
    assert(offset + 1 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    return Platform.getByte(array, offset);
  }

  @Override
  public final void putByte(long offset, byte value) {
    assert(offset + 1 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    Platform.putByte(array, offset, value);
  }

  @Override
  public final short getShort(long offset) {
    assert(offset + 2 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    return Platform.getShort(array, offset);
  }

  @Override
  public final void putShort(long offset, short value) {
    assert(offset + 2 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    Platform.putShort(array, offset, value);
  }

  @Override
  public final long getLong(long offset) {
    assert(offset + 8 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    return Platform.getLong(array, offset);
  }

  @Override
  public final void putLong(long offset, long value) {
    assert(offset + 8 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    Platform.putLong(array, offset, value);
  }

  @Override
  public final float getFloat(long offset) {
    assert(offset + 4 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    return Platform.getFloat(array, offset);
  }

  @Override
  public final void putFloat(long offset, float value) {
    assert(offset + 4 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    Platform.putFloat(array, offset, value);
  }

  @Override
  public final double getDouble(long offset) {
    assert(offset + 8 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    return Platform.getDouble(array, offset);
  }

  @Override
  public final void putDouble(long offset, double value) {
    assert(offset + 8 - Platform.LONG_ARRAY_OFFSET <= array.length * 8L);
    Platform.putDouble(array, offset, value);
  }
}
