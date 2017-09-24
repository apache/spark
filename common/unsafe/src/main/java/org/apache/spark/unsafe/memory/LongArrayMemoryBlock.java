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
 * A consecutive block of memory, starting at a {@link MemoryLocation} with a fixed size.
 */
public final class LongArrayMemoryBlock extends MemoryBlock {

  private final long[] array;

  public LongArrayMemoryBlock(long[] obj, long offset, long size) {
    super(obj, offset, size);
    this.array = obj;
  }

  @Override
  public MemoryBlock allocate(long offset, long size) {
    return new LongArrayMemoryBlock(array, offset, size);
  }

  public long[] getLongArray() { return array; }

  /**
   * Creates a memory block pointing to the memory used by the long array.
   */
  public static LongArrayMemoryBlock fromArray(final long[] array) {
    return new LongArrayMemoryBlock(array, Platform.LONG_ARRAY_OFFSET, array.length * 8);
  }


  public final int getInt(long offset) {
    return Platform.getInt(array, offset);
  }

  public final void putInt(long offset, int value) {
    Platform.putInt(array, offset, value);
  }

  public final boolean getBoolean(long offset) {
    return Platform.getBoolean(array, offset);
  }

  public final void putBoolean(long offset, boolean value) {
    Platform.putBoolean(array, offset, value);
  }

  public final byte getByte(long offset) {
    return Platform.getByte(array, offset);
  }

  public final void putByte(long offset, byte value) {
    Platform.putByte(array, offset, value);
  }

  public final short getShort(long offset) {
    return Platform.getShort(array, offset);
  }

  public final void putShort(long offset, short value) {
    Platform.putShort(array, offset, value);
  }

  public final long getLong(long offset) {
    return Platform.getLong(array, offset);
  }

  public final void putLong(long offset, long value) {
    Platform.putLong(array, offset, value);
  }

  public final float getFloat(long offset) {
    return Platform.getFloat(array, offset);
  }

  public final void putFloat(long offset, float value) {
    Platform.putFloat(array, offset, value);
  }

  public final double getDouble(long offset) {
    return Platform.getDouble(array, offset);
  }

  public final void putDouble(long offset, double value) {
    Platform.putDouble(array, offset, value);
  }

  public final Object getObjectVolatile(long offset) {
    return Platform.getObjectVolatile(array, offset);
  }

  public final void putObjectVolatile(long offset, Object value) {
    Platform.putObjectVolatile(array, offset, value);
  }
}
