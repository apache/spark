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
    return array[(int)(offset - Platform.BYTE_ARRAY_OFFSET)];
  }

  public final void putByte(long offset, byte value) {
    array[(int)(offset - Platform.BYTE_ARRAY_OFFSET)] = value;
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
}
