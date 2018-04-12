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
 * A consecutive block of memory with a byte array on Java heap.
 */
public final class ByteArrayMemoryBlock extends MemoryBlock {

  private final byte[] array;

  public ByteArrayMemoryBlock(byte[] obj, long offset, long size) {
    super(obj, offset, size);
    this.array = obj;
    assert(offset + size <= Platform.BYTE_ARRAY_OFFSET + obj.length) :
      "The sum of size " + size + " and offset " + offset + " should not be larger than " +
        "the size of the given memory space " + (obj.length + Platform.BYTE_ARRAY_OFFSET);
  }

  public ByteArrayMemoryBlock(long length) {
    this(new byte[Ints.checkedCast(length)], Platform.BYTE_ARRAY_OFFSET, length);
  }

  @Override
  public MemoryBlock subBlock(long offset, long size) {
    checkSubBlockRange(offset, size);
    if (offset == 0 && size == this.size()) return this;
    return new ByteArrayMemoryBlock(array, this.offset + offset, size);
  }

  public byte[] getByteArray() { return array; }

  /**
   * Creates a memory block pointing to the memory used by the byte array.
   */
  public static ByteArrayMemoryBlock fromArray(final byte[] array) {
    return new ByteArrayMemoryBlock(array, Platform.BYTE_ARRAY_OFFSET, array.length);
  }

  @Override
  public int getInt(long offset) {
    return Platform.getInt(array, this.offset + offset);
  }

  @Override
  public void putInt(long offset, int value) {
    Platform.putInt(array, this.offset + offset, value);
  }

  @Override
  public boolean getBoolean(long offset) {
    return Platform.getBoolean(array, this.offset + offset);
  }

  @Override
  public void putBoolean(long offset, boolean value) {
    Platform.putBoolean(array, this.offset + offset, value);
  }

  @Override
  public byte getByte(long offset) {
    return array[(int)(this.offset + offset - Platform.BYTE_ARRAY_OFFSET)];
  }

  @Override
  public void putByte(long offset, byte value) {
    array[(int)(this.offset + offset - Platform.BYTE_ARRAY_OFFSET)] = value;
  }

  @Override
  public short getShort(long offset) {
    return Platform.getShort(array, this.offset + offset);
  }

  @Override
  public void putShort(long offset, short value) {
    Platform.putShort(array, this.offset + offset, value);
  }

  @Override
  public long getLong(long offset) {
    return Platform.getLong(array, this.offset + offset);
  }

  @Override
  public void putLong(long offset, long value) {
    Platform.putLong(array, this.offset + offset, value);
  }

  @Override
  public float getFloat(long offset) {
    return Platform.getFloat(array, this.offset + offset);
  }

  @Override
  public void putFloat(long offset, float value) {
    Platform.putFloat(array, this.offset + offset, value);
  }

  @Override
  public double getDouble(long offset) {
    return Platform.getDouble(array, this.offset + offset);
  }

  @Override
  public void putDouble(long offset, double value) {
    Platform.putDouble(array, this.offset + offset, value);
  }
}
