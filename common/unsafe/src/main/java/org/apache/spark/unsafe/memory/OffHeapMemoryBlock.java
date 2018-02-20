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
    return new OffHeapMemoryBlock(offset, size);
  }

  public final int getInt(long offset) {
    return Platform.getInt(null, offset);
  }

  public final void putInt(long offset, int value) {
    Platform.putInt(null, offset, value);
  }

  public final boolean getBoolean(long offset) {
    return Platform.getBoolean(null, offset);
  }

  public final void putBoolean(long offset, boolean value) {
    Platform.putBoolean(null, offset, value);
  }

  public final byte getByte(long offset) {
    return Platform.getByte(null, offset);
  }

  public final void putByte(long offset, byte value) {
    Platform.putByte(null, offset, value);
  }

  public final short getShort(long offset) {
    return Platform.getShort(null, offset);
  }

  public final void putShort(long offset, short value) {
    Platform.putShort(null, offset, value);
  }

  public final long getLong(long offset) {
    return Platform.getLong(null, offset);
  }

  public final void putLong(long offset, long value) {
    Platform.putLong(null, offset, value);
  }

  public final float getFloat(long offset) {
    return Platform.getFloat(null, offset);
  }

  public final void putFloat(long offset, float value) {
    Platform.putFloat(null, offset, value);
  }

  public final double getDouble(long offset) {
    return Platform.getDouble(null, offset);
  }

  public final void putDouble(long offset, double value) {
    Platform.putDouble(null, offset, value);
  }

  public final Object getObjectVolatile(long offset) {
    return Platform.getObjectVolatile(null, offset);
  }

  public final void putObjectVolatile(long offset, Object value) {
    Platform.putObjectVolatile(null, offset, value);
  }
}
