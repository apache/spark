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

package org.apache.spark.unsafe.array;

import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * An array of long values. Compared with native JVM arrays, this:
 * <ul>
 *   <li>supports using both in-heap and off-heap memory</li>
 *   <li>supports 64-bit addressing, i.e. array length greater than {@code Integer.MAX_VALUE}</li>
 *   <li>has no bound checking, and thus can crash the JVM process when assert is turned off</li>
 * </ul>
 */
public final class LongArray {

  private static final int WIDTH = 8;
  private static final long ARRAY_OFFSET = PlatformDependent.LONG_ARRAY_OFFSET;

  private final MemoryBlock memory;
  private final Object baseObj;
  private final long baseOffset;

  private final long length;

  public LongArray(MemoryBlock memory) {
    assert memory.size() % WIDTH == 0 : "Memory not aligned (" + memory.size() + ")";
    this.memory = memory;
    this.baseObj = memory.getBaseObject();
    this.baseOffset = memory.getBaseOffset();
    this.length = memory.size() / WIDTH;
  }

  public MemoryBlock memoryBlock() {
    return memory;
  }

  /**
   * Returns the number of elements this array can hold.
   */
  public long size() {
    return length;
  }

  /**
   * Sets the value at position {@code index}.
   */
  public void set(long index, long value) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < length : "index (" + index + ") should < length (" + length + ")";
    PlatformDependent.UNSAFE.putLong(baseObj, baseOffset + index * WIDTH, value);
  }

  /**
   * Returns the value at position {@code index}.
   */
  public long get(long index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < length : "index (" + index + ") should < length (" + length + ")";
    return PlatformDependent.UNSAFE.getLong(baseObj, baseOffset + index * WIDTH);
  }

  /**
   * Returns a copy of the array as a JVM native array. The caller should make sure this array's
   * length is less than {@code Integer.MAX_VALUE}.
   */
  public long[] toJvmArray() throws IndexOutOfBoundsException {
    if (length > Integer.MAX_VALUE) {
      throw new IndexOutOfBoundsException(
        "array size (" + length + ") too large and cannot be converted into JVM array");
    }

    final long[] arr = new long[(int) length];
    PlatformDependent.UNSAFE.copyMemory(
      baseObj,
      baseOffset,
      arr,
      ARRAY_OFFSET,
      length * WIDTH);
    return arr;
  }
}
