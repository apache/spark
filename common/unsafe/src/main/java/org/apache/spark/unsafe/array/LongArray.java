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

import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * An array of long values. Compared with native JVM arrays, this:
 * <ul>
 *   <li>supports using both in-heap and off-heap memory</li>
 *   <li>has no bound checking, and thus can crash the JVM process when assert is turned off</li>
 * </ul>
 */
public final class LongArray {

  // This is a long so that we perform long multiplications when computing offsets.
  private static final long WIDTH = 8;

  private final MemoryBlock memory;

  private final long length;

  public LongArray(MemoryBlock memory) {
    assert memory.size() < (long) Integer.MAX_VALUE * 8: "Array size >= Integer.MAX_VALUE elements";
    this.memory = memory;
    this.length = memory.size() / WIDTH;
  }

  public MemoryBlock memoryBlock() {
    return memory;
  }

  public Object getBaseObject() {
    return memory.getBaseObject();
  }

  public long getBaseOffset() {
    return memory.getBaseOffset();
  }

  /**
   * Returns the number of elements this array can hold.
   */
  public long size() {
    return length;
  }

  /**
   * Fill this all with 0L.
   */
  public void zeroOut() {
    for (long off = 0; off < length * WIDTH; off += WIDTH) {
      memory.putLong(off, 0);
    }
  }

  /**
   * Sets the value at position {@code index}.
   */
  public void set(int index, long value) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < length : "index (" + index + ") should < length (" + length + ")";
    memory.putLong(index * WIDTH, value);
  }

  /**
   * Returns the value at position {@code index}.
   */
  public long get(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < length : "index (" + index + ") should < length (" + length + ")";
    return memory.getLong(index * WIDTH);
  }
}
