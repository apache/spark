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

package org.apache.spark.util.sketch;

import java.util.Arrays;

public final class BitArray {
  private final long[] data;
  private long bitCount;

  static int numWords(long numBits) {
    long numWords = (long) Math.ceil(numBits / 64.0);
    if (numWords > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Can't allocate enough space for " + numBits + " bits");
    }
    return (int) numWords;
  }

  BitArray(long numBits) {
    if (numBits <= 0) {
      throw new IllegalArgumentException("numBits must be positive");
    }
    this.data = new long[numWords(numBits)];
    long bitCount = 0;
    for (long value : data) {
      bitCount += Long.bitCount(value);
    }
    this.bitCount = bitCount;
  }

  /** Returns true if the bit changed value. */
  boolean set(long index) {
    if (!get(index)) {
      data[(int) (index >>> 6)] |= (1L << index);
      bitCount++;
      return true;
    }
    return false;
  }

  boolean get(long index) {
    return (data[(int) (index >>> 6)] & (1L << index)) != 0;
  }

  /** Number of bits */
  long bitSize() {
    return (long) data.length * Long.SIZE;
  }

  /** Number of set bits (1s) */
  long cardinality() {
    return bitCount;
  }

  /** Combines the two BitArrays using bitwise OR. */
  void putAll(BitArray array) {
    assert data.length == array.data.length : "BitArrays must be of equal length when merging";
    long bitCount = 0;
    for (int i = 0; i < data.length; i++) {
      data[i] |= array.data[i];
      bitCount += Long.bitCount(data[i]);
    }
    this.bitCount = bitCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !(o instanceof BitArray)) return false;

    BitArray bitArray = (BitArray) o;
    return Arrays.equals(data, bitArray.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }
}
