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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class DefaultBloomFilter extends BloomFilter {

  private final int numHashFunctions;
  private final BitArray bits;

  public DefaultBloomFilter(int numHashFunctions, long numBits) {
    this.numHashFunctions = numHashFunctions;
    this.bits = new BitArray(numBits);
  }

  @Override
  public double expectedFpp() {
    return Math.pow((double) bits.bitCount() / bits.bitSize(), numHashFunctions);
  }

  @Override
  public long bitSize() {
    return bits.bitSize();
  }

  private static long hashObjectToLong(Object item) {
    if (item instanceof String) {
      try {
        byte[] bytes = ((String) item).getBytes("utf-8");
        return hashBytesToLong(bytes);
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Only support utf-8 string", e);
      }
    } else {
      long longValue;

      if (item instanceof Long) {
        longValue = (Long) item;
      } else if (item instanceof Integer) {
        longValue = ((Integer) item).longValue();
      } else if (item instanceof Short) {
        longValue = ((Short) item).longValue();
      } else if (item instanceof Byte) {
        longValue = ((Byte) item).longValue();
      } else {
        throw new IllegalArgumentException(
          "Support for " + item.getClass().getName() + " not implemented"
        );
      }

      int h1 = Murmur3_x86_32.hashLong(longValue, 0);
      int h2 = Murmur3_x86_32.hashLong(longValue, h1);
      return (((long) h1) << 32) | (h2 & 0xFFFFFFFFL);
    }
  }

  private static long hashBytesToLong(byte[] bytes) {
    int h1 = Murmur3_x86_32.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length, 0);
    int h2 = Murmur3_x86_32.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length, h1);
    return (((long) h1) << 32) | (h2 & 0xFFFFFFFFL);
  }

  @Override
  public boolean put(Object item) {
    long bitSize = bits.bitSize();
    long hash64 = hashObjectToLong(item);
    int h1 = (int) (hash64 >> 32);
    int h2 = (int) hash64;

    boolean bitsChanged = false;
    for (int i = 1; i <= numHashFunctions; i++) {
      int combinedHash = h1 + (i * h2);
      // Flip all the bits if it's negative (guaranteed positive number)
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      bitsChanged |= bits.set(combinedHash % bitSize);
    }
    return bitsChanged;
  }

  @Override
  public boolean mightContain(Object item) {
    long bitSize = bits.bitSize();
    long hash64 = hashObjectToLong(item);
    int h1 = (int) (hash64 >> 32);
    int h2 = (int) hash64;

    for (int i = 1; i <= numHashFunctions; i++) {
      int combinedHash = h1 + (i * h2);
      // Flip all the bits if it's negative (guaranteed positive number)
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      if (!bits.get(combinedHash % bitSize)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isCompatible(BloomFilter other) {
    if (other == null) {
      return false;
    }

    if (!(other instanceof DefaultBloomFilter)) {
      return false;
    }

    DefaultBloomFilter that = (DefaultBloomFilter) other;
    return this.bitSize() == that.bitSize() && this.numHashFunctions == that.numHashFunctions;
  }

  @Override
  public BloomFilter mergeInPlace(BloomFilter other) {
    if (!isCompatible(other)) {
      throw new IllegalArgumentException("Can't merge incompatible bloom filter");
    }

    DefaultBloomFilter that = (DefaultBloomFilter) other;
    this.bits.putAll(that.bits);
    return this;
  }

  static final class BitArray {
    final long[] data;
    long bitCount;

    static int safeCast(long numBits) {
      long numWords = (long) Math.ceil(numBits / 64.0);
      assert numWords <= Integer.MAX_VALUE : "Can't allocate enough space for " + numBits + " bits";
      return (int) numWords;
    }

    BitArray(long numBits) {
      this(new long[safeCast(numBits)]);
    }

    BitArray(long[] data) {
      assert data.length > 0 : "data length is zero!";
      this.data = data;
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
    long bitCount() {
      return bitCount;
    }

    BitArray copy() {
      return new BitArray(data.clone());
    }

    /** Combines the two BitArrays using bitwise OR. */
    void putAll(BitArray array) {
      assert data.length == array.data.length : "BitArrays must be of equal length when merging";
      bitCount = 0;
      for (int i = 0; i < data.length; i++) {
        data[i] |= array.data[i];
        bitCount += Long.bitCount(data[i]);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof BitArray) {
        BitArray bitArray = (BitArray) o;
        return Arrays.equals(data, bitArray.data);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }
  }
}
