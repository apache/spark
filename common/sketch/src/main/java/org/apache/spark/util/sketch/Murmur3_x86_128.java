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

import java.nio.ByteOrder;

/**
 * 128-bit Murmur3 hasher. This is based on Guava's Murmur3_128HashFunction .
 */
// This class is duplicated from `org.apache.spark.unsafe.hash.Murmur3_x86_128` to make sure
// spark-sketch has no external dependencies.
public class Murmur3_x86_128 {
  private static final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  private static final long C1 = 0x87c37b91114253d5L;
  private static final long C2 = 0x4cf5ad432745937fL;

  private final int seed;

  Murmur3_x86_128(int seed) {
    this.seed = seed;
  }

  @Override
  public String toString() {
    return "Murmur3_128(seed=" + seed + ")";
  }

  public static class HashObject {
    private final long h1;
    private final long h2;

    public HashObject(long h1, long h2) {
      this.h1 = h1;
      this.h2 = h2;
    }

    public long getHash1() {
      return h1;
    }

    public long getHash2() {
      return h2;
    }
  }

  public HashObject hashInt(int input) {
    return hashInt(input, seed);
  }

  public static HashObject hashInt(int input, int seed) {
    return hashLong(((long) input) << 32, seed);
  }

  public HashObject hashLong(long input) {
    return hashLong(input, seed);
  }

  public static HashObject hashLong(long input, int seed) {
    long k1 = mixK1(input);
    // Since k2 is 0, h2 is also 0
    long h1 = bmix64H1(seed, k1, 0);
    return hashResult(h1, 0, 8);
  }

  public long hashUnsafeWords(Object base, long offset, int lengthInBytes) {
    return hashUnsafeWords(base, offset, lengthInBytes, seed);
  }

  public static long hashUnsafeWords(Object base, long offset, int lengthInBytes, int seed) {
    assert (lengthInBytes % 8 == 0) : "lengthInBytes must be a multiple of 8 (word-aligned)";
    long h1 = seed;
    long h2 = seed;
    long[] res = hashRemainingBytes(base, offset, lengthInBytes, h1, h2);
    return fmix64(res[0]);
  }

  public static HashObject hashUnsafeBytes(Object base, long offset, int lengthInBytes, int seed) {
    // This is compatible with original and another implementations.    `   `
    assert (lengthInBytes >= 0) : "lengthInBytes cannot be negative";
    int remainingBytes = lengthInBytes % 16;
    int lengthAligned = lengthInBytes - remainingBytes;

    long[] res = hashBytesBy2Long(base, offset, lengthAligned, seed);
    long h1 = res[0];
    long h2 = res[1];
    offset = res[2];
    res = hashRemainingBytes(base, offset, remainingBytes, h1, h2);
    h1 = res[0];
    h2 = res[1];

    return hashResult(h1, h2, lengthInBytes);
  }

  private static long[] hashBytesBy2Long(Object base, long offset, int lengthInBytes, int seed) {
    assert (lengthInBytes % 16 == 0);
    long h1 = seed;
    long h2 = seed;
    for (int i = 0; i < lengthInBytes; i += 16, offset += 16) {
      long word1 = Platform.getLong(base, offset);
      long word2 = Platform.getLong(base, offset + 8);
      if (isBigEndian) {
        word1 = Long.reverseBytes(word1);
        word2 = Long.reverseBytes(word2);
      }
      long[] res = bmix64(h1, word1, h2, word2);
      h1 = res[0];
      h2 = res[1];
    }
    return new long[]{h1, h2, offset};
  }

  // This is based on Guava's `Murmur3_128Hasher.processRemaining(ByteBuffer)` method.
  private static long[] hashRemainingBytes(
      Object base,
      long offset,
      int remainingBytes,
      long h1,
      long h2){
    long k1 = 0;
    long k2 = 0;
    switch (remainingBytes) {
      case 15:
        k2 ^= (long) toInt(Platform.getByte(base, offset + 14)) << 48;
        remainingBytes--; // fallthru
      case 14:
        k2 ^= (long) toInt(Platform.getByte(base, offset + 13)) << 40;
        remainingBytes--; // fallthru
      case 13:
        k2 ^= (long) toInt(Platform.getByte(base, offset + 12)) << 32;
        remainingBytes--; // fallthru
      case 12:
        k2 ^= (long) toInt(Platform.getByte(base, offset + 11)) << 24;
        remainingBytes--; // fallthru
      case 11:
        k2 ^= (long) toInt(Platform.getByte(base, offset + 10)) << 16;
        remainingBytes--; // fallthru
      case 10:
        k2 ^= (long) toInt(Platform.getByte(base, offset + 9)) << 8;
        remainingBytes--; // fallthru
      case 9:
        k2 ^= (long) toInt(Platform.getByte(base, offset + 8));
        remainingBytes--; // fallthru
      case 8:
        k1 ^= Platform.getLong(base, offset);
        break;
      case 7:
        k1 ^= (long) toInt(Platform.getByte(base, offset + 6)) << 48;
        remainingBytes--; // fallthru
      case 6:
        k1 ^= (long) toInt(Platform.getByte(base, offset + 5)) << 40;
        remainingBytes--; // fallthru
      case 5:
        k1 ^= (long) toInt(Platform.getByte(base, offset + 4)) << 32;
        remainingBytes--; // fallthru
      case 4:
        k1 ^= (long) toInt(Platform.getByte(base, offset + 3)) << 24;
        remainingBytes--; // fallthru
      case 3:
        k1 ^= (long) toInt(Platform.getByte(base, offset + 2)) << 16;
        remainingBytes--; // fallthru
      case 2:
        k1 ^= (long) toInt(Platform.getByte(base, offset + 1)) << 8;
        remainingBytes--; // fallthru
      case 1:
        k1 ^= (long) toInt(Platform.getByte(base, offset));
        break;
    }
    h1 ^= mixK1(k1);
    h2 ^= mixK2(k2);
    return new long[]{h1, h2};
  }

  private static long[] bmix64(long h1, long k1, long h2, long k2) {
    h1 = bmix64H1(h1, k1, h2);
    h2 = bmix64H2(h2, k2, h1);
    return new long[]{h1, h2};
  }

  private static HashObject hashResult(long h1, long h2, int lengthInBytes) {
    h1 ^= lengthInBytes;
    h2 ^= lengthInBytes;
    h1 += h2;
    h2 += h1;
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 += h2;
    h2 += h1;
    return new HashObject(h1, h2);
  }

  private static long bmix64H1(long h1, long k1, long h2) {
    h1 ^= mixK1(k1);
    h1 = Long.rotateLeft(h1, 27);
    h1 += h2;
    return h1 * 5 + 0x52dce729;
  }

  private static long bmix64H2(long h2, long k2, long h1) {
    h2 ^= mixK2(k2);
    h2 = Long.rotateLeft(h2, 31);
    h2 += h1;
    return h2 * 5 + 0x38495ab5;
  }

  private static long fmix64(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;
    return k;
  }

  private static long mixK1(long k1) {
    k1 *= C1;
    k1 = Long.rotateLeft(k1, 31);
    k1 *= C2;
    return k1;
  }

  private static long mixK2(long k2) {
    k2 *= C2;
    k2 = Long.rotateLeft(k2, 33);
    k2 *= C1;
    return k2;
  }

  // This method is copied from Guava's UnsignedBytes class to reduce dependency.
  private static int toInt(byte value) {
    return value & 255;
  }
}
