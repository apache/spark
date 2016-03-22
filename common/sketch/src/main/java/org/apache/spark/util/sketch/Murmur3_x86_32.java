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

/**
 * 32-bit Murmur3 hasher.  This is based on Guava's Murmur3_32HashFunction.
 */
// This class is duplicated from `org.apache.spark.unsafe.hash.Murmur3_x86_32` to make sure
// spark-sketch has no external dependencies.
final class Murmur3_x86_32 {
  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;

  private final int seed;

  Murmur3_x86_32(int seed) {
    this.seed = seed;
  }

  @Override
  public String toString() {
    return "Murmur3_32(seed=" + seed + ")";
  }

  public int hashInt(int input) {
    return hashInt(input, seed);
  }

  public static int hashInt(int input, int seed) {
    int k1 = mixK1(input);
    int h1 = mixH1(seed, k1);

    return fmix(h1, 4);
  }

  public int hashUnsafeWords(Object base, long offset, int lengthInBytes) {
    return hashUnsafeWords(base, offset, lengthInBytes, seed);
  }

  public static int hashUnsafeWords(Object base, long offset, int lengthInBytes, int seed) {
    // This is based on Guava's `Murmur32_Hasher.processRemaining(ByteBuffer)` method.
    assert (lengthInBytes % 8 == 0): "lengthInBytes must be a multiple of 8 (word-aligned)";
    int h1 = hashBytesByInt(base, offset, lengthInBytes, seed);
    return fmix(h1, lengthInBytes);
  }

  public static int hashUnsafeBytes(Object base, long offset, int lengthInBytes, int seed) {
    assert (lengthInBytes >= 0): "lengthInBytes cannot be negative";
    int lengthAligned = lengthInBytes - lengthInBytes % 4;
    int h1 = hashBytesByInt(base, offset, lengthAligned, seed);
    for (int i = lengthAligned; i < lengthInBytes; i++) {
      int halfWord = Platform.getByte(base, offset + i);
      int k1 = mixK1(halfWord);
      h1 = mixH1(h1, k1);
    }
    return fmix(h1, lengthInBytes);
  }

  private static int hashBytesByInt(Object base, long offset, int lengthInBytes, int seed) {
    assert (lengthInBytes % 4 == 0);
    int h1 = seed;
    for (int i = 0; i < lengthInBytes; i += 4) {
      int halfWord = Platform.getInt(base, offset + i);
      int k1 = mixK1(halfWord);
      h1 = mixH1(h1, k1);
    }
    return h1;
  }

  public int hashLong(long input) {
    return hashLong(input, seed);
  }

  public static int hashLong(long input, int seed) {
    int low = (int) input;
    int high = (int) (input >>> 32);

    int k1 = mixK1(low);
    int h1 = mixH1(seed, k1);

    k1 = mixK1(high);
    h1 = mixH1(h1, k1);

    return fmix(h1, 8);
  }

  private static int mixK1(int k1) {
    k1 *= C1;
    k1 = Integer.rotateLeft(k1, 15);
    k1 *= C2;
    return k1;
  }

  private static int mixH1(int h1, int k1) {
    h1 ^= k1;
    h1 = Integer.rotateLeft(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
  }

  // Finalization mix - force all bits of a hash block to avalanche
  private static int fmix(int h1, int length) {
    h1 ^= length;
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;
    return h1;
  }
}
