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

import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link BloomFilterImplV2} that takes each bit position of an item from its own mix of the
 * item's 64-bit hash. V2 takes all of them from two 32-bit values modulo the number of bits. So
 * its positions repeat when the second value and the number of bits share a large factor. A small
 * V2 filter also has only as many patterns of positions as the square of its number of bits.
 */
class BloomFilterImplV3 extends BloomFilterImplV2 {

  BloomFilterImplV3(int numHashFunctions, long numBits, int seed) {
    super(numHashFunctions, numBits, seed);
  }

  private BloomFilterImplV3() {}

  @Override
  Version version() {
    return Version.V3;
  }

  // The 64-bit hash of an item, from its two 32-bit hashes.
  private static long hash64(HiLoHash inputHash) {
    return ((long) inputHash.hi() << 32) | (inputHash.lo() & 0xffffffffL);
  }

  // The 64-bit golden ratio. The i-th bit position of an item comes from its 64-bit hash plus i
  // times this, as in SplitMix64.
  private static final long GOLDEN_GAMMA = 0x9E3779B97F4A7C15L;

  // A bit position from `state`. The 64-bit finalizer of MurmurHash3 mixes it, and `reduce` maps
  // the mix below `bitSize`.
  private static long bitIndex(long state, long bitSize) {
    long z = (state ^ (state >>> 33)) * 0xff51afd7ed558ccdL;
    z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
    z ^= z >>> 33;
    return reduce(z, bitSize);
  }

  // The high 64 bits of the unsigned product of `z` and `bitSize`, a value below `bitSize`. That
  // avoids a 64-bit division. `Math.multiplyHigh` takes `z` as signed, so this adds `bitSize` back
  // when the top bit of `z` is set. Visible for testing.
  static long reduce(long z, long bitSize) {
    return Math.multiplyHigh(z, bitSize) + ((z >> 63) & bitSize);
  }

  @Override
  protected boolean scatterHashAndSetAllBits(HiLoHash inputHash) {
    long state = hash64(inputHash);
    long bitSize = bits.bitSize();
    boolean bitsChanged = false;
    for (int i = 0; i < numHashFunctions; i++) {
      state += GOLDEN_GAMMA;
      bitsChanged |= bits.set(bitIndex(state, bitSize));
    }
    return bitsChanged;
  }

  @Override
  protected boolean scatterHashAndGetAllBits(HiLoHash inputHash) {
    long state = hash64(inputHash);
    long bitSize = bits.bitSize();
    for (int i = 0; i < numHashFunctions; i++) {
      state += GOLDEN_GAMMA;
      if (!bits.get(bitIndex(state, bitSize))) {
        return false;
      }
    }
    return true;
  }

  public static BloomFilterImplV3 readFrom(InputStream in) throws IOException {
    BloomFilterImplV3 filter = new BloomFilterImplV3();
    filter.readFrom0(in);
    return filter;
  }
}
