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

package org.apache.spark.sql.catalyst.expressions;

import java.util.HexFormat;

import org.apache.spark.unsafe.types.UTF8String;

/**
 * A Java port of the XXH3 64-bit and 128-bit hash functions (from the reference implementation at
 * https://github.com/Cyan4973/xxHash, as specified in doc/xxhash_spec.md). The output is byte
 * compatible with the reference implementation, so it matches `xxhsum` and other XXH3 tools.
 *
 * <p>The multiplication and mixing machinery operates on 64-bit lanes; Java's signed
 * {@code long} is used as an unsigned 64-bit integer throughout ({@code >>>} for logical shift,
 * {@link Long#rotateLeft}, and {@link #unsignedMultiplyHigh} for the high half of 64x64
 * products). The 128-bit hash's 1-3 byte inputs are the exception: they are first composed as
 * 32-bit {@code int}s (see {@link #len1to3128}) before being widened.
 */
public final class XXH3 {

  private XXH3() {}

  private static final long PRIME32_1 = 0x9E3779B1L;
  private static final long PRIME32_2 = 0x85EBCA77L;
  private static final long PRIME32_3 = 0xC2B2AE3DL;
  private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
  private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
  private static final long PRIME64_3 = 0x165667B19E3779F9L;
  private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
  private static final long PRIME64_5 = 0x27D4EB2F165667C5L;
  private static final long PRIME_MX1 = 0x165667919E3779F9L;
  private static final long PRIME_MX2 = 0x9FB21C651E98DF25L;

  private static final int SECRET_SIZE = 192;
  private static final int SECRET_SIZE_MIN = 136;
  private static final int STRIPE_LEN = 64;
  private static final int SECRET_MERGEACCS_START = 11;
  private static final int SECRET_LASTACC_START = 7;
  private static final int NB_STRIPES_PER_BLOCK = (SECRET_SIZE - STRIPE_LEN) / 8;
  private static final int BLOCK_LEN = STRIPE_LEN * NB_STRIPES_PER_BLOCK;

  // The default 192-byte secret from the reference implementation.
  private static final byte[] SECRET = HexFormat.of().parseHex(
      "b8fe6c3923a44bbe7c01812cf721ad1c" +
      "ded46de9839097db7240a4a4b7b3671f" +
      "cb79e64eccc0e578825ad07dccff7221" +
      "b8084674f743248ee03590e6813a264c" +
      "3c2852bb91c300cb88d0658b1b532ea3" +
      "71644897a20df94e3819ef46a9deacd8" +
      "a8fa763fe39c343ff9dcbbc7c70b4f1d" +
      "8a51e04bcdb45931c89f7ec9d9787364" +
      "eac5ac8334d3ebc3c581a0fffa1363eb" +
      "170ddd51b7f0da49d316552629d4689e" +
      "2b16be587d47a1fc8ff8b8d17ad031ce" +
      "45cb3a8f95160428afd7fbcabb4b407e");

  // ---- little-endian reads / writes ----

  private static long readLE64(byte[] data, int offset) {
    return (data[offset] & 0xFFL)
        | ((data[offset + 1] & 0xFFL) << 8)
        | ((data[offset + 2] & 0xFFL) << 16)
        | ((data[offset + 3] & 0xFFL) << 24)
        | ((data[offset + 4] & 0xFFL) << 32)
        | ((data[offset + 5] & 0xFFL) << 40)
        | ((data[offset + 6] & 0xFFL) << 48)
        | ((data[offset + 7] & 0xFFL) << 56);
  }

  // Reads 4 little-endian bytes as an unsigned 32-bit value (in the low 32 bits of the result).
  private static long readLE32(byte[] data, int offset) {
    return (data[offset] & 0xFFL)
        | ((data[offset + 1] & 0xFFL) << 8)
        | ((data[offset + 2] & 0xFFL) << 16)
        | ((data[offset + 3] & 0xFFL) << 24);
  }

  private static void writeLE64(byte[] data, int offset, long value) {
    for (int i = 0; i < 8; i++) {
      data[offset + i] = (byte) (value >>> (8 * i));
    }
  }

  // ---- mixing helpers ----

  private static long xxh64Avalanche(long h) {
    h ^= h >>> 33;
    h *= PRIME64_2;
    h ^= h >>> 29;
    h *= PRIME64_3;
    h ^= h >>> 32;
    return h;
  }

  private static long xxh3Avalanche(long h) {
    h ^= h >>> 37;
    h *= PRIME_MX1;
    h ^= h >>> 32;
    return h;
  }

  // Unsigned high 64 bits of the 128-bit product a*b (Math.multiplyHigh is signed).
  private static long unsignedMultiplyHigh(long a, long b) {
    return Math.multiplyHigh(a, b) + ((a >> 63) & b) + ((b >> 63) & a);
  }

  // Low 64 bits XOR high 64 bits of the 128-bit product a*b.
  private static long mul128Fold64(long a, long b) {
    return (a * b) ^ unsignedMultiplyHigh(a, b);
  }

  private static long mix16B(byte[] input, int inOff, int secOff, long seed) {
    long lo = readLE64(input, inOff) ^ (readLE64(SECRET, secOff) + seed);
    long hi = readLE64(input, inOff + 8) ^ (readLE64(SECRET, secOff + 8) - seed);
    return mul128Fold64(lo, hi);
  }

  // ---- XXH3 64-bit ----

  public static long hash64(byte[] input, long seed) {
    int len = input.length;
    if (len <= 16) {
      if (len > 8) {
        return len9to16(input, len, seed);
      } else if (len >= 4) {
        return len4to8(input, len, seed);
      } else if (len > 0) {
        return len1to3(input, len, seed);
      }
      return xxh64Avalanche(seed ^ readLE64(SECRET, 56) ^ readLE64(SECRET, 64));
    } else if (len <= 128) {
      return len17to128(input, len, seed);
    } else if (len <= 240) {
      return len129to240(input, len, seed);
    }
    return hashLong(input, len, seed);
  }

  private static long len1to3(byte[] input, int len, long seed) {
    long combined = ((input[0] & 0xFFL) << 16)
        | ((input[len >> 1] & 0xFFL) << 24)
        | (input[len - 1] & 0xFFL)
        | ((long) len << 8);
    long flip = (readLE32(SECRET, 0) ^ readLE32(SECRET, 4)) + seed;
    return xxh64Avalanche(combined ^ flip);
  }

  private static long len4to8(byte[] input, int len, long seed) {
    seed ^= ((long) Integer.reverseBytes((int) seed) & 0xFFFFFFFFL) << 32;
    long in1 = readLE32(input, 0);
    long in2 = readLE32(input, len - 4);
    long combined = in2 | (in1 << 32);
    long flip = (readLE64(SECRET, 8) ^ readLE64(SECRET, 16)) - seed;
    long x = combined ^ flip;
    x ^= Long.rotateLeft(x, 49) ^ Long.rotateLeft(x, 24);
    x *= PRIME_MX2;
    x ^= (x >>> 35) + len;
    x *= PRIME_MX2;
    x ^= x >>> 28;
    return x;
  }

  private static long len9to16(byte[] input, int len, long seed) {
    long flip1 = (readLE64(SECRET, 24) ^ readLE64(SECRET, 32)) + seed;
    long flip2 = (readLE64(SECRET, 40) ^ readLE64(SECRET, 48)) - seed;
    long in1 = readLE64(input, 0) ^ flip1;
    long in2 = readLE64(input, len - 8) ^ flip2;
    long acc = len + Long.reverseBytes(in1) + in2 + mul128Fold64(in1, in2);
    return xxh3Avalanche(acc);
  }

  private static long len17to128(byte[] input, int len, long seed) {
    long acc = (long) len * PRIME64_1;
    for (int i = (len - 1) >> 5; i >= 0; i--) {
      acc += mix16B(input, 16 * i, 32 * i, seed);
      acc += mix16B(input, len - 16 * (i + 1), 32 * i + 16, seed);
    }
    return xxh3Avalanche(acc);
  }

  private static long len129to240(byte[] input, int len, long seed) {
    long acc = (long) len * PRIME64_1;
    int nbRounds = len / 16;
    for (int i = 0; i < 8; i++) {
      acc += mix16B(input, 16 * i, 16 * i, seed);
    }
    acc = xxh3Avalanche(acc);
    for (int i = 8; i < nbRounds; i++) {
      acc += mix16B(input, 16 * i, 16 * (i - 8) + 3, seed);
    }
    acc += mix16B(input, len - 16, SECRET_SIZE_MIN - 17, seed);
    return xxh3Avalanche(acc);
  }

  // ---- long input (> 240 bytes) ----

  private static byte[] customSecret(long seed) {
    if (seed == 0) {
      return SECRET;
    }
    byte[] secret = new byte[SECRET_SIZE];
    for (int i = 0; i < SECRET_SIZE / 16; i++) {
      writeLE64(secret, 16 * i, readLE64(SECRET, 16 * i) + seed);
      writeLE64(secret, 16 * i + 8, readLE64(SECRET, 16 * i + 8) - seed);
    }
    return secret;
  }

  private static void accumulate512(
      long[] acc, byte[] input, int inOff, byte[] secret, int secOff) {
    for (int i = 0; i < 8; i++) {
      long data = readLE64(input, inOff + 8 * i);
      long key = data ^ readLE64(secret, secOff + 8 * i);
      acc[i ^ 1] += data;
      acc[i] += (key & 0xFFFFFFFFL) * (key >>> 32);
    }
  }

  private static void scrambleAcc(long[] acc, byte[] secret, int secOff) {
    for (int i = 0; i < 8; i++) {
      acc[i] ^= acc[i] >>> 47;
      acc[i] ^= readLE64(secret, secOff + 8 * i);
      acc[i] *= PRIME32_1;
    }
  }

  private static long mergeAccs(long[] acc, byte[] secret, int secOff, long start) {
    long result = start;
    for (int i = 0; i < 4; i++) {
      long a0 = acc[2 * i] ^ readLE64(secret, secOff + 16 * i);
      long a1 = acc[2 * i + 1] ^ readLE64(secret, secOff + 16 * i + 8);
      result += mul128Fold64(a0, a1);
    }
    return xxh3Avalanche(result);
  }

  private static long[] hashLongAccumulate(byte[] input, int len, byte[] secret) {
    long[] acc = {PRIME32_3, PRIME64_1, PRIME64_2, PRIME64_3, PRIME64_4, PRIME32_2, PRIME64_5,
        PRIME32_1};
    int nbBlocks = (len - 1) / BLOCK_LEN;
    for (int n = 0; n < nbBlocks; n++) {
      for (int s = 0; s < NB_STRIPES_PER_BLOCK; s++) {
        accumulate512(acc, input, n * BLOCK_LEN + s * STRIPE_LEN, secret, s * 8);
      }
      scrambleAcc(acc, secret, SECRET_SIZE - STRIPE_LEN);
    }
    int nbStripes = ((len - 1) - BLOCK_LEN * nbBlocks) / STRIPE_LEN;
    for (int s = 0; s < nbStripes; s++) {
      accumulate512(acc, input, nbBlocks * BLOCK_LEN + s * STRIPE_LEN, secret, s * 8);
    }
    accumulate512(
        acc, input, len - STRIPE_LEN, secret, SECRET_SIZE - STRIPE_LEN - SECRET_LASTACC_START);
    return acc;
  }

  private static long hashLong(byte[] input, int len, long seed) {
    byte[] secret = customSecret(seed);
    long[] acc = hashLongAccumulate(input, len, secret);
    return mergeAccs(acc, secret, SECRET_MERGEACCS_START, (long) len * PRIME64_1);
  }

  /** Hashes the input with the default seed 0. */
  public static long hash64(byte[] input) {
    return hash64(input, 0L);
  }

  // ---- XXH3 128-bit ----

  private static long mult32to64(long a, long b) {
    return (a & 0xFFFFFFFFL) * (b & 0xFFFFFFFFL);
  }

  private static void mix32B(long[] acc, byte[] input, int in1, int in2, int secOff, long seed) {
    acc[0] += mix16B(input, in1, secOff, seed);
    acc[0] ^= readLE64(input, in2) + readLE64(input, in2 + 8);
    acc[1] += mix16B(input, in2, secOff + 16, seed);
    acc[1] ^= readLE64(input, in1) + readLE64(input, in1 + 8);
  }

  private static long[] finalize128(long[] acc, int len, long seed) {
    long low = xxh3Avalanche(acc[0] + acc[1]);
    long high = -xxh3Avalanche(
        acc[0] * PRIME64_1 + acc[1] * PRIME64_4 + ((long) len - seed) * PRIME64_2);
    return new long[] {low, high};
  }

  /** Returns the XXH3 128-bit hash as {@code {low64, high64}}. */
  public static long[] hash128(byte[] input, long seed) {
    int len = input.length;
    if (len <= 16) {
      if (len > 8) {
        return len9to16128(input, len, seed);
      } else if (len >= 4) {
        return len4to8128(input, len, seed);
      } else if (len > 0) {
        return len1to3128(input, len, seed);
      }
      long low = xxh64Avalanche(seed ^ readLE64(SECRET, 64) ^ readLE64(SECRET, 72));
      long high = xxh64Avalanche(seed ^ readLE64(SECRET, 80) ^ readLE64(SECRET, 88));
      return new long[] {low, high};
    } else if (len <= 128) {
      return len17to128128(input, len, seed);
    } else if (len <= 240) {
      return len129to240128(input, len, seed);
    }
    return hashLong128(input, len, seed);
  }

  private static long[] len1to3128(byte[] input, int len, long seed) {
    int c1 = input[0] & 0xFF;
    int c2 = input[len >> 1] & 0xFF;
    int c3 = input[len - 1] & 0xFF;
    int combinedl = (c1 << 16) | (c2 << 24) | c3 | (len << 8);
    int combinedh = Integer.rotateLeft(Integer.reverseBytes(combinedl), 13);
    long bitflipl = (readLE32(SECRET, 0) ^ readLE32(SECRET, 4)) + seed;
    long bitfliph = (readLE32(SECRET, 8) ^ readLE32(SECRET, 12)) - seed;
    long low = xxh64Avalanche((combinedl & 0xFFFFFFFFL) ^ bitflipl);
    long high = xxh64Avalanche((combinedh & 0xFFFFFFFFL) ^ bitfliph);
    return new long[] {low, high};
  }

  private static long[] len4to8128(byte[] input, int len, long seed) {
    seed ^= ((long) Integer.reverseBytes((int) seed) & 0xFFFFFFFFL) << 32;
    long inputLo = readLE32(input, 0);
    long inputHi = readLE32(input, len - 4);
    long input64 = inputLo | (inputHi << 32);
    long keyed = input64 ^ ((readLE64(SECRET, 16) ^ readLE64(SECRET, 24)) + seed);
    long mul = PRIME64_1 + ((long) len << 2);
    long lo = keyed * mul;
    long hi = unsignedMultiplyHigh(keyed, mul);
    hi += lo << 1;
    lo ^= hi >>> 3;
    lo ^= lo >>> 35;
    lo *= PRIME_MX2;
    lo ^= lo >>> 28;
    return new long[] {lo, xxh3Avalanche(hi)};
  }

  private static long[] len9to16128(byte[] input, int len, long seed) {
    long bitflipl = (readLE64(SECRET, 32) ^ readLE64(SECRET, 40)) - seed;
    long bitfliph = (readLE64(SECRET, 48) ^ readLE64(SECRET, 56)) + seed;
    long inputLo = readLE64(input, 0);
    long inputHi = readLE64(input, len - 8);
    long m0 = inputLo ^ inputHi ^ bitflipl;
    long lo = m0 * PRIME64_1;
    long hi = unsignedMultiplyHigh(m0, PRIME64_1);
    lo += (long) (len - 1) << 54;
    inputHi ^= bitfliph;
    hi += inputHi + mult32to64(inputHi, PRIME32_2 - 1);
    lo ^= Long.reverseBytes(hi);
    long h2lo = lo * PRIME64_2;
    long h2hi = unsignedMultiplyHigh(lo, PRIME64_2) + hi * PRIME64_2;
    return new long[] {xxh3Avalanche(h2lo), xxh3Avalanche(h2hi)};
  }

  private static long[] len17to128128(byte[] input, int len, long seed) {
    long[] acc = {(long) len * PRIME64_1, 0L};
    int i = (len - 1) / 32;
    do {
      mix32B(acc, input, 16 * i, len - 16 * (i + 1), 32 * i, seed);
    } while (i-- != 0);
    return finalize128(acc, len, seed);
  }

  private static long[] len129to240128(byte[] input, int len, long seed) {
    long[] acc = {(long) len * PRIME64_1, 0L};
    for (int i = 0; i < 4; i++) {
      mix32B(acc, input, 32 * i, 32 * i + 16, 32 * i, seed);
    }
    acc[0] = xxh3Avalanche(acc[0]);
    acc[1] = xxh3Avalanche(acc[1]);
    for (int i = 4; i < (len >> 5); i++) {
      mix32B(acc, input, 32 * i, 32 * i + 16, (i - 4) * 32 + 3, seed);
    }
    mix32B(acc, input, len - 16, len - 32, 103, -seed);
    return finalize128(acc, len, seed);
  }

  private static long[] hashLong128(byte[] input, int len, long seed) {
    byte[] secret = customSecret(seed);
    long[] acc = hashLongAccumulate(input, len, secret);
    long low = mergeAccs(acc, secret, SECRET_MERGEACCS_START, (long) len * PRIME64_1);
    long high = mergeAccs(acc, secret, SECRET_SIZE - STRIPE_LEN - SECRET_MERGEACCS_START,
        ~((long) len * PRIME64_2));
    return new long[] {low, high};
  }

  private static final byte[] HEX_DIGITS = {
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  // Hex-writing siblings of the length-branch methods above: same arithmetic, but they encode
  // the two lanes directly into the caller's hex buffer instead of returning a long[]. This
  // avoids the result-pair array for these Into variants; hashLongAccumulate (>240 bytes,
  // used by hashLong128Into below) still allocates its own accumulator. hash128 above is kept
  // array-returning for callers that need the raw pair.
  private static void len1to3128Into(byte[] input, int len, long seed, byte[] hex) {
    int c1 = input[0] & 0xFF;
    int c2 = input[len >> 1] & 0xFF;
    int c3 = input[len - 1] & 0xFF;
    int combinedl = (c1 << 16) | (c2 << 24) | c3 | (len << 8);
    int combinedh = Integer.rotateLeft(Integer.reverseBytes(combinedl), 13);
    long bitflipl = (readLE32(SECRET, 0) ^ readLE32(SECRET, 4)) + seed;
    long bitfliph = (readLE32(SECRET, 8) ^ readLE32(SECRET, 12)) - seed;
    long low = xxh64Avalanche((combinedl & 0xFFFFFFFFL) ^ bitflipl);
    long high = xxh64Avalanche((combinedh & 0xFFFFFFFFL) ^ bitfliph);
    writeHex64(hex, 0, high);
    writeHex64(hex, 16, low);
  }

  private static void len4to8128Into(byte[] input, int len, long seed, byte[] hex) {
    seed ^= ((long) Integer.reverseBytes((int) seed) & 0xFFFFFFFFL) << 32;
    long inputLo = readLE32(input, 0);
    long inputHi = readLE32(input, len - 4);
    long input64 = inputLo | (inputHi << 32);
    long keyed = input64 ^ ((readLE64(SECRET, 16) ^ readLE64(SECRET, 24)) + seed);
    long mul = PRIME64_1 + ((long) len << 2);
    long lo = keyed * mul;
    long hi = unsignedMultiplyHigh(keyed, mul);
    hi += lo << 1;
    lo ^= hi >>> 3;
    lo ^= lo >>> 35;
    lo *= PRIME_MX2;
    lo ^= lo >>> 28;
    writeHex64(hex, 0, xxh3Avalanche(hi));
    writeHex64(hex, 16, lo);
  }

  private static void len9to16128Into(byte[] input, int len, long seed, byte[] hex) {
    long bitflipl = (readLE64(SECRET, 32) ^ readLE64(SECRET, 40)) - seed;
    long bitfliph = (readLE64(SECRET, 48) ^ readLE64(SECRET, 56)) + seed;
    long inputLo = readLE64(input, 0);
    long inputHi = readLE64(input, len - 8);
    long m0 = inputLo ^ inputHi ^ bitflipl;
    long lo = m0 * PRIME64_1;
    long hi = unsignedMultiplyHigh(m0, PRIME64_1);
    lo += (long) (len - 1) << 54;
    inputHi ^= bitfliph;
    hi += inputHi + mult32to64(inputHi, PRIME32_2 - 1);
    lo ^= Long.reverseBytes(hi);
    long h2lo = lo * PRIME64_2;
    long h2hi = unsignedMultiplyHigh(lo, PRIME64_2) + hi * PRIME64_2;
    writeHex64(hex, 0, xxh3Avalanche(h2hi));
    writeHex64(hex, 16, xxh3Avalanche(h2lo));
  }

  // Scalar-returning siblings of mix32B's two lanes, used so the Into variants below thread the
  // accumulator through local variables instead of a long[2] array.
  private static long mixLowLane(long acc0, byte[] input, int in1, int in2, int secOff, long seed) {
    acc0 += mix16B(input, in1, secOff, seed);
    acc0 ^= readLE64(input, in2) + readLE64(input, in2 + 8);
    return acc0;
  }

  private static long mixHighLane(
      long acc1, byte[] input, int in1, int in2, int secOff, long seed) {
    acc1 += mix16B(input, in2, secOff + 16, seed);
    acc1 ^= readLE64(input, in1) + readLE64(input, in1 + 8);
    return acc1;
  }

  private static void len17to128128Into(byte[] input, int len, long seed, byte[] hex) {
    long acc0 = (long) len * PRIME64_1;
    long acc1 = 0L;
    int i = (len - 1) / 32;
    do {
      acc0 = mixLowLane(acc0, input, 16 * i, len - 16 * (i + 1), 32 * i, seed);
      acc1 = mixHighLane(acc1, input, 16 * i, len - 16 * (i + 1), 32 * i, seed);
    } while (i-- != 0);
    finalize128Into(acc0, acc1, len, seed, hex);
  }

  private static void len129to240128Into(byte[] input, int len, long seed, byte[] hex) {
    long acc0 = (long) len * PRIME64_1;
    long acc1 = 0L;
    for (int i = 0; i < 4; i++) {
      acc0 = mixLowLane(acc0, input, 32 * i, 32 * i + 16, 32 * i, seed);
      acc1 = mixHighLane(acc1, input, 32 * i, 32 * i + 16, 32 * i, seed);
    }
    acc0 = xxh3Avalanche(acc0);
    acc1 = xxh3Avalanche(acc1);
    for (int i = 4; i < (len >> 5); i++) {
      acc0 = mixLowLane(acc0, input, 32 * i, 32 * i + 16, (i - 4) * 32 + 3, seed);
      acc1 = mixHighLane(acc1, input, 32 * i, 32 * i + 16, (i - 4) * 32 + 3, seed);
    }
    acc0 = mixLowLane(acc0, input, len - 16, len - 32, 103, -seed);
    acc1 = mixHighLane(acc1, input, len - 16, len - 32, 103, -seed);
    finalize128Into(acc0, acc1, len, seed, hex);
  }

  private static void finalize128Into(long acc0, long acc1, int len, long seed, byte[] hex) {
    long low = xxh3Avalanche(acc0 + acc1);
    long high = -xxh3Avalanche(
        acc0 * PRIME64_1 + acc1 * PRIME64_4 + ((long) len - seed) * PRIME64_2);
    writeHex64(hex, 0, high);
    writeHex64(hex, 16, low);
  }

  private static void hashLong128Into(byte[] input, int len, long seed, byte[] hex) {
    byte[] secret = customSecret(seed);
    long[] acc = hashLongAccumulate(input, len, secret);
    long low = mergeAccs(acc, secret, SECRET_MERGEACCS_START, (long) len * PRIME64_1);
    long high = mergeAccs(acc, secret, SECRET_SIZE - STRIPE_LEN - SECRET_MERGEACCS_START,
        ~((long) len * PRIME64_2));
    writeHex64(hex, 0, high);
    writeHex64(hex, 16, low);
  }

  private static void hashHex128(byte[] input, long seed, byte[] hex) {
    int len = input.length;
    if (len <= 16) {
      if (len > 8) {
        len9to16128Into(input, len, seed, hex);
        return;
      } else if (len >= 4) {
        len4to8128Into(input, len, seed, hex);
        return;
      } else if (len > 0) {
        len1to3128Into(input, len, seed, hex);
        return;
      }
      long low = xxh64Avalanche(seed ^ readLE64(SECRET, 64) ^ readLE64(SECRET, 72));
      long high = xxh64Avalanche(seed ^ readLE64(SECRET, 80) ^ readLE64(SECRET, 88));
      writeHex64(hex, 0, high);
      writeHex64(hex, 16, low);
    } else if (len <= 128) {
      len17to128128Into(input, len, seed, hex);
    } else if (len <= 240) {
      len129to240128Into(input, len, seed, hex);
    } else {
      hashLong128Into(input, len, seed, hex);
    }
  }

  /** Returns the XXH3 128-bit hash as a 32-character lowercase hex string (default seed 0). */
  public static UTF8String hash128Hex(byte[] input) {
    return hash128Hex(input, 0L);
  }

  /**
   * Returns the XXH3 128-bit hash as a 32-character lowercase hex string (canonical big-endian).
   */
  public static UTF8String hash128Hex(byte[] input, long seed) {
    byte[] hex = new byte[32];
    hashHex128(input, seed, hex);
    return UTF8String.fromBytes(hex);
  }

  private static void writeHex64(byte[] hex, int offset, long value) {
    for (int i = 0; i < 8; i++) {
      int b = (int) (value >>> (56 - 8 * i)) & 0xFF;
      hex[offset + i * 2] = HEX_DIGITS[b >>> 4];
      hex[offset + i * 2 + 1] = HEX_DIGITS[b & 0x0F];
    }
  }
}
