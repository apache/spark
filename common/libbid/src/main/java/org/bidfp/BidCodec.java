/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bidfp;

/**
 * Generic IEEE 754 BID combination-field encoder and decoder for any
 * {@link FormatParams} width.
 *
 * <p>Packed encodings are little-endian {@code long[]} of
 * {@link FormatParams#wordCount()} words. Unused high bits of the last word
 * are zero.
 */
final class BidCodec {
  private BidCodec() {
  }

  static void clear(FormatParams format, long[] words) {
    int n = format.wordCount();
    for (int i = 0; i < n; i++) {
      words[i] = 0L;
    }
  }

  static void packFinite(
      FormatParams format,
      boolean negative,
      int biasedExponent,
      UIntLimbs coefficient,
      long[] words) {
    clear(format, words);
    int tPlus3 = format.form1CoeffBits();
    if (coefficient.lessThanPow2(tPlus3)) {
      writeLimbs(words, coefficient);
      maskToWidth(words, tPlus3);
      orUInt(words, tPlus3, format.expBits(), biasedExponent);
    } else {
      writeLimbs(words, coefficient);
      maskToWidth(words, format.form2CoeffBits());
      orUInt(words, format.form2CoeffBits(), format.expBits(), biasedExponent);
      setBit(words, format.packedBits() - 2);
      setBit(words, format.packedBits() - 3);
    }
    if (negative) {
      setBit(words, format.signBit());
    }
    maskToWidth(words, format.packedBits());
  }

  static void packInfinity(FormatParams format, boolean negative, long[] words) {
    clear(format, words);
    // G[0:4] = 11110 at bits k-2 .. k-6.
    setBit(words, format.packedBits() - 2);
    setBit(words, format.packedBits() - 3);
    setBit(words, format.packedBits() - 4);
    setBit(words, format.packedBits() - 5);
    if (negative) {
      setBit(words, format.signBit());
    }
  }

  static void packNaN(
      FormatParams format, boolean negative, boolean signaling, long[] words) {
    clear(format, words);
    // G[0:4] = 11111 at bits k-2 .. k-6.
    setBit(words, format.packedBits() - 2);
    setBit(words, format.packedBits() - 3);
    setBit(words, format.packedBits() - 4);
    setBit(words, format.packedBits() - 5);
    setBit(words, format.packedBits() - 6);
    if (signaling) {
      setBit(words, format.packedBits() - 7);
    }
    if (negative) {
      setBit(words, format.signBit());
    }
  }

  static boolean isSigned(FormatParams format, long[] words) {
    return bit(words, format.signBit());
  }

  static boolean isNaN(FormatParams format, long[] words) {
    return bitsEqual11111(words, format.packedBits() - 6);
  }

  static boolean isSignalingNaN(FormatParams format, long[] words) {
    return isNaN(format, words) && bit(words, format.packedBits() - 7);
  }

  static boolean isInfinite(FormatParams format, long[] words) {
    return bit(words, format.packedBits() - 2)
        && bit(words, format.packedBits() - 3)
        && bit(words, format.packedBits() - 4)
        && bit(words, format.packedBits() - 5)
        && !bit(words, format.packedBits() - 6);
  }

  static boolean isFinite(FormatParams format, long[] words) {
    return !isInfinite(format, words) && !isNaN(format, words);
  }

  static int unpackBiasedExponent(FormatParams format, long[] words) {
    if (steering11(format, words)) {
      return (int) readUInt(words, format.form2CoeffBits(), format.expBits());
    }
    return (int) readUInt(words, format.form1CoeffBits(), format.expBits());
  }

  static void unpackCoefficient(FormatParams format, long[] words, UIntLimbs dest) {
    dest.clear();
    if (steering11(format, words)) {
      readLimbs(words, format.form2CoeffBits(), dest);
      dest.orPow2(format.form1CoeffBits());
    } else {
      readLimbs(words, format.form1CoeffBits(), dest);
    }
  }

  static void copyWords(FormatParams format, long[] src, long[] dest) {
    int n = format.wordCount();
    System.arraycopy(src, 0, dest, 0, n);
  }

  static long[] cloneWords(FormatParams format, long[] src) {
    long[] dest = new long[format.wordCount()];
    copyWords(format, src, dest);
    return dest;
  }

  static boolean steering11(FormatParams format, long[] words) {
    return bit(words, format.packedBits() - 2) && bit(words, format.packedBits() - 3);
  }

  static boolean bit(long[] words, int index) {
    return (words[index >>> 6] & (1L << (index & 63))) != 0L;
  }

  static void setBit(long[] words, int index) {
    words[index >>> 6] |= 1L << (index & 63);
  }

  static void clearBit(long[] words, int index) {
    words[index >>> 6] &= ~(1L << (index & 63));
  }

  static void maskToWidth(long[] words, int width) {
    if (width <= 0) {
      for (int i = 0; i < words.length; i++) {
        words[i] = 0L;
      }
      return;
    }
    int last = width >>> 6;
    int shift = width & 63;
    if (shift == 0) {
      for (int i = last; i < words.length; i++) {
        words[i] = 0L;
      }
      return;
    }
    words[last] &= (1L << shift) - 1L;
    for (int i = last + 1; i < words.length; i++) {
      words[i] = 0L;
    }
  }

  private static boolean bitsEqual11111(long[] words, int low) {
    return bit(words, low)
        && bit(words, low + 1)
        && bit(words, low + 2)
        && bit(words, low + 3)
        && bit(words, low + 4);
  }

  private static void writeLimbs(long[] words, UIntLimbs coefficient) {
    int n = Math.min(words.length, UIntLimbs.MAX_LIMBS);
    for (int i = 0; i < n; i++) {
      words[i] = coefficient.limb(i);
    }
  }

  private static void readLimbs(long[] words, int width, UIntLimbs dest) {
    dest.copyFrom(words);
    dest.maskLowBits(width);
  }

  static void orUInt(long[] words, int bitOffset, int width, long value) {
    if (width <= 0 || value == 0L) {
      return;
    }
    int word = bitOffset >>> 6;
    int shift = bitOffset & 63;
    words[word] |= value << shift;
    if (shift != 0 && width + shift > 64) {
      words[word + 1] |= value >>> (64 - shift);
    }
  }

  static long readUInt(long[] words, int bitOffset, int width) {
    int word = bitOffset >>> 6;
    int shift = bitOffset & 63;
    long value = words[word] >>> shift;
    if (shift != 0 && width + shift > 64 && word + 1 < words.length) {
      value |= words[word + 1] << (64 - shift);
    }
    if (width >= 64) {
      return value;
    }
    return value & ((1L << width) - 1L);
  }
}
