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
 * Mutable unsigned integer of at most 256 bits, stored little-endian in four
 * {@code long} limbs. Packed BID-k coefficients live here; wide add/mul/div
 * uses {@link LimbMath} workspaces of {@link LimbMath#WORK} limbs.
 */
final class UIntLimbs {
  static final int MAX_LIMBS = 4;

  private final long[] w = new long[MAX_LIMBS];

  void clear() {
    w[0] = 0L;
    w[1] = 0L;
    w[2] = 0L;
    w[3] = 0L;
  }

  void set(long value) {
    w[0] = value;
    w[1] = 0L;
    w[2] = 0L;
    w[3] = 0L;
  }

  void set3(long l0, long l1, long l2) {
    w[0] = l0;
    w[1] = l1;
    w[2] = l2;
    w[3] = 0L;
  }

  void set4(long l0, long l1, long l2, long l3) {
    w[0] = l0;
    w[1] = l1;
    w[2] = l2;
    w[3] = l3;
  }

  void copyFrom(UIntLimbs other) {
    w[0] = other.w[0];
    w[1] = other.w[1];
    w[2] = other.w[2];
    w[3] = other.w[3];
  }

  void copyFrom(long[] words) {
    clear();
    int n = Math.min(words.length, MAX_LIMBS);
    System.arraycopy(words, 0, w, 0, n);
  }

  void copyTo(long[] dest) {
    int n = Math.min(dest.length, MAX_LIMBS);
    System.arraycopy(w, 0, dest, 0, n);
  }

  long[] limbs() {
    return w;
  }

  long limb(int index) {
    return w[index];
  }

  boolean isZero() {
    return (w[0] | w[1] | w[2] | w[3]) == 0L;
  }

  boolean bit(int index) {
    if (index < 0 || index >= 256) {
      return false;
    }
    return (w[index >>> 6] & (1L << (index & 63))) != 0L;
  }

  void setBit(int index) {
    if (index < 0 || index >= 256) {
      throw new IllegalArgumentException("bit index out of range");
    }
    w[index >>> 6] |= 1L << (index & 63);
  }

  /** Returns whether {@code this < 2^n}. */
  boolean lessThanPow2(int n) {
    if (n <= 0) {
      return isZero();
    }
    if (n >= 256) {
      return true;
    }
    int word = n >>> 6;
    int shift = n & 63;
    for (int i = word + 1; i < MAX_LIMBS; i++) {
      if (w[i] != 0L) {
        return false;
      }
    }
    if (shift == 0) {
      return w[word] == 0L;
    }
    return (w[word] >>> shift) == 0L;
  }

  void maskLowBits(int width) {
    if (width <= 0) {
      clear();
      return;
    }
    if (width >= 256) {
      return;
    }
    int word = width >>> 6;
    int shift = width & 63;
    if (shift == 0) {
      for (int i = word; i < MAX_LIMBS; i++) {
        w[i] = 0L;
      }
      return;
    }
    w[word] &= (1L << shift) - 1L;
    for (int i = word + 1; i < MAX_LIMBS; i++) {
      w[i] = 0L;
    }
  }

  void orPow2(int n) {
    setBit(n);
  }

  int compare(UIntLimbs other) {
    return compare(other.w);
  }

  int compare(long[] other) {
    for (int i = MAX_LIMBS - 1; i >= 0; i--) {
      long right = i < other.length ? other[i] : 0L;
      int cmp = Long.compareUnsigned(w[i], right);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  void multiplySmall(int factor) {
    if (factor == 0) {
      clear();
      return;
    }
    long m = factor & 0xffffffffL;
    long carry = 0L;
    for (int i = 0; i < MAX_LIMBS; i++) {
      long prodLo = w[i] * m;
      long prodHi = UInt128.unsignedMultiplyHigh(w[i], m);
      long sum = prodLo + carry;
      long c2 = Long.compareUnsigned(sum, prodLo) < 0 ? 1L : 0L;
      w[i] = sum;
      carry = prodHi + c2;
    }
    if (carry != 0L) {
      throw new ArithmeticException("UIntLimbs overflow");
    }
  }

  void addSmall(int addend) {
    if (addend == 0) {
      return;
    }
    long carry = addend & 0xffffffffL;
    for (int i = 0; i < MAX_LIMBS && carry != 0L; i++) {
      long sum = w[i] + carry;
      carry = Long.compareUnsigned(sum, w[i]) < 0 ? 1L : 0L;
      w[i] = sum;
    }
    if (carry != 0L) {
      throw new ArithmeticException("UIntLimbs overflow");
    }
  }

  void subtractOne() {
    for (int i = 0; i < MAX_LIMBS; i++) {
      long next = w[i] - 1L;
      w[i] = next;
      if (next != -1L) {
        return;
      }
    }
  }

  static long[] pow10MinusOne(int precision) {
    UIntLimbs value = new UIntLimbs();
    value.set(1L);
    for (int i = 0; i < precision; i++) {
      value.multiplySmall(10);
    }
    value.subtractOne();
    long[] words = new long[MAX_LIMBS];
    value.copyTo(words);
    return words;
  }

  @Override
  public String toString() {
    return String.format("0x%016x%016x%016x%016x", w[3], w[2], w[1], w[0]);
  }
}
