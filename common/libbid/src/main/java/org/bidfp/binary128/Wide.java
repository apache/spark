/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   * Neither the name of Intel Corporation nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.bidfp.binary128;

import java.math.BigInteger;

/** Unsigned 64/128/256-bit helpers for the DPML 64-bit UX digit path. */
final class Wide {
  private Wide() {
  }

  static long umulh(long x, long y) {
    long result = Math.multiplyHigh(x, y);
    if (x < 0) {
      result += y;
    }
    if (y < 0) {
      result += x;
    }
    return result;
  }

  static int cmp128(long aHi, long aLo, long bHi, long bLo) {
    int high = Long.compareUnsigned(aHi, bHi);
    return high != 0 ? high : Long.compareUnsigned(aLo, bLo);
  }

  static boolean add128(long aHi, long aLo, long bHi, long bLo, long[] out) {
    long lo = aLo + bLo;
    long carry = Long.compareUnsigned(lo, aLo) < 0 ? 1L : 0L;
    long hi = aHi + bHi + carry;
    boolean overflow = Long.compareUnsigned(aHi + bHi, aHi) < 0
        || (carry != 0L && Long.compareUnsigned(hi, aHi + bHi) < 0);
    out[0] = hi;
    out[1] = lo;
    return overflow;
  }

  static void sub128(long aHi, long aLo, long bHi, long bLo, long[] out) {
    long borrow = Long.compareUnsigned(aLo, bLo) < 0 ? 1L : 0L;
    out[0] = aHi - bHi - borrow;
    out[1] = aLo - bLo;
  }

  static void mul128x128(long aHi, long aLo, long bHi, long bLo, long[] out4) {
    long p0 = aLo * bLo;
    long p0h = umulh(aLo, bLo);
    long p1l = aLo * bHi;
    long p1h = umulh(aLo, bHi);
    long p2l = aHi * bLo;
    long p2h = umulh(aHi, bLo);
    long p3l = aHi * bHi;
    long p3h = umulh(aHi, bHi);

    long t1 = p0h;
    long c;
    t1 += p1l;
    c = Long.compareUnsigned(t1, p1l) < 0 ? 1L : 0L;
    long t2 = p1h + c;
    long t3 = Long.compareUnsigned(t2, p1h) < 0 ? 1L : 0L;

    t1 += p2l;
    c = Long.compareUnsigned(t1, p2l) < 0 ? 1L : 0L;
    long t2b = t2;
    t2 += p2h + c;
    t3 += Long.compareUnsigned(t2, t2b) < 0 ? 1L : 0L;

    t2b = t2;
    t2 += p3l;
    t3 += Long.compareUnsigned(t2, t2b) < 0 ? 1L : 0L;
    t3 += p3h;

    out4[0] = t3;
    out4[1] = t2;
    out4[2] = t1;
    out4[3] = p0;
  }

  static void shiftLeft128(long hi, long lo, int n, long[] out) {
    if (n <= 0) {
      out[0] = hi;
      out[1] = lo;
      return;
    }
    if (n >= 128) {
      out[0] = 0L;
      out[1] = 0L;
      return;
    }
    if (n >= 64) {
      out[0] = lo << (n - 64);
      out[1] = 0L;
      return;
    }
    out[0] = (hi << n) | (lo >>> (64 - n));
    out[1] = lo << n;
  }

  static long shiftRight128Sticky(long hi, long lo, int n, long[] out) {
    if (n <= 0) {
      out[0] = hi;
      out[1] = lo;
      return 0L;
    }
    if (n >= 128) {
      out[0] = 0L;
      out[1] = 0L;
      return (hi | lo) == 0L ? 0L : 1L;
    }
    if (n >= 64) {
      int s = n - 64;
      long lost = lo;
      if (s == 0) {
        out[0] = 0L;
        out[1] = hi;
        return lost == 0L ? 0L : 1L;
      }
      lost |= hi & ((1L << s) - 1L);
      out[0] = 0L;
      out[1] = hi >>> s;
      return lost == 0L ? 0L : 1L;
    }
    long lost = lo & ((1L << n) - 1L);
    out[0] = hi >>> n;
    out[1] = (hi << (64 - n)) | (lo >>> n);
    return lost == 0L ? 0L : 1L;
  }

  static BigInteger u128(long hi, long lo) {
    byte[] b = new byte[16];
    for (int i = 0; i < 8; i++) {
      b[i] = (byte) (hi >>> (56 - 8 * i));
      b[8 + i] = (byte) (lo >>> (56 - 8 * i));
    }
    return new BigInteger(1, b);
  }

  static void toU128(BigInteger v, long[] out) {
    byte[] mag = v.toByteArray();
    long hi = 0L;
    long lo = 0L;
    int n = mag.length;
    for (int i = 0; i < n; i++) {
      int bit = mag[n - 1 - i] & 0xff;
      if (i < 8) {
        lo |= ((long) bit) << (8 * i);
      } else if (i < 16) {
        hi |= ((long) bit) << (8 * (i - 8));
      }
    }
    out[0] = hi;
    out[1] = lo;
  }

  /**
   * {@code (a * 2^128) / b} for 128-bit fractions.
   * out[0] is the extra high quotient bit, out[1]:out[2] is the quotient
   * body, and out[3]:out[4] is the remainder.
   */
  static void divFrac128(
      long aHi, long aLo, long bHi, long bLo, long[] out) {
    if ((bHi | bLo) == 0L) {
      throw new ArithmeticException("division by zero");
    }
    if (aHi >= 0L || bHi >= 0L) {
      throw new IllegalArgumentException("fractions must be normalized");
    }

    long u3 = aHi;
    long u2 = aLo;
    if (cmp128(u3, u2, bHi, bLo) >= 0) {
      long borrow = borrow(u2, bLo);
      u2 -= bLo;
      u3 = u3 - bHi - borrow;
      out[0] = 1L;
    } else {
      out[0] = 0L;
    }

    long u1 = 0L;
    long qHi = quotientDigit(u3, u2, u1, bHi, bLo);
    long product0 = qHi * bLo;
    long carry = umulh(qHi, bLo);
    long product1Base = qHi * bHi;
    long product1 = product1Base + carry;
    long product2 = umulh(qHi, bHi) + carry(product1Base, product1);
    long difference1 = u1 - product0;
    long borrow = borrow(u1, product0);
    long difference2 = u2 - product1;
    long nextBorrow = borrow(u2, product1);
    long withBorrow = difference2 - borrow;
    nextBorrow |= borrow(difference2, borrow);
    long difference3 = u3 - product2;
    long finalBorrow = borrow(u3, product2);
    long highWithBorrow = difference3 - nextBorrow;
    finalBorrow |= borrow(difference3, nextBorrow);
    u1 = difference1;
    u2 = withBorrow;
    u3 = highWithBorrow;
    if (finalBorrow != 0L) {
      qHi--;
      long sum = u1 + bLo;
      carry = carry(u1, sum);
      u1 = sum;
      sum = u2 + bHi;
      long nextCarry = carry(u2, sum);
      long sumWithCarry = sum + carry;
      nextCarry |= carry(sum, sumWithCarry);
      u2 = sumWithCarry;
      u3 += nextCarry;
    }

    long u0 = 0L;
    long qLo = quotientDigit(u2, u1, u0, bHi, bLo);
    product0 = qLo * bLo;
    carry = umulh(qLo, bLo);
    product1Base = qLo * bHi;
    product1 = product1Base + carry;
    product2 = umulh(qLo, bHi) + carry(product1Base, product1);
    long difference0 = u0 - product0;
    borrow = borrow(u0, product0);
    difference1 = u1 - product1;
    nextBorrow = borrow(u1, product1);
    withBorrow = difference1 - borrow;
    nextBorrow |= borrow(difference1, borrow);
    difference2 = u2 - product2;
    finalBorrow = borrow(u2, product2);
    highWithBorrow = difference2 - nextBorrow;
    finalBorrow |= borrow(difference2, nextBorrow);
    u0 = difference0;
    u1 = withBorrow;
    u2 = highWithBorrow;
    if (finalBorrow != 0L) {
      qLo--;
      long sum = u0 + bLo;
      carry = carry(u0, sum);
      u0 = sum;
      sum = u1 + bHi;
      long nextCarry = carry(u1, sum);
      long sumWithCarry = sum + carry;
      nextCarry |= carry(sum, sumWithCarry);
      u1 = sumWithCarry;
      u2 += nextCarry;
    }

    out[1] = qHi;
    out[2] = qLo;
    out[3] = u1;
    out[4] = u0;
  }

  private static long quotientDigit(
      long high, long low, long next, long divisorHigh, long divisorLow) {
    long guess;
    long remainder;
    boolean remainderOverflow;
    if (high == divisorHigh) {
      guess = -1L;
      remainder = high + low;
      remainderOverflow = Long.compareUnsigned(remainder, high) < 0;
    } else {
      guess = divide128By64(high, low, divisorHigh);
      remainder = low - guess * divisorHigh;
      remainderOverflow = false;
    }
    while (!remainderOverflow
        && productGreaterThanPair(guess, divisorLow, remainder, next)) {
      guess--;
      long previous = remainder;
      remainder += divisorHigh;
      remainderOverflow = Long.compareUnsigned(remainder, previous) < 0;
    }
    return guess;
  }

  private static long divide128By64(long high, long low, long divisor) {
    int shift = Long.numberOfLeadingZeros(divisor);
    long normalizedDivisor = divisor << shift;
    long normalizedHigh = shift == 0
        ? high
        : (high << shift) | (low >>> (64 - shift));
    long normalizedLow = low << shift;
    long divisorHigh = normalizedDivisor >>> 32;
    long divisorLow = normalizedDivisor & 0xffff_ffffL;
    long dividendMiddle = normalizedLow >>> 32;
    long dividendLow = normalizedLow & 0xffff_ffffL;
    long base = 0x1_0000_0000L;
    long quotientHigh = Long.divideUnsigned(normalizedHigh, divisorHigh);
    long remainder = normalizedHigh - quotientHigh * divisorHigh;
    while (quotientHigh >= base
        || Long.compareUnsigned(
            quotientHigh * divisorLow, remainder << 32 | dividendMiddle) > 0) {
      quotientHigh--;
      remainder += divisorHigh;
      if (remainder >= base) {
        break;
      }
    }
    long partial = (normalizedHigh << 32)
        + dividendMiddle
        - quotientHigh * normalizedDivisor;
    long quotientLow = Long.divideUnsigned(partial, divisorHigh);
    remainder = partial - quotientLow * divisorHigh;
    while (quotientLow >= base
        || Long.compareUnsigned(
            quotientLow * divisorLow, remainder << 32 | dividendLow) > 0) {
      quotientLow--;
      remainder += divisorHigh;
      if (remainder >= base) {
        break;
      }
    }
    return quotientHigh << 32 | quotientLow;
  }

  private static boolean productGreaterThanPair(
      long left, long right, long pairHigh, long pairLow) {
    long productLow = left * right;
    long productHigh = umulh(left, right);
    int comparison = Long.compareUnsigned(productHigh, pairHigh);
    return comparison > 0
        || comparison == 0 && Long.compareUnsigned(productLow, pairLow) > 0;
  }

  private static long carry(long source, long result) {
    return Long.compareUnsigned(result, source) < 0 ? 1L : 0L;
  }

  private static long borrow(long left, long right) {
    return Long.compareUnsigned(left, right) < 0 ? 1L : 0L;
  }

  /**
   * Computes {@code floor(sqrt((m << odd) * 2^128))}.
   *
   * <p>The normalized 128-bit input is consumed two bits at a time by the
   * restoring square-root algorithm. The result uses root[0] as an extra high
   * bit and root[1]:root[2] as its 128-bit body. The return value reports a
   * nonzero remainder.
   */
  static boolean sqrtScaled128(long mHi, long mLo, boolean odd, long[] root) {
    return sqrtScaled128(
        mHi, mLo, odd, root, new long[3], new long[3]);
  }

  static boolean sqrtScaled128(
      long mHi,
      long mLo,
      boolean odd,
      long[] root,
      long[] remainder,
      long[] trial) {
    if (mHi >= 0L) {
      throw new IllegalArgumentException("fraction must be normalized");
    }
    remainder[0] = 0L;
    remainder[1] = 0L;
    remainder[2] = 0L;
    root[0] = 0L;
    root[1] = 0L;
    root[2] = 0L;
    int pairs = odd ? 129 : 128;
    int shift = odd ? 129 : 128;
    for (int pair = pairs - 1; pair >= 0; pair--) {
      shiftLeft(remainder, 2);
      int lowBit = pair << 1;
      remainder[2] |= (long) bitOfShifted128(mHi, mLo, shift, lowBit + 1) << 1;
      remainder[2] |= bitOfShifted128(mHi, mLo, shift, lowBit);

      trial[0] = root[0];
      trial[1] = root[1];
      trial[2] = root[2];
      shiftLeft(trial, 2);
      trial[2] |= 1L;
      shiftLeft(root, 1);
      if (cmp192(remainder, trial) >= 0) {
        sub192(remainder, trial);
        root[2] |= 1L;
      }
    }
    return (remainder[0] | remainder[1] | remainder[2]) != 0L;
  }

  private static int bitOfShifted128(
      long mHi, long mLo, int shift, int resultBit) {
    int sourceBit = resultBit - shift;
    if (sourceBit < 0 || sourceBit >= 128) {
      return 0;
    }
    if (sourceBit >= 64) {
      return (int) ((mHi >>> (sourceBit - 64)) & 1L);
    }
    return (int) ((mLo >>> sourceBit) & 1L);
  }

  private static void shiftLeft(long[] value, int bits) {
    value[0] = (value[0] << bits) | (value[1] >>> (64 - bits));
    value[1] = (value[1] << bits) | (value[2] >>> (64 - bits));
    value[2] <<= bits;
  }

  private static int cmp192(long[] a, long[] b) {
    int high = Long.compareUnsigned(a[0], b[0]);
    if (high != 0) {
      return high;
    }
    int middle = Long.compareUnsigned(a[1], b[1]);
    return middle != 0 ? middle : Long.compareUnsigned(a[2], b[2]);
  }

  private static void sub192(long[] a, long[] b) {
    long low = a[2] - b[2];
    long lowBorrow = Long.compareUnsigned(a[2], b[2]) < 0 ? 1L : 0L;
    long middleSubtrahend = b[1] + lowBorrow;
    long middleCarry = Long.compareUnsigned(middleSubtrahend, b[1]) < 0 ? 1L : 0L;
    long middleBorrow = Long.compareUnsigned(a[1], middleSubtrahend) < 0
        || middleCarry != 0L ? 1L : 0L;
    a[2] = low;
    a[1] -= middleSubtrahend;
    a[0] = a[0] - b[0] - middleBorrow;
  }
}
