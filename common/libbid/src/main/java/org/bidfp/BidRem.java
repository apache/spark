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
package org.bidfp;

final class BidRem {
  private BidRem() {
  }

  static long rem64(long x, long y, StatusFlags flags) {
    if (Bid64Raw.isNaN(x) || Bid64Raw.isNaN(y)) {
      if (Bid64Raw.isSignalingNaN(x) || Bid64Raw.isSignalingNaN(y)) {
        flags.raise(StatusFlags.INVALID);
      }
      return BidIntegral.canonicalizeNaN64(
          Bid64Raw.isNaN(x) ? x : y, new StatusFlags());
    }
    if (Bid64Raw.isInf(x) || Bid64Raw.isZero(y)) {
      flags.raise(StatusFlags.INVALID);
      return Bid64.MASK_NAN;
    }
    if (Bid64Raw.isZero(x)) {
      int exponent = Bid64.biasedExponentBits(x);
      if (Bid64Raw.isFinite(y)) {
        exponent = Math.min(exponent, Bid64.biasedExponentBits(y));
      }
      return Bid64.finiteRawBits(Bid64Raw.isSigned(x), exponent, 0L);
    }
    if (Bid64Raw.isInf(y)) {
      return x;
    }
    long fast = remainder64(x, y, true);
    if (fast != Bid64.MASK_NAN) {
      return fast;
    }
    DecNum result = remainder(
        DecNum.ofCoefficient(false, Bid64.significandBits(x),
            Bid64.biasedExponentBits(x) - 398),
        DecNum.ofCoefficient(false, Bid64.significandBits(y),
            Bid64.biasedExponentBits(y) - 398),
        Bid64Raw.isSigned(x),
        true);
    return result.packBid64(RoundingMode.TIES_TO_EVEN, flags);
  }

  static long fmod64(long x, long y, StatusFlags flags) {
    if (Bid64Raw.isNaN(x) || Bid64Raw.isNaN(y)) {
      if (Bid64Raw.isSignalingNaN(x) || Bid64Raw.isSignalingNaN(y)) {
        flags.raise(StatusFlags.INVALID);
      }
      return BidIntegral.canonicalizeNaN64(
          Bid64Raw.isNaN(x) ? x : y, new StatusFlags());
    }
    if (Bid64Raw.isInf(x) || Bid64Raw.isZero(y)) {
      flags.raise(StatusFlags.INVALID);
      return Bid64.MASK_NAN;
    }
    if (Bid64Raw.isZero(x)) {
      int exponent = Bid64.biasedExponentBits(x);
      if (Bid64Raw.isFinite(y)) {
        exponent = Math.min(exponent, Bid64.biasedExponentBits(y));
      }
      return Bid64.finiteRawBits(Bid64Raw.isSigned(x), exponent, 0L);
    }
    if (Bid64Raw.isInf(y)) {
      return x;
    }
    long fast = remainder64(x, y, false);
    if (fast != Bid64.MASK_NAN) {
      return fast;
    }
    DecNum result = remainder(
        DecNum.ofCoefficient(false, Bid64.significandBits(x),
            Bid64.biasedExponentBits(x) - 398),
        DecNum.ofCoefficient(false, Bid64.significandBits(y),
            Bid64.biasedExponentBits(y) - 398),
        Bid64Raw.isSigned(x),
        false);
    return result.packBid64(RoundingMode.TOWARD_ZERO, flags);
  }

  static void rem128(
      long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    remainder128Dispatch(xh, xl, yh, yl, flags, out, true);
  }

  static void fmod128(
      long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    remainder128Dispatch(xh, xl, yh, yl, flags, out, false);
  }

  private static void remainder128Dispatch(
      long xh, long xl, long yh, long yl, StatusFlags flags, long[] out,
      boolean nearestEven) {
    if (isNaN128(xh) || isNaN128(yh)) {
      if (isSignalingNaN128(xh) || isSignalingNaN128(yh)) {
        flags.raise(StatusFlags.INVALID);
      }
      long nanHigh = isNaN128(xh) ? xh : yh;
      long nanLow = isNaN128(xh) ? xl : yl;
      BidIntegral.canonicalizeNaN128(nanHigh, nanLow, new StatusFlags(), out);
      return;
    }
    if (isInf128(xh) || isZero128(yh, yl)) {
      flags.raise(StatusFlags.INVALID);
      out[0] = Bid128.MASK_NAN;
      out[1] = 0L;
      return;
    }
    if (isZero128(xh, xl)) {
      int exponent = isFinite128(yh)
          ? Math.min(biasedExponent128(xh), biasedExponent128(yh))
          : biasedExponent128(xh);
      out[0] = (xh & Bid128.MASK_SIGN) | ((long) exponent << 49);
      out[1] = 0L;
      return;
    }
    if (isInf128(yh)) {
      out[0] = xh;
      out[1] = xl;
      return;
    }
    if (remainder128(xh, xl, yh, yl, nearestEven, out)) {
      return;
    }
    DecNum result = remainder(
        unpack128(xh, xl), unpack128(yh, yl),
        (xh & Bid128.MASK_SIGN) != 0, nearestEven);
    result.packBid128(
        nearestEven ? RoundingMode.TIES_TO_EVEN : RoundingMode.TOWARD_ZERO,
        flags, out);
  }

  private static boolean isNaN128(long high) {
    return (high & Bid128.MASK_NAN) == Bid128.MASK_NAN;
  }

  private static boolean isSignalingNaN128(long high) {
    return (high & Bid128.MASK_SIGNALING_NAN) == Bid128.MASK_SIGNALING_NAN;
  }

  private static boolean isFinite128(long high) {
    return (high & Bid128.MASK_INFINITY) != Bid128.MASK_INFINITY;
  }

  private static boolean isInf128(long high) {
    return (high & Bid128.MASK_INFINITY) == Bid128.MASK_INFINITY
        && (high & Bid128.MASK_NAN) != Bid128.MASK_NAN;
  }

  private static boolean isZero128(long high, long low) {
    if (!isFinite128(high)) {
      return false;
    }
    if (!Bid128.isCanonicalFinite(high, low)) {
      return true;
    }
    return ((high & Bid128.MASK_COEFFICIENT) | low) == 0L;
  }

  private static int biasedExponent128(long high) {
    if ((high & Bid128.MASK_STEERING_BITS) == Bid128.MASK_STEERING_BITS
        && isFinite128(high)) {
      return (int) ((high >>> 47) & 0x3fffL);
    }
    return (int) ((high & Bid128.MASK_EXPONENT) >>> 49);
  }

  private static DecNum unpack128(long high, long low) {
    boolean canonical = Bid128.isCanonicalFinite(high, low);
    long coefficientHigh = canonical ? high & Bid128.MASK_COEFFICIENT : 0L;
    long coefficientLow = canonical ? low : 0L;
    DecNum result = DecNum.ofUnsigned(coefficientHigh, coefficientLow);
    result.shiftExp(biasedExponent128(high) - 6176);
    return result;
  }

  private static DecNum remainder(
      DecNum numerator, DecNum denominator, boolean negative, boolean nearestEven) {
    int commonExponent = Math.min(numerator.exp(), denominator.exp());
    int numeratorZeros = numerator.exp() - commonExponent;
    int denominatorZeros = denominator.exp() - commonExponent;
    String numeratorDigits = numerator.toDigits();
    String denominatorDigits = denominator.toDigits();
    int numeratorLength = numeratorDigits.length() + numeratorZeros;
    int denominatorLength = denominatorDigits.length() + denominatorZeros;
    DecNum divisor = DecNum.ofLong(0L);
    for (int i = 0; i < denominatorDigits.length(); i++) {
      divisor.multiplySmall(10);
      divisor.addDigit(denominatorDigits.charAt(i) - '0');
    }
    if (denominatorLength > numeratorLength) {
      DecNum result = DecNum.ofLong(0L);
      for (int i = 0; i < numeratorDigits.length(); i++) {
        result.multiplySmall(10);
        result.addDigit(numeratorDigits.charAt(i) - '0');
      }
      result.multiplyPow10(numeratorZeros);
      result.shiftExp(commonExponent);
      if (negative) {
        result.setNegative();
      }
      return result;
    }
    divisor.multiplyPow10(denominatorZeros);
    DecNum result = DecNum.ofLong(0L);
    int quotientLastDigit = 0;
    for (int i = 0; i < numeratorLength; i++) {
      int digit = i < numeratorDigits.length() ? numeratorDigits.charAt(i) - '0' : 0;
      result.multiplySmall(10);
      result.addDigit(digit);
      quotientLastDigit = 0;
      while (result.compareAbsolute(divisor) >= 0) {
        result.subtractAbsolute(divisor);
        quotientLastDigit++;
      }
    }
    if (nearestEven && !result.isZero()) {
      DecNum twice = DecNum.ofLong(0L);
      twice.copyFrom(result);
      twice.multiplySmall(2);
      int halfComparison = twice.compareAbsolute(divisor);
      if (halfComparison > 0 || halfComparison == 0 && (quotientLastDigit & 1) != 0) {
        DecNum rounded = DecNum.ofLong(0L);
        rounded.copyFrom(divisor);
        rounded.subtractAbsolute(result);
        result = rounded;
        negative = !negative;
      }
    }
    result.shiftExp(commonExponent);
    if (negative) {
      result.setNegative();
    }
    return result;
  }

  private static final long BID64_MAX_COEFFICIENT = 9_999_999_999_999_999L;
  private static final long BID128_MAX_COEFFICIENT_HIGH = 0x0001_ed09_bead_87c0L;
  private static final long BID128_MAX_COEFFICIENT_LOW = 0x378d_8e63_ffff_ffffL;

  /**
   * Exact finite remainder packed as BID64. Returns {@link Bid64#MASK_NAN} to
   * request the {@link DecNum} fallback (the finite path never produces NaN).
   */
  private static long remainder64(long x, long y, boolean nearestEven) {
    long cx = Bid64.significandBits(x);
    long cy = Bid64.significandBits(y);
    if (cy == 0L) {
      return Bid64.MASK_NAN;
    }
    int ex = Bid64.biasedExponentBits(x);
    int ey = Bid64.biasedExponentBits(y);
    int common = Math.min(ex, ey);
    boolean negative = Bid64Raw.isSigned(x);
    long rem;
    int lastQuotientDigit;
    long divisor;
    if (ex >= ey) {
      divisor = cy;
      rem = Long.remainderUnsigned(cx, cy);
      lastQuotientDigit = (int) (Long.divideUnsigned(cx, cy) % 10L);
      int scale = ex - ey;
      for (int i = 0; i < scale; i++) {
        rem = rem * 10L;
        lastQuotientDigit = (int) Long.divideUnsigned(rem, cy);
        rem = Long.remainderUnsigned(rem, cy);
      }
    } else {
      int scale = ey - ex;
      if (scale >= PowersOfTen.LONG.length) {
        rem = cx;
        lastQuotientDigit = 0;
        divisor = 0L;
      } else {
        long power = PowersOfTen.LONG[scale];
        long prodHigh = UInt128.unsignedMultiplyHigh(cy, power);
        long prodLow = cy * power;
        if (prodHigh != 0L) {
          rem = cx;
          lastQuotientDigit = 0;
          divisor = 0L;
        } else {
          divisor = prodLow;
          rem = Long.remainderUnsigned(cx, divisor);
          lastQuotientDigit = (int) (Long.divideUnsigned(cx, divisor) % 10L);
        }
      }
    }
    if (nearestEven && rem != 0L && divisor != 0L) {
      int halfComparison = Long.compareUnsigned(rem, divisor - rem);
      if (halfComparison > 0
          || halfComparison == 0 && (lastQuotientDigit & 1) != 0) {
        rem = divisor - rem;
        negative = !negative;
      }
    }
    if (rem > BID64_MAX_COEFFICIENT) {
      return Bid64.MASK_NAN;
    }
    return Bid64.finiteRawBits(negative, common, rem);
  }

  private static boolean remainder128(
      long xHigh, long xLow, long yHigh, long yLow,
      boolean nearestEven, long[] out) {
    boolean xCanonical = Bid128.isCanonicalFinite(xHigh, xLow);
    boolean yCanonical = Bid128.isCanonicalFinite(yHigh, yLow);
    long cxh = xCanonical ? xHigh & Bid128.MASK_COEFFICIENT : 0L;
    long cxl = xCanonical ? xLow : 0L;
    long cyh = yCanonical ? yHigh & Bid128.MASK_COEFFICIENT : 0L;
    long cyl = yCanonical ? yLow : 0L;
    if ((cyh | cyl) == 0L) {
      return false;
    }
    int ex = biasedExponent128(xHigh);
    int ey = biasedExponent128(yHigh);
    int common = Math.min(ex, ey);
    boolean negative = (xHigh & Bid128.MASK_SIGN) != 0;
    int lastQuotientDigit;
    long divh;
    long divl;
    if (ex >= ey) {
      divh = cyh;
      divl = cyl;
      lastQuotientDigit = divide128(cxh, cxl, cyh, cyl, out);
      int scale = ex - ey;
      for (int i = 0; i < scale; i++) {
        multiplyByTen(out);
        lastQuotientDigit = subtractDivisor(out, cyh, cyl);
      }
    } else {
      int scale = ey - ex;
      out[0] = cyh;
      out[1] = cyl;
      if (!multiplyPow10Fits128(out, scale)) {
        out[0] = cxh;
        out[1] = cxl;
        lastQuotientDigit = 0;
        divh = 0L;
        divl = 0L;
      } else {
        divh = out[0];
        divl = out[1];
        if (compareUnsigned128(cxh, cxl, divh, divl) < 0) {
          out[0] = cxh;
          out[1] = cxl;
          lastQuotientDigit = 0;
        } else {
          lastQuotientDigit = divide128(cxh, cxl, divh, divl, out);
        }
      }
    }
    long remh = out[0];
    long reml = out[1];
    if (nearestEven && (remh | reml) != 0L && (divh | divl) != 0L) {
      int halfComparison = compareDoubleRemainder128(remh, reml, divh, divl);
      if (halfComparison > 0
          || halfComparison == 0 && (lastQuotientDigit & 1) != 0) {
        long borrow = Long.compareUnsigned(divl, reml) < 0 ? 1L : 0L;
        reml = divl - reml;
        remh = divh - remh - borrow;
        negative = !negative;
      }
    }
    if (compareUnsigned128(
        remh, reml,
        BID128_MAX_COEFFICIENT_HIGH, BID128_MAX_COEFFICIENT_LOW) > 0) {
      return false;
    }
    long sign = negative ? Bid128.MASK_SIGN : 0L;
    out[0] = sign | ((long) common << 49) | remh;
    out[1] = reml;
    return true;
  }

  private static int compareDoubleRemainder128(
      long remh, long reml, long divh, long divl) {
    long twiceH = (remh << 1) | (reml >>> 63);
    long twiceL = reml << 1;
    return compareUnsigned128(twiceH, twiceL, divh, divl);
  }

  private static int compareUnsigned128(long ah, long al, long bh, long bl) {
    int cmp = Long.compareUnsigned(ah, bh);
    return cmp != 0 ? cmp : Long.compareUnsigned(al, bl);
  }

  private static void multiplyByTen(long[] value) {
    long high = value[0];
    long low = value[1];
    long productLow = low * 10L;
    long carry = UInt128.unsignedMultiplyHigh(low, 10L);
    value[0] = high * 10L + carry;
    value[1] = productLow;
  }

  private static boolean multiplyPow10Fits128(long[] value, int scale) {
    for (int i = 0; i < scale; i++) {
      long high = value[0];
      long low = value[1];
      if (UInt128.unsignedMultiplyHigh(high, 10L) != 0L) {
        return false;
      }
      long carry = UInt128.unsignedMultiplyHigh(low, 10L);
      long productHigh = high * 10L;
      long sum = productHigh + carry;
      if (Long.compareUnsigned(sum, productHigh) < 0) {
        return false;
      }
      value[0] = sum;
      value[1] = low * 10L;
    }
    return true;
  }

  /**
   * {@code remainder = numerator % divisor}. Returns the last decimal digit of
   * the quotient (enough for IEEE remainder ties-to-even).
   */
  private static int divide128(
      long nh, long nl, long dh, long dl, long[] remainder) {
    long remh = 0L;
    long reml = 0L;
    long qh = 0L;
    long ql = 0L;
    for (int bit = 127; bit >= 0; bit--) {
      boolean overflow = remh < 0L;
      remh = (remh << 1) | (reml >>> 63);
      reml = (reml << 1) | bit128(nh, nl, bit);
      if (overflow || compareUnsigned128(remh, reml, dh, dl) >= 0) {
        long borrow = Long.compareUnsigned(reml, dl) < 0 ? 1L : 0L;
        reml -= dl;
        remh = remh - dh - borrow;
        if (bit >= 64) {
          qh |= 1L << (bit - 64);
        } else {
          ql |= 1L << bit;
        }
      }
    }
    remainder[0] = remh;
    remainder[1] = reml;
    long last = (Long.remainderUnsigned(qh, 10L) * 6L
        + Long.remainderUnsigned(ql, 10L)) % 10L;
    return (int) last;
  }

  private static long bit128(long high, long low, int index) {
    return index >= 64
        ? (high >>> (index - 64)) & 1L
        : (low >>> index) & 1L;
  }

  private static int subtractDivisor(long[] rem, long dh, long dl) {
    int digit = 0;
    while (compareUnsigned128(rem[0], rem[1], dh, dl) >= 0) {
      long borrow = Long.compareUnsigned(rem[1], dl) < 0 ? 1L : 0L;
      rem[1] -= dl;
      rem[0] = rem[0] - dh - borrow;
      digit++;
    }
    return digit;
  }
}
