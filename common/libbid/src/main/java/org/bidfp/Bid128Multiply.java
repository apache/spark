/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   * Neither the name of Intel Corporation nor the names of its contributors may
 *     be used to endorse or promote products derived from this software without
 *     specific prior written permission.
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
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.bidfp;

import java.util.Objects;

/** Pure-Java, fixed-limb port of Intel RDFP {@code bid128_mul}. */
public final class Bid128Multiply {
  private static final int EXPONENT_BIAS = 6176;
  private static final int MAX_EXPONENT = 12_287;
  private static final long MAX_COEFFICIENT_HIGH = 0x0001_ed09_bead_87c0L;
  private static final long MAX_COEFFICIENT_LOW = 0x378d_8e63_ffff_ffffL;
  private static final long MAX_DIV_TEN_HIGH = 0x0000_314d_c644_8d93L;
  private static final long MAX_DIV_TEN_LOW = 0x38c1_5b09_ffff_ffffL;
  private static final long MIN_NORMAL_HIGH = 0x0000_314d_c644_8d93L;
  private static final long MIN_NORMAL_LOW = 0x38c1_5b0a_0000_0000L;
  private static final long CARRY_HIGH = 0x0001_ed09_bead_87c0L;
  private static final long CARRY_LOW = 0x378d_8e64_0000_0000L;
  private static final UInt128 MAX_NAN_PAYLOAD =
      new UInt128(0x0000_314d_c644_8d93L, 0x38c1_5b09_ffff_ffffL);

  private Bid128Multiply() {
  }

  /**
   * Multiplies two BID decimal128 values and accumulates IEEE 754 status flags.
   *
   * @param x left operand
   * @param y right operand
   * @param roundingMode rounding-direction attribute
   * @param flags mutable status flags, which are accumulated rather than cleared
   */
  public static Bid128 multiply(
      Bid128 x, Bid128 y, RoundingMode roundingMode, StatusFlags flags) {
    Objects.requireNonNull(x, "x");
    Objects.requireNonNull(y, "y");
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");

    boolean negative = x.isSigned() ^ y.isSigned();
    if (!x.isFinite()) {
      if (y.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      if (x.isNaN()) {
        return quietNaN(x, flags);
      }
      if (y.isNaN()) {
        return quietNaN(y, flags);
      }
      if (y.isZero()) {
        flags.raise(StatusFlags.INVALID);
        return Bid128.QUIET_NAN;
      }
      return infinity(negative);
    }
    if (!y.isFinite()) {
      if (y.isNaN()) {
        return quietNaN(y, flags);
      }
      if (x.isZero()) {
        flags.raise(StatusFlags.INVALID);
        return Bid128.QUIET_NAN;
      }
      return infinity(negative);
    }

    int exponent = encodedExponent(x) + encodedExponent(y) - EXPONENT_BIAS;
    long xBits = x.highBits();
    long yBits = y.highBits();
    boolean xCanonical = Bid128.isCanonicalFinite(xBits, x.lowBits());
    boolean yCanonical = Bid128.isCanonicalFinite(yBits, y.lowBits());
    long xHigh = xCanonical ? xBits & Bid128.MASK_COEFFICIENT : 0L;
    long xLow = xCanonical ? x.lowBits() : 0L;
    long yHigh = yCanonical ? yBits & Bid128.MASK_COEFFICIENT : 0L;
    long yLow = yCanonical ? y.lowBits() : 0L;
    if ((xHigh | xLow) == 0L || (yHigh | yLow) == 0L) {
      return Bid128.rawFinite(negative, clampExponent(exponent), 0L, 0L);
    }

    UInt256 product = UInt256.multiply(xHigh, xLow, yHigh, yLow);
    int productDigits = decimalDigits(product);
    int extraDigits = Math.max(0, productDigits - 34);
    int resultExponent = exponent + extraDigits;
    if (resultExponent < 0) {
      extraDigits -= resultExponent;
      resultExponent = 0;
    }
    boolean inexact = product.round(
        productDigits, extraDigits, negative, roundingMode);
    long coefficientHigh = product.l1;
    long coefficientLow = product.l0;
    if (coefficientHigh == CARRY_HIGH && coefficientLow == CARRY_LOW) {
      coefficientHigh = MIN_NORMAL_HIGH;
      coefficientLow = MIN_NORMAL_LOW;
      resultExponent++;
    }

    while (resultExponent > MAX_EXPONENT
        && compare(
            coefficientHigh, coefficientLow, MAX_DIV_TEN_HIGH, MAX_DIV_TEN_LOW) <= 0) {
      coefficientHigh = coefficientHigh * 10
          + UInt128.unsignedMultiplyHigh(coefficientLow, 10);
      coefficientLow *= 10;
      resultExponent--;
    }
    if (resultExponent > MAX_EXPONENT) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
      return overflowResult(negative, roundingMode);
    }

    if (inexact) {
      flags.raise(StatusFlags.INEXACT);
      if (resultExponent == 0
          && compare(
              coefficientHigh, coefficientLow, MIN_NORMAL_HIGH, MIN_NORMAL_LOW) < 0) {
        flags.raise(StatusFlags.UNDERFLOW);
      }
    }
    return Bid128.rawFinite(
        negative, resultExponent, coefficientHigh, coefficientLow);
  }

  private static Bid128 quietNaN(Bid128 value, StatusFlags flags) {
    if (value.isSignalingNaN()) {
      flags.raise(StatusFlags.INVALID);
    }
    UInt128 payload =
        new UInt128(value.highBits() & 0x0000_3fff_ffff_ffffL, value.lowBits());
    if (payload.compareTo(MAX_NAN_PAYLOAD) > 0) {
      payload = UInt128.ZERO;
    }
    long high = (value.isSigned() ? Bid128.MASK_SIGN : 0L)
        | Bid128.MASK_NAN | payload.high();
    return Bid128.fromRawBits(high, payload.low());
  }

  private static Bid128 infinity(boolean negative) {
    return negative ? Bid128.NEGATIVE_INFINITY : Bid128.POSITIVE_INFINITY;
  }

  private static Bid128 overflowResult(boolean negative, RoundingMode mode) {
    boolean toInfinity;
    switch (mode) {
      case TIES_TO_EVEN:
      case TIES_AWAY:
        toInfinity = true;
        break;
      case TOWARD_POSITIVE:
        toInfinity = !negative;
        break;
      case TOWARD_NEGATIVE:
        toInfinity = negative;
        break;
      case TOWARD_ZERO:
        toInfinity = false;
        break;
      default:
        throw new AssertionError(mode);
    }
    return toInfinity
        ? infinity(negative)
        : Bid128.rawFinite(
            negative, MAX_EXPONENT, MAX_COEFFICIENT_HIGH, MAX_COEFFICIENT_LOW);
  }

  private static int compare(long high, long low, long otherHigh, long otherLow) {
    int comparison = Long.compareUnsigned(high, otherHigh);
    return comparison != 0 ? comparison : Long.compareUnsigned(low, otherLow);
  }

  private static int clampExponent(int exponent) {
    return Math.max(0, Math.min(MAX_EXPONENT, exponent));
  }

  private static int encodedExponent(Bid128 value) {
    long high = value.highBits();
    if ((high & Bid128.MASK_STEERING_BITS) == Bid128.MASK_STEERING_BITS) {
      return (int) (((high << 2) & Bid128.MASK_EXPONENT) >>> 49);
    }
    return value.biasedExponent();
  }

  private static int decimalDigits(UInt256 value) {
    int bits = value.bitLength();
    int digits = (((bits - 1) * 1233) >>> 12) + 1;
    if (value.compareTo(UInt256.POWERS_OF_TEN[digits]) >= 0) {
      digits++;
    }
    return digits;
  }

  /** Unsigned 256-bit value stored as four fixed little-endian limbs. */
  private static final class UInt256 {
    private static final long WORD_MASK = 0xffff_ffffL;
    private static final int[] SMALL_POWERS_OF_TEN = {
      1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000,
      100_000_000, 1_000_000_000
    };
    private static final UInt256[] POWERS_OF_TEN = createPowersOfTen();

    private long l0;
    private long l1;
    private long l2;
    private long l3;

    private UInt256() {
    }

    private UInt256(UInt256 value) {
      l0 = value.l0;
      l1 = value.l1;
      l2 = value.l2;
      l3 = value.l3;
    }

    private static UInt256 multiply(
        long xHigh, long xLow, long yHigh, long yLow) {
      UInt256 result = new UInt256();
      long p00High = unsignedMultiplyHigh(xLow, yLow);
      long p01Low = xLow * yHigh;
      long p01High = unsignedMultiplyHigh(xLow, yHigh);
      long p10Low = xHigh * yLow;
      long p10High = unsignedMultiplyHigh(xHigh, yLow);
      long p11Low = xHigh * yHigh;
      long p11High = unsignedMultiplyHigh(xHigh, yHigh);

      result.l0 = xLow * yLow;
      long sum = p00High + p01Low;
      long carry = Long.compareUnsigned(sum, p00High) < 0 ? 1L : 0L;
      long next = sum + p10Low;
      carry += Long.compareUnsigned(next, sum) < 0 ? 1L : 0L;
      result.l1 = next;

      sum = p01High + p10High;
      long highCarry = Long.compareUnsigned(sum, p01High) < 0 ? 1L : 0L;
      next = sum + p11Low;
      highCarry += Long.compareUnsigned(next, sum) < 0 ? 1L : 0L;
      sum = next + carry;
      highCarry += Long.compareUnsigned(sum, next) < 0 ? 1L : 0L;
      result.l2 = sum;
      result.l3 = p11High + highCarry;
      return result;
    }

    private boolean round(
        int valueDigits, int digits, boolean negative, RoundingMode mode) {
      if (digits == 0) {
        return false;
      }
      if (digits > valueDigits) {
        boolean increment =
            mode == RoundingMode.TOWARD_POSITIVE && !negative
                || mode == RoundingMode.TOWARD_NEGATIVE && negative;
        l0 = increment ? 1L : 0L;
        l1 = 0L;
        l2 = 0L;
        l3 = 0L;
        return true;
      }

      boolean sticky = false;
      int remaining = digits;
      while (remaining > 9) {
        sticky |= divideByBillion() != 0;
        remaining -= 9;
      }
      int remainder = divideByPowerOfTen(remaining);
      int leadingDivisor = SMALL_POWERS_OF_TEN[remaining - 1];
      int roundDigit = remainder / leadingDivisor;
      sticky |= remainder % leadingDivisor != 0;
      boolean inexact = roundDigit != 0 || sticky;
      boolean increment;
      switch (mode) {
        case TIES_TO_EVEN:
          increment = roundDigit > 5
              || roundDigit == 5 && (sticky || (l0 & 1L) != 0);
          break;
        case TIES_AWAY:
          increment = roundDigit >= 5;
          break;
        case TOWARD_POSITIVE:
          increment = !negative && inexact;
          break;
        case TOWARD_NEGATIVE:
          increment = negative && inexact;
          break;
        case TOWARD_ZERO:
          increment = false;
          break;
        default:
          throw new AssertionError(mode);
      }
      if (increment) {
        l0++;
        if (l0 == 0L) {
          l1++;
        }
      }
      return inexact;
    }

    private static long unsignedMultiplyHigh(long x, long y) {
      long result = Math.multiplyHigh(x, y);
      result += x < 0 ? y : 0L;
      result += y < 0 ? x : 0L;
      return result;
    }

    private int divideByBillion() {
      return divideByInt(1_000_000_000);
    }

    private int divideByPowerOfTen(int digits) {
      switch (digits) {
        case 1:
          return divideByInt(10);
        case 2:
          return divideByInt(100);
        case 3:
          return divideByInt(1_000);
        case 4:
          return divideByInt(10_000);
        case 5:
          return divideByInt(100_000);
        case 6:
          return divideByInt(1_000_000);
        case 7:
          return divideByInt(10_000_000);
        case 8:
          return divideByInt(100_000_000);
        case 9:
          return divideByBillion();
        default:
          throw new AssertionError(digits);
      }
    }

    private int divideByInt(long divisor) {
      long remainder = 0L;
      long q7 = 0L;
      long q6 = 0L;
      long q5 = 0L;
      long q4 = 0L;
      long q3 = 0L;
      long q2 = 0L;
      long dividend;
      if (l3 != 0L) {
        dividend = l3 >>> 32;
        q7 = dividend / divisor;
        remainder = dividend - q7 * divisor;
        dividend = (remainder << 32) | (l3 & WORD_MASK);
        q6 = dividend / divisor;
        remainder = dividend - q6 * divisor;
      }
      if (l2 != 0L || remainder != 0L) {
        dividend = (remainder << 32) | (l2 >>> 32);
        q5 = dividend / divisor;
        remainder = dividend - q5 * divisor;
        dividend = (remainder << 32) | (l2 & WORD_MASK);
        q4 = dividend / divisor;
        remainder = dividend - q4 * divisor;
      }
      if (l1 != 0L || remainder != 0L) {
        dividend = (remainder << 32) | (l1 >>> 32);
        q3 = dividend / divisor;
        remainder = dividend - q3 * divisor;
        dividend = (remainder << 32) | (l1 & WORD_MASK);
        q2 = dividend / divisor;
        remainder = dividend - q2 * divisor;
      }
      dividend = (remainder << 32) | (l0 >>> 32);
      long q1 = dividend / divisor;
      remainder = dividend - q1 * divisor;
      dividend = (remainder << 32) | (l0 & WORD_MASK);
      long q0 = dividend / divisor;
      remainder = dividend - q0 * divisor;

      l3 = (q7 << 32) | q6;
      l2 = (q5 << 32) | q4;
      l1 = (q3 << 32) | q2;
      l0 = (q1 << 32) | q0;
      return (int) remainder;
    }

    private int bitLength() {
      if (l3 != 0L) {
        return 256 - Long.numberOfLeadingZeros(l3);
      }
      if (l2 != 0L) {
        return 192 - Long.numberOfLeadingZeros(l2);
      }
      if (l1 != 0L) {
        return 128 - Long.numberOfLeadingZeros(l1);
      }
      return 64 - Long.numberOfLeadingZeros(l0);
    }

    private int compareTo(UInt256 other) {
      int comparison = Long.compareUnsigned(l3, other.l3);
      if (comparison == 0) {
        comparison = Long.compareUnsigned(l2, other.l2);
      }
      if (comparison == 0) {
        comparison = Long.compareUnsigned(l1, other.l1);
      }
      if (comparison == 0) {
        comparison = Long.compareUnsigned(l0, other.l0);
      }
      return comparison;
    }

    private static UInt256[] createPowersOfTen() {
      UInt256[] powers = new UInt256[69];
      UInt256 value = new UInt256();
      value.l0 = 1L;
      for (int i = 0; i < powers.length; i++) {
        powers[i] = new UInt256(value);
        value.multiplyByTen();
      }
      return powers;
    }

    private void multiplyByTen() {
      long carry = unsignedMultiplyHigh(l0, 10L);
      l0 *= 10L;
      long next = l1 * 10L + carry;
      carry = unsignedMultiplyHigh(l1, 10L)
          + (Long.compareUnsigned(next, carry) < 0 ? 1L : 0L);
      l1 = next;
      next = l2 * 10L + carry;
      carry = unsignedMultiplyHigh(l2, 10L)
          + (Long.compareUnsigned(next, carry) < 0 ? 1L : 0L);
      l2 = next;
      l3 = l3 * 10L + carry;
    }
  }
}
