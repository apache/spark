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

/** Pure-Java, fixed-limb port of Intel RDFP {@code bid128_add} and {@code bid128_sub}. */
public final class Bid128Add {
  private static final int MAX_EXPONENT = 12_287;
  private static final long MAX_COEFFICIENT_HIGH = 0x0001_ed09_bead_87c0L;
  private static final long MAX_COEFFICIENT_LOW = 0x378d_8e63_ffff_ffffL;
  private static final long MIN_NORMAL_HIGH = 0x0000_314d_c644_8d93L;
  private static final long MIN_NORMAL_LOW = 0x38c1_5b0a_0000_0000L;
  private static final long HALF_NORMAL_HIGH = 0x0000_f684_df56_c3e0L;
  private static final long HALF_NORMAL_LOW = 0x1bc6_c732_0000_0000L;
  private static final long[][] POW10 = powersOfTen();
  private static final UInt128 MAX_NAN_PAYLOAD =
      new UInt128(0x0000_314d_c644_8d93L, 0x38c1_5b09_ffff_ffffL);
  private static final ThreadLocal<UInt256> MAGNITUDE =
      ThreadLocal.withInitial(UInt256::new);

  private Bid128Add() {
  }

  /** Adds two BID decimal128 values and accumulates IEEE 754 status flags. */
  public static Bid128 add(
      Bid128 x, Bid128 y, RoundingMode roundingMode, StatusFlags flags) {
    Objects.requireNonNull(x, "x");
    Objects.requireNonNull(y, "y");
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");
    return addInternal(x, y, roundingMode, flags);
  }

  /** Subtracts two BID decimal128 values and accumulates IEEE 754 status flags. */
  public static Bid128 subtract(
      Bid128 x, Bid128 y, RoundingMode roundingMode, StatusFlags flags) {
    Objects.requireNonNull(x, "x");
    Objects.requireNonNull(y, "y");
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");
    return addInternal(x, y.isNaN() ? y : y.negate(), roundingMode, flags);
  }

  private static Bid128 addInternal(
      Bid128 x, Bid128 y, RoundingMode roundingMode, StatusFlags flags) {
    if (x.isNaN()) {
      if (x.isSignalingNaN() || y.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      return quietNaN(x);
    }
    if (x.isInfinite()) {
      if (y.isNaN()) {
        if (y.isSignalingNaN()) {
          flags.raise(StatusFlags.INVALID);
        }
        return quietNaN(y);
      }
      if (y.isInfinite() && x.isSigned() != y.isSigned()) {
        flags.raise(StatusFlags.INVALID);
        return Bid128.QUIET_NAN;
      }
      return x.isSigned() ? Bid128.NEGATIVE_INFINITY : Bid128.POSITIVE_INFINITY;
    }
    if (y.isNaN()) {
      if (y.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      return quietNaN(y);
    }
    if (y.isInfinite()) {
      return y.isSigned() ? Bid128.NEGATIVE_INFINITY : Bid128.POSITIVE_INFINITY;
    }

    long leftBits = x.highBits();
    boolean leftNonCanonical = isNonCanonical(leftBits);
    int leftExponent = leftNonCanonical
        ? (int) (((leftBits << 2) & Bid128.MASK_EXPONENT) >>> 49)
        : (int) ((leftBits & Bid128.MASK_EXPONENT) >>> 49);
    long leftHigh = leftNonCanonical ? 0 : leftBits & Bid128.MASK_COEFFICIENT;
    long leftLow = leftNonCanonical ? 0 : x.lowBits();
    long rightBits = y.highBits();
    boolean rightNonCanonical = isNonCanonical(rightBits);
    int rightExponent = rightNonCanonical
        ? (int) (((rightBits << 2) & Bid128.MASK_EXPONENT) >>> 49)
        : (int) ((rightBits & Bid128.MASK_EXPONENT) >>> 49);
    long rightHigh = rightNonCanonical ? 0 : rightBits & Bid128.MASK_COEFFICIENT;
    long rightLow = rightNonCanonical ? 0 : y.lowBits();
    if (compareToMax(leftHigh, leftLow) > 0) {
      leftHigh = 0;
      leftLow = 0;
    }
    if (compareToMax(rightHigh, rightLow) > 0) {
      rightHigh = 0;
      rightLow = 0;
    }
    if (leftExponent == rightExponent) {
      return addSameExponent(
          x.isSigned(),
          leftHigh,
          leftLow,
          y.isSigned(),
          rightHigh,
          rightLow,
          leftExponent,
          roundingMode,
          flags);
    }
    return addUnequalExponents(
        x.isSigned(),
        leftExponent,
        leftHigh,
        leftLow,
        y.isSigned(),
        rightExponent,
        rightHigh,
        rightLow,
        roundingMode,
        flags);
  }

  private static Bid128 addUnequalExponents(
      boolean leftNegative,
      int leftExponent,
      long leftHigh,
      long leftLow,
      boolean rightNegative,
      int rightExponent,
      long rightHigh,
      long rightLow,
      RoundingMode roundingMode,
      StatusFlags flags) {
    if (isZero(leftHigh, leftLow) && isZero(rightHigh, rightLow)) {
      boolean negative = leftNegative == rightNegative
          ? leftNegative
          : roundingMode == RoundingMode.TOWARD_NEGATIVE;
      return finite(negative, Math.min(leftExponent, rightExponent), 0, 0);
    }
    if (isZero(leftHigh, leftLow)) {
      return withPreferredExponent(
          rightNegative,
          rightExponent,
          rightHigh,
          rightLow,
          leftExponent);
    }
    if (isZero(rightHigh, rightLow)) {
      return withPreferredExponent(
          leftNegative,
          leftExponent,
          leftHigh,
          leftLow,
          rightExponent);
    }
    boolean leftHasLargerExponent = leftExponent >= rightExponent;
    boolean aNegative = leftHasLargerExponent ? leftNegative : rightNegative;
    int aExponent = leftHasLargerExponent ? leftExponent : rightExponent;
    long aHigh = leftHasLargerExponent ? leftHigh : rightHigh;
    long aLow = leftHasLargerExponent ? leftLow : rightLow;
    boolean bNegative = leftHasLargerExponent ? rightNegative : leftNegative;
    int bExponent = leftHasLargerExponent ? rightExponent : leftExponent;
    long bHigh = leftHasLargerExponent ? rightHigh : leftHigh;
    long bLow = leftHasLargerExponent ? rightLow : leftLow;
    int difference = aExponent - bExponent;
    int scale = Math.min(34 - decimalDigits(aHigh, aLow), difference);
    scale = Math.min(scale, aExponent);
    for (int i = 0; i < scale; i++) {
      aHigh = aHigh * 10 + unsignedMultiplyHigh(aLow, 10);
      aLow *= 10;
    }
    aExponent -= scale;
    difference -= scale;
    if (difference == 0) {
      return addSameExponent(
          aNegative,
          aHigh,
          aLow,
          bNegative,
          bHigh,
          bLow,
          bExponent,
          roundingMode,
          flags);
    }
    if (difference > 34) {
      return addWithLargeExponentDifference(
          aNegative,
          aExponent,
          aHigh,
          aLow,
          bNegative,
          bHigh,
          bLow,
          difference,
          roundingMode,
          flags);
    }

    UInt256 magnitude = MAGNITUDE.get();
    magnitude.set128(aHigh, aLow);
    magnitude.multiplyPower10(difference);
    boolean negative;
    if (aNegative == bNegative) {
      magnitude.add128(bHigh, bLow);
      negative = aNegative;
    } else {
      int comparison = magnitude.compare128(bHigh, bLow);
      if (comparison == 0) {
        negative = roundingMode == RoundingMode.TOWARD_NEGATIVE;
        return finite(negative, bExponent, 0, 0);
      }
      if (comparison > 0) {
        magnitude.subtract128(bHigh, bLow);
        negative = aNegative;
      } else {
        magnitude.reverseSubtract128(bHigh, bLow);
        negative = bNegative;
      }
    }
    return roundAndPack(magnitude, negative, bExponent, roundingMode, flags);
  }

  private static Bid128 withPreferredExponent(
      boolean negative,
      int exponent,
      long high,
      long low,
      int zeroExponent) {
    if (exponent <= zeroExponent) {
      return finite(negative, exponent, high, low);
    }
    int scale = Math.min(
        34 - decimalDigits(high, low), exponent - zeroExponent);
    for (int i = 0; i < scale; i++) {
      high = high * 10 + unsignedMultiplyHigh(low, 10);
      low *= 10;
    }
    return finite(negative, exponent - scale, high, low);
  }

  private static Bid128 addSameExponent(
      boolean leftNegative,
      long leftHigh,
      long leftLow,
      boolean rightNegative,
      long rightHigh,
      long rightLow,
      int exponent,
      RoundingMode roundingMode,
      StatusFlags flags) {
    if (leftNegative != rightNegative) {
      int comparison = compare(leftHigh, leftLow, rightHigh, rightLow);
      if (comparison == 0) {
        boolean negative = roundingMode == RoundingMode.TOWARD_NEGATIVE;
        return finite(negative, exponent, 0, 0);
      }
      long largerHigh = comparison > 0 ? leftHigh : rightHigh;
      long largerLow = comparison > 0 ? leftLow : rightLow;
      long smallerHigh = comparison > 0 ? rightHigh : leftHigh;
      long smallerLow = comparison > 0 ? rightLow : leftLow;
      long low = largerLow - smallerLow;
      long borrow = Long.compareUnsigned(largerLow, smallerLow) < 0 ? 1 : 0;
      long high = largerHigh - smallerHigh - borrow;
      return finite(comparison > 0 ? leftNegative : rightNegative, exponent, high, low);
    }

    long low = leftLow + rightLow;
    long carry = Long.compareUnsigned(low, leftLow) < 0 ? 1 : 0;
    long high = leftHigh + rightHigh + carry;
    if (compareToMax(high, low) <= 0) {
      return finite(leftNegative, exponent, high, low);
    }
    return roundSingleExtraDigit(
        leftNegative, exponent, high, low, roundingMode, flags);
  }

  private static Bid128 addWithLargeExponentDifference(
      boolean aNegative,
      int aExponent,
      long aHigh,
      long aLow,
      boolean bNegative,
      long bHigh,
      long bLow,
      int difference,
      RoundingMode roundingMode,
      StatusFlags flags) {
    flags.raise(StatusFlags.INEXACT);
    int direction = 0;
    switch (roundingMode) {
      case TOWARD_POSITIVE:
        direction = bNegative ? 0 : 1;
        break;
      case TOWARD_NEGATIVE:
        direction = bNegative ? -1 : 0;
        break;
      case TOWARD_ZERO:
        if (aNegative != bNegative) {
          direction = aNegative ? 1 : -1;
        }
        break;
      case TIES_TO_EVEN:
      case TIES_AWAY:
        if (difference == 35
            && aHigh == MIN_NORMAL_HIGH
            && aLow == MIN_NORMAL_LOW
            && aNegative != bNegative
            && compare(bHigh, bLow, HALF_NORMAL_HIGH, HALF_NORMAL_LOW) > 0) {
          direction = aNegative ? 1 : -1;
        }
        break;
      default:
        throw new AssertionError(roundingMode);
    }
    if (direction == 0) {
      return pack(aNegative, aExponent, aHigh, aLow, roundingMode, flags);
    }
    boolean increaseMagnitude = aNegative ? direction < 0 : direction > 0;
    aLow = increaseMagnitude ? aLow + 1 : aLow - 1;
    if (increaseMagnitude && aLow == 0) {
      aHigh++;
    } else if (!increaseMagnitude && aLow == -1) {
      aHigh--;
    }
    if (increaseMagnitude && compareToMax(aHigh, aLow) > 0) {
      aHigh = MIN_NORMAL_HIGH;
      aLow = MIN_NORMAL_LOW;
      aExponent++;
    } else if (!increaseMagnitude
        && compare(aHigh, aLow, MIN_NORMAL_HIGH, MIN_NORMAL_LOW) < 0) {
      aHigh = MAX_COEFFICIENT_HIGH;
      aLow = MAX_COEFFICIENT_LOW;
      aExponent--;
    }
    return pack(aNegative, aExponent, aHigh, aLow, roundingMode, flags);
  }

  private static Bid128 roundSingleExtraDigit(
      boolean negative,
      int exponent,
      long high,
      long low,
      RoundingMode roundingMode,
      StatusFlags flags) {
    long dividend = high >>> 32;
    long quotientHigh = dividend / 10 << 32;
    long remainder = dividend % 10;
    dividend = (remainder << 32) | (high & 0xffff_ffffL);
    quotientHigh |= dividend / 10;
    remainder = dividend % 10;
    dividend = (remainder << 32) | (low >>> 32);
    long quotientLow = dividend / 10 << 32;
    remainder = dividend % 10;
    dividend = (remainder << 32) | (low & 0xffff_ffffL);
    quotientLow |= dividend / 10;
    int roundDigit = (int) (dividend % 10);

    boolean increment;
    switch (roundingMode) {
      case TIES_TO_EVEN:
        increment = roundDigit > 5
            || roundDigit == 5 && (quotientLow & 1) != 0;
        break;
      case TIES_AWAY:
        increment = roundDigit >= 5;
        break;
      case TOWARD_POSITIVE:
        increment = !negative && roundDigit != 0;
        break;
      case TOWARD_NEGATIVE:
        increment = negative && roundDigit != 0;
        break;
      case TOWARD_ZERO:
        increment = false;
        break;
      default:
        throw new AssertionError(roundingMode);
    }
    if (increment) {
      quotientLow++;
      if (quotientLow == 0) {
        quotientHigh++;
      }
    }
    if (roundDigit != 0) {
      flags.raise(StatusFlags.INEXACT);
    }
    return pack(
        negative, exponent + 1, quotientHigh, quotientLow, roundingMode, flags);
  }

  private static Bid128 pack(
      boolean negative,
      int exponent,
      long high,
      long low,
      RoundingMode roundingMode,
      StatusFlags flags) {
    while (exponent > MAX_EXPONENT) {
      long nextLow = low * 10;
      long nextHigh = high * 10 + unsignedMultiplyHigh(low, 10);
      if (compareToMax(nextHigh, nextLow) > 0) {
        break;
      }
      high = nextHigh;
      low = nextLow;
      exponent--;
    }
    if (exponent <= MAX_EXPONENT) {
      return finite(negative, exponent, high, low);
    }
    flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
    boolean infinity;
    switch (roundingMode) {
      case TIES_TO_EVEN:
      case TIES_AWAY:
        infinity = true;
        break;
      case TOWARD_POSITIVE:
        infinity = !negative;
        break;
      case TOWARD_NEGATIVE:
        infinity = negative;
        break;
      case TOWARD_ZERO:
        infinity = false;
        break;
      default:
        throw new AssertionError(roundingMode);
    }
    if (infinity) {
      return negative ? Bid128.NEGATIVE_INFINITY : Bid128.POSITIVE_INFINITY;
    }
    return finite(
        negative, MAX_EXPONENT, MAX_COEFFICIENT_HIGH, MAX_COEFFICIENT_LOW);
  }

  private static int decimalDigits(long high, long low) {
    for (int digits = 1; digits < 35; digits++) {
      if (compare(high, low, POW10[0][digits], POW10[1][digits]) < 0) {
        return digits;
      }
    }
    return 35;
  }

  private static int compareToMax(long high, long low) {
    return compare(high, low, MAX_COEFFICIENT_HIGH, MAX_COEFFICIENT_LOW);
  }

  private static boolean isZero(long high, long low) {
    return high == 0 && low == 0;
  }

  private static int compare(long high, long low, long otherHigh, long otherLow) {
    int highComparison = Long.compareUnsigned(high, otherHigh);
    return highComparison != 0
        ? highComparison
        : Long.compareUnsigned(low, otherLow);
  }

  private static long unsignedMultiplyHigh(long left, long right) {
    long result = Math.multiplyHigh(left, right);
    if (left < 0) {
      result += right;
    }
    if (right < 0) {
      result += left;
    }
    return result;
  }

  private static long[][] powersOfTen() {
    long[][] result = new long[2][35];
    result[1][0] = 1;
    for (int i = 1; i < 35; i++) {
      result[0][i] = result[0][i - 1] * 10
          + unsignedMultiplyHigh(result[1][i - 1], 10);
      result[1][i] = result[1][i - 1] * 10;
    }
    return result;
  }

  private static Bid128 roundAndPack(
      UInt256 magnitude,
      boolean negative,
      int exponent,
      RoundingMode roundingMode,
      StatusFlags flags) {
    int extraDigits = 0;
    int roundDigit = 0;
    boolean sticky = false;
    while (magnitude.compareToMax() > 0) {
      sticky |= roundDigit != 0;
      if (magnitude.needsNineDigitReduction()) {
        sticky |= magnitude.divideByBillion() != 0;
        extraDigits += 9;
        roundDigit = 0;
      } else {
        roundDigit = magnitude.divide10();
        extraDigits++;
      }
    }
    boolean inexact = roundDigit != 0 || sticky;
    boolean increment;
    switch (roundingMode) {
      case TIES_TO_EVEN:
        increment = roundDigit > 5
            || roundDigit == 5 && (sticky || magnitude.isOdd());
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
        throw new AssertionError(roundingMode);
    }
    if (increment) {
      magnitude.increment();
    }
    exponent += extraDigits;
    if (magnitude.compareToMax() > 0) {
      magnitude.set128(MIN_NORMAL_HIGH, MIN_NORMAL_LOW);
      exponent++;
    }
    if (inexact) {
      flags.raise(StatusFlags.INEXACT);
      if (exponent == 0
          && magnitude.compare128(MIN_NORMAL_HIGH, MIN_NORMAL_LOW) < 0) {
        flags.raise(StatusFlags.UNDERFLOW);
      }
    }
    return pack(
        negative, exponent, magnitude.midLow, magnitude.low, roundingMode, flags);
  }

  private static boolean isNonCanonical(long high) {
    return (high & Bid128.MASK_STEERING_BITS) == Bid128.MASK_STEERING_BITS;
  }

  private static Bid128 finite(
      boolean negative, int exponent, long coefficientHigh, long coefficientLow) {
    return Bid128.rawFinite(negative, exponent, coefficientHigh, coefficientLow);
  }

  private static Bid128 quietNaN(Bid128 value) {
    long payloadHigh = value.highBits() & 0x0000_3fff_ffff_ffffL;
    long payloadLow = value.lowBits();
    if (compare(
        payloadHigh,
        payloadLow,
        MAX_NAN_PAYLOAD.high(),
        MAX_NAN_PAYLOAD.low()) > 0) {
      payloadHigh = 0L;
      payloadLow = 0L;
    }
    long high = (value.highBits() & 0xfc00_0000_0000_0000L) | payloadHigh;
    return Bid128.fromRawBits(high, payloadLow);
  }

  /** Mutable unsigned 256-bit value used only by unequal-exponent finite addition. */
  private static final class UInt256 {
    private long high;
    private long midHigh;
    private long midLow;
    private long low;

    private UInt256() {
    }

    private void multiplyPower10(int power) {
      if (power == 0) {
        return;
      }
      long yHigh = POW10[0][power];
      long yLow = POW10[1][power];
      long p00High = unsignedMultiplyHigh(low, yLow);
      long p01Low = low * yHigh;
      long p01High = unsignedMultiplyHigh(low, yHigh);
      long p10Low = midLow * yLow;
      long p10High = unsignedMultiplyHigh(midLow, yLow);
      long p11Low = midLow * yHigh;
      long p11High = unsignedMultiplyHigh(midLow, yHigh);
      low = low * yLow;
      long sum = p00High + p01Low;
      long carry = Long.compareUnsigned(sum, p00High) < 0 ? 1L : 0L;
      long next = sum + p10Low;
      carry += Long.compareUnsigned(next, sum) < 0 ? 1L : 0L;
      midLow = next;
      sum = p01High + p10High;
      long highCarry = Long.compareUnsigned(sum, p01High) < 0 ? 1L : 0L;
      next = sum + p11Low;
      highCarry += Long.compareUnsigned(next, sum) < 0 ? 1L : 0L;
      sum = next + carry;
      highCarry += Long.compareUnsigned(sum, next) < 0 ? 1L : 0L;
      midHigh = sum;
      high = p11High + highCarry;
    }

    private void add128(long otherHigh, long otherLow) {
      long nextLow = low + otherLow;
      long carry0 = Long.compareUnsigned(nextLow, low) < 0 ? 1 : 0;
      long partial = midLow + otherHigh;
      long carry1 = Long.compareUnsigned(partial, midLow) < 0 ? 1 : 0;
      long nextMidLow = partial + carry0;
      if (Long.compareUnsigned(nextMidLow, partial) < 0) {
        carry1++;
      }
      long nextMidHigh = midHigh + carry1;
      long carry2 = Long.compareUnsigned(nextMidHigh, midHigh) < 0 ? 1 : 0;
      low = nextLow;
      midLow = nextMidLow;
      midHigh = nextMidHigh;
      high += carry2;
    }

    private void subtract128(long otherHigh, long otherLow) {
      long nextLow = low - otherLow;
      long borrow0 = Long.compareUnsigned(low, otherLow) < 0 ? 1 : 0;
      long partial = midLow - otherHigh;
      long borrow1 = Long.compareUnsigned(midLow, otherHigh) < 0 ? 1 : 0;
      long nextMidLow = partial - borrow0;
      if (borrow0 != 0 && partial == 0) {
        borrow1++;
      }
      long nextMidHigh = midHigh - borrow1;
      long borrow2 = Long.compareUnsigned(midHigh, borrow1) < 0 ? 1 : 0;
      low = nextLow;
      midLow = nextMidLow;
      midHigh = nextMidHigh;
      high -= borrow2;
    }

    private void reverseSubtract128(long otherHigh, long otherLow) {
      long nextLow = otherLow - low;
      long borrow = Long.compareUnsigned(otherLow, low) < 0 ? 1 : 0;
      midLow = otherHigh - midLow - borrow;
      low = nextLow;
      midHigh = 0;
      high = 0;
    }

    private int compare128(long otherHigh, long otherLow) {
      if (high != 0 || midHigh != 0) {
        return 1;
      }
      return compare(midLow, low, otherHigh, otherLow);
    }

    private int compareToMax() {
      return compare128(MAX_COEFFICIENT_HIGH, MAX_COEFFICIENT_LOW);
    }

    private boolean needsNineDigitReduction() {
      if (high != 0 || Long.compareUnsigned(midHigh, 0x4_7bf1L) > 0) {
        return true;
      }
      if (midHigh < 0x4_7bf1L) {
        return false;
      }
      return compare(midLow, low, 0x9673_df52_e37f_2410L, 0x011d_1000_0000_0000L) >= 0;
    }

    private int divideByBillion() {
      return divideSmall(1_000_000_000L);
    }

    private int divideSmall(long divisor) {
      long remainder = 0;
      long dividend = (remainder << 32) | (high >>> 32);
      long nextHigh = Long.divideUnsigned(dividend, divisor) << 32;
      remainder = Long.remainderUnsigned(dividend, divisor);
      dividend = (remainder << 32) | (high & 0xffff_ffffL);
      nextHigh |= Long.divideUnsigned(dividend, divisor);
      remainder = Long.remainderUnsigned(dividend, divisor);

      dividend = (remainder << 32) | (midHigh >>> 32);
      long nextMidHigh = Long.divideUnsigned(dividend, divisor) << 32;
      remainder = Long.remainderUnsigned(dividend, divisor);
      dividend = (remainder << 32) | (midHigh & 0xffff_ffffL);
      nextMidHigh |= Long.divideUnsigned(dividend, divisor);
      remainder = Long.remainderUnsigned(dividend, divisor);

      dividend = (remainder << 32) | (midLow >>> 32);
      long nextMidLow = Long.divideUnsigned(dividend, divisor) << 32;
      remainder = Long.remainderUnsigned(dividend, divisor);
      dividend = (remainder << 32) | (midLow & 0xffff_ffffL);
      nextMidLow |= Long.divideUnsigned(dividend, divisor);
      remainder = Long.remainderUnsigned(dividend, divisor);

      dividend = (remainder << 32) | (low >>> 32);
      long nextLow = Long.divideUnsigned(dividend, divisor) << 32;
      remainder = Long.remainderUnsigned(dividend, divisor);
      dividend = (remainder << 32) | (low & 0xffff_ffffL);
      nextLow |= Long.divideUnsigned(dividend, divisor);
      remainder = Long.remainderUnsigned(dividend, divisor);

      high = nextHigh;
      midHigh = nextMidHigh;
      midLow = nextMidLow;
      low = nextLow;
      return (int) remainder;
    }

    private int divide10() {
      long dividend = high >>> 32;
      long nextHigh = dividend / 10 << 32;
      long remainder = dividend % 10;
      dividend = (remainder << 32) | (high & 0xffff_ffffL);
      nextHigh |= dividend / 10;
      remainder = dividend % 10;

      dividend = (remainder << 32) | (midHigh >>> 32);
      long nextMidHigh = dividend / 10 << 32;
      remainder = dividend % 10;
      dividend = (remainder << 32) | (midHigh & 0xffff_ffffL);
      nextMidHigh |= dividend / 10;
      remainder = dividend % 10;

      dividend = (remainder << 32) | (midLow >>> 32);
      long nextMidLow = dividend / 10 << 32;
      remainder = dividend % 10;
      dividend = (remainder << 32) | (midLow & 0xffff_ffffL);
      nextMidLow |= dividend / 10;
      remainder = dividend % 10;

      dividend = (remainder << 32) | (low >>> 32);
      long nextLow = dividend / 10 << 32;
      remainder = dividend % 10;
      dividend = (remainder << 32) | (low & 0xffff_ffffL);
      nextLow |= dividend / 10;
      remainder = dividend % 10;

      high = nextHigh;
      midHigh = nextMidHigh;
      midLow = nextMidLow;
      low = nextLow;
      return (int) remainder;
    }

    private void increment() {
      low++;
      if (low != 0) {
        return;
      }
      midLow++;
      if (midLow != 0) {
        return;
      }
      midHigh++;
      if (midHigh == 0) {
        high++;
      }
    }

    private boolean isOdd() {
      return (low & 1) != 0;
    }

    private void set128(long coefficientHigh, long coefficientLow) {
      high = 0;
      midHigh = 0;
      midLow = coefficientHigh;
      low = coefficientLow;
    }
  }
}
