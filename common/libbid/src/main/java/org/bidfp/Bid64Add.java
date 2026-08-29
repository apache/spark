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

/** Pure-Java, fixed-limb port of Intel RDFP {@code bid64_add} and {@code bid64_sub}. */
public final class Bid64Add {
  private static final int MAX_EXPONENT = 767;
  private static final long MAX_COEFFICIENT = 9_999_999_999_999_999L;
  private static final long MIN_NORMAL_COEFFICIENT = 1_000_000_000_000_000L;
  private static final long MAX_NAN_PAYLOAD = 999_999_999_999_999L;
  private static final long NAN_PAYLOAD_MASK = 0x0003_ffff_ffff_ffffL;
  private static final long QUIET_NAN_MASK = 0xfdff_ffff_ffff_ffffL;
  private static final long[] POW10 = {
    1L,
    10L,
    100L,
    1_000L,
    10_000L,
    100_000L,
    1_000_000L,
    10_000_000L,
    100_000_000L,
    1_000_000_000L,
    10_000_000_000L,
    100_000_000_000L,
    1_000_000_000_000L,
    10_000_000_000_000L,
    100_000_000_000_000L,
    1_000_000_000_000_000L,
    10_000_000_000_000_000L,
    100_000_000_000_000_000L,
    1_000_000_000_000_000_000L
  };
  private static final long[] POW10_HIGH = {
    0x0000_0000_0000_0000L, 0x0000_0000_0000_0000L,
    0x0000_0000_0000_0000L, 0x0000_0000_0000_0000L,
    0x0000_0000_0000_0005L, 0x0000_0000_0000_0036L,
    0x0000_0000_0000_021eL, 0x0000_0000_0000_152dL,
    0x0000_0000_0000_d3c2L, 0x0000_0000_0008_4595L,
    0x0000_0000_0052_b7d2L, 0x0000_0000_033b_2e3cL,
    0x0000_0000_204f_ce5eL, 0x0000_0001_431e_0faeL,
    0x0000_000c_9f2c_9cd0L, 0x0000_007e_37be_2022L,
    0x0000_04ee_2d6d_415bL
  };
  private static final long[] POW10_LOW = {
    0x0023_86f2_6fc1_0000L, 0x0163_4578_5d8a_0000L,
    0x0de0_b6b3_a764_0000L, 0x8ac7_2304_89e8_0000L,
    0x6bc7_5e2d_6310_0000L, 0x35c9_adc5_dea0_0000L,
    0x19e0_c9ba_b240_0000L, 0x02c7_e14a_f680_0000L,
    0x1bce_cced_a100_0000L, 0x1614_0148_4a00_0000L,
    0xdcc8_0cd2_e400_0000L, 0x9fd0_803c_e800_0000L,
    0x3e25_0261_1000_0000L, 0x6d72_17ca_a000_0000L,
    0x4674_edea_4000_0000L, 0xc091_4b26_8000_0000L,
    0x85ac_ef81_0000_0000L
  };

  private Bid64Add() {
  }

  /**
   * Adds two BID decimal64 values and accumulates IEEE 754 status flags.
   *
   * @param x left operand
   * @param y right operand
   * @param roundingMode rounding-direction attribute
   * @param flags mutable status flags, which are accumulated rather than cleared
   */
  public static Bid64 add(
      Bid64 x, Bid64 y, RoundingMode roundingMode, StatusFlags flags) {
    Objects.requireNonNull(x, "x");
    Objects.requireNonNull(y, "y");
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");

    return Bid64.fromRawBits(
        addRawBits(x.toRawBits(), y.toRawBits(), roundingMode, flags));
  }

  /**
   * Subtracts two BID decimal64 values and accumulates IEEE 754 status flags.
   */
  public static Bid64 subtract(
      Bid64 x, Bid64 y, RoundingMode roundingMode, StatusFlags flags) {
    Objects.requireNonNull(x, "x");
    Objects.requireNonNull(y, "y");
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");

    return Bid64.fromRawBits(
        subtractRawBits(x.toRawBits(), y.toRawBits(), roundingMode, flags));
  }

  public static long subtractRawBits(
      long xBits, long yBits, RoundingMode roundingMode, StatusFlags flags) {
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");
    long addendBits = (yBits & Bid64.MASK_NAN) == Bid64.MASK_NAN
        ? yBits
        : yBits ^ Bid64.MASK_SIGN;
    return addRawBitsUnchecked(xBits, addendBits, roundingMode, flags);
  }

  public static long addRawBits(
      long xBits, long yBits, RoundingMode roundingMode, StatusFlags flags) {
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");
    return addRawBitsUnchecked(xBits, yBits, roundingMode, flags);
  }

  private static long addRawBitsUnchecked(
      long xBits, long yBits, RoundingMode roundingMode, StatusFlags flags) {
    if ((xBits & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY
        || (yBits & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY) {
      return addSpecial(xBits, yBits, flags);
    }

    boolean leftNegative = (xBits & Bid64.MASK_SIGN) != 0;
    boolean rightNegative = (yBits & Bid64.MASK_SIGN) != 0;
    int leftExponent = Bid64.biasedExponentBits(xBits);
    int rightExponent = Bid64.biasedExponentBits(yBits);
    long leftCoefficient = Bid64.significandBits(xBits);
    long rightCoefficient = Bid64.significandBits(yBits);
    if (leftExponent == rightExponent) {
      return addSameExponent(
          leftNegative,
          leftCoefficient,
          rightNegative,
          rightCoefficient,
          leftExponent,
          roundingMode,
          flags);
    }
    if (leftCoefficient == 0 && rightCoefficient == 0) {
      boolean negative = leftNegative == rightNegative
          ? leftNegative
          : roundingMode == RoundingMode.TOWARD_NEGATIVE;
      return Bid64.finiteRawBits(negative, Math.min(leftExponent, rightExponent), 0L);
    }
    if (leftCoefficient == 0 && rightExponent <= leftExponent) {
      return yBits;
    }
    if (rightCoefficient == 0 && rightExponent >= leftExponent) {
      return xBits;
    }

    boolean aNegative;
    int aExponent;
    long aCoefficient;
    boolean bNegative;
    int bExponent;
    long bCoefficient;
    if (leftExponent >= rightExponent) {
      aNegative = leftNegative;
      aExponent = leftExponent;
      aCoefficient = leftCoefficient;
      bNegative = rightNegative;
      bExponent = rightExponent;
      bCoefficient = rightCoefficient;
    } else {
      aNegative = rightNegative;
      aExponent = rightExponent;
      aCoefficient = rightCoefficient;
      bNegative = leftNegative;
      bExponent = leftExponent;
      bCoefficient = leftCoefficient;
    }
    int difference = aExponent - bExponent;
    if (difference > 16) {
      int scale = 16 - decimalDigits(aCoefficient);
      aExponent -= scale;
      aCoefficient *= POW10[scale];
      difference -= scale;
      if (difference > 16) {
        return addWithLargeExponentDifference(
            aNegative,
            aExponent,
            aCoefficient,
            bNegative,
            bCoefficient,
            difference,
            roundingMode,
            flags);
      }
    }

    long power = POW10[difference];
    if (aCoefficient <= Long.MAX_VALUE / power
        && (aNegative != bNegative
            || aCoefficient * power <= Long.MAX_VALUE - bCoefficient)) {
      long scaledA = aCoefficient * power;
      long magnitude;
      boolean negative;
      if (aNegative == bNegative) {
        magnitude = scaledA + bCoefficient;
        negative = aNegative;
      } else if (scaledA == bCoefficient) {
        negative = roundingMode == RoundingMode.TOWARD_NEGATIVE;
        return Bid64.finiteRawBits(negative, bExponent, 0L);
      } else if (scaledA > bCoefficient) {
        magnitude = scaledA - bCoefficient;
        negative = aNegative;
      } else {
        magnitude = bCoefficient - scaledA;
        negative = bNegative;
      }
      return roundAndPack(magnitude, negative, bExponent, roundingMode, flags);
    }

    long scaledHigh = unsignedMultiplyHigh(aCoefficient, power);
    long scaledLow = aCoefficient * power;
    long magnitudeHigh;
    long magnitudeLow;
    boolean negative;
    if (aNegative == bNegative) {
      magnitudeLow = scaledLow + bCoefficient;
      long carry = Long.compareUnsigned(magnitudeLow, scaledLow) < 0 ? 1L : 0L;
      magnitudeHigh = scaledHigh + carry;
      negative = aNegative;
    } else {
      int comparison = scaledHigh == 0L
          ? Long.compareUnsigned(scaledLow, bCoefficient)
          : 1;
      if (comparison == 0) {
        negative = roundingMode == RoundingMode.TOWARD_NEGATIVE;
        return Bid64.finiteRawBits(negative, bExponent, 0L);
      } else if (comparison > 0) {
        magnitudeLow = scaledLow - bCoefficient;
        long borrow = Long.compareUnsigned(scaledLow, bCoefficient) < 0 ? 1L : 0L;
        magnitudeHigh = scaledHigh - borrow;
        negative = aNegative;
      } else {
        magnitudeLow = bCoefficient - scaledLow;
        magnitudeHigh = 0L;
        negative = bNegative;
      }
    }
    return roundAndPack(
        magnitudeHigh,
        magnitudeLow,
        negative,
        bExponent,
        roundingMode,
        flags);
  }

  private static long addSameExponent(
      boolean leftNegative,
      long leftCoefficient,
      boolean rightNegative,
      long rightCoefficient,
      int exponent,
      RoundingMode roundingMode,
      StatusFlags flags) {
    if (leftNegative != rightNegative) {
      if (leftCoefficient == rightCoefficient) {
        boolean negative = roundingMode == RoundingMode.TOWARD_NEGATIVE;
        return Bid64.finiteRawBits(negative, exponent, 0L);
      }
      boolean leftLarger = leftCoefficient > rightCoefficient;
      long coefficient = leftLarger
          ? leftCoefficient - rightCoefficient
          : rightCoefficient - leftCoefficient;
      boolean negative = leftLarger ? leftNegative : rightNegative;
      return Bid64.finiteRawBits(negative, exponent, coefficient);
    }

    long coefficient = leftCoefficient + rightCoefficient;
    if (coefficient <= MAX_COEFFICIENT) {
      return Bid64.finiteRawBits(leftNegative, exponent, coefficient);
    }
    long remainder = coefficient % 10L;
    coefficient /= 10L;
    boolean inexact = remainder != 0L;
    if (shouldIncrement(
        remainder, 10L, coefficient, leftNegative, roundingMode)) {
      coefficient++;
    }
    if (inexact) {
      flags.raise(StatusFlags.INEXACT);
    }
    return pack(leftNegative, exponent + 1, coefficient, roundingMode, flags);
  }

  private static long addSpecial(long xBits, long yBits, StatusFlags flags) {
    boolean xNaN = (xBits & Bid64.MASK_NAN) == Bid64.MASK_NAN;
    boolean yNaN = (yBits & Bid64.MASK_NAN) == Bid64.MASK_NAN;
    boolean xSignaling =
        (xBits & Bid64.MASK_SIGNALING_NAN) == Bid64.MASK_SIGNALING_NAN;
    boolean ySignaling =
        (yBits & Bid64.MASK_SIGNALING_NAN) == Bid64.MASK_SIGNALING_NAN;
    if (xNaN) {
      if (xSignaling || ySignaling) {
        flags.raise(StatusFlags.INVALID);
      }
      return quietNaN(xBits);
    }
    if (yNaN) {
      if (ySignaling) {
        flags.raise(StatusFlags.INVALID);
      }
      return quietNaN(yBits);
    }
    if ((xBits & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY
        && (yBits & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY
        && ((xBits ^ yBits) & Bid64.MASK_SIGN) != 0) {
      flags.raise(StatusFlags.INVALID);
      return Bid64.MASK_NAN;
    }
    long infinityBits = (xBits & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY
        ? xBits
        : yBits;
    return (infinityBits & Bid64.MASK_SIGN) | Bid64.MASK_INFINITY;
  }

  private static long addWithLargeExponentDifference(
      boolean aNegative,
      int aExponent,
      long aCoefficient,
      boolean bNegative,
      long bCoefficient,
      int difference,
      RoundingMode roundingMode,
      StatusFlags flags) {
    if (bCoefficient == 0) {
      return pack(aNegative, aExponent, aCoefficient, roundingMode, flags);
    }

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
        if (difference == 17
            && aCoefficient == MIN_NORMAL_COEFFICIENT
            && aNegative != bNegative
            && bCoefficient > 5_000_000_000_000_000L) {
          direction = aNegative ? 1 : -1;
        }
        break;
      default:
        throw new AssertionError(roundingMode);
    }

    if (direction == 0) {
      return pack(aNegative, aExponent, aCoefficient, roundingMode, flags);
    }
    boolean increaseMagnitude = aNegative ? direction < 0 : direction > 0;
    long coefficient = aCoefficient;
    int exponent = aExponent;
    if (increaseMagnitude) {
      coefficient++;
      if (coefficient > MAX_COEFFICIENT) {
        coefficient = MIN_NORMAL_COEFFICIENT;
        exponent++;
      }
    } else {
      coefficient--;
      if (coefficient < MIN_NORMAL_COEFFICIENT) {
        coefficient = MAX_COEFFICIENT;
        exponent--;
      }
    }
    return pack(aNegative, exponent, coefficient, roundingMode, flags);
  }

  private static long roundAndPack(
      long magnitudeHigh,
      long magnitudeLow,
      boolean negative,
      int exponent,
      RoundingMode roundingMode,
      StatusFlags flags) {
    int extraDigits = extraDecimalDigits(magnitudeHigh, magnitudeLow);
    long coefficient;
    long remainder;
    if (extraDigits == 0) {
      coefficient = magnitudeLow;
      remainder = 0L;
    } else {
      long divisor = POW10[extraDigits];
      coefficient = Bid64Divide.divide128By64(magnitudeHigh, magnitudeLow, divisor);
      remainder = magnitudeLow - coefficient * divisor;
    }

    boolean inexact = remainder != 0;
    boolean increment;
    long divisor = POW10[extraDigits];
    if (roundingMode == RoundingMode.TIES_TO_EVEN) {
      long doubled = remainder * 2L;
      increment = doubled > divisor || doubled == divisor && (coefficient & 1L) != 0;
    } else {
      switch (roundingMode) {
      case TIES_AWAY:
        increment = remainder * 2L >= divisor;
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
    }
    if (increment) {
      coefficient++;
    }
    exponent += extraDigits;
    if (coefficient > MAX_COEFFICIENT) {
      coefficient = MIN_NORMAL_COEFFICIENT;
      exponent++;
    }
    if (inexact) {
      flags.raise(StatusFlags.INEXACT);
      if (exponent == 0 && coefficient < MIN_NORMAL_COEFFICIENT) {
        flags.raise(StatusFlags.UNDERFLOW);
      }
    }
    return pack(negative, exponent, coefficient, roundingMode, flags);
  }

  private static long roundAndPack(
      long magnitude,
      boolean negative,
      int exponent,
      RoundingMode roundingMode,
      StatusFlags flags) {
    int digits = decimalDigits(magnitude);
    int extraDigits = Math.max(0, digits - 16);
    long divisor = POW10[extraDigits];
    long coefficient = magnitude / divisor;
    long remainder = magnitude - coefficient * divisor;
    boolean inexact = remainder != 0L;
    if (shouldIncrement(remainder, divisor, coefficient, negative, roundingMode)) {
      coefficient++;
    }
    exponent += extraDigits;
    if (coefficient > MAX_COEFFICIENT) {
      coefficient = MIN_NORMAL_COEFFICIENT;
      exponent++;
    }
    if (inexact) {
      flags.raise(StatusFlags.INEXACT);
      if (exponent == 0 && coefficient < MIN_NORMAL_COEFFICIENT) {
        flags.raise(StatusFlags.UNDERFLOW);
      }
    }
    return pack(negative, exponent, coefficient, roundingMode, flags);
  }

  private static boolean shouldIncrement(
      long remainder,
      long divisor,
      long coefficient,
      boolean negative,
      RoundingMode roundingMode) {
    boolean inexact = remainder != 0L;
    switch (roundingMode) {
      case TIES_TO_EVEN:
        long doubled = remainder * 2L;
        return doubled > divisor || doubled == divisor && (coefficient & 1L) != 0L;
      case TIES_AWAY:
        return remainder * 2L >= divisor;
      case TOWARD_POSITIVE:
        return !negative && inexact;
      case TOWARD_NEGATIVE:
        return negative && inexact;
      case TOWARD_ZERO:
        return false;
      default:
        throw new AssertionError(roundingMode);
    }
  }

  private static long pack(
      boolean negative,
      int exponent,
      long coefficient,
      RoundingMode roundingMode,
      StatusFlags flags) {
    while (exponent > MAX_EXPONENT && coefficient <= MAX_COEFFICIENT / 10L) {
      coefficient *= 10L;
      exponent--;
    }
    if (exponent <= MAX_EXPONENT) {
      return Bid64.finiteRawBits(negative, exponent, coefficient);
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
      return (negative ? Bid64.MASK_SIGN : 0L) | Bid64.MASK_INFINITY;
    }
    return Bid64.finiteRawBits(negative, MAX_EXPONENT, MAX_COEFFICIENT);
  }

  private static long quietNaN(long bits) {
    long payload = bits & NAN_PAYLOAD_MASK;
    if (payload > MAX_NAN_PAYLOAD) {
      payload = 0L;
    }
    long canonical = (bits & (Bid64.MASK_SIGN | Bid64.MASK_NAN)) | payload;
    return canonical & QUIET_NAN_MASK;
  }

  private static int decimalDigits(long value) {
    int bits = 64 - Long.numberOfLeadingZeros(value);
    int digits = (((bits - 1) * 1233) >>> 12) + 1;
    if (digits < POW10.length && value >= POW10[digits]) {
      digits++;
    }
    return digits;
  }

  private static int extraDecimalDigits(long valueHigh, long valueLow) {
    int low = 0;
    int high = POW10_HIGH.length;
    while (low < high) {
      int middle = (low + high) >>> 1;
      if (compare(
          valueHigh, valueLow, POW10_HIGH[middle], POW10_LOW[middle]) >= 0) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  private static int compare(
      long valueHigh, long valueLow, long high, long low) {
    int highComparison = Long.compareUnsigned(valueHigh, high);
    return highComparison != 0
        ? highComparison
        : Long.compareUnsigned(valueLow, low);
  }

  private static long unsignedMultiplyHigh(long x, long y) {
    long result = Math.multiplyHigh(x, y);
    if (x < 0) {
      result += y;
    }
    if (y < 0) {
      result += x;
    }
    return result;
  }
}
