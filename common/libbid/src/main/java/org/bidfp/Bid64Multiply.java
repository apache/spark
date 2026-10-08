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

/** Pure-Java, fixed-limb port of Intel RDFP {@code bid64_mul}. */
public final class Bid64Multiply {
  private static final int EXPONENT_BIAS = 398;
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
    10_000_000_000_000_000L
  };
  private static final long[] POW10_HIGH = {
    0L, 0L, 0L, 0L,
    0x5L, 0x36L, 0x21eL, 0x152dL,
    0xd3c2L, 0x8_4595L, 0x52_b7d2L, 0x033b_2e3cL,
    0x204f_ce5eL, 0x1_431e_0faeL, 0xc_9f2c_9cd0L,
    0x7e_37be_2022L, 0x04ee_2d6d_415bL
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

  private Bid64Multiply() {
  }

  /**
   * Multiplies two BID decimal64 values and accumulates IEEE 754 status flags.
   *
   * @param x left operand
   * @param y right operand
   * @param roundingMode rounding-direction attribute
   * @param flags mutable status flags, which are accumulated rather than cleared
   */
  public static Bid64 multiply(
      Bid64 x, Bid64 y, RoundingMode roundingMode, StatusFlags flags) {
    Objects.requireNonNull(x, "x");
    Objects.requireNonNull(y, "y");
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");

    return Bid64.fromRawBits(
        multiplyRawBits(x.toRawBits(), y.toRawBits(), roundingMode, flags));
  }

  public static long multiplyRawBits(
      long xBits, long yBits, RoundingMode roundingMode, StatusFlags flags) {
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");
    boolean negative = ((xBits ^ yBits) & Bid64.MASK_SIGN) != 0;
    if ((xBits & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY
        || (yBits & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY) {
      return multiplySpecial(xBits, yBits, negative, flags);
    }

    int exponent = Bid64.biasedExponentBits(xBits)
        + Bid64.biasedExponentBits(yBits) - EXPONENT_BIAS;
    long xCoefficient = Bid64.significandBits(xBits);
    long yCoefficient = Bid64.significandBits(yBits);
    if (xCoefficient == 0 || yCoefficient == 0) {
      return Bid64.finiteRawBits(negative, clampExponent(exponent), 0L);
    }

    long productLow = xCoefficient * yCoefficient;
    long productHigh = unsignedMultiplyHigh(xCoefficient, yCoefficient);
    int extraDigits = extraDecimalDigits(productHigh, productLow);
    if (extraDigits == 0 && exponent >= 0 && exponent <= MAX_EXPONENT) {
      return Bid64.finiteRawBits(negative, exponent, productLow);
    }
    int productDigits = extraDigits == 0 ? 0 : extraDigits + 16;
    int resultExponent = exponent + extraDigits;
    if (resultExponent < 0) {
      extraDigits -= resultExponent;
      resultExponent = 0;
    }

    long coefficient;
    boolean inexact;
    if (extraDigits <= 16) {
      long remainder;
      if (extraDigits == 0) {
        coefficient = productLow;
        remainder = 0L;
      } else {
        long divisor = POW10[extraDigits];
        coefficient = productHigh == 0
            ? Long.divideUnsigned(productLow, divisor)
            : Bid64Divide.divide128By64(productHigh, productLow, divisor);
        remainder = productLow - coefficient * divisor;
      }
      inexact = remainder != 0;
      if (shouldIncrement(
          remainder, POW10[extraDigits], coefficient, negative, roundingMode)) {
        coefficient++;
      }
    } else {
      if (productDigits == 0) {
        productDigits = decimalDigits(productLow);
      }
      if (extraDigits > productDigits) {
        coefficient = 0L;
        inexact = true;
        if (shouldIncrementDiscarded(0, true, coefficient, negative, roundingMode)) {
          coefficient++;
        }
      } else {
        long highQuotient =
            Bid64Divide.divide128By64(productHigh, productLow, POW10[16]);
        long lowRemainder = productLow - highQuotient * POW10[16];
        int remainingDigits = extraDigits - 16;
        long divisor = POW10[remainingDigits];
        coefficient = highQuotient / divisor;
        long highRemainder = highQuotient - coefficient * divisor;
        long roundDivisor = POW10[remainingDigits - 1];
        int roundDigit = (int) (highRemainder / roundDivisor);
        boolean sticky = highRemainder % roundDivisor != 0L || lowRemainder != 0L;
        inexact = roundDigit != 0 || sticky;
        if (shouldIncrementDiscarded(
            roundDigit, sticky, coefficient, negative, roundingMode)) {
          coefficient++;
        }
      }
    }
    if (coefficient == 10_000_000_000_000_000L) {
      coefficient = MIN_NORMAL_COEFFICIENT;
      resultExponent++;
    }

    while (resultExponent > MAX_EXPONENT
        && coefficient <= MAX_COEFFICIENT / 10L) {
      coefficient *= 10L;
      resultExponent--;
    }
    if (resultExponent > MAX_EXPONENT) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
      return overflowResult(negative, roundingMode);
    }

    if (inexact) {
      flags.raise(StatusFlags.INEXACT);
      if (resultExponent == 0 && coefficient < MIN_NORMAL_COEFFICIENT) {
        flags.raise(StatusFlags.UNDERFLOW);
      }
    }
    return Bid64.finiteRawBits(negative, resultExponent, coefficient);
  }

  private static long multiplySpecial(
      long xBits, long yBits, boolean negative, StatusFlags flags) {
    boolean xNaN = (xBits & Bid64.MASK_NAN) == Bid64.MASK_NAN;
    boolean yNaN = (yBits & Bid64.MASK_NAN) == Bid64.MASK_NAN;
    boolean xSignaling =
        (xBits & Bid64.MASK_SIGNALING_NAN) == Bid64.MASK_SIGNALING_NAN;
    boolean ySignaling =
        (yBits & Bid64.MASK_SIGNALING_NAN) == Bid64.MASK_SIGNALING_NAN;
    if (xNaN) {
      return quietNaN(xBits, xSignaling || ySignaling, flags);
    }
    if (yNaN) {
      return quietNaN(yBits, ySignaling, flags);
    }
    long finiteBits = (xBits & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY
        ? yBits
        : xBits;
    if (Bid64.significandBits(finiteBits) == 0) {
      flags.raise(StatusFlags.INVALID);
      return Bid64.MASK_NAN;
    }
    return infinity(negative);
  }

  private static long quietNaN(long bits, boolean signaling, StatusFlags flags) {
    if (signaling) {
      flags.raise(StatusFlags.INVALID);
    }
    long payload = bits & NAN_PAYLOAD_MASK;
    if (payload > MAX_NAN_PAYLOAD) {
      payload = 0L;
    }
    long canonical = (bits & (Bid64.MASK_SIGN | Bid64.MASK_NAN)) | payload;
    return canonical & QUIET_NAN_MASK;
  }

  private static long infinity(boolean negative) {
    return (negative ? Bid64.MASK_SIGN : 0L) | Bid64.MASK_INFINITY;
  }

  private static long overflowResult(boolean negative, RoundingMode mode) {
    boolean infinity;
    switch (mode) {
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
        throw new IllegalStateException(String.valueOf(mode));
    }
    return infinity
        ? infinity(negative)
        : Bid64.finiteRawBits(negative, MAX_EXPONENT, MAX_COEFFICIENT);
  }

  private static int clampExponent(int exponent) {
    return Math.max(0, Math.min(MAX_EXPONENT, exponent));
  }

  private static int extraDecimalDigits(long valueHigh, long valueLow) {
    if (valueHigh == 0) {
      if (Long.compareUnsigned(valueLow, MAX_COEFFICIENT) <= 0) {
        return 0;
      }
      if (Long.compareUnsigned(valueLow, POW10_LOW[1]) < 0) {
        return 1;
      }
      if (Long.compareUnsigned(valueLow, POW10_LOW[2]) < 0) {
        return 2;
      }
      if (Long.compareUnsigned(valueLow, POW10_LOW[3]) < 0) {
        return 3;
      }
      return 4;
    }
    int bits = 128 - Long.numberOfLeadingZeros(valueHigh);
    int digits = (((bits - 1) * 1233) >>> 12) + 1;
    int threshold = digits - 16;
    if (compare(
        valueHigh, valueLow, POW10_HIGH[threshold], POW10_LOW[threshold]) >= 0) {
      digits++;
    }
    return digits - 16;
  }

  private static int decimalDigits(long value) {
    int bits = 64 - Long.numberOfLeadingZeros(value);
    int digits = (((bits - 1) * 1233) >>> 12) + 1;
    if (digits < POW10.length && value >= POW10[digits]) {
      digits++;
    }
    return digits;
  }

  private static boolean shouldIncrementDiscarded(
      int roundDigit,
      boolean sticky,
      long coefficient,
      boolean negative,
      RoundingMode mode) {
    boolean inexact = roundDigit != 0 || sticky;
    switch (mode) {
      case TIES_TO_EVEN:
        return roundDigit > 5
            || roundDigit == 5 && (sticky || (coefficient & 1L) != 0);
      case TIES_AWAY:
        return roundDigit >= 5;
      case TOWARD_POSITIVE:
        return !negative && inexact;
      case TOWARD_NEGATIVE:
        return negative && inexact;
      case TOWARD_ZERO:
        return false;
      default:
        throw new IllegalStateException(String.valueOf(mode));
    }
  }

  private static boolean shouldIncrement(
      long remainder,
      long divisor,
      long coefficient,
      boolean negative,
      RoundingMode mode) {
    boolean inexact = remainder != 0;
    if (mode == RoundingMode.TIES_TO_EVEN) {
      long doubled = remainder * 2L;
      return doubled > divisor || doubled == divisor && (coefficient & 1L) != 0;
    }
    switch (mode) {
      case TIES_AWAY:
        return remainder * 2L >= divisor;
      case TOWARD_POSITIVE:
        return !negative && inexact;
      case TOWARD_NEGATIVE:
        return negative && inexact;
      case TOWARD_ZERO:
        return false;
      default:
        throw new IllegalStateException(String.valueOf(mode));
    }
  }

  private static int compare(long valueHigh, long valueLow, long high, long low) {
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
