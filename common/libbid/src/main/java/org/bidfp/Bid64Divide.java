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

/** Pure-Java, fixed-limb port of Intel RDFP {@code bid64_div}. */
public final class Bid64Divide {
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

  private Bid64Divide() {
  }

  /**
   * Divides two BID decimal64 values and accumulates IEEE 754 status flags.
   *
   * @param x dividend
   * @param y divisor
   * @param roundingMode rounding-direction attribute
   * @param flags mutable status flags, which are accumulated rather than cleared
   */
  public static Bid64 divide(
      Bid64 x, Bid64 y, RoundingMode roundingMode, StatusFlags flags) {
    Objects.requireNonNull(x, "x");
    Objects.requireNonNull(y, "y");
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");

    return Bid64.fromRawBits(
        divideRawBits(x.toRawBits(), y.toRawBits(), roundingMode, flags));
  }

  public static long divideRawBits(
      long xBits, long yBits, RoundingMode roundingMode, StatusFlags flags) {
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");
    boolean negative = ((xBits ^ yBits) & Bid64.MASK_SIGN) != 0;
    if ((xBits & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY
        || (yBits & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY) {
      return divideSpecial(xBits, yBits, negative, flags);
    }

    long xCoefficient = Bid64.significandBits(xBits);
    long yCoefficient = Bid64.significandBits(yBits);
    int exponent = Bid64.biasedExponentBits(xBits)
        - Bid64.biasedExponentBits(yBits) + EXPONENT_BIAS;
    if (xCoefficient == 0) {
      if (yCoefficient == 0) {
        flags.raise(StatusFlags.INVALID);
        return Bid64.MASK_NAN;
      }
      return Bid64.finiteRawBits(negative, clampExponent(exponent), 0L);
    }
    if (yCoefficient == 0) {
      flags.raise(StatusFlags.DIVIDE_BY_ZERO);
      return infinity(negative);
    }

    int scale = decimalScale(xCoefficient, yCoefficient);
    long numeratorHigh;
    long numeratorLow;
    if (scale <= 16) {
      long power = POW10[scale];
      numeratorHigh = unsignedMultiplyHigh(xCoefficient, power);
      numeratorLow = xCoefficient * power;
    } else {
      long baseLow = xCoefficient * POW10[16];
      long baseHigh = unsignedMultiplyHigh(xCoefficient, POW10[16]);
      long power = POW10[scale - 16];
      numeratorHigh = baseHigh * power + unsignedMultiplyHigh(baseLow, power);
      numeratorLow = baseLow * power;
    }
    long coefficient = divide128By64(numeratorHigh, numeratorLow, yCoefficient);
    long remainder = numeratorLow - coefficient * yCoefficient;
    exponent -= scale;

    if (remainder == 0) {
      int trailingZeros = decimalTrailingZeros(coefficient);
      coefficient /= POW10[trailingZeros];
      exponent += trailingZeros;
    }

    boolean inexact = remainder != 0;
    if (exponent < 0) {
      int discarded = -exponent;
      long discardedValue;
      long divisor;
      boolean sticky = inexact;
      if (discarded > 16) {
        discardedValue = coefficient;
        coefficient = 0L;
        divisor = 0L;
      } else {
        divisor = POW10[discarded];
        discardedValue = coefficient % divisor;
        coefficient /= divisor;
      }
      inexact = sticky || discardedValue != 0;
      int roundDigit;
      if (discarded > 16) {
        roundDigit = 0;
        sticky = inexact;
      } else {
        long roundDivisor = POW10[discarded - 1];
        roundDigit = (int) (discardedValue / roundDivisor);
        sticky |= discardedValue % roundDivisor != 0;
      }
      if (shouldIncrement(
          roundDigit, sticky, coefficient, negative, roundingMode)) {
        coefficient++;
      }
      exponent = 0;
    } else if (inexact
        && shouldIncrementRational(remainder, yCoefficient, coefficient, negative, roundingMode)) {
      coefficient++;
    }

    if (coefficient == 10_000_000_000_000_000L) {
      coefficient = MIN_NORMAL_COEFFICIENT;
      exponent++;
    }
    while (exponent > MAX_EXPONENT && coefficient <= MAX_COEFFICIENT / 10L) {
      coefficient *= 10L;
      exponent--;
    }
    if (exponent > MAX_EXPONENT) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
      return overflowResult(negative, roundingMode);
    }

    if (inexact) {
      flags.raise(StatusFlags.INEXACT);
      if (exponent == 0 && coefficient < MIN_NORMAL_COEFFICIENT) {
        flags.raise(StatusFlags.UNDERFLOW);
      }
    }
    return Bid64.finiteRawBits(negative, exponent, coefficient);
  }

  private static boolean shouldIncrementRational(
      long remainder,
      long divisor,
      long coefficient,
      boolean negative,
      RoundingMode mode) {
    if (mode == RoundingMode.TIES_TO_EVEN) {
      long twice = remainder * 2L;
      return twice > divisor || twice == divisor && (coefficient & 1L) != 0;
    }
    switch (mode) {
      case TIES_AWAY:
        return remainder * 2L >= divisor;
      case TOWARD_POSITIVE:
        return !negative;
      case TOWARD_NEGATIVE:
        return negative;
      case TOWARD_ZERO:
        return false;
      default:
        throw new AssertionError(mode);
    }
  }

  private static boolean shouldIncrement(
      int roundDigit,
      boolean sticky,
      long coefficient,
      boolean negative,
      RoundingMode mode) {
    boolean inexact = roundDigit != 0 || sticky;
    if (mode == RoundingMode.TIES_TO_EVEN) {
      return roundDigit > 5
          || roundDigit == 5 && (sticky || (coefficient & 1L) != 0);
    }
    switch (mode) {
      case TIES_AWAY:
        return roundDigit >= 5;
      case TOWARD_POSITIVE:
        return !negative && inexact;
      case TOWARD_NEGATIVE:
        return negative && inexact;
      case TOWARD_ZERO:
        return false;
      default:
        throw new AssertionError(mode);
    }
  }

  private static long divideSpecial(
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
    boolean xInfinite = (xBits & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY;
    boolean yInfinite = (yBits & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY;
    if (xInfinite && yInfinite) {
      flags.raise(StatusFlags.INVALID);
      return Bid64.MASK_NAN;
    }
    if (xInfinite) {
      return infinity(negative);
    }
    return negative ? Bid64.MASK_SIGN : 0L;
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

  private static int decimalScale(long dividend, long divisor) {
    int ratioExponent = decimalDigits(dividend) - decimalDigits(divisor);
    boolean belowPower;
    if (ratioExponent >= 0) {
      long power = POW10[ratioExponent];
      long scaledHigh = unsignedMultiplyHigh(divisor, power);
      long scaledLow = divisor * power;
      belowPower = scaledHigh != 0L
          || Long.compareUnsigned(dividend, scaledLow) < 0;
    } else {
      long power = POW10[-ratioExponent];
      long scaledHigh = unsignedMultiplyHigh(dividend, power);
      long scaledLow = dividend * power;
      belowPower = scaledHigh == 0L
          && Long.compareUnsigned(scaledLow, divisor) < 0;
    }
    if (belowPower) {
      ratioExponent--;
    }
    return 15 - ratioExponent;
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
        throw new AssertionError(mode);
    }
    return infinity
        ? infinity(negative)
        : Bid64.finiteRawBits(negative, MAX_EXPONENT, MAX_COEFFICIENT);
  }

  static long divide128By64(long high, long low, long divisor) {
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
            quotientHigh * divisorLow, (remainder << 32) + dividendMiddle) > 0) {
      quotientHigh--;
      remainder += divisorHigh;
      if (remainder >= base) {
        break;
      }
    }

    long partialDividend = (normalizedHigh << 32)
        + dividendMiddle
        - quotientHigh * normalizedDivisor;
    long quotientLow = Long.divideUnsigned(partialDividend, divisorHigh);
    remainder = partialDividend - quotientLow * divisorHigh;
    while (quotientLow >= base
        || Long.compareUnsigned(
            quotientLow * divisorLow, (remainder << 32) + dividendLow) > 0) {
      quotientLow--;
      remainder += divisorHigh;
      if (remainder >= base) {
        break;
      }
    }
    return (quotientHigh << 32) + quotientLow;
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

  private static int decimalDigits(long value) {
    int bits = 64 - Long.numberOfLeadingZeros(value);
    int digits = (((bits - 1) * 1233) >>> 12) + 1;
    if (digits < POW10.length && value >= POW10[digits]) {
      digits++;
    }
    return digits;
  }

  private static int decimalTrailingZeros(long value) {
    int zeros = 0;
    if (value != 0 && value % POW10[8] == 0) {
      value /= POW10[8];
      zeros += 8;
    }
    if (value != 0 && value % POW10[4] == 0) {
      value /= POW10[4];
      zeros += 4;
    }
    if (value != 0 && value % POW10[2] == 0) {
      value /= POW10[2];
      zeros += 2;
    }
    if (value != 0 && value % 10L == 0) {
      zeros++;
    }
    return zeros;
  }

  private static int clampExponent(int exponent) {
    return Math.max(0, Math.min(MAX_EXPONENT, exponent));
  }

}
