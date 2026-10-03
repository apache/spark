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

/** Pure-Java, fixed-limb port of Intel RDFP {@code bid128_div}. */
public final class Bid128Divide {
  private static final int EXPONENT_BIAS = 6176;
  private static final int MAX_EXPONENT = 12_287;
  private static final UInt128 TEN = UInt128.fromLong(10);
  private static final UInt128 TEN_TO_33 =
      new UInt128(0x0000_314d_c644_8d93L, 0x38c1_5b0a_0000_0000L);
  private static final UInt128 TEN_TO_34 =
      new UInt128(0x0001_ed09_bead_87c0L, 0x378d_8e64_0000_0000L);
  private static final UInt128 MAX_COEFFICIENT = TEN_TO_34.subtract(1);
  private static final UInt128 MAX_NAN_PAYLOAD = TEN_TO_33.subtract(1);
  private static final UInt128[] POW10 = powersOfTen();
  private static final long NAN_PAYLOAD_HIGH_MASK = 0x0000_3fff_ffff_ffffL;
  private static final long QUIET_NAN_MASK = 0xfdff_ffff_ffff_ffffL;
  private static final ThreadLocal<FixedDivision> DIVISION =
      ThreadLocal.withInitial(FixedDivision::new);

  private Bid128Divide() {
  }

  /**
   * Divides two BID decimal128 values and accumulates IEEE 754 status flags.
   *
   * @param x dividend
   * @param y divisor
   * @param roundingMode rounding-direction attribute
   * @param flags mutable status flags, which are accumulated rather than cleared
   */
  public static Bid128 divide(
      Bid128 x, Bid128 y, RoundingMode roundingMode, StatusFlags flags) {
    Objects.requireNonNull(x, "x");
    Objects.requireNonNull(y, "y");
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");
    long[] out = new long[2];
    divide128(
        x.highBits(),
        x.lowBits(),
        y.highBits(),
        y.lowBits(),
        roundingMode,
        flags,
        out);
    return Bid128.fromRawBits(out[0], out[1]);
  }

  static void divide128(
      long xHighBits,
      long xLowBits,
      long yHighBits,
      long yLowBits,
      RoundingMode roundingMode,
      StatusFlags flags,
      long[] out) {
    Objects.requireNonNull(roundingMode, "roundingMode");
    Objects.requireNonNull(flags, "flags");
    Objects.requireNonNull(out, "out");

    boolean negative = ((xHighBits ^ yHighBits) & Bid128.MASK_SIGN) != 0L;
    boolean xFinite = (xHighBits & Bid128.MASK_INFINITY) != Bid128.MASK_INFINITY;
    boolean yFinite = (yHighBits & Bid128.MASK_INFINITY) != Bid128.MASK_INFINITY;
    if (!xFinite) {
      boolean ySignaling = isSignalingNaN(yHighBits);
      if (ySignaling) {
        flags.raise(StatusFlags.INVALID);
      }
      if (isNaN(xHighBits)) {
        quietNaN(xHighBits, xLowBits, isSignalingNaN(xHighBits), flags, out);
        return;
      }
      if (isNaN(yHighBits)) {
        quietNaN(yHighBits, yLowBits, ySignaling, flags, out);
        return;
      }
      if (!yFinite) {
        flags.raise(StatusFlags.INVALID);
        store(Bid128.MASK_NAN, 0L, out);
        return;
      }
      infinity(negative, out);
      return;
    }
    if (!yFinite) {
      if (isNaN(yHighBits)) {
        quietNaN(yHighBits, yLowBits, isSignalingNaN(yHighBits), flags, out);
        return;
      }
      signedZero(negative, 0, out);
      return;
    }

    boolean xCanonical = Bid128.isCanonicalFinite(xHighBits, xLowBits);
    boolean yCanonical = Bid128.isCanonicalFinite(yHighBits, yLowBits);
    long xHigh = xCanonical ? xHighBits & Bid128.MASK_COEFFICIENT : 0L;
    long xLow = xCanonical ? xLowBits : 0L;
    long yHigh = yCanonical ? yHighBits & Bid128.MASK_COEFFICIENT : 0L;
    long yLow = yCanonical ? yLowBits : 0L;
    int exponent =
        unpackedExponent(xHighBits) - unpackedExponent(yHighBits) + EXPONENT_BIAS;
    if ((xHigh | xLow) == 0L) {
      if ((yHigh | yLow) == 0L) {
        flags.raise(StatusFlags.INVALID);
        store(Bid128.MASK_NAN, 0L, out);
        return;
      }
      signedZero(negative, clampExponent(exponent), out);
      return;
    }
    if ((yHigh | yLow) == 0L) {
      flags.raise(StatusFlags.DIVIDE_BY_ZERO);
      infinity(negative, out);
      return;
    }

    int generated = decimalScale(xHigh, xLow, yHigh, yLow);
    FixedDivision scaled = divideScaled(xHigh, xLow, yHigh, yLow, generated);
    exponent -= generated;

    if ((scaled.remainderHigh | scaled.remainderLow) == 0L && generated != 0) {
      while (generated != 0) {
        int digit = scaled.divideQuotientByTen();
        if (digit != 0) {
          scaled.restoreQuotientDigit(digit);
          break;
        }
        exponent++;
        generated--;
      }
    }

    boolean inexact = (scaled.remainderHigh | scaled.remainderLow) != 0L;
    if (exponent < 0) {
      scaled.discardQuotientDigits(-exponent, inexact);
      inexact = scaled.discardedInexact;
      if (shouldIncrement(
          scaled.roundDigit,
          scaled.sticky,
          scaled.quotientLow,
          negative,
          roundingMode)) {
        scaled.incrementQuotient();
      }
      exponent = 0;
      if (inexact) {
        flags.raise(StatusFlags.INEXACT);
        if (compare(
            scaled.quotientHigh,
            scaled.quotientLow,
            TEN_TO_33.high(),
            TEN_TO_33.low()) < 0) {
          flags.raise(StatusFlags.UNDERFLOW);
        }
      }
    } else if (inexact) {
      if (shouldIncrementRational(
          scaled.remainderHigh,
          scaled.remainderLow,
          yHigh,
          yLow,
          scaled.quotientLow,
          negative,
          roundingMode)) {
        scaled.incrementQuotient();
      }
      flags.raise(StatusFlags.INEXACT);
    }

    if (scaled.quotientHigh == TEN_TO_34.high()
        && scaled.quotientLow == TEN_TO_34.low()) {
      scaled.quotientHigh = TEN_TO_33.high();
      scaled.quotientLow = TEN_TO_33.low();
      exponent++;
    }
    while (exponent > MAX_EXPONENT
        && compare(
            scaled.quotientHigh,
            scaled.quotientLow,
            TEN_TO_33.high(),
            TEN_TO_33.low()) < 0
        && (scaled.quotientHigh | scaled.quotientLow) != 0L) {
      scaled.multiplyQuotientByTen();
      exponent--;
    }
    if (exponent > MAX_EXPONENT) {
      if ((scaled.quotientHigh | scaled.quotientLow) == 0L) {
        signedZero(negative, MAX_EXPONENT, out);
        return;
      }
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
      overflowResult(negative, roundingMode, out);
      return;
    }
    finite(negative, exponent, scaled.quotientHigh, scaled.quotientLow, out);
  }

  private static int unpackedExponent(long high) {
    if ((high & Bid128.MASK_STEERING_BITS) == Bid128.MASK_STEERING_BITS) {
      return (int) ((high >>> 47) & 0x3fffL);
    }
    return (int) ((high >>> 49) & 0x3fffL);
  }

  private static int decimalScale(
      long dividendHigh, long dividendLow, long divisorHigh, long divisorLow) {
    int dividendDigits = decimalDigits(dividendHigh, dividendLow);
    int divisorDigits = decimalDigits(divisorHigh, divisorLow);
    int ratioExponent = dividendDigits - divisorDigits;
    boolean belowPower;
    if (ratioExponent >= 0) {
      belowPower = compareScaled(
          dividendHigh,
          dividendLow,
          divisorHigh,
          divisorLow,
          ratioExponent) < 0;
    } else {
      belowPower = compareScaled(
          divisorHigh,
          divisorLow,
          dividendHigh,
          dividendLow,
          -ratioExponent) > 0;
    }
    if (belowPower) {
      ratioExponent--;
    }
    return 33 - ratioExponent;
  }

  /** Divides {@code dividend * 10^scale} by {@code divisor} with fixed limbs. */
  private static FixedDivision divideScaled(
      long dividendHigh,
      long dividendLow,
      long divisorHigh,
      long divisorLow,
      int scale) {
    FixedDivision division = DIVISION.get();
    division.reset(dividendHigh, dividendLow);
    division.multiplyPowerOfTen(scale);
    division.divide(divisorHigh, divisorLow);
    return division;
  }

  private static boolean shouldIncrementRational(
      long remainderHigh,
      long remainderLow,
      long divisorHigh,
      long divisorLow,
      long coefficientLow,
      boolean negative,
      RoundingMode mode) {
    switch (mode) {
      case TIES_TO_EVEN:
        int comparison = compareTwice(
            remainderHigh, remainderLow, divisorHigh, divisorLow);
        return comparison > 0 || comparison == 0 && (coefficientLow & 1L) != 0;
      case TIES_AWAY:
        return compareTwice(
            remainderHigh, remainderLow, divisorHigh, divisorLow) >= 0;
      case TOWARD_POSITIVE:
        return !negative;
      case TOWARD_NEGATIVE:
        return negative;
      case TOWARD_ZERO:
        return false;
      default:
        throw new IllegalStateException(String.valueOf(mode));
    }
  }

  private static boolean shouldIncrement(
      int roundDigit,
      boolean sticky,
      long coefficientLow,
      boolean negative,
      RoundingMode mode) {
    boolean inexact = roundDigit != 0 || sticky;
    switch (mode) {
      case TIES_TO_EVEN:
        return roundDigit > 5
            || roundDigit == 5 && (sticky || (coefficientLow & 1L) != 0);
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

  private static boolean isNaN(long high) {
    return (high & Bid128.MASK_NAN) == Bid128.MASK_NAN;
  }

  private static boolean isSignalingNaN(long high) {
    return (high & Bid128.MASK_SIGNALING_NAN) == Bid128.MASK_SIGNALING_NAN;
  }

  private static void quietNaN(
      long highBits,
      long lowBits,
      boolean signaling,
      StatusFlags flags,
      long[] out) {
    if (signaling) {
      flags.raise(StatusFlags.INVALID);
    }
    long payloadHigh = highBits & NAN_PAYLOAD_HIGH_MASK;
    long payloadLow = lowBits;
    if (compare(
        payloadHigh,
        payloadLow,
        MAX_NAN_PAYLOAD.high(),
        MAX_NAN_PAYLOAD.low()) > 0) {
      payloadHigh = 0L;
      payloadLow = 0L;
    }
    long high = (highBits & (Bid128.MASK_SIGN | Bid128.MASK_NAN)) | payloadHigh;
    store(high & QUIET_NAN_MASK, payloadLow, out);
  }

  private static void signedZero(boolean negative, int exponent, long[] out) {
    finite(negative, exponent, 0L, 0L, out);
  }

  private static void infinity(boolean negative, long[] out) {
    store((negative ? Bid128.MASK_SIGN : 0L) | Bid128.MASK_INFINITY, 0L, out);
  }

  private static void overflowResult(
      boolean negative, RoundingMode mode, long[] out) {
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
    if (infinity) {
      infinity(negative, out);
    } else {
      finite(
          negative,
          MAX_EXPONENT,
          MAX_COEFFICIENT.high(),
          MAX_COEFFICIENT.low(),
          out);
    }
  }

  private static void finite(
      boolean negative,
      int exponent,
      long coefficientHigh,
      long coefficientLow,
      long[] out) {
    long sign = negative ? Bid128.MASK_SIGN : 0L;
    store(sign | (long) exponent << 49 | coefficientHigh, coefficientLow, out);
  }

  private static void store(long high, long low, long[] out) {
    out[0] = high;
    out[1] = low;
  }

  private static int decimalDigits(long high, long low) {
    for (int digits = 1; digits < POW10.length; digits++) {
      if (compare(high, low, POW10[digits].high(), POW10[digits].low()) < 0) {
        return digits;
      }
    }
    return 34;
  }

  private static int compareScaled(
      long leftHigh,
      long leftLow,
      long rightHigh,
      long rightLow,
      int power) {
    UInt128 factor = POW10[power];
    long scaledLow = rightLow * factor.low();
    long scaledHigh = UInt128.unsignedMultiplyHigh(rightLow, factor.low())
        + rightLow * factor.high()
        + rightHigh * factor.low();
    return compare(leftHigh, leftLow, scaledHigh, scaledLow);
  }

  private static int compareTwice(
      long high, long low, long otherHigh, long otherLow) {
    boolean overflow = high < 0L;
    long doubledHigh = high << 1 | low >>> 63;
    long doubledLow = low << 1;
    return overflow ? 1 : compare(doubledHigh, doubledLow, otherHigh, otherLow);
  }

  private static int compare(
      long high, long low, long otherHigh, long otherLow) {
    int comparison = Long.compareUnsigned(high, otherHigh);
    return comparison != 0 ? comparison : Long.compareUnsigned(low, otherLow);
  }

  private static int clampExponent(int exponent) {
    return Math.max(0, Math.min(MAX_EXPONENT, exponent));
  }

  private static UInt128[] powersOfTen() {
    UInt128[] result = new UInt128[35];
    result[0] = UInt128.fromLong(1);
    for (int i = 1; i < result.length; i++) {
      result[i] = result[i - 1].multiply(TEN);
    }
    return result;
  }

  /** Mutable normalized numerator and two-limb quotient/remainder. */
  private static final class FixedDivision {
    private long limb4;
    private long limb3;
    private long limb2;
    private long limb1;
    private long limb0;
    private long quotientHigh;
    private long quotientLow;
    private long remainderHigh;
    private long remainderLow;
    private int roundDigit;
    private boolean sticky;
    private boolean discardedInexact;

    private FixedDivision() {
    }

    private void reset(long high, long low) {
      limb4 = 0L;
      limb3 = 0L;
      limb2 = 0L;
      limb1 = high;
      limb0 = low;
      quotientHigh = 0L;
      quotientLow = 0L;
      remainderHigh = 0L;
      remainderLow = 0L;
      roundDigit = 0;
      sticky = false;
      discardedInexact = false;
    }

    private void multiplyPowerOfTen(int scale) {
      while (scale >= 9) {
        multiply(1_000_000_000L);
        scale -= 9;
      }
      if (scale != 0) {
        long factor = 1L;
        for (int i = 0; i < scale; i++) {
          factor *= 10L;
        }
        multiply(factor);
      }
    }

    private void multiply(long factor) {
      long product0 = limb0 * factor;
      long carry = UInt128.unsignedMultiplyHigh(limb0, factor);
      long base1 = limb1 * factor;
      long product1 = base1 + carry;
      carry = UInt128.unsignedMultiplyHigh(limb1, factor)
          + carry(base1, product1);
      long base2 = limb2 * factor;
      long product2 = base2 + carry;
      carry = UInt128.unsignedMultiplyHigh(limb2, factor)
          + carry(base2, product2);
      long base3 = limb3 * factor;
      limb3 = base3 + carry;
      limb2 = product2;
      limb1 = product1;
      limb0 = product0;
    }

    private void divide(long divisorHigh, long divisorLow) {
      int dividendLength = significantLength();
      if (divisorHigh == 0L) {
        long remainder = 0L;
        for (int index = dividendLength - 1; index >= 0; index--) {
          long value = limb(index);
          long quotient = divide128By64(remainder, value, divisorLow);
          remainder = value - quotient * divisorLow;
          if (index == 1) {
            quotientHigh = quotient;
          } else if (index == 0) {
            quotientLow = quotient;
          } else if (quotient != 0L) {
            throw new ArithmeticException("decimal128 quotient does not fit");
          }
        }
        remainderLow = remainder;
        return;
      }

      int shift = Long.numberOfLeadingZeros(divisorHigh);
      long normalizedHigh = shift == 0
          ? divisorHigh
          : divisorHigh << shift | divisorLow >>> (64 - shift);
      long normalizedLow = divisorLow << shift;
      normalize(shift);
      int quotientLength = dividendLength - 1;
      for (int index = quotientLength - 1; index >= 0; index--) {
        long high = limb(index + 2);
        long low = limb(index + 1);
        long guess;
        long guessRemainder;
        boolean remainderOverflow;
        if (high == normalizedHigh) {
          guess = -1L;
          guessRemainder = high + low;
          remainderOverflow = Long.compareUnsigned(guessRemainder, high) < 0;
        } else {
          guess = divide128By64(high, low, normalizedHigh);
          guessRemainder = low - guess * normalizedHigh;
          remainderOverflow = false;
        }
        long next = limb(index);
        while (!remainderOverflow
            && productGreaterThanPair(
                guess, normalizedLow, guessRemainder, next)) {
          guess--;
          long previous = guessRemainder;
          guessRemainder += normalizedHigh;
          remainderOverflow =
              Long.compareUnsigned(guessRemainder, previous) < 0;
        }
        if (subtractProduct(index, normalizedLow, normalizedHigh, guess)) {
          guess--;
          addBack(index, normalizedLow, normalizedHigh);
        }
        if (index == 1) {
          quotientHigh = guess;
        } else if (index == 0) {
          quotientLow = guess;
        } else if (guess != 0L) {
          throw new ArithmeticException("decimal128 quotient does not fit");
        }
      }
      if (shift == 0) {
        remainderHigh = limb1;
        remainderLow = limb0;
      } else {
        remainderHigh = limb1 >>> shift;
        remainderLow = limb0 >>> shift | limb1 << (64 - shift);
      }
    }

    private int divideQuotientByTen() {
      long dividend = quotientHigh >>> 32;
      long nextHigh = dividend / 10L << 32;
      long remainder = dividend % 10L;
      dividend = remainder << 32 | quotientHigh & 0xffff_ffffL;
      nextHigh |= dividend / 10L;
      remainder = dividend % 10L;
      dividend = remainder << 32 | quotientLow >>> 32;
      long nextLow = dividend / 10L << 32;
      remainder = dividend % 10L;
      dividend = remainder << 32 | quotientLow & 0xffff_ffffL;
      nextLow |= dividend / 10L;
      quotientHigh = nextHigh;
      quotientLow = nextLow;
      return (int) (dividend % 10L);
    }

    private void restoreQuotientDigit(int digit) {
      multiplyQuotientByTen();
      quotientLow += digit;
      if (Long.compareUnsigned(quotientLow, digit) < 0) {
        quotientHigh++;
      }
    }

    private void multiplyQuotientByTen() {
      quotientHigh = quotientHigh * 10L
          + UInt128.unsignedMultiplyHigh(quotientLow, 10L);
      quotientLow *= 10L;
    }

    private void incrementQuotient() {
      quotientLow++;
      if (quotientLow == 0L) {
        quotientHigh++;
      }
    }

    private void discardQuotientDigits(int count, boolean initialSticky) {
      sticky = initialSticky;
      roundDigit = 0;
      for (int i = 0; i < count; i++) {
        if (i > 0 && roundDigit != 0) {
          sticky = true;
        }
        roundDigit = divideQuotientByTen();
        if ((quotientHigh | quotientLow) == 0L && i + 1 < count) {
          sticky |= roundDigit != 0;
          roundDigit = 0;
          break;
        }
      }
      discardedInexact = roundDigit != 0 || sticky;
    }

    private int significantLength() {
      if (limb3 != 0L) {
        return 4;
      }
      if (limb2 != 0L) {
        return 3;
      }
      return limb1 != 0L ? 2 : 1;
    }

    private void normalize(int shift) {
      if (shift == 0) {
        return;
      }
      int inverse = 64 - shift;
      limb4 = limb3 >>> inverse;
      limb3 = limb3 << shift | limb2 >>> inverse;
      limb2 = limb2 << shift | limb1 >>> inverse;
      limb1 = limb1 << shift | limb0 >>> inverse;
      limb0 <<= shift;
    }

    private boolean subtractProduct(
        int offset, long divisorLow, long divisorHigh, long factor) {
      long carry = 0L;
      long borrow = 0L;
      for (int index = 0; index < 2; index++) {
        long divisor = index == 0 ? divisorLow : divisorHigh;
        long low = factor * divisor;
        long high = UInt128.unsignedMultiplyHigh(factor, divisor);
        long product = low + carry;
        high += carry(low, product);
        long value = limb(offset + index);
        long difference = value - product;
        long nextBorrow = borrow(value, product);
        long withBorrow = difference - borrow;
        nextBorrow |= borrow(difference, borrow);
        setLimb(offset + index, withBorrow);
        carry = high;
        borrow = nextBorrow;
      }
      long value = limb(offset + 2);
      long difference = value - carry;
      long finalBorrow = borrow(value, carry);
      long withBorrow = difference - borrow;
      finalBorrow |= borrow(difference, borrow);
      setLimb(offset + 2, withBorrow);
      return finalBorrow != 0L;
    }

    private void addBack(int offset, long divisorLow, long divisorHigh) {
      long carry = 0L;
      for (int index = 0; index < 2; index++) {
        long divisor = index == 0 ? divisorLow : divisorHigh;
        long value = limb(offset + index);
        long sum = value + divisor;
        long nextCarry = carry(value, sum);
        long withCarry = sum + carry;
        nextCarry |= carry(sum, withCarry);
        setLimb(offset + index, withCarry);
        carry = nextCarry;
      }
      setLimb(offset + 2, limb(offset + 2) + carry);
    }

    private long limb(int index) {
      return switch (index) {
        case 0 -> limb0;
        case 1 -> limb1;
        case 2 -> limb2;
        case 3 -> limb3;
        case 4 -> limb4;
        default -> 0L;
      };
    }

    private void setLimb(int index, long value) {
      switch (index) {
        case 0:
          limb0 = value;
          break;
        case 1:
          limb1 = value;
          break;
        case 2:
          limb2 = value;
          break;
        case 3:
          limb3 = value;
          break;
        case 4:
          limb4 = value;
          break;
        default:
          throw new IllegalStateException(String.valueOf(index));
      }
    }

    private static long divide128By64(long high, long low, long divisor) {
      int shift = Long.numberOfLeadingZeros(divisor);
      long normalizedDivisor = divisor << shift;
      long normalizedHigh = shift == 0
          ? high
          : high << shift | low >>> (64 - shift);
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
              quotientHigh * divisorLow,
              remainder << 32 | dividendMiddle) > 0) {
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
              quotientLow * divisorLow,
              remainder << 32 | dividendLow) > 0) {
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
      long productHigh = UInt128.unsignedMultiplyHigh(left, right);
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

    private static int compare(
        long high, long low, long otherHigh, long otherLow) {
      int comparison = Long.compareUnsigned(high, otherHigh);
      return comparison != 0 ? comparison : Long.compareUnsigned(low, otherLow);
    }
  }

}
