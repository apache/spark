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

import java.math.BigInteger;

import org.bidfp.binary128.Binary128;

/**
 * BID64/BID128 &lt;-&gt; binary128 conversion. Finite values use
 * {@link DecNum} / integer remainder rounding (no Intel coefficient tables).
 * NaN payloads follow Intel {@code bid_binarydecimal.c} macros.
 */
final class BidBinary128Convert {
  private static final BigInteger TWO_112 = BigInteger.ONE.shiftLeft(112);
  private static final BigInteger TWO_113 = BigInteger.ONE.shiftLeft(113);
  private static final BigInteger MASK64 =
      BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
  private static final long BID64_NAN_PAYLOAD_MAX = 999_999_999_999_999L;
  private static final long BID64_NAN_COEFF_MASK = 0x0003_ffff_ffff_ffffL;

  private BidBinary128Convert() {
  }

  static void toBinary128From64(
      long x, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid64Raw.isNaN(x)) {
      if (Bid64Raw.isSignalingNaN(x)) {
        flags.raise(StatusFlags.INVALID);
      }
      long payload = x & BID64_NAN_COEFF_MASK;
      if (payload > BID64_NAN_PAYLOAD_MAX) {
        payload = 0L;
      }
      long cHi = payload << 14;
      storeBinary128Nan((x & Bid64.MASK_SIGN) != 0L, cHi, 0L, out);
      return;
    }
    if (Bid64Raw.isInf(x)) {
      Binary128 inf = Bid64Raw.isSigned(x)
          ? Binary128.NEGATIVE_INFINITY : Binary128.POSITIVE_INFINITY;
      store(inf, out);
      return;
    }
    if (Bid64Raw.isZero(x)) {
      store(Bid64Raw.isSigned(x) ? Binary128.NEGATIVE_ZERO : Binary128.ZERO, out);
      return;
    }
    DecNum number = DecNum.ofCoefficient(
        Bid64Raw.isSigned(x),
        Bid64.significandBits(x),
        Bid64.biasedExponentBits(x) - 398);
    packBinary128(number, mode, flags, out);
  }

  static void toBinary128From128(
      long high, long low, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128 value = Bid128.fromRawBits(high, low);
    if (value.isNaN()) {
      if (value.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      UInt128 payload = new UInt128(high & 0x0000_3fff_ffff_ffffL, low);
      long cHi;
      long cLo;
      if (payload.compareTo(PowersOfTen.MAX_33) > 0) {
        cHi = 0L;
        cLo = 0L;
      } else {
        cHi = (high << 18) + (low >>> 46);
        cLo = low << 18;
      }
      storeBinary128Nan(value.isSigned(), cHi, cLo, out);
      return;
    }
    if (value.isInfinite()) {
      store(value.isSigned() ? Binary128.NEGATIVE_INFINITY : Binary128.POSITIVE_INFINITY,
          out);
      return;
    }
    if (value.isZero()) {
      store(value.isSigned() ? Binary128.NEGATIVE_ZERO : Binary128.ZERO, out);
      return;
    }
    UInt128 coeff = value.coefficient();
    DecNum number = DecNum.ofUnsigned(coeff.high(), coeff.low());
    if (value.isSigned()) {
      number.setNegative();
    }
    number.shiftExp(value.biasedExponent() - 6176);
    packBinary128(number, mode, flags, out);
  }

  static long fromBinary128To64(
      long high, long low, RoundingMode mode, StatusFlags flags) {
    Binary128 x = Binary128.fromRawBits(high, low);
    if (x.isNaN()) {
      if (x.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      long cHi = (x.fractionHigh() << 17) + (x.fractionLow() >>> 47);
      long payload = cHi >>> 14;
      if (payload > BID64_NAN_PAYLOAD_MAX) {
        payload = 0L;
      }
      return (high & Bid64.MASK_SIGN) | Bid64.MASK_NAN | payload;
    }
    if (x.isInfinite()) {
      return (x.isSigned() ? Bid64.MASK_SIGN : 0L) | Bid64.MASK_INFINITY;
    }
    if (x.isZero()) {
      return Bid64.finiteRawBits(x.isSigned(), 398, 0L);
    }
    if (x.isSubnormal()) {
      flags.raise(StatusFlags.DENORMAL);
    }
    return fromBinary(mantissa(x), exp2(x), x.isSigned()).packBid64(mode, flags);
  }

  static void fromBinary128To128(
      long high, long low, RoundingMode mode, StatusFlags flags, long[] out) {
    Binary128 x = Binary128.fromRawBits(high, low);
    if (x.isNaN()) {
      if (x.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      long cHi = (x.fractionHigh() << 17) + (x.fractionLow() >>> 47);
      long cLo = x.fractionLow() << 17;
      UInt128 payload = new UInt128(cHi >>> 18, (cLo >>> 18) + (cHi << 46));
      if (payload.compareTo(PowersOfTen.MAX_33) > 0) {
        payload = UInt128.ZERO;
      }
      out[0] = (high & Bid128.MASK_SIGN) | Bid128.MASK_NAN | payload.high();
      out[1] = payload.low();
      return;
    }
    if (x.isInfinite()) {
      DecNum.store128(
          x.isSigned() ? Bid128.NEGATIVE_INFINITY : Bid128.POSITIVE_INFINITY, out);
      return;
    }
    if (x.isZero()) {
      DecNum.store128(Bid128.finite(x.isSigned(), 6176, 0L, 0L), out);
      return;
    }
    if (x.isSubnormal()) {
      flags.raise(StatusFlags.DENORMAL);
    }
    fromBinary(mantissa(x), exp2(x), x.isSigned()).packBid128(mode, flags, out);
  }

  /**
   * Split a finite BID128 into high+low binary128 parts that sum to the
   * input (Dekker-style remainder after converting the high part).
   */
  static void toBinary128TwoPart(
      long high, long low, long[] highOut, long[] lowOut) {
    Bid128 value = Bid128.fromRawBits(high, low);
    StatusFlags discard = new StatusFlags();
    toBinary128From128(high, low, RoundingMode.TIES_TO_EVEN, discard, highOut);
    if (!value.isFinite() || value.isZero() || value.isNaN()) {
      lowOut[0] = 0L;
      lowOut[1] = 0L;
      return;
    }
    Binary128 highPart = Binary128.fromRawBits(highOut[0], highOut[1]);
    DecNum exact = ofBid128(value);
    DecNum approx = fromBinary(mantissa(highPart), exp2(highPart), highPart.isSigned());
    packBinary128(signedSubtract(exact, approx), RoundingMode.TIES_TO_EVEN, discard, lowOut);
  }

  private static DecNum ofBid128(Bid128 value) {
    UInt128 coeff = value.coefficient();
    DecNum number = DecNum.ofUnsigned(coeff.high(), coeff.low());
    number.setNegative(value.isSigned());
    number.shiftExp(value.biasedExponent() - 6176);
    return number;
  }

  private static DecNum signedSubtract(DecNum left, DecNum right) {
    DecNum result = new DecNum();
    result.copyFrom(left);
    int comparison = left.compareAbsolute(right);
    if (left.isNegative() == right.isNegative()) {
      if (comparison >= 0) {
        result.subtractAbsolute(right);
        result.setNegative(left.isNegative() && !result.isZero());
      } else {
        result.copyFrom(right);
        result.subtractAbsolute(left);
        result.setNegative(!left.isNegative() && !result.isZero());
      }
    } else {
      result.addAbsolute(right);
      result.setNegative(left.isNegative());
    }
    return result;
  }

  static DecNum fromBinary(UInt128 mantissa, int exp2, boolean sign) {
    DecNum number = DecNum.ofUnsigned(mantissa.high(), mantissa.low());
    if (sign) {
      number.setNegative();
    }
    if (exp2 >= 0) {
      number.multiplyPow2(exp2);
    } else {
      int n = -exp2;
      number.multiplyPow5(n);
      number.shiftExp(-n);
    }
    number.stripTrailingZeros(0);
    return number;
  }

  private static void packBinary128(
      DecNum number, RoundingMode mode, StatusFlags flags, long[] out) {
    if (number.isZero()) {
      store(number.isNegative() ? Binary128.NEGATIVE_ZERO : Binary128.ZERO, out);
      return;
    }
    BigInteger significand = number.toBigIntegerAbsolute();
    int exp10 = number.exp();
    BigInteger numerator;
    BigInteger denominator;
    if (exp10 >= 0) {
      numerator = significand.multiply(BigInteger.TEN.pow(exp10));
      denominator = BigInteger.ONE;
    } else {
      numerator = significand;
      denominator = BigInteger.TEN.pow(-exp10);
    }
    store(roundBinary128(number.isNegative(), numerator, denominator, mode, flags),
        out);
  }

  private static UInt128 mantissa(Binary128 x) {
    int biased = x.biasedExponent();
    long fracHi = x.fractionHigh();
    long fracLo = x.fractionLow();
    if (biased != 0) {
      fracHi |= 1L << 48;
    }
    return new UInt128(fracHi, fracLo);
  }

  /** Unbiased exponent of the integer significand {@link #mantissa}. */
  private static int exp2(Binary128 x) {
    int biased = x.biasedExponent();
    return biased == 0 ? -16494 : biased - 16495;
  }

  private static void storeBinary128Nan(
      boolean sign, long cHi, long cLo, long[] out) {
    long fracHi = (cHi >>> 17) + (1L << 47);
    long fracLo = (cLo >>> 17) + (cHi << 47);
    store(Binary128.fromFields(sign, 0x7fff, fracHi, fracLo), out);
  }

  private static void store(Binary128 value, long[] out) {
    out[0] = value.highBits();
    out[1] = value.lowBits();
  }

  /**
   * Round {@code +/- numerator/denominator} to binary128. Tininess is detected
   * after rounding (IEEE default, matching Intel binary convert).
   */
  private static Binary128 roundBinary128(
      boolean negative,
      BigInteger numerator,
      BigInteger denominator,
      RoundingMode mode,
      StatusFlags status) {
    if (numerator.signum() == 0) {
      return negative ? Binary128.NEGATIVE_ZERO : Binary128.ZERO;
    }
    int topExponent = floorLog2(numerator, denominator, 0);
    if (topExponent > 16383) {
      return overflow128(negative, mode, status);
    }
    boolean tinyBefore = topExponent < -16382;
    Rounded rounded;
    int biased;
    if (topExponent >= -16382) {
      rounded = quotient(
          numerator, denominator, -(topExponent - 112), negative, mode);
      if (rounded.value.equals(TWO_113)) {
        rounded = new Rounded(TWO_112, rounded.inexact);
        topExponent++;
      }
      if (topExponent > 16383) {
        return overflow128(negative, mode, status);
      }
      biased = topExponent + Binary128.BIAS;
    } else {
      rounded = quotient(numerator, denominator, 16494, negative, mode);
      if (rounded.value.compareTo(TWO_112) >= 0) {
        rounded = new Rounded(TWO_112, rounded.inexact);
        biased = 1;
      } else {
        biased = 0;
      }
    }
    if (rounded.inexact) {
      status.raise(StatusFlags.INEXACT);
      if (biased == 0 || tinyBefore) {
        status.raise(StatusFlags.UNDERFLOW);
      }
    }
    if (rounded.value.signum() == 0) {
      return negative ? Binary128.NEGATIVE_ZERO : Binary128.ZERO;
    }
    BigInteger fraction = biased == 0 ? rounded.value : rounded.value.clearBit(112);
    return Binary128.fromFields(
        negative, biased, unsignedShift(fraction, 64), toLong(fraction));
  }

  private static int floorLog2(
      BigInteger numerator, BigInteger denominator, int exponent) {
    int candidate = numerator.bitLength() - denominator.bitLength() + exponent;
    if (compareScaled(numerator, denominator, exponent - candidate) < 0) {
      candidate--;
    }
    return candidate;
  }

  private static int compareScaled(
      BigInteger numerator, BigInteger denominator, int binaryShift) {
    if (binaryShift >= 0) {
      return numerator.shiftLeft(binaryShift).compareTo(denominator);
    }
    return numerator.compareTo(denominator.shiftLeft(-binaryShift));
  }

  private static Rounded quotient(
      BigInteger numerator,
      BigInteger denominator,
      int binaryShift,
      boolean negative,
      RoundingMode mode) {
    BigInteger scaledNumerator = numerator;
    BigInteger scaledDenominator = denominator;
    if (binaryShift >= 0) {
      scaledNumerator = scaledNumerator.shiftLeft(binaryShift);
    } else {
      scaledDenominator = scaledDenominator.shiftLeft(-binaryShift);
    }
    BigInteger[] division = scaledNumerator.divideAndRemainder(scaledDenominator);
    boolean inexact = division[1].signum() != 0;
    if (inexact && increment(
        division[0], division[1], scaledDenominator, negative, mode)) {
      division[0] = division[0].add(BigInteger.ONE);
    }
    return new Rounded(division[0], inexact);
  }

  private static boolean increment(
      BigInteger quotient,
      BigInteger remainder,
      BigInteger denominator,
      boolean negative,
      RoundingMode mode) {
    switch (mode) {
      case TOWARD_ZERO:
        return false;
      case TOWARD_POSITIVE:
        return !negative;
      case TOWARD_NEGATIVE:
        return negative;
      case TIES_AWAY:
        return remainder.shiftLeft(1).compareTo(denominator) >= 0;
      case TIES_TO_EVEN:
        int comparison = remainder.shiftLeft(1).compareTo(denominator);
        return comparison > 0 || (comparison == 0 && quotient.testBit(0));
      default:
        throw new IllegalStateException();
    }
  }

  private static Binary128 overflow128(
      boolean negative, RoundingMode mode, StatusFlags status) {
    status.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
    if (BidRound.overflowToInfinity(negative, mode)) {
      return negative ? Binary128.NEGATIVE_INFINITY : Binary128.POSITIVE_INFINITY;
    }
    return negative ? Binary128.NEGATIVE_MAX : Binary128.POSITIVE_MAX;
  }

  private static long unsignedShift(BigInteger value, int bits) {
    return toLong(value.shiftRight(bits));
  }

  private static long toLong(BigInteger value) {
    return value.and(MASK64).longValue();
  }

  private static final class Rounded {
    final BigInteger value;
    final boolean inexact;

    Rounded(BigInteger value, boolean inexact) {
      this.value = value;
      this.inexact = inexact;
    }
  }
}
