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

final class BidScale {
  private BidScale() {
  }

  static long frexp64(long x, int[] exponentOut, StatusFlags flags) {
    exponentOut[0] = 0;
    if (Bid64Raw.isNaN(x)) {
      return BidIntegral.canonicalizeNaN64(x, new StatusFlags());
    }
    if (Bid64Raw.isInf(x)) {
      return (x & Bid64.MASK_SIGN) | Bid64.MASK_INFINITY;
    }
    int exponent = Bid64.biasedExponentBits(x);
    if (Bid64Raw.isZero(x)) {
      return Bid64.finiteRawBits(Bid64Raw.isSigned(x), exponent, 0L);
    }
    long coefficient = Bid64.significandBits(x);
    int digits = PowersOfTen.decimalDigits(coefficient);
    exponentOut[0] = exponent - 398 + digits;
    return Bid64.finiteRawBits(Bid64Raw.isSigned(x), 398 - digits, coefficient);
  }

  static void frexp128(
      long hi, long lo, int[] exponentOut, StatusFlags flags, long[] out) {
    exponentOut[0] = 0;
    Bid128 value = Bid128.fromRawBits(hi, lo);
    if (value.isNaN()) {
      BidIntegral.canonicalizeNaN128(hi, lo, new StatusFlags(), out);
      return;
    }
    if (value.isInfinite()) {
      DecNum.store128(
          value.isSigned() ? Bid128.NEGATIVE_INFINITY : Bid128.POSITIVE_INFINITY, out);
      return;
    }
    if (value.isZero()) {
      DecNum.store128(Bid128.finite(value.isSigned(), value.biasedExponent(), 0L, 0L), out);
      return;
    }
    UInt128 coefficient = value.coefficient();
    int digits = PowersOfTen.decimalDigits(coefficient);
    exponentOut[0] = value.biasedExponent() - 6176 + digits;
    DecNum.store128(
        Bid128.finite(
            value.isSigned(), 6176 - digits, coefficient.high(), coefficient.low()),
        out);
  }

  static long scalbn64(long x, int n, RoundingMode mode, StatusFlags flags) {
    if (Bid64Raw.isNaN(x)) {
      return BidIntegral.canonicalizeNaN64(x, flags);
    }
    if (Bid64Raw.isInf(x)) {
      return (x & Bid64.MASK_SIGN) | Bid64.MASK_INFINITY;
    }
    long coeff = Bid64.significandBits(x);
    int biased = Bid64.biasedExponentBits(x);
    long scaledBiased = (long) biased + n;
    if (scaledBiased >= 0 && scaledBiased <= 767) {
      return Bid64.finiteRawBits(
          Bid64Raw.isSigned(x), (int) scaledBiased, coeff);
    }
    int scaledExp = clamp(scaledBiased - 398, -1_000, 1_000);
    DecNum number = DecNum.ofCoefficient(Bid64Raw.isSigned(x), coeff, scaledExp);
    return number.packBid64(mode, flags);
  }

  static void scalbn128(
      long high, long low, int n, RoundingMode mode, StatusFlags flags, long[] out) {
    if ((high & Bid128.MASK_NAN) == Bid128.MASK_NAN) {
      BidIntegral.canonicalizeNaN128(high, low, flags, out);
      return;
    }
    if ((high & Bid128.MASK_INFINITY) == Bid128.MASK_INFINITY) {
      out[0] = (high & Bid128.MASK_SIGN) | Bid128.MASK_INFINITY;
      out[1] = 0L;
      return;
    }
    int biased = (int) ((high & Bid128.MASK_EXPONENT) >>> 49);
    long scaledBiased = (long) biased + n;
    if (scaledBiased >= 0
        && scaledBiased <= 12_287
        && Bid128.isCanonicalFinite(high, low)) {
      out[0] = (high & (Bid128.MASK_SIGN | Bid128.MASK_COEFFICIENT))
          | (scaledBiased << 49);
      out[1] = low;
      return;
    }
    Bid128 value = Bid128.fromRawBits(high, low);
    UInt128 coeff = value.coefficient();
    int exp = clamp((long) value.biasedExponent() - 6176 + n, -13_000, 13_000);
    DecNum number = DecNum.ofUnsigned(coeff.high(), coeff.low());
    if (value.isSigned()) {
      number.setNegative();
    }
    number.shiftExp(exp);
    number.packBid128(mode, flags, out);
  }

  static int ilogb64(long x, StatusFlags flags) {
    if (!Bid64Raw.isFinite(x) || Bid64Raw.isZero(x)) {
      flags.raise(StatusFlags.INVALID);
      return Bid64Raw.isInf(x) ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    }
    long coeff = Bid64.significandBits(x);
    int exp = Bid64.biasedExponentBits(x) - 398;
    return exp + PowersOfTen.decimalDigits(coeff) - 1;
  }

  static int ilogb128(long high, long low, StatusFlags flags) {
    Bid128 value = Bid128.fromRawBits(high, low);
    if (!value.isFinite() || value.isZero()) {
      flags.raise(StatusFlags.INVALID);
      return value.isInfinite() ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    }
    return value.biasedExponent() - 6176
        + PowersOfTen.decimalDigits(value.coefficient()) - 1;
  }

  static long logb64(long x, StatusFlags flags) {
    if (Bid64Raw.isNaN(x)) {
      return BidIntegral.canonicalizeNaN64(x, flags);
    }
    if (Bid64Raw.isInf(x)) {
      return Bid64.POSITIVE_INFINITY.toRawBits();
    }
    if (Bid64Raw.isZero(x)) {
      flags.raise(StatusFlags.DIVIDE_BY_ZERO);
      return Bid64.NEGATIVE_INFINITY.toRawBits();
    }
    return Bid64Raw.fromInt32(ilogb64(x, new StatusFlags()));
  }

  static void logb128(long high, long low, StatusFlags flags, long[] out) {
    Bid128 value = Bid128.fromRawBits(high, low);
    if (value.isNaN()) {
      BidIntegral.canonicalizeNaN128(high, low, flags, out);
      return;
    }
    if (value.isInfinite()) {
      DecNum.store128(Bid128.POSITIVE_INFINITY, out);
      return;
    }
    if (value.isZero()) {
      flags.raise(StatusFlags.DIVIDE_BY_ZERO);
      DecNum.store128(Bid128.NEGATIVE_INFINITY, out);
      return;
    }
    BidConvert.fromInt64To128(
        ilogb128(high, low, new StatusFlags()),
        RoundingMode.TIES_TO_EVEN,
        new StatusFlags(),
        out);
  }

  static int quantexp64(long x) {
    if (!Bid64Raw.isFinite(x)) {
      return Integer.MIN_VALUE;
    }
    return Bid64.biasedExponentBits(x) - 398;
  }

  static int quantexp128(long high, long low) {
    Bid128 value = Bid128.fromRawBits(high, low);
    if (!value.isFinite()) {
      return Integer.MIN_VALUE;
    }
    return value.biasedExponent() - 6176;
  }

  static long quantum64(long x) {
    if (Bid64Raw.isInf(x)) {
      return (x & Bid64.MASK_SIGN) | Bid64.MASK_INFINITY;
    }
    if (Bid64Raw.isNaN(x)) {
      return Bid64.QUIET_NAN.toRawBits();
    }
    int exp = Bid64.biasedExponentBits(x);
    return Bid64.finiteRawBits(false, exp, 1L);
  }

  static void quantum128(long high, long low, long[] out) {
    Bid128 value = Bid128.fromRawBits(high, low);
    if (value.isInfinite()) {
      DecNum.store128(value.isSigned() ? Bid128.NEGATIVE_INFINITY : Bid128.POSITIVE_INFINITY, out);
      return;
    }
    if (value.isNaN()) {
      DecNum.store128(Bid128.QUIET_NAN, out);
      return;
    }
    DecNum.store128(Bid128.finite(false, value.biasedExponent(), 0L, 1L), out);
  }

  private static int clamp(long value, int minimum, int maximum) {
    if (value < minimum) {
      return minimum;
    }
    if (value > maximum) {
      return maximum;
    }
    return (int) value;
  }
}
