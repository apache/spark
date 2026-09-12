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

/** Intel {@code bid*_quantize} plus scale/logb helpers. */
final class BidQuantize {
  private BidQuantize() {
  }

  static long quantize64(long x, long y, RoundingMode mode, StatusFlags flags) {
    if (Bid64Raw.isNaN(x) || Bid64Raw.isNaN(y)) {
      if (Bid64Raw.isSignalingNaN(x) || Bid64Raw.isSignalingNaN(y)) {
        flags.raise(StatusFlags.INVALID);
      }
      long nan = Bid64Raw.isNaN(x) ? x : y;
      return BidIntegral.canonicalizeNaN64(nan, new StatusFlags());
    }
    if (Bid64Raw.isInf(x) || Bid64Raw.isInf(y)) {
      if (Bid64Raw.isInf(x) && Bid64Raw.isInf(y)) {
        return (x & Bid64.MASK_SIGN) | Bid64.MASK_INFINITY;
      }
      flags.raise(StatusFlags.INVALID);
      return Bid64.MASK_NAN;
    }
    int targetExp = Bid64.biasedExponentBits(y) - 398;
    long coeff = Bid64.significandBits(x);
    int exp = Bid64.biasedExponentBits(x) - 398;
    boolean negative = Bid64Raw.isSigned(x);
    if (coeff == 0L) {
      int biased = targetExp + 398;
      if (biased < 0 || biased > 767) {
        flags.raise(StatusFlags.INVALID);
        return Bid64.MASK_NAN;
      }
      return Bid64.finiteRawBits(negative, biased, 0L);
    }
    int digits = PowersOfTen.decimalDigits(coeff);
    int total = digits + (exp - targetExp);
    if (total > 16) {
      flags.raise(StatusFlags.INVALID);
      return Bid64.MASK_NAN;
    }
    DecNum number = DecNum.ofCoefficient(negative, coeff, exp);
    int shift = exp - targetExp;
    if (shift >= 0) {
      number.multiplyPow10(shift);
      number.shiftExp(-shift);
    } else {
      boolean[] sticky = {false};
      int first = number.dividePow10(-shift, sticky);
      long kept = number.low64();
      if (BidRound.shouldIncrement(negative, kept, first, sticky[0], mode)) {
        number.addOne();
      }
      if (first != 0 || sticky[0]) {
        flags.raise(StatusFlags.INEXACT);
      }
    }
    if (number.digitCount() > 16 && !number.isZero()) {
      flags.raise(StatusFlags.INVALID);
      return Bid64.MASK_NAN;
    }
    int biased = targetExp + 398;
    if (biased < 0 || biased > 767) {
      flags.raise(StatusFlags.INVALID);
      return Bid64.MASK_NAN;
    }
    return Bid64.finiteRawBits(negative, biased, number.toUInt128().low());
  }

  static void quantize128(
      long xHigh,
      long xLow,
      long yHigh,
      long yLow,
      RoundingMode mode,
      StatusFlags flags,
      long[] payloadOut) {
    Bid128 x = Bid128.fromRawBits(xHigh, xLow);
    Bid128 y = Bid128.fromRawBits(yHigh, yLow);
    if (x.isNaN() || y.isNaN()) {
      if (x.isSignalingNaN() || y.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      Bid128 nan = x.isNaN() ? x : y;
      BidIntegral.canonicalizeNaN128(
          nan.highBits(), nan.lowBits(), new StatusFlags(), payloadOut);
      return;
    }
    if (x.isInfinite() || y.isInfinite()) {
      if (x.isInfinite() && y.isInfinite()) {
        payloadOut[0] = (xHigh & Bid128.MASK_SIGN) | Bid128.MASK_INFINITY;
        payloadOut[1] = 0L;
        return;
      }
      flags.raise(StatusFlags.INVALID);
      DecNum.store128(Bid128.QUIET_NAN, payloadOut);
      return;
    }
    int targetExp = y.biasedExponent() - 6176;
    UInt128 coeff = x.coefficient();
    int exp = x.biasedExponent() - 6176;
    boolean negative = x.isSigned();
    if (coeff.isZero()) {
      int biased = targetExp + 6176;
      if (biased < 0 || biased > 12_287) {
        flags.raise(StatusFlags.INVALID);
        DecNum.store128(Bid128.QUIET_NAN, payloadOut);
        return;
      }
      DecNum.store128(Bid128.finite(negative, biased, 0L, 0L), payloadOut);
      return;
    }
    int digits = PowersOfTen.decimalDigits(coeff);
    int total = digits + (exp - targetExp);
    if (total > 34) {
      flags.raise(StatusFlags.INVALID);
      DecNum.store128(Bid128.QUIET_NAN, payloadOut);
      return;
    }
    DecNum number = DecNum.ofUnsigned(coeff.high(), coeff.low());
    if (negative) {
      number.setNegative();
    }
    number.shiftExp(exp);
    int shift = exp - targetExp;
    if (shift >= 0) {
      number.multiplyPow10(shift);
      number.shiftExp(-shift);
    } else {
      boolean[] sticky = {false};
      int first = number.dividePow10(-shift, sticky);
      if (BidRound.shouldIncrement(negative, number.low64(), first, sticky[0], mode)) {
        number.addOne();
      }
      if (first != 0 || sticky[0]) {
        flags.raise(StatusFlags.INEXACT);
      }
    }
    if (number.digitCount() > 34) {
      flags.raise(StatusFlags.INVALID);
      DecNum.store128(Bid128.QUIET_NAN, payloadOut);
      return;
    }
    int biased = targetExp + 6176;
    UInt128 rounded = number.toUInt128();
    DecNum.store128(Bid128.finite(negative, biased, rounded.high(), rounded.low()), payloadOut);
  }
}
