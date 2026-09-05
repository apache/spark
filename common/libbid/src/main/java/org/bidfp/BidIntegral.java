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

/** Round-to-integral kernels for BID64 and BID128. */
final class BidIntegral {
  private static final long[][] POW10_128 = powersOfTen128();

  private BidIntegral() {
  }

  static long round64(long x, RoundingMode mode, StatusFlags flags, boolean exact) {
    if (Bid64Raw.isNaN(x)) {
      return canonicalizeNaN64(x, flags);
    }
    if (Bid64Raw.isInf(x)) {
      return (x & Bid64.MASK_SIGN) | Bid64.MASK_INFINITY;
    }
    long coeff = Bid64.significandBits(x);
    int exp = Bid64.biasedExponentBits(x) - 398;
    boolean negative = Bid64Raw.isSigned(x);
    if (coeff == 0L) {
      int biased = Math.max(exp, 0) + 398;
      return Bid64.finiteRawBits(negative, biased, 0L);
    }
    if (exp >= 0) {
      return Bid64.finiteRawBits(negative, exp + 398, coeff);
    }
    int places = -exp;
    long kept;
    int first;
    boolean sticky;
    if (places < PowersOfTen.LONG.length) {
      long divisor = PowersOfTen.LONG[places];
      kept = coeff / divisor;
      long remainder = coeff - kept * divisor;
      long firstDivisor = PowersOfTen.LONG[places - 1];
      first = (int) (remainder / firstDivisor);
      sticky = remainder - first * firstDivisor != 0;
    } else {
      kept = 0L;
      first = 0;
      sticky = true;
    }
    boolean inexact = first != 0 || sticky;
    if (BidRound.shouldIncrement(negative, kept, first, sticky, mode)) {
      kept++;
    }
    if (exact && inexact) {
      flags.raise(StatusFlags.INEXACT);
    }
    return Bid64.finiteRawBits(negative, 398, kept);
  }

  static long canonicalizeNaN64(long x, StatusFlags flags) {
    long payload = x & 0x0003_ffff_ffff_ffffL;
    if (payload > 999_999_999_999_999L) {
      x = x & 0xfe00_0000_0000_0000L;
    } else {
      x = x & 0xfe03_ffff_ffff_ffffL;
    }
    if ((x & Bid64.MASK_SIGNALING_NAN) == Bid64.MASK_SIGNALING_NAN) {
      flags.raise(StatusFlags.INVALID);
      x = x & 0xfdff_ffff_ffff_ffffL;
    }
    return x;
  }

  static void round128(
      long high,
      long low,
      RoundingMode mode,
      StatusFlags flags,
      boolean exact,
      long[] payloadOut) {
    if ((high & Bid128.MASK_NAN) == Bid128.MASK_NAN) {
      canonicalizeNaN128(high, low, flags, payloadOut);
      return;
    }
    if ((high & Bid128.MASK_INFINITY) == Bid128.MASK_INFINITY) {
      payloadOut[0] = (high & Bid128.MASK_SIGN) | Bid128.MASK_INFINITY;
      payloadOut[1] = 0L;
      return;
    }
    boolean negative = (high & Bid128.MASK_SIGN) != 0L;
    int biased = (high & Bid128.MASK_STEERING_BITS) == Bid128.MASK_STEERING_BITS
        ? (int) ((high >>> 47) & 0x3fffL)
        : (int) ((high & Bid128.MASK_EXPONENT) >>> 49);
    long coeffHigh = high & Bid128.MASK_COEFFICIENT;
    long coeffLow = low;
    if (!Bid128.isCanonicalFinite(high, low)) {
      coeffHigh = 0L;
      coeffLow = 0L;
    }
    int exp = biased - 6176;
    if ((coeffHigh | coeffLow) == 0L) {
      int resultBiased = Math.max(exp, 0) + 6176;
      payloadOut[0] = (negative ? Bid128.MASK_SIGN : 0L)
          | ((long) resultBiased << 49);
      payloadOut[1] = 0L;
      return;
    }
    if (exp >= 0) {
      payloadOut[0] = high;
      payloadOut[1] = low;
      return;
    }
    int places = -exp;
    if (roundSmall128(
        coeffHigh, coeffLow, places, negative, mode, flags, exact, payloadOut)) {
      return;
    }
    Bid128 value = Bid128.fromRawBits(high, low);
    UInt128 coeff = value.coefficient();
    DecNum number = DecNum.ofUnsigned(coeff.high(), coeff.low());
    if (negative) {
      number.setNegative();
    }
    boolean[] sticky = {false};
    int first = number.dividePow10(-exp, sticky);
    boolean inexact = first != 0 || sticky[0];
    long keptLow = number.low64();
    if (BidRound.shouldIncrement(negative, keptLow, first, sticky[0], mode)) {
      number.addOne();
    }
    if (exact && inexact) {
      flags.raise(StatusFlags.INEXACT);
    }
    UInt128 rounded = number.toUInt128();
    DecNum.store128(
        Bid128.finite(negative, 6176, rounded.high(), rounded.low()),
        payloadOut);
  }

  private static boolean roundSmall128(
      long coefficientHigh,
      long coefficientLow,
      int places,
      boolean negative,
      RoundingMode mode,
      StatusFlags flags,
      boolean exact,
      long[] out) {
    if (places > 34) {
      storeRoundedSmall128(negative, 0L, 0, true, mode, flags, exact, out);
      return true;
    }
    if (places < 34
        && compare128(
            coefficientHigh,
            coefficientLow,
            POW10_128[places + 1][0],
            POW10_128[places + 1][1]) >= 0) {
      return false;
    }

    long remainderHigh = coefficientHigh;
    long remainderLow = coefficientLow;
    long divisorHigh = POW10_128[places][0];
    long divisorLow = POW10_128[places][1];
    long kept = 0L;
    while (compare128(remainderHigh, remainderLow, divisorHigh, divisorLow) >= 0) {
      long nextLow = remainderLow - divisorLow;
      long borrow = Long.compareUnsigned(remainderLow, divisorLow) < 0 ? 1L : 0L;
      remainderHigh = remainderHigh - divisorHigh - borrow;
      remainderLow = nextLow;
      kept++;
    }

    long firstHigh = POW10_128[places - 1][0];
    long firstLow = POW10_128[places - 1][1];
    int first = 0;
    while (compare128(remainderHigh, remainderLow, firstHigh, firstLow) >= 0) {
      long nextLow = remainderLow - firstLow;
      long borrow = Long.compareUnsigned(remainderLow, firstLow) < 0 ? 1L : 0L;
      remainderHigh = remainderHigh - firstHigh - borrow;
      remainderLow = nextLow;
      first++;
    }
    boolean sticky = (remainderHigh | remainderLow) != 0L;
    storeRoundedSmall128(negative, kept, first, sticky, mode, flags, exact, out);
    return true;
  }

  private static void storeRoundedSmall128(
      boolean negative,
      long kept,
      int first,
      boolean sticky,
      RoundingMode mode,
      StatusFlags flags,
      boolean exact,
      long[] out) {
    boolean inexact = first != 0 || sticky;
    if (BidRound.shouldIncrement(negative, kept, first, sticky, mode)) {
      kept++;
    }
    if (exact && inexact) {
      flags.raise(StatusFlags.INEXACT);
    }
    out[0] = (negative ? Bid128.MASK_SIGN : 0L) | (6176L << 49);
    out[1] = kept;
  }

  private static int compare128(
      long high, long low, long otherHigh, long otherLow) {
    int highComparison = Long.compareUnsigned(high, otherHigh);
    return highComparison != 0
        ? highComparison
        : Long.compareUnsigned(low, otherLow);
  }

  private static long[][] powersOfTen128() {
    long[][] powers = new long[35][2];
    for (int i = 0; i < powers.length; i++) {
      UInt128 value = PowersOfTen.pow10(i);
      powers[i][0] = value.high();
      powers[i][1] = value.low();
    }
    return powers;
  }

  static void canonicalizeNaN128(
      long high, long low, StatusFlags flags, long[] payloadOut) {
    UInt128 payload = new UInt128(high & 0x0000_3fff_ffff_ffffL, low);
    long canonicalHigh = high & 0xfc00_0000_0000_0000L;
    long canonicalLow = 0L;
    if (payload.compareTo(PowersOfTen.MAX_33) <= 0) {
      canonicalHigh |= payload.high();
      canonicalLow = payload.low();
    }
    if (Bid128.fromRawBits(high, low).isSignalingNaN()) {
      flags.raise(StatusFlags.INVALID);
      canonicalHigh &= ~0x0200_0000_0000_0000L;
    }
    payloadOut[0] = canonicalHigh;
    payloadOut[1] = canonicalLow;
  }
}
