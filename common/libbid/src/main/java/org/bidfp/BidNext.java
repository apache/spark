/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

final class BidNext {
  private static final long MAX_FINITE_64 = Bid64.finiteRawBits(false, 767, PowersOfTen.MAX_16);
  private static final long MIN_POS_64 = Bid64.finiteRawBits(false, 0, 1L);

  private BidNext() {
  }

  static long nextUp64(long x, StatusFlags flags) {
    if (Bid64Raw.isNaN(x)) {
      return BidIntegral.canonicalizeNaN64(x, flags);
    }
    if (Bid64Raw.isInf(x)) {
      return Bid64Raw.isSigned(x) ? (Bid64.MASK_SIGN | MAX_FINITE_64) : Bid64.MASK_INFINITY;
    }
    if (Bid64Raw.isZero(x)) {
      return MIN_POS_64;
    }
    if (x == MAX_FINITE_64) {
      return Bid64.MASK_INFINITY;
    }
    if (x == (Bid64.MASK_SIGN | MIN_POS_64)) {
      return Bid64.MASK_SIGN;
    }
    long coeff = Bid64.significandBits(x);
    int exp = Bid64.biasedExponentBits(x);
    boolean negative = Bid64Raw.isSigned(x);
    int digits = PowersOfTen.decimalDigits(coeff);
    if (digits < 16 && exp > 0) {
      int pad = Math.min(16 - digits, exp);
      coeff *= PowersOfTen.LONG[pad];
      exp -= pad;
    }
    if (!negative) {
      coeff++;
      if (coeff == 10_000_000_000_000_000L) {
        coeff = 1_000_000_000_000_000L;
        exp++;
      }
    } else {
      coeff--;
      if (coeff == 999_999_999_999_999L && exp != 0) {
        coeff = PowersOfTen.MAX_16;
        exp--;
      }
    }
    return Bid64.finiteRawBits(negative, exp, coeff);
  }

  static long nextDown64(long x, StatusFlags flags) {
    return negatePreservingNaN(nextUp64(negatePreservingNaN(x), flags));
  }

  static long nextAfter64(long x, long y, StatusFlags flags) {
    if (Bid64Raw.isNaN(x) || Bid64Raw.isNaN(y)) {
      if (Bid64Raw.isSignalingNaN(x) || Bid64Raw.isSignalingNaN(y)) {
        flags.raise(StatusFlags.INVALID);
      }
      long nan = Bid64Raw.isNaN(x) ? x : y;
      return BidIntegral.canonicalizeNaN64(nan, new StatusFlags());
    }
    if (Bid64Raw.quietEqual(x, y, new StatusFlags())) {
      return Bid64Raw.isZero(x) && Bid64Raw.isZero(y)
          ? Bid64.finiteRawBits(
              Bid64Raw.isSigned(y), Bid64.biasedExponentBits(x), 0L)
          : x;
    }
    long result = Bid64Raw.quietLess(x, y, new StatusFlags())
        ? nextUp64(x, flags)
        : nextDown64(x, flags);
    if (Bid64Raw.isInf(result) && Bid64Raw.isFinite(x)) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
    } else if ((Bid64Raw.isZero(result)
        || Bid64.fromRawBits(result).isSubnormal())) {
      flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
    }
    return result;
  }

  static void nextUp128(long high, long low, StatusFlags flags, long[] out) {
    Bid128 x = Bid128.fromRawBits(high, low);
    if (x.isNaN()) {
      BidIntegral.canonicalizeNaN128(high, low, flags, out);
      return;
    }
    if (x.isInfinite()) {
      if (!x.isSigned()) {
        DecNum.store128(Bid128.POSITIVE_INFINITY, out);
      } else {
        DecNum.store128(
            Bid128.finite(true, 12_287, PowersOfTen.MAX_34.high(), PowersOfTen.MAX_34.low()),
            out);
      }
      return;
    }
    if (x.isZero()) {
      DecNum.store128(Bid128.finite(false, 0, 0L, 1L), out);
      return;
    }
    UInt128 coefficient = x.coefficient();
    int exponent = x.biasedExponent();
    int digits = PowersOfTen.decimalDigits(coefficient);
    if (digits < 34 && exponent > 0) {
      int pad = Math.min(34 - digits, exponent);
      coefficient = coefficient.multiply(PowersOfTen.pow10(pad));
      exponent -= pad;
    }
    if (x.isSigned()) {
      coefficient = coefficient.subtract(1L);
      if (coefficient.equals(PowersOfTen.MAX_33) && exponent != 0) {
        coefficient = PowersOfTen.MAX_34;
        exponent--;
      }
    } else {
      coefficient = coefficient.add(1L);
      if (coefficient.equals(PowersOfTen.TEN_34)) {
        coefficient = PowersOfTen.pow10(33);
        exponent++;
      }
    }
    if (exponent > 12_287) {
      DecNum.store128(Bid128.POSITIVE_INFINITY, out);
      return;
    }
    DecNum.store128(
        Bid128.finite(x.isSigned(), exponent, coefficient.high(), coefficient.low()), out);
  }

  static void nextDown128(long high, long low, StatusFlags flags, long[] out) {
    nextUp128(high ^ Bid128.MASK_SIGN, low, flags, out);
    out[0] ^= Bid128.MASK_SIGN;
  }

  static void nextAfter128(
      long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    Bid128 x = Bid128.fromRawBits(xh, xl);
    Bid128 y = Bid128.fromRawBits(yh, yl);
    StatusFlags local = new StatusFlags();
    if (x.isNaN() || y.isNaN()) {
      if (x.isSignalingNaN() || y.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      if (x.isNaN()) {
        BidIntegral.canonicalizeNaN128(xh, xl, new StatusFlags(), out);
      } else {
        BidIntegral.canonicalizeNaN128(yh, yl, new StatusFlags(), out);
      }
      return;
    }
    if (x.quietEqual(y, local)) {
      if (x.isZero() && y.isZero()) {
        DecNum.store128(
            Bid128.finite(y.isSigned(), x.biasedExponent(), 0L, 0L),
            out);
      } else {
        out[0] = xh;
        out[1] = xl;
      }
      return;
    }
    if (x.quietLess(y, local)) {
      nextUp128(xh, xl, flags, out);
    } else {
      nextDown128(xh, xl, flags, out);
    }
    Bid128 result = Bid128.fromRawBits(out[0], out[1]);
    if (result.isInfinite() && x.isFinite()) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
    } else if (result.isZero() || result.isSubnormal()) {
      flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
    }
  }

  private static long negatePreservingNaN(long x) {
    return x ^ Bid64.MASK_SIGN;
  }
}
