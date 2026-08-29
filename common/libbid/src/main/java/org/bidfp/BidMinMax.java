/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

final class BidMinMax {
  private BidMinMax() {
  }

  static long minnum64(long x, long y, StatusFlags flags) {
    return select64(x, y, flags, true, false);
  }

  static long maxnum64(long x, long y, StatusFlags flags) {
    return select64(x, y, flags, false, false);
  }

  static long minnumMag64(long x, long y, StatusFlags flags) {
    return select64(x, y, flags, true, true);
  }

  static long maxnumMag64(long x, long y, StatusFlags flags) {
    return select64(x, y, flags, false, true);
  }

  static long fdim64(long x, long y, RoundingMode mode, StatusFlags flags) {
    if (Bid64Raw.isNaN(x) || Bid64Raw.isNaN(y)) {
      return Bid64Raw.sub(x, y, mode, flags);
    }
    if (Bid64Raw.quietGreater(x, y, new StatusFlags())) {
      return Bid64Raw.sub(x, y, mode, flags);
    }
    return Bid64.finiteRawBits(false, 398, 0L);
  }

  static void minnum128(
      long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    select128(xh, xl, yh, yl, flags, true, false, out);
  }

  static void maxnum128(
      long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    select128(xh, xl, yh, yl, flags, false, false, out);
  }

  static void minnumMag128(
      long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    select128(xh, xl, yh, yl, flags, true, true, out);
  }

  static void maxnumMag128(
      long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    select128(xh, xl, yh, yl, flags, false, true, out);
  }

  static void fdim128(
      long xh, long xl, long yh, long yl, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128 x = Bid128.fromRawBits(xh, xl);
    Bid128 y = Bid128.fromRawBits(yh, yl);
    if (x.isNaN() || y.isNaN()) {
      DecNum.store128(Bid128Add.subtract(x, y, mode, flags), out);
      return;
    }
    if (x.quietGreater(y, new StatusFlags())) {
      DecNum.store128(Bid128Add.subtract(x, y, mode, flags), out);
      return;
    }
    DecNum.store128(Bid128.finite(false, 6176, 0L, 0L), out);
  }

  private static long select64(
      long x, long y, StatusFlags flags, boolean min, boolean mag) {
    if (Bid64Raw.isNaN(x)) {
      if (Bid64Raw.isSignalingNaN(x) || Bid64Raw.isSignalingNaN(y)) {
        flags.raise(StatusFlags.INVALID);
      }
      if (Bid64Raw.isSignalingNaN(x) || Bid64Raw.isNaN(y)) {
        return BidIntegral.canonicalizeNaN64(x, new StatusFlags());
      }
      return canonicalFinite64(y);
    }
    if (Bid64Raw.isNaN(y)) {
      if (Bid64Raw.isSignalingNaN(y)) {
        flags.raise(StatusFlags.INVALID);
        return BidIntegral.canonicalizeNaN64(y, new StatusFlags());
      }
      return canonicalFinite64(x);
    }
    long a = mag ? Bid64Raw.abs(x) : x;
    long b = mag ? Bid64Raw.abs(y) : y;
    boolean less = Bid64Raw.quietLess(a, b, new StatusFlags());
    if (Bid64Raw.quietEqual(a, b, new StatusFlags())) {
      if (mag) {
        long chosen = Bid64Raw.isSigned(x) == Bid64Raw.isSigned(y)
            ? (min ? x : y)
            : (Bid64Raw.isSigned(x) == min ? x : y);
        return canonicalFinite64(chosen);
      }
      if (Bid64Raw.isZero(x) && Bid64Raw.isZero(y)) {
        if (Bid64Raw.isSigned(x) != Bid64Raw.isSigned(y)) {
          return canonicalFinite64(min ? y : x);
        }
        boolean xFirst = min ? Bid64Raw.totalOrder(x, y) : Bid64Raw.totalOrder(y, x);
        return canonicalFinite64(xFirst ? x : y);
      }
      return canonicalFinite64(y);
    }
    return canonicalFinite64(less == min ? x : y);
  }

  private static void select128(
      long xh, long xl,
      long yh, long yl,
      StatusFlags flags,
      boolean min,
      boolean mag,
      long[] out) {
    Bid128 x = Bid128.fromRawBits(xh, xl);
    Bid128 y = Bid128.fromRawBits(yh, yl);
    if (x.isNaN()) {
      if (x.isSignalingNaN() || y.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      if (x.isSignalingNaN() || y.isNaN()) {
        BidIntegral.canonicalizeNaN128(xh, xl, new StatusFlags(), out);
      } else {
        storeCanonicalFinite128(y, out);
      }
      return;
    }
    if (y.isNaN()) {
      if (y.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
        BidIntegral.canonicalizeNaN128(yh, yl, new StatusFlags(), out);
        return;
      }
      storeCanonicalFinite128(x, out);
      return;
    }
    Bid128 a = mag ? x.abs() : x;
    Bid128 b = mag ? y.abs() : y;
    boolean less = a.quietLess(b, new StatusFlags());
    Bid128 chosen;
    if (a.quietEqual(b, new StatusFlags())) {
      if (mag) {
        chosen = x.isZero() && y.isZero()
            ? (min ? x : y)
            : (y.isSigned() == min ? y : x);
      } else if (x.isZero() && y.isZero()) {
        chosen = x;
      } else {
        boolean xHasGreaterExponent = x.biasedExponent() > y.biasedExponent();
        boolean chooseGreaterExponent = min != x.isSigned();
        chosen = xHasGreaterExponent == chooseGreaterExponent ? x : y;
      }
    } else {
      chosen = less == min ? x : y;
    }
    storeCanonicalFinite128(chosen, out);
  }

  private static long canonicalFinite64(long value) {
    if (Bid64Raw.isInf(value)) {
      return (value & Bid64.MASK_SIGN) | Bid64.MASK_INFINITY;
    }
    if (Bid64Raw.isZero(value)) {
      return Bid64.finiteRawBits(
          Bid64Raw.isSigned(value), Bid64.biasedExponentBits(value), 0L);
    }
    return value;
  }

  private static void storeCanonicalFinite128(Bid128 value, long[] out) {
    if (value.isInfinite()) {
      DecNum.store128(
          value.isSigned() ? Bid128.NEGATIVE_INFINITY : Bid128.POSITIVE_INFINITY,
          out);
    } else if (value.isZero()) {
      DecNum.store128(
          Bid128.finite(value.isSigned(), value.biasedExponent(), 0L, 0L),
          out);
    } else {
      DecNum.store128(value, out);
    }
  }
}
