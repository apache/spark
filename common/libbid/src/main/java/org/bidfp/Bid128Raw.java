/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

import org.bidfp.binary128.Dpml;

/**
 * Raw BID128 kernel: {@code hi}/{@code lo} payloads, results in
 * {@code long[2]} ({@code [0]=hi}, {@code [1]=lo}) matching DBR JNI.
 */
public final class Bid128Raw {
  private Bid128Raw() {
  }

  public static void copy(long hi, long lo, long[] out) {
    store(hi, lo, out);
  }

  public static void negate(long hi, long lo, long[] out) {
    store(hi ^ Bid128.MASK_SIGN, lo, out);
  }

  public static void abs(long hi, long lo, long[] out) {
    store(hi & ~Bid128.MASK_SIGN, lo, out);
  }

  public static void copySign(long hi, long lo, long signHi, long[] out) {
    store((hi & ~Bid128.MASK_SIGN) | (signHi & Bid128.MASK_SIGN), lo, out);
  }

  public static void nan(long[] out) {
    store(Bid128.MASK_NAN, 0L, out);
  }

  public static void inf(long[] out) {
    store(Bid128.MASK_INFINITY, 0L, out);
  }

  public static int radix() {
    return 10;
  }

  public static boolean isNaN(long hi, long lo) {
    return Bid128.fromRawBits(hi, lo).isNaN();
  }

  public static boolean isInf(long hi, long lo) {
    return Bid128.fromRawBits(hi, lo).isInfinite();
  }

  public static boolean isZero(long hi, long lo) {
    return Bid128.fromRawBits(hi, lo).isZero();
  }

  public static boolean isCanonical(long hi, long lo) {
    return Bid128.fromRawBits(hi, lo).isCanonical();
  }

  public static boolean isFinite(long hi, long lo) {
    return Bid128.fromRawBits(hi, lo).isFinite();
  }

  public static boolean isSignalingNaN(long hi, long lo) {
    return Bid128.fromRawBits(hi, lo).isSignalingNaN();
  }

  public static boolean isSigned(long hi, long lo) {
    return Bid128.fromRawBits(hi, lo).isSigned();
  }

  public static boolean isNormal(long hi, long lo) {
    return Bid128.fromRawBits(hi, lo).isNormal();
  }

  public static boolean isSubnormal(long hi, long lo) {
    return Bid128.fromRawBits(hi, lo).isSubnormal();
  }

  public static DecimalClass classify(long hi, long lo) {
    return Bid128.fromRawBits(hi, lo).classify();
  }

  public static void add(
      long xh, long xl, long yh, long yl, RoundingMode mode, StatusFlags flags, long[] out) {
    DecNum.store128(
        Bid128Add.add(Bid128.fromRawBits(xh, xl), Bid128.fromRawBits(yh, yl), mode, flags),
        out);
  }

  public static void sub(
      long xh, long xl, long yh, long yl, RoundingMode mode, StatusFlags flags, long[] out) {
    DecNum.store128(
        Bid128Add.subtract(Bid128.fromRawBits(xh, xl), Bid128.fromRawBits(yh, yl), mode, flags),
        out);
  }

  public static void mul(
      long xh, long xl, long yh, long yl, RoundingMode mode, StatusFlags flags, long[] out) {
    DecNum.store128(
        Bid128Multiply.multiply(
            Bid128.fromRawBits(xh, xl), Bid128.fromRawBits(yh, yl), mode, flags),
        out);
  }

  public static void div(
      long xh, long xl, long yh, long yl, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Divide.divide128(xh, xl, yh, yl, mode, flags, out);
  }

  public static void add(
      long xh, long xl, long yh, long yl, int rounding, long[] out, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    add(xh, xl, yh, yl, RoundingMode.fromIntel(rounding), flags, out);
    flags.copyTo(statusOut);
  }

  public static void sub(
      long xh, long xl, long yh, long yl, int rounding, long[] out, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    sub(xh, xl, yh, yl, RoundingMode.fromIntel(rounding), flags, out);
    flags.copyTo(statusOut);
  }

  public static void mul(
      long xh, long xl, long yh, long yl, int rounding, long[] out, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    mul(xh, xl, yh, yl, RoundingMode.fromIntel(rounding), flags, out);
    flags.copyTo(statusOut);
  }

  public static void div(
      long xh, long xl, long yh, long yl, int rounding, long[] out, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    div(xh, xl, yh, yl, RoundingMode.fromIntel(rounding), flags, out);
    flags.copyTo(statusOut);
  }

  public static boolean quietEqual(long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl).quietEqual(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean quietLess(long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl).quietLess(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean quietGreater(long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl).quietGreater(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean quietNotEqual(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl).quietNotEqual(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean quietLessEqual(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl).quietLessEqual(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean quietGreaterEqual(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl).quietGreaterEqual(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean quietOrdered(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl).quietOrdered(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean quietUnordered(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl).quietUnordered(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean quietGreaterUnordered(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    Bid128 x = Bid128.fromRawBits(xh, xl);
    Bid128 y = Bid128.fromRawBits(yh, yl);
    return x.quietGreater(y, flags) || x.quietUnordered(y, flags);
  }

  public static boolean quietLessUnordered(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    Bid128 x = Bid128.fromRawBits(xh, xl);
    Bid128 y = Bid128.fromRawBits(yh, yl);
    return x.quietLess(y, flags) || x.quietUnordered(y, flags);
  }

  public static boolean quietNotGreater(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return !quietGreater(xh, xl, yh, yl, flags);
  }

  public static boolean quietNotLess(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return !quietLess(xh, xl, yh, yl, flags);
  }

  public static boolean signalingLess(long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl).signalingLess(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean signalingGreater(long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl).signalingGreater(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean signalingEqual(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl)
        .signalingEqual(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean signalingNotEqual(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl)
        .signalingNotEqual(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean signalingLessEqual(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl)
        .signalingLessEqual(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean signalingGreaterEqual(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl)
        .signalingGreaterEqual(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean signalingGreaterUnordered(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl)
        .signalingGreaterUnordered(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean signalingLessUnordered(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl)
        .signalingLessUnordered(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean signalingNotGreater(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl)
        .signalingNotGreater(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean signalingNotLess(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl)
        .signalingNotLess(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean signalingOrdered(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl)
        .signalingOrdered(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean signalingUnordered(
      long xh, long xl, long yh, long yl, StatusFlags flags) {
    return Bid128.fromRawBits(xh, xl)
        .signalingUnordered(Bid128.fromRawBits(yh, yl), flags);
  }

  public static boolean sameQuantum(long xh, long xl, long yh, long yl) {
    return Bid128.fromRawBits(xh, xl).sameQuantum(Bid128.fromRawBits(yh, yl));
  }

  public static boolean totalOrder(long xh, long xl, long yh, long yl) {
    return Bid128.fromRawBits(xh, xl).totalOrder(Bid128.fromRawBits(yh, yl));
  }

  public static boolean totalOrderMag(long xh, long xl, long yh, long yl) {
    return Bid128.fromRawBits(xh, xl).totalOrderMag(Bid128.fromRawBits(yh, yl));
  }

  public static void fromString(String text, RoundingMode mode, StatusFlags flags, long[] out) {
    BidConvert.fromString128(text, mode, flags, out);
  }

  public static String toString(long hi, long lo) {
    return BidConvert.toString128(hi, lo);
  }

  public static void fromInt64(long value, RoundingMode mode, StatusFlags flags, long[] out) {
    BidConvert.fromInt64To128(value, mode, flags, out);
  }

  public static void fromInt32(int value, long[] out) {
    BidConvert.fromInt64To128(
        value, RoundingMode.TIES_TO_EVEN, new StatusFlags(), out);
  }

  public static void fromUInt64(
      long value, RoundingMode mode, StatusFlags flags, long[] out) {
    BidConvert.fromUInt64To128(value, mode, flags, out);
  }

  public static void fromUInt32(int value, long[] out) {
    BidConvert.fromUInt64To128(
        Integer.toUnsignedLong(value), RoundingMode.TIES_TO_EVEN, new StatusFlags(), out);
  }

  public static long toInt64(long hi, long lo, RoundingMode mode, StatusFlags flags) {
    return toInt64(hi, lo, mode, flags, false);
  }

  public static long toInt64(
      long hi, long lo, RoundingMode mode, StatusFlags flags, boolean signalInexact) {
    return BidConvert.toInt64From128(
        hi, lo, mode, flags, true, 64, signalInexact);
  }

  public static long toInteger(
      long hi, long lo, boolean signed, int width, RoundingMode mode,
      boolean signalInexact, StatusFlags flags) {
    if (width != 8 && width != 16 && width != 32 && width != 64) {
      throw new IllegalArgumentException("integer width must be 8, 16, 32, or 64");
    }
    return BidConvert.toInt64From128(
        hi, lo, mode, flags, signed, width, signalInexact);
  }

  public static long toInt64Int(long hi, long lo, StatusFlags flags) {
    return BidConvert.toInt64From128(
        hi, lo, RoundingMode.TOWARD_ZERO, flags, true, 64, false);
  }

  public static long toInt64Xint(long hi, long lo, StatusFlags flags) {
    return BidConvert.toInt64From128(
        hi, lo, RoundingMode.TOWARD_ZERO, flags, true, 64, true);
  }

  public static long toInt64Floor(long hi, long lo, StatusFlags flags) {
    return BidConvert.toInt64From128(
        hi, lo, RoundingMode.TOWARD_NEGATIVE, flags, true, 64, false);
  }

  public static long toInt64Ceil(long hi, long lo, StatusFlags flags) {
    return BidConvert.toInt64From128(
        hi, lo, RoundingMode.TOWARD_POSITIVE, flags, true, 64, false);
  }

  public static long toInt64Rnint(long hi, long lo, StatusFlags flags) {
    return BidConvert.toInt64From128(
        hi, lo, RoundingMode.TIES_TO_EVEN, flags, true, 64, false);
  }

  public static long toInt64Rninta(long hi, long lo, StatusFlags flags) {
    return BidConvert.toInt64From128(
        hi, lo, RoundingMode.TIES_AWAY, flags, true, 64, false);
  }

  public static long toInt64Xfloor(long hi, long lo, StatusFlags flags) {
    return BidConvert.toInt64From128(
        hi, lo, RoundingMode.TOWARD_NEGATIVE, flags, true, 64, true);
  }

  public static long toInt64Xceil(long hi, long lo, StatusFlags flags) {
    return BidConvert.toInt64From128(
        hi, lo, RoundingMode.TOWARD_POSITIVE, flags, true, 64, true);
  }

  public static long toInt64Xrnint(long hi, long lo, StatusFlags flags) {
    return BidConvert.toInt64From128(
        hi, lo, RoundingMode.TIES_TO_EVEN, flags, true, 64, true);
  }

  public static long toInt64Xrninta(long hi, long lo, StatusFlags flags) {
    return BidConvert.toInt64From128(
        hi, lo, RoundingMode.TIES_AWAY, flags, true, 64, true);
  }

  public static int toInt32(
      long hi, long lo, RoundingMode mode, StatusFlags flags, boolean signalInexact) {
    return (int) BidConvert.toInt64From128(
        hi, lo, mode, flags, true, 32, signalInexact);
  }

  public static long toUInt64(
      long hi, long lo, RoundingMode mode, StatusFlags flags, boolean signalInexact) {
    return BidConvert.toInt64From128(
        hi, lo, mode, flags, false, 64, signalInexact);
  }

  public static int toUInt32(
      long hi, long lo, RoundingMode mode, StatusFlags flags, boolean signalInexact) {
    return (int) BidConvert.toInt64From128(
        hi, lo, mode, flags, false, 32, signalInexact);
  }

  public static void fromBinary64(double value, RoundingMode mode, StatusFlags flags, long[] out) {
    BidConvert.fromBinary64To128(value, mode, flags, out);
  }

  public static void fromBinary32(float value, RoundingMode mode, StatusFlags flags, long[] out) {
    BidConvert.fromBinary32To128(value, mode, flags, out);
  }

  public static double toBinary64(long hi, long lo, RoundingMode mode, StatusFlags flags) {
    return BidConvert.toBinary64From128(hi, lo, mode, flags);
  }

  public static float toBinary32(long hi, long lo, RoundingMode mode, StatusFlags flags) {
    return BidConvert.toBinary32From128(hi, lo, mode, flags);
  }

  public static void toBinary128(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    BidConvert.toBinary128From128(hi, lo, mode, flags, out);
  }

  public static void fromBinary128(
      long high, long low, RoundingMode mode, StatusFlags flags, long[] out) {
    BidConvert.fromBinary128To128(high, low, mode, flags, out);
  }

  public static long toBid64(long hi, long lo, RoundingMode mode, StatusFlags flags) {
    return BidConvert.bid128ToBid64(hi, lo, mode, flags);
  }

  public static void roundIntegral(
      long hi, long lo, RoundingMode mode, StatusFlags flags, boolean exact, long[] out) {
    BidIntegral.round128(hi, lo, mode, flags, exact, out);
  }

  public static void roundIntegralZero(long hi, long lo, StatusFlags flags, long[] out) {
    BidIntegral.round128(hi, lo, RoundingMode.TOWARD_ZERO, flags, false, out);
  }

  public static void roundIntegralNegative(long hi, long lo, StatusFlags flags, long[] out) {
    BidIntegral.round128(hi, lo, RoundingMode.TOWARD_NEGATIVE, flags, false, out);
  }

  public static void roundIntegralPositive(long hi, long lo, StatusFlags flags, long[] out) {
    BidIntegral.round128(hi, lo, RoundingMode.TOWARD_POSITIVE, flags, false, out);
  }

  public static void roundIntegralNearestEven(
      long hi, long lo, StatusFlags flags, long[] out) {
    BidIntegral.round128(hi, lo, RoundingMode.TIES_TO_EVEN, flags, false, out);
  }

  public static void roundIntegralNearestAway(
      long hi, long lo, StatusFlags flags, long[] out) {
    BidIntegral.round128(hi, lo, RoundingMode.TIES_AWAY, flags, false, out);
  }

  public static void floor(long hi, long lo, StatusFlags flags, long[] out) {
    BidIntegral.round128(hi, lo, RoundingMode.TOWARD_NEGATIVE, flags, false, out);
  }

  public static void ceil(long hi, long lo, StatusFlags flags, long[] out) {
    BidIntegral.round128(hi, lo, RoundingMode.TOWARD_POSITIVE, flags, false, out);
  }

  public static void roundIntegralExact(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    BidIntegral.round128(hi, lo, mode, flags, true, out);
  }

  public static void quantize(
      long xh, long xl, long yh, long yl, RoundingMode mode, StatusFlags flags, long[] out) {
    BidQuantize.quantize128(xh, xl, yh, yl, mode, flags, out);
  }

  public static void scalbn(
      long hi, long lo, int n, RoundingMode mode, StatusFlags flags, long[] out) {
    BidScale.scalbn128(hi, lo, n, mode, flags, out);
  }

  public static void ldexp(
      long hi, long lo, int n, RoundingMode mode, StatusFlags flags, long[] out) {
    BidScale.scalbn128(hi, lo, n, mode, flags, out);
  }

  public static void scalbln(
      long hi, long lo, long n, RoundingMode mode, StatusFlags flags, long[] out) {
    int clamped = n > Integer.MAX_VALUE
        ? Integer.MAX_VALUE
        : n < Integer.MIN_VALUE ? Integer.MIN_VALUE : (int) n;
    BidScale.scalbn128(hi, lo, clamped, mode, flags, out);
  }

  public static int ilogb(long hi, long lo, StatusFlags flags) {
    return BidScale.ilogb128(hi, lo, flags);
  }

  public static void logb(long hi, long lo, StatusFlags flags, long[] out) {
    BidScale.logb128(hi, lo, flags, out);
  }

  public static int quantexp(long hi, long lo) {
    return BidScale.quantexp128(hi, lo);
  }

  public static int quantexp(long hi, long lo, StatusFlags flags) {
    if (!isFinite(hi, lo)) {
      flags.raise(StatusFlags.INVALID);
    }
    return BidScale.quantexp128(hi, lo);
  }

  public static long llquantexp(long hi, long lo) {
    return BidScale.quantexp128(hi, lo);
  }

  public static long llquantexp(long hi, long lo, StatusFlags flags) {
    if (!isFinite(hi, lo)) {
      flags.raise(StatusFlags.INVALID);
    }
    return BidScale.quantexp128(hi, lo);
  }

  public static void quantum(long hi, long lo, long[] out) {
    BidScale.quantum128(hi, lo, out);
  }

  public static void nearbyint(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    BidIntegral.round128(hi, lo, mode, flags, false, out);
  }

  public static void frexp(
      long hi, long lo, int[] exponentOut, StatusFlags flags, long[] out) {
    BidScale.frexp128(hi, lo, exponentOut, flags, out);
  }

  public static void modf(
      long hi, long lo, long[] integralOut, StatusFlags flags, long[] out) {
    Bid128 value = Bid128.fromRawBits(hi, lo);
    if (value.isNaN()) {
      BidIntegral.canonicalizeNaN128(hi, lo, flags, out);
      integralOut[0] = out[0];
      integralOut[1] = out[1];
      return;
    }
    if (value.isInfinite()) {
      DecNum.store128(
          value.isSigned() ? Bid128.NEGATIVE_INFINITY : Bid128.POSITIVE_INFINITY,
          integralOut);
      DecNum.store128(Bid128.finite(value.isSigned(), 12_287, 0L, 0L), out);
      return;
    }
    BidIntegral.round128(
        hi, lo, RoundingMode.TOWARD_ZERO, flags, false, integralOut);
    Bid128 integral = Bid128.fromRawBits(integralOut[0], integralOut[1]);
    if (value.quietEqual(integral, new StatusFlags())) {
      DecNum.store128(Bid128.finite(value.isSigned(), 12_287, 0L, 0L), out);
      return;
    }
    Bid128 fractional = Bid128Add.subtract(
        value,
        integral,
        RoundingMode.TOWARD_ZERO,
        flags);
    DecNum.store128(fractional, out);
  }

  public static int lrint(
      long hi, long lo, RoundingMode mode, StatusFlags flags) {
    return (int) BidConvert.toInt64From128(
        hi, lo, mode, flags, true, 32, true);
  }

  public static long llrint(
      long hi, long lo, RoundingMode mode, StatusFlags flags) {
    return BidConvert.toInt64From128(hi, lo, mode, flags, true, 64, true);
  }

  public static int lround(long hi, long lo, StatusFlags flags) {
    return (int) BidConvert.toInt64From128(
        hi, lo, RoundingMode.TIES_AWAY, flags, true, 32, false);
  }

  public static long llround(long hi, long lo, StatusFlags flags) {
    return BidConvert.toInt64From128(
        hi, lo, RoundingMode.TIES_AWAY, flags, true, 64, false);
  }

  public static void sqrt(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    BidSqrt.sqrt128(hi, lo, mode, flags, out);
  }

  public static void rem(long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    BidRem.rem128(xh, xl, yh, yl, flags, out);
  }

  public static void fmod(long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    BidRem.fmod128(xh, xl, yh, yl, flags, out);
  }

  public static void fma(
      long xh, long xl, long yh, long yl, long zh, long zl,
      RoundingMode mode, StatusFlags flags, long[] out) {
    BidFma.fma128(xh, xl, yh, yl, zh, zl, mode, flags, out);
  }

  public static void nextUp(long hi, long lo, StatusFlags flags, long[] out) {
    BidNext.nextUp128(hi, lo, flags, out);
  }

  public static void nextDown(long hi, long lo, StatusFlags flags, long[] out) {
    BidNext.nextDown128(hi, lo, flags, out);
  }

  public static void nextAfter(
      long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    BidNext.nextAfter128(xh, xl, yh, yl, flags, out);
  }

  public static void nextToward(
      long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    BidNext.nextAfter128(xh, xl, yh, yl, flags, out);
  }

  public static void minnum(long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    BidMinMax.minnum128(xh, xl, yh, yl, flags, out);
  }

  public static void maxnum(long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    BidMinMax.maxnum128(xh, xl, yh, yl, flags, out);
  }

  public static void minnumMag(long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    BidMinMax.minnumMag128(xh, xl, yh, yl, flags, out);
  }

  public static void maxnumMag(long xh, long xl, long yh, long yl, StatusFlags flags, long[] out) {
    BidMinMax.maxnumMag128(xh, xl, yh, yl, flags, out);
  }

  public static void fdim(
      long xh, long xl, long yh, long yl, RoundingMode mode, StatusFlags flags, long[] out) {
    BidMinMax.fdim128(xh, xl, yh, yl, mode, flags, out);
  }

  public static void toDpd(long hi, long lo, long[] out) {
    BidDpd.bid128ToDpd(hi, lo, out);
  }

  public static void fromDpd(long hi, long lo, long[] out) {
    BidDpd.dpdToBid128(hi, lo, out);
  }

  public static void exp(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Exp.exp(hi, lo, mode, flags, out);
  }

  public static void expm1(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Expm1.expm1(hi, lo, mode, flags, out);
  }

  public static void exp2(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Exp2.exp2(hi, lo, mode, flags, out);
  }

  public static void exp10(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Exp10.exp10(hi, lo, mode, flags, out);
  }

  public static void log(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Log.log(hi, lo, mode, flags, out);
  }

  public static void log10(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Log.log10(hi, lo, mode, flags, out);
  }

  public static void log2(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Log.log2(hi, lo, mode, flags, out);
  }

  public static void log1p(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Log1p.log1p(hi, lo, mode, flags, out);
  }

  public static void pow(
      long xh, long xl, long yh, long yl, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Pow.pow(xh, xl, yh, yl, mode, flags, out);
  }

  public static void sin(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Trig.sin(hi, lo, mode, flags, out);
  }

  public static void tan(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Trig.tan(hi, lo, mode, flags, out);
  }

  public static void cos(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Trig.cos(hi, lo, mode, flags, out);
  }

  public static void asin(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128InvTrig.asin(hi, lo, mode, flags, out);
  }

  public static void acos(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128InvTrig.acos(hi, lo, mode, flags, out);
  }

  public static void atan(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.tinyOddFma(hi, lo, mode, flags, out)) {
      return;
    }
    BidTranscendental.unary128(hi, lo, mode, flags, Dpml::atan, out);
  }

  public static void atan2(
      long yh, long yl, long xh, long xl,
      RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Atan2.atan2(yh, yl, xh, xl, mode, flags, out);
  }

  public static void sinh(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.tinyOddFma(hi, lo, mode, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    Bid128 abs = Bid128.fromRawBits(hi & ~Bid128.MASK_SIGN, lo);
    if (abs.quietGreater(Bid128Libm.ONE, new StatusFlags())) {
      long[] exp = new long[2];
      long[] inv = new long[2];
      Bid128Exp.exp(hi & ~Bid128.MASK_SIGN, lo, mode, flags, exp);
      Bid128Raw.div(
          Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
          exp[0], exp[1], mode, flags, inv);
      Bid128Raw.sub(exp[0], exp[1], inv[0], inv[1], mode, flags, out);
      Bid128 half = Bid128.fromRawBits(0x303e_0000_0000_0000L, 5L);
      Bid128Raw.mul(out[0], out[1], half.highBits(), half.lowBits(), mode, flags, out);
      if (x.isSigned()) {
        out[0] ^= Bid128.MASK_SIGN;
      }
      return;
    }
    BidTranscendental.unary128(hi, lo, mode, flags, Dpml::sinh, out);
  }

  public static void cosh(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128 abs = Bid128.fromRawBits(hi & ~Bid128.MASK_SIGN, lo);
    if (abs.quietGreater(Bid128Libm.ONE, new StatusFlags())) {
      long[] exp = new long[2];
      long[] inv = new long[2];
      Bid128Exp.exp(hi & ~Bid128.MASK_SIGN, lo, mode, flags, exp);
      Bid128Raw.div(
          Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
          exp[0], exp[1], mode, flags, inv);
      Bid128Raw.add(exp[0], exp[1], inv[0], inv[1], mode, flags, out);
      Bid128 half = Bid128.fromRawBits(0x303e_0000_0000_0000L, 5L);
      Bid128Raw.mul(out[0], out[1], half.highBits(), half.lowBits(), mode, flags, out);
      return;
    }
    BidTranscendental.unary128(hi, lo, mode, flags, Dpml::cosh, out);
  }

  public static void tanh(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.tinyOddFma(hi, lo, mode, flags, out)) {
      return;
    }
    BidTranscendental.unary128(hi, lo, mode, flags, Dpml::tanh, out);
  }

  public static void asinh(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Asinh.asinh(hi, lo, mode, flags, out);
  }

  public static void acosh(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Acosh.acosh(hi, lo, mode, flags, out);
  }

  public static void atanh(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128InvTrig.atanh(hi, lo, mode, flags, out);
  }

  public static void erf(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Erf.erf(hi, lo, mode, flags, out);
  }

  public static void erfc(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Erf.erfc(hi, lo, mode, flags, out);
  }

  public static void tgamma(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Tgamma.tgamma(hi, lo, mode, flags, out);
  }

  public static void lgamma(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Lgamma.lgamma(hi, lo, mode, flags, out);
  }

  public static void hypot(
      long xh, long xl, long yh, long yl, RoundingMode mode, StatusFlags flags, long[] out) {
    BidTranscendental.hypot128(xh, xl, yh, yl, mode, flags, out);
  }

  public static void cbrt(long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Cbrt.cbrt(hi, lo, mode, flags, out);
  }

  private static void store(long hi, long lo, long[] out) {
    out[0] = hi;
    out[1] = lo;
  }
}
