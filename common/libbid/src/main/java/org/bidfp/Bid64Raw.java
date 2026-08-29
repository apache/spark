/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

import java.util.Objects;

import org.bidfp.binary128.Dpml;

/**
 * Raw BID64 kernel matching Intel RDFP payload layout ({@code long} bits) and
 * JNI marshalling ({@code int} rounding codes, {@code int[]} status).
 */
public final class Bid64Raw {
  static final int RADIX = 10;
  private static final long INTEGER_INDEFINITE = 0x8000_0000_0000_0000L;

  private Bid64Raw() {
  }

  public static long copy(long x) {
    return x;
  }

  public static long negate(long x) {
    return x ^ Bid64.MASK_SIGN;
  }

  public static long abs(long x) {
    return x & ~Bid64.MASK_SIGN;
  }

  public static long copySign(long x, long y) {
    return (x & ~Bid64.MASK_SIGN) | (y & Bid64.MASK_SIGN);
  }

  public static long nan() {
    return Bid64.MASK_NAN;
  }

  public static long inf() {
    return Bid64.MASK_INFINITY;
  }

  public static int radix() {
    return RADIX;
  }

  public static boolean isNaN(long x) {
    return (x & Bid64.MASK_NAN) == Bid64.MASK_NAN;
  }

  public static boolean isSignalingNaN(long x) {
    return (x & Bid64.MASK_SIGNALING_NAN) == Bid64.MASK_SIGNALING_NAN;
  }

  public static boolean isInf(long x) {
    return (x & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY
        && (x & Bid64.MASK_NAN) != Bid64.MASK_NAN;
  }

  public static boolean isFinite(long x) {
    return (x & Bid64.MASK_INFINITY) != Bid64.MASK_INFINITY;
  }

  public static boolean isCanonical(long x) {
    return Bid64.fromRawBits(x).isCanonical();
  }

  public static boolean isZero(long x) {
    return isFinite(x)
        && (Bid64.significandBits(x) == 0L
            || Bid64.rawSignificandBits(x) > PowersOfTen.MAX_16);
  }

  public static boolean isSigned(long x) {
    return (x & Bid64.MASK_SIGN) != 0L;
  }

  public static boolean isNormal(long x) {
    return Bid64.fromRawBits(x).isNormal();
  }

  public static boolean isSubnormal(long x) {
    return Bid64.fromRawBits(x).isSubnormal();
  }

  public static DecimalClass classify(long x) {
    return Bid64.fromRawBits(x).classify();
  }

  public static long add(long x, long y, RoundingMode mode, StatusFlags flags) {
    return Bid64Add.addRawBits(x, y, mode, flags);
  }

  public static long sub(long x, long y, RoundingMode mode, StatusFlags flags) {
    return Bid64Add.subtractRawBits(x, y, mode, flags);
  }

  public static long mul(long x, long y, RoundingMode mode, StatusFlags flags) {
    return Bid64Multiply.multiplyRawBits(x, y, mode, flags);
  }

  public static long div(long x, long y, RoundingMode mode, StatusFlags flags) {
    return Bid64Divide.divideRawBits(x, y, mode, flags);
  }

  public static long add(long x, long y, int rounding, int[] statusOut) {
    return withFlags(statusOut, flags -> add(x, y, RoundingMode.fromIntel(rounding), flags));
  }

  public static long sub(long x, long y, int rounding, int[] statusOut) {
    return withFlags(statusOut, flags -> sub(x, y, RoundingMode.fromIntel(rounding), flags));
  }

  public static long mul(long x, long y, int rounding, int[] statusOut) {
    return withFlags(statusOut, flags -> mul(x, y, RoundingMode.fromIntel(rounding), flags));
  }

  public static long div(long x, long y, int rounding, int[] statusOut) {
    return withFlags(statusOut, flags -> div(x, y, RoundingMode.fromIntel(rounding), flags));
  }

  public static boolean quietEqual(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).quietEqual(Bid64.fromRawBits(y), flags);
  }

  public static boolean quietLess(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).quietLess(Bid64.fromRawBits(y), flags);
  }

  public static boolean quietGreater(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).quietGreater(Bid64.fromRawBits(y), flags);
  }

  public static boolean quietLessEqual(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).quietLessEqual(Bid64.fromRawBits(y), flags);
  }

  public static boolean quietGreaterEqual(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).quietGreaterEqual(Bid64.fromRawBits(y), flags);
  }

  public static boolean quietNotEqual(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).quietNotEqual(Bid64.fromRawBits(y), flags);
  }

  public static boolean quietOrdered(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).quietOrdered(Bid64.fromRawBits(y), flags);
  }

  public static boolean quietUnordered(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).quietUnordered(Bid64.fromRawBits(y), flags);
  }

  public static boolean quietGreaterUnordered(long x, long y, StatusFlags flags) {
    int c = compare(x, y, flags, false);
    return c == 1 || c == 2;
  }

  public static boolean quietLessUnordered(long x, long y, StatusFlags flags) {
    int c = compare(x, y, flags, false);
    return c == -1 || c == 2;
  }

  public static boolean quietNotGreater(long x, long y, StatusFlags flags) {
    return !quietGreater(x, y, flags);
  }

  public static boolean quietNotLess(long x, long y, StatusFlags flags) {
    return !quietLess(x, y, flags);
  }

  public static boolean signalingLess(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).signalingLess(Bid64.fromRawBits(y), flags);
  }

  public static boolean signalingGreater(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).signalingGreater(Bid64.fromRawBits(y), flags);
  }

  public static boolean signalingLessEqual(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).signalingLessEqual(Bid64.fromRawBits(y), flags);
  }

  public static boolean signalingGreaterEqual(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).signalingGreaterEqual(Bid64.fromRawBits(y), flags);
  }

  public static boolean signalingNotEqual(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).signalingNotEqual(Bid64.fromRawBits(y), flags);
  }

  public static boolean signalingEqual(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).signalingEqual(Bid64.fromRawBits(y), flags);
  }

  public static boolean signalingOrdered(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).signalingOrdered(Bid64.fromRawBits(y), flags);
  }

  public static boolean signalingUnordered(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).signalingUnordered(Bid64.fromRawBits(y), flags);
  }

  public static boolean signalingGreaterUnordered(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).signalingGreaterUnordered(Bid64.fromRawBits(y), flags);
  }

  public static boolean signalingLessUnordered(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).signalingLessUnordered(Bid64.fromRawBits(y), flags);
  }

  public static boolean signalingNotGreater(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).signalingNotGreater(Bid64.fromRawBits(y), flags);
  }

  public static boolean signalingNotLess(long x, long y, StatusFlags flags) {
    return Bid64.fromRawBits(x).signalingNotLess(Bid64.fromRawBits(y), flags);
  }

  public static boolean sameQuantum(long x, long y) {
    return Bid64.fromRawBits(x).sameQuantum(Bid64.fromRawBits(y));
  }

  public static boolean totalOrder(long x, long y) {
    return Bid64.fromRawBits(x).totalOrder(Bid64.fromRawBits(y));
  }

  public static boolean totalOrderMag(long x, long y) {
    return Bid64.fromRawBits(x).totalOrderMag(Bid64.fromRawBits(y));
  }

  public static long fromString(String text, RoundingMode mode, StatusFlags flags) {
    return BidConvert.fromString64(text, mode, flags);
  }

  public static String toString(long x) {
    return BidConvert.toString64(x);
  }

  public static long fromInt32(int value) {
    return fromInt64(value, RoundingMode.TIES_TO_EVEN, new StatusFlags());
  }

  public static long fromUInt32(int value) {
    return fromInt64(Integer.toUnsignedLong(value), RoundingMode.TIES_TO_EVEN, new StatusFlags());
  }

  public static long fromInt64(long value, RoundingMode mode, StatusFlags flags) {
    return BidConvert.fromInt64To64(value, mode, flags);
  }

  public static long fromUInt64(long value, RoundingMode mode, StatusFlags flags) {
    return BidConvert.fromUInt64To64(value, mode, flags);
  }

  public static long toInt64(long x, RoundingMode mode, StatusFlags flags, boolean signalInexact) {
    return BidConvert.toInt64(x, mode, flags, true, 64, signalInexact);
  }

  public static long toInteger(
      long x, boolean signed, int width, RoundingMode mode,
      boolean signalInexact, StatusFlags flags) {
    if (width != 8 && width != 16 && width != 32 && width != 64) {
      throw new IllegalArgumentException("integer width must be 8, 16, 32, or 64");
    }
    return BidConvert.toInt64(x, mode, flags, signed, width, signalInexact);
  }

  public static long toInt64Int(long x, StatusFlags flags) {
    return toInt64(x, RoundingMode.TOWARD_ZERO, flags, false);
  }

  public static long toInt64Xint(long x, StatusFlags flags) {
    return toInt64(x, RoundingMode.TOWARD_ZERO, flags, true);
  }

  public static long toInt64Floor(long x, StatusFlags flags) {
    return toInt64(x, RoundingMode.TOWARD_NEGATIVE, flags, false);
  }

  public static long toInt64Ceil(long x, StatusFlags flags) {
    return toInt64(x, RoundingMode.TOWARD_POSITIVE, flags, false);
  }

  public static long toInt64Rnint(long x, StatusFlags flags) {
    return toInt64(x, RoundingMode.TIES_TO_EVEN, flags, false);
  }

  public static long toInt64Rninta(long x, StatusFlags flags) {
    return toInt64(x, RoundingMode.TIES_AWAY, flags, false);
  }

  public static long toInt64Xfloor(long x, StatusFlags flags) {
    return toInt64(x, RoundingMode.TOWARD_NEGATIVE, flags, true);
  }

  public static long toInt64Xceil(long x, StatusFlags flags) {
    return toInt64(x, RoundingMode.TOWARD_POSITIVE, flags, true);
  }

  public static long toInt64Xrnint(long x, StatusFlags flags) {
    return toInt64(x, RoundingMode.TIES_TO_EVEN, flags, true);
  }

  public static long toInt64Xrninta(long x, StatusFlags flags) {
    return toInt64(x, RoundingMode.TIES_AWAY, flags, true);
  }

  public static int toInt32(long x, RoundingMode mode, StatusFlags flags, boolean signalInexact) {
    return (int) BidConvert.toInt64(x, mode, flags, true, 32, signalInexact);
  }

  public static long toUInt64(long x, RoundingMode mode, StatusFlags flags, boolean signalInexact) {
    return BidConvert.toInt64(x, mode, flags, false, 64, signalInexact);
  }

  public static int toUInt32(long x, RoundingMode mode, StatusFlags flags, boolean signalInexact) {
    return (int) BidConvert.toInt64(x, mode, flags, false, 32, signalInexact);
  }

  public static long fromBinary64(double value, RoundingMode mode, StatusFlags flags) {
    return BidConvert.fromBinary64To64(value, mode, flags);
  }

  public static long fromBinary32(float value, RoundingMode mode, StatusFlags flags) {
    return BidConvert.fromBinary32To64(value, mode, flags);
  }

  public static double toBinary64(long x, RoundingMode mode, StatusFlags flags) {
    return BidConvert.toBinary64From64(x, mode, flags);
  }

  public static float toBinary32(long x, RoundingMode mode, StatusFlags flags) {
    return BidConvert.toBinary32From64(x, mode, flags);
  }

  public static void toBinary128(
      long x, RoundingMode mode, StatusFlags flags, long[] out) {
    BidConvert.toBinary128From64(x, mode, flags, out);
  }

  public static long fromBinary128(
      long high, long low, RoundingMode mode, StatusFlags flags) {
    return BidConvert.fromBinary128To64(high, low, mode, flags);
  }

  public static void toBid128(long x, long[] payloadOut, StatusFlags flags) {
    BidConvert.bid64ToBid128(x, payloadOut, flags);
  }

  public static long roundIntegral(long x, RoundingMode mode, StatusFlags flags, boolean exact) {
    return BidIntegral.round64(x, mode, flags, exact);
  }

  public static long roundIntegralZero(long x, StatusFlags flags) {
    return roundIntegral(x, RoundingMode.TOWARD_ZERO, flags, false);
  }

  public static long roundIntegralNegative(long x, StatusFlags flags) {
    return roundIntegral(x, RoundingMode.TOWARD_NEGATIVE, flags, false);
  }

  public static long roundIntegralPositive(long x, StatusFlags flags) {
    return roundIntegral(x, RoundingMode.TOWARD_POSITIVE, flags, false);
  }

  public static long roundIntegralNearestEven(long x, StatusFlags flags) {
    return roundIntegral(x, RoundingMode.TIES_TO_EVEN, flags, false);
  }

  public static long roundIntegralNearestAway(long x, StatusFlags flags) {
    return roundIntegral(x, RoundingMode.TIES_AWAY, flags, false);
  }

  public static long floor(long x, StatusFlags flags) {
    return roundIntegralNegative(x, flags);
  }

  public static long ceil(long x, StatusFlags flags) {
    return roundIntegralPositive(x, flags);
  }

  public static long roundIntegralExact(long x, RoundingMode mode, StatusFlags flags) {
    return roundIntegral(x, mode, flags, true);
  }

  public static long nearbyint(long x, RoundingMode mode, StatusFlags flags) {
    return roundIntegral(x, mode, flags, false);
  }

  public static long quantize(long x, long y, RoundingMode mode, StatusFlags flags) {
    return BidQuantize.quantize64(x, y, mode, flags);
  }

  public static long scalbn(long x, int n, RoundingMode mode, StatusFlags flags) {
    return BidScale.scalbn64(x, n, mode, flags);
  }

  public static long ldexp(long x, int n, RoundingMode mode, StatusFlags flags) {
    return scalbn(x, n, mode, flags);
  }

  public static long scalbln(long x, long n, RoundingMode mode, StatusFlags flags) {
    int clamped = n > Integer.MAX_VALUE
        ? Integer.MAX_VALUE
        : n < Integer.MIN_VALUE ? Integer.MIN_VALUE : (int) n;
    return scalbn(x, clamped, mode, flags);
  }

  public static int ilogb(long x, StatusFlags flags) {
    return BidScale.ilogb64(x, flags);
  }

  public static long logb(long x, StatusFlags flags) {
    return BidScale.logb64(x, flags);
  }

  public static int quantexp(long x) {
    return BidScale.quantexp64(x);
  }

  public static int quantexp(long x, StatusFlags flags) {
    if (!isFinite(x)) {
      flags.raise(StatusFlags.INVALID);
    }
    return BidScale.quantexp64(x);
  }

  public static long llquantexp(long x) {
    return BidScale.quantexp64(x);
  }

  public static long llquantexp(long x, StatusFlags flags) {
    if (!isFinite(x)) {
      flags.raise(StatusFlags.INVALID);
    }
    return BidScale.quantexp64(x);
  }

  public static long quantum(long x) {
    return BidScale.quantum64(x);
  }

  public static long sqrt(long x, RoundingMode mode, StatusFlags flags) {
    return BidSqrt.sqrt64(x, mode, flags);
  }

  public static long cbrt(long x, RoundingMode mode, StatusFlags flags) {
    return BidTranscendental.unary64(x, mode, flags, Dpml::cbrt);
  }

  public static long rem(long x, long y, StatusFlags flags) {
    return BidRem.rem64(x, y, flags);
  }

  public static long fmod(long x, long y, StatusFlags flags) {
    return BidRem.fmod64(x, y, flags);
  }

  public static long fma(long x, long y, long z, RoundingMode mode, StatusFlags flags) {
    return BidFma.fma64(x, y, z, mode, flags);
  }

  public static long nextUp(long x, StatusFlags flags) {
    return BidNext.nextUp64(x, flags);
  }

  public static long nextDown(long x, StatusFlags flags) {
    return BidNext.nextDown64(x, flags);
  }

  public static long nextAfter(long x, long y, StatusFlags flags) {
    return BidNext.nextAfter64(x, y, flags);
  }

  public static long nextToward(
      long x, long targetHi, long targetLo, StatusFlags flags) {
    Bid128 target = Bid128.fromRawBits(targetHi, targetLo);
    if (isNaN(x)) {
      if (target.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      return BidIntegral.canonicalizeNaN64(x, flags);
    }
    if (target.isNaN()) {
      return BidConvert.bid128ToBid64(
          targetHi, targetLo, RoundingMode.TIES_TO_EVEN, flags);
    }
    long[] widened = new long[2];
    BidConvert.bid64ToBid128(x, widened, new StatusFlags());
    Bid128 source = Bid128.fromRawBits(widened[0], widened[1]);
    if (source.quietEqual(target, new StatusFlags())) {
      if (isZero(x) && target.isZero()) {
        return Bid64.finiteRawBits(target.isSigned(), Bid64.biasedExponentBits(x), 0L);
      }
      return x;
    }
    long result = source.quietLess(target, new StatusFlags())
        ? nextUp(x, flags) : nextDown(x, flags);
    if (isInf(result) && isFinite(x)) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
    } else if (isZero(result) || Bid64.fromRawBits(result).isSubnormal()) {
      flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
    }
    return result;
  }

  public static long minnum(long x, long y, StatusFlags flags) {
    return BidMinMax.minnum64(x, y, flags);
  }

  public static long maxnum(long x, long y, StatusFlags flags) {
    return BidMinMax.maxnum64(x, y, flags);
  }

  public static long minnumMag(long x, long y, StatusFlags flags) {
    return BidMinMax.minnumMag64(x, y, flags);
  }

  public static long maxnumMag(long x, long y, StatusFlags flags) {
    return BidMinMax.maxnumMag64(x, y, flags);
  }

  public static long fdim(long x, long y, RoundingMode mode, StatusFlags flags) {
    return BidMinMax.fdim64(x, y, mode, flags);
  }

  public static long toDpd(long x) {
    return BidDpd.bid64ToDpd(x);
  }

  public static long fromDpd(long x) {
    return BidDpd.dpdToBid64(x);
  }

  public static long exp(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Exp.exp(x, mode, flags);
  }

  public static long expm1(long x, RoundingMode mode, StatusFlags flags) {
    return BidTranscendental.unary64(x, mode, flags, Dpml::expm1);
  }

  public static long exp2(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Exp.exp2(x, mode, flags);
  }

  public static long exp10(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Exp.exp10(x, mode, flags);
  }

  public static long log(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Log.log(x, mode, flags);
  }

  public static long log10(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Log.log10(x, mode, flags);
  }

  public static long log2(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Log.log2(x, mode, flags);
  }

  public static long log1p(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Log1p.log1p(x, mode, flags);
  }

  public static long pow(long x, long y, RoundingMode mode, StatusFlags flags) {
    return Bid64Pow.pow(x, y, mode, flags);
  }

  public static long hypot(long x, long y, RoundingMode mode, StatusFlags flags) {
    return BidTranscendental.hypot64(x, y, mode, flags);
  }

  public static long sin(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Trig.sin(x, mode, flags);
  }

  public static long cos(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Trig.cos(x, mode, flags);
  }

  public static long tan(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Trig.tan(x, mode, flags);
  }

  public static long asin(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Domain.asin(x, mode, flags);
  }

  public static long acos(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Domain.acos(x, mode, flags);
  }

  public static long atan(long x, RoundingMode mode, StatusFlags flags) {
    return BidTranscendental.unary64(x, mode, flags, Dpml::atan);
  }

  public static long atan2(long y, long x, RoundingMode mode, StatusFlags flags) {
    return BidTranscendental.binary64(y, x, mode, flags, Dpml::atan2);
  }

  public static long sinh(long x, RoundingMode mode, StatusFlags flags) {
    return BidTranscendental.unary64(x, mode, flags, Dpml::sinh);
  }

  public static long cosh(long x, RoundingMode mode, StatusFlags flags) {
    return BidTranscendental.unary64(x, mode, flags, Dpml::cosh);
  }

  public static long tanh(long x, RoundingMode mode, StatusFlags flags) {
    return BidTranscendental.unary64(x, mode, flags, Dpml::tanh);
  }

  public static long asinh(long x, RoundingMode mode, StatusFlags flags) {
    return BidTranscendental.unary64(x, mode, flags, Dpml::asinh);
  }

  public static long acosh(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Domain.acosh(x, mode, flags);
  }

  public static long atanh(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Domain.atanh(x, mode, flags);
  }

  public static long erf(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Erf.erf(x, mode, flags);
  }

  public static long erfc(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Erf.erfc(x, mode, flags);
  }

  public static long tgamma(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Tgamma.tgamma(x, mode, flags);
  }

  public static long lgamma(long x, RoundingMode mode, StatusFlags flags) {
    return Bid64Lgamma.lgamma(x, mode, flags);
  }

  public static long modf(long x, long[] integralOut, StatusFlags flags) {
    if (isNaN(x)) {
      long nan = BidIntegral.canonicalizeNaN64(x, flags);
      if (integralOut != null && integralOut.length > 0) {
        integralOut[0] = nan;
      }
      return nan;
    }
    if (isInf(x)) {
      if (integralOut != null && integralOut.length > 0) {
        integralOut[0] = (x & Bid64.MASK_SIGN) | Bid64.MASK_INFINITY;
      }
      return Bid64.finiteRawBits(isSigned(x), 767, 0L);
    }
    long integral = roundIntegralZero(x, flags);
    if (integralOut != null && integralOut.length > 0) {
      integralOut[0] = integral;
    }
    if (quietEqual(x, integral, new StatusFlags())) {
      return Bid64.finiteRawBits(isSigned(x), 767, 0L);
    }
    return sub(x, integral, RoundingMode.TOWARD_ZERO, flags);
  }

  public static long frexp(long x, int[] exponentOut, StatusFlags flags) {
    return BidScale.frexp64(x, exponentOut, flags);
  }

  public static int lrint(long x, RoundingMode mode, StatusFlags flags) {
    return (int) BidConvert.toInt64(x, mode, flags, true, 32, true);
  }

  public static long llrint(long x, RoundingMode mode, StatusFlags flags) {
    return BidConvert.toInt64(x, mode, flags, true, 64, true);
  }

  public static int lround(long x, StatusFlags flags) {
    return (int) BidConvert.toInt64(
        x, RoundingMode.TIES_AWAY, flags, true, 32, false);
  }

  public static long llround(long x, StatusFlags flags) {
    return BidConvert.toInt64(x, RoundingMode.TIES_AWAY, flags, true, 64, false);
  }

  public static long integerIndefinite() {
    return INTEGER_INDEFINITE;
  }

  private static int compare(long x, long y, StatusFlags flags, boolean signaling) {
    Bid64 left = Bid64.fromRawBits(x);
    Bid64 right = Bid64.fromRawBits(y);
    if (signaling) {
      if (left.signalingLess(right, flags)) {
        return -1;
      }
      if (left.signalingGreater(right, flags)) {
        return 1;
      }
      if (left.isNaN() || right.isNaN()) {
        return 2;
      }
      return 0;
    }
    if (left.quietLess(right, flags)) {
      return -1;
    }
    if (left.quietGreater(right, flags)) {
      return 1;
    }
    if (left.quietUnordered(right, flags)) {
      return 2;
    }
    return 0;
  }

  private static long withFlags(int[] statusOut, FlagOp op) {
    StatusFlags flags = new StatusFlags();
    long result = op.apply(flags);
    flags.copyTo(statusOut);
    return result;
  }

  @FunctionalInterface
  private interface FlagOp {
    long apply(StatusFlags flags);
  }
}
