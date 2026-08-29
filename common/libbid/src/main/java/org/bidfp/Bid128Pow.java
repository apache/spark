/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

import org.bidfp.binary128.Binary128;
import org.bidfp.binary128.Dpml;

/** Intel {@code bid128_pow.c} specials and integer exponent; else DPML. */
final class Bid128Pow {
  private static final Bid128 ZERO =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 0L);
  private static final Bid128 ONE =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 1L);
  private static final Bid128 NAN =
      Bid128.fromRawBits(0x7c00_0000_0000_0000L, 0L);
  private static final Bid128 INF =
      Bid128.fromRawBits(Bid128.MASK_INFINITY, 0L);

  private Bid128Pow() {
  }

  static void pow(
      long xh, long xl, long yh, long yl,
      RoundingMode mode, StatusFlags flags, long[] out) {
    int smallExponent = smallPositiveInteger(yh, yl);
    if (smallExponent >= 2
        && isFinite(xh)
        && !isZero(xh, xl)) {
      powSmallInteger(xh, xl, smallExponent, mode, flags, out);
      return;
    }
    Bid128 x = Bid128.fromRawBits(xh, xl);
    Bid128 y = Bid128.fromRawBits(yh, yl);
    if (x.isSignalingNaN() || y.isSignalingNaN()) {
      flags.raise(StatusFlags.INVALID);
    }
    if (y.isZero() && !x.isSignalingNaN()) {
      DecNum.store128(ONE, out);
      return;
    }
    if (x.quietEqual(ONE, new StatusFlags()) && !x.isSignalingNaN()) {
      DecNum.store128(ONE, out);
      return;
    }
    if (x.isNaN()) {
      Bid128Libm.canonNan(xh, xl, flags, out);
      return;
    }
    if (y.isNaN()) {
      Bid128Libm.canonNan(yh, yl, flags, out);
      return;
    }
    long[] yInt = new long[2];
    Bid128Raw.roundIntegralNearestEven(yh, yl, new StatusFlags(), yInt);
    boolean isInt = Bid128.fromRawBits(yInt[0], yInt[1]).quietEqual(y, new StatusFlags());
    boolean odd = false;
    if (isInt) {
      int e = (int) ((yInt[0] >>> 49) & 0x3fff);
      if (e == 6176 && (yInt[1] & 1L) != 0L) {
        odd = true;
      }
    }
    if (y.isInfinite()) {
      Bid128 abs = Bid128.fromRawBits(xh & ~Bid128.MASK_SIGN, xl);
      if (abs.quietEqual(ONE, new StatusFlags())) {
        DecNum.store128(ONE, out);
        return;
      }
      boolean less = abs.quietLess(ONE, new StatusFlags());
      Bid128 result = less == y.isSigned() ? INF : ZERO;
      DecNum.store128(result, out);
      return;
    }
    if (x.isInfinite()) {
      Bid128 result = y.isSigned() ? ZERO : INF;
      if (odd && x.isSigned()) {
        result = Bid128.fromRawBits(result.highBits() ^ Bid128.MASK_SIGN, result.lowBits());
      }
      DecNum.store128(result, out);
      return;
    }
    if (x.isZero()) {
      Bid128 result;
      if (y.isSigned()) {
        flags.raise(StatusFlags.DIVIDE_BY_ZERO);
        result = INF;
      } else {
        result = ZERO;
      }
      if (odd && x.isSigned()) {
        result = Bid128.fromRawBits(result.highBits() ^ Bid128.MASK_SIGN, result.lowBits());
      }
      DecNum.store128(result, out);
      return;
    }
    if (x.isSigned() && !isInt) {
      flags.raise(StatusFlags.INVALID);
      DecNum.store128(NAN, out);
      return;
    }
    StatusFlags intFlags = new StatusFlags();
    int exactY = Bid128Raw.toInt32(
        yh, yl, RoundingMode.TIES_TO_EVEN, intFlags, true);
    if ((intFlags.bits() & (StatusFlags.INEXACT | StatusFlags.INVALID)) == 0
        && exactY != Integer.MIN_VALUE) {
      if (exactY != 0) {
        powInteger(xh, xl, exactY, mode, flags, out);
        return;
      }
    }
    long[] xHiBits = new long[2];
    long[] xLoBits = new long[2];
    long[] yHiBits = new long[2];
    long[] yLoBits = new long[2];
    BidBinary128Convert.toBinary128TwoPart(
        xh & ~Bid128.MASK_SIGN, xl, xHiBits, xLoBits);
    BidBinary128Convert.toBinary128TwoPart(yh, yl, yHiBits, yLoBits);
    Binary128 xHi = Binary128.fromRawBits(xHiBits[0], xHiBits[1]);
    Binary128 xLo = Binary128.fromRawBits(xLoBits[0], xLoBits[1]);
    Binary128 yHi = Binary128.fromRawBits(yHiBits[0], yHiBits[1]);
    Binary128 yLo = Binary128.fromRawBits(yLoBits[0], yLoBits[1]);
    if (xHi.isZero() || xHi.isInfinite() || yHi.isInfinite()) {
      wideRangePow(x, y, odd, mode, flags, out);
      return;
    }
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 result = Dpml.pow(xHi, yHi, binaryMode, local);
    Binary128 delta = Dpml.mul(
        yHi, Dpml.div(xLo, xHi, binaryMode, local), binaryMode, local);
    delta = Dpml.add(
        delta,
        Dpml.mul(yLo, Dpml.log(xHi, binaryMode, local), binaryMode, local),
        binaryMode,
        local);
    result = Dpml.add(
        result, Dpml.mul(result, delta, binaryMode, local), binaryMode, local);
    flags.raise(local.bits());
    BidConvert.fromBinary128To128(
        result.highBits(), result.lowBits(), mode, flags, out);
    if (odd && x.isSigned()) {
      out[0] ^= Bid128.MASK_SIGN;
    }
  }

  private static void wideRangePow(
      Bid128 x,
      Bid128 y,
      boolean odd,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    long[] integerBits = new long[2];
    Bid128Raw.roundIntegralNearestEven(
        y.highBits(), y.lowBits(), new StatusFlags(), integerBits);
    StatusFlags intFlags = new StatusFlags();
    int integer = Bid128Raw.toInt32(
        integerBits[0], integerBits[1],
        RoundingMode.TIES_TO_EVEN, intFlags, false);
    if ((intFlags.bits() & StatusFlags.INVALID) == 0
        && integer != Integer.MIN_VALUE) {
      long[] fraction = new long[2];
      long[] base = new long[2];
      long[] log = new long[2];
      long[] exponent = new long[2];
      long[] correction = new long[2];
      Bid128Raw.sub(
          y.highBits(), y.lowBits(), integerBits[0], integerBits[1],
          mode, flags, fraction);
      if (integer == 0) {
        DecNum.store128(ONE, base);
      } else {
        powInteger(
            x.highBits(), x.lowBits(), integer, mode, flags, base);
      }
      Bid128Log.log(
          x.highBits() & ~Bid128.MASK_SIGN, x.lowBits(), mode, flags, log);
      Bid128Raw.mul(
          fraction[0], fraction[1], log[0], log[1],
          mode, flags, exponent);
      Bid128Exp.exp(exponent[0], exponent[1], mode, flags, correction);
      Bid128Raw.mul(
          base[0], base[1], correction[0], correction[1],
          mode, flags, out);
      return;
    }
    Bid128 abs = Bid128.fromRawBits(x.highBits() & ~Bid128.MASK_SIGN, x.lowBits());
    boolean grows = abs.quietGreater(ONE, new StatusFlags()) != y.isSigned();
    if (grows) {
      out[0] = Bid128.MASK_INFINITY;
      out[1] = 0L;
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
    } else {
      out[0] = 0L;
      out[1] = 0L;
      flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
    }
    if (odd && x.isSigned()) {
      out[0] ^= Bid128.MASK_SIGN;
    }
  }

  private static void powInteger(
      long xh,
      long xl,
      int signedExponent,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    int exponent = Math.abs(signedExponent);
    long[] result = {ONE.highBits(), ONE.lowBits()};
    long[] power = {xh, xl};
    while (exponent != 0) {
      if ((exponent & 1) != 0) {
        Bid128Raw.mul(
            result[0], result[1], power[0], power[1],
            mode, flags, result);
      }
      exponent >>>= 1;
      if (exponent != 0) {
        Bid128Raw.mul(
            power[0], power[1], power[0], power[1],
            mode, flags, power);
      }
    }
    if (signedExponent < 0) {
      Bid128Raw.div(
          ONE.highBits(), ONE.lowBits(), result[0], result[1],
          mode, flags, out);
    } else {
      out[0] = result[0];
      out[1] = result[1];
    }
  }

  private static int smallPositiveInteger(long high, long low) {
    if (!isFinite(high)
        || (high & Bid128.MASK_SIGN) != 0
        || biasedExponent(high) != 6176
        || (high & Bid128.MASK_COEFFICIENT) != 0
        || low < 2L
        || low > 5L) {
      return -1;
    }
    return (int) low;
  }

  private static boolean isFinite(long high) {
    return (high & Bid128.MASK_INFINITY) != Bid128.MASK_INFINITY;
  }

  private static boolean isZero(long high, long low) {
    return !Bid128.isCanonicalFinite(high, low)
        || ((high & Bid128.MASK_COEFFICIENT) | low) == 0L;
  }

  private static int biasedExponent(long high) {
    if ((high & Bid128.MASK_STEERING_BITS) == Bid128.MASK_STEERING_BITS) {
      return (int) ((high >>> 47) & 0x3fffL);
    }
    return (int) ((high & Bid128.MASK_EXPONENT) >>> 49);
  }

  private static void powSmallInteger(
      long high, long low, int exponent,
      RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128Raw.mul(high, low, high, low, mode, flags, out);
    if (exponent == 2) {
      return;
    }
    if (exponent == 3) {
      Bid128Raw.mul(out[0], out[1], high, low, mode, flags, out);
      return;
    }
    Bid128Raw.mul(out[0], out[1], out[0], out[1], mode, flags, out);
    if (exponent == 5) {
      Bid128Raw.mul(out[0], out[1], high, low, mode, flags, out);
    }
  }
}
