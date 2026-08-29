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

/** Intel {@code bid128_lgamma.c}: tiny -log|x|, huge Stirling, poles. */
final class Bid128Lgamma {
  private static final Bid128 INF =
      Bid128.fromRawBits(Bid128.MASK_INFINITY, 0L);
  private static final Bid128 HALF =
      Bid128.fromRawBits(0x303e_0000_0000_0000L, 5L);
  private static final Bid128 LOG_2PI_OVER_2 =
      Bid128.fromRawBits(0x2ffd_c512_596b_f2beL, 0x8512_e0b1_f71b_1870L);
  private static final Bid128 MAX =
      Bid128.fromRawBits(0x5fff_ed09_bead_87c0L, 0x378d_8e63_ffff_ffffL);
  private static final Bid128 ROUND_TO_MAX_BOUNDARY =
      Bid128.fromRawBits(0x5ff7_5cb6_4c34_c034L, 0xb482_9613_f77e_c7e2L);
  private static final Bid128 OVERFLOW_BOUNDARY =
      Bid128.fromRawBits(0x5ff7_5cb6_4c34_c034L, 0xb482_9613_f77e_c7e3L);
  private static final Binary128 C_M1E34 =
      Binary128.fromRawBits(0xc06f_ed09_defd_561eL, 0x75b2_90c5_1000_0000L);
  private static final Binary128 C_1E34 =
      Binary128.fromRawBits(0x406f_ed09_defd_561eL, 0x75b2_90c5_1000_0000L);
  private static final Binary128 C_HALF =
      Binary128.fromRawBits(0x3ffe_0000_0000_0000L, 0L);
  private static final Binary128 C_ONE =
      Binary128.fromRawBits(0x3fff_0000_0000_0000L, 0L);
  private static final Binary128 C_MINUS_ONE =
      Binary128.fromRawBits(0xbfff_0000_0000_0000L, 0L);
  private static final Binary128 C_LOG_PI =
      Binary128.fromRawBits(0x3fff_250d_048e_7a1bL, 0xd0bd_5f95_6c6a_843fL);
  private static final Binary128 C_PI =
      Binary128.fromRawBits(0x4000_921f_b544_42d1L, 0x8469_898c_c517_01b8L);
  private static final Binary128 C_1EM100 =
      Binary128.fromRawBits(0x3eb2_bff2_ee48_e052L, 0xfd7a_b2f0_fc57_2779L);

  private Bid128Lgamma() {
  }

  static void lgamma(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (x.isZero()) {
      flags.raise(StatusFlags.DIVIDE_BY_ZERO);
      DecNum.store128(INF, out);
      return;
    }
    if (x.isInfinite()) {
      DecNum.store128(INF, out);
      return;
    }
    long[] hiPart = new long[2];
    long[] loPart = new long[2];
    BidBinary128Convert.toBinary128TwoPart(hi, lo, hiPart, loPart);
    Binary128 xdHi = Binary128.fromRawBits(hiPart[0], hiPart[1]);
    if (Bid128Libm.lessEqual(xdHi, C_M1E34)) {
      flags.raise(StatusFlags.DIVIDE_BY_ZERO);
      DecNum.store128(INF, out);
      return;
    }
    if (!x.isSigned() && x.quietGreaterEqual(OVERFLOW_BOUNDARY, new StatusFlags())) {
      Bid128Raw.mul(
          MAX.highBits(), MAX.lowBits(), MAX.highBits(), MAX.lowBits(),
          mode, flags, out);
      return;
    }
    if (!x.isSigned() && x.quietGreaterEqual(
        ROUND_TO_MAX_BOUNDARY, new StatusFlags())) {
      flags.raise(StatusFlags.INEXACT);
      DecNum.store128(MAX, out);
      return;
    }
    if (!Bid128Libm.less(xdHi, C_1E34) || x.biasedExponent() - 6176 >= 34) {
      long[] lg1 = new long[2];
      long[] lg2 = new long[2];
      long[] lg3 = new long[2];
      Bid128Raw.sub(
          hi, lo, HALF.highBits(), HALF.lowBits(), mode, flags, lg1);
      Bid128Log.log(hi, lo, mode, flags, lg2);
      Bid128Raw.sub(
          LOG_2PI_OVER_2.highBits(), LOG_2PI_OVER_2.lowBits(),
          hi, lo, mode, flags, lg3);
      Bid128Raw.fma(
          lg1[0], lg1[1], lg2[0], lg2[1], lg3[0], lg3[1], mode, flags, out);
      return;
    }
    if (Bid128Libm.lessEqual(xdHi, C_HALF)) {
      long[] xInt = new long[2];
      Bid128Raw.roundIntegralNearestEven(hi, lo, new StatusFlags(), xInt);
      if (Bid128.fromRawBits(xInt[0], xInt[1]).quietEqual(x, new StatusFlags())) {
        flags.raise(StatusFlags.DIVIDE_BY_ZERO);
        DecNum.store128(INF, out);
        return;
      }
    }
    if (!Bid128Libm.less(xdHi, C_HALF)) {
      Binary128 xdLo = Binary128.fromRawBits(loPart[0], loPart[1]);
      org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
      org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
      Binary128 yd = interpolatedLgamma(xdHi, xdLo, binaryMode, local);
      flags.raise(local.bits());
      BidConvert.fromBinary128To128(
          yd.highBits(), yd.lowBits(), mode, flags, out);
      return;
    }
    if (Bid128Libm.lessEqual(xdHi.abs(), C_1EM100)) {
      long[] logAbs = new long[2];
      Bid128Log.log(hi & ~Bid128.MASK_SIGN, lo, mode, flags, logAbs);
      out[0] = logAbs[0] ^ Bid128.MASK_SIGN;
      out[1] = logAbs[1];
      return;
    }
    long[] xInt = new long[2];
    long[] xFrac = new long[2];
    Bid128Raw.roundIntegralNearestEven(hi, lo, new StatusFlags(), xInt);
    Bid128Raw.sub(hi, lo, xInt[0], xInt[1], mode, flags, xFrac);
    Binary128 originalLo = Binary128.fromRawBits(loPart[0], loPart[1]);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 transformedHi = Dpml.sub(C_ONE, xdHi, binaryMode, local);
    Binary128 remainder;
    if (Bid128Libm.lessEqual(xdHi, C_MINUS_ONE)) {
      remainder = Dpml.add(transformedHi, xdHi, binaryMode, local);
      remainder = Dpml.sub(C_ONE, remainder, binaryMode, local);
    } else {
      remainder = Dpml.sub(C_ONE, transformedHi, binaryMode, local);
      remainder = Dpml.sub(remainder, xdHi, binaryMode, local);
    }
    Binary128 transformedLo = Dpml.sub(remainder, originalLo, binaryMode, local);
    Binary128 yd = interpolatedLgamma(
        transformedHi, transformedLo, binaryMode, local);
    long[] fracPacked = new long[2];
    BidConvert.toBinary128From128(
        xFrac[0], xFrac[1], mode, flags, fracPacked);
    Binary128 fd = Binary128.fromRawBits(fracPacked[0], fracPacked[1]);
    Binary128 rt = Dpml.mul(C_PI, fd, binaryMode, local);
    rt = Dpml.log(Dpml.sin(rt, binaryMode, local).abs(), binaryMode, local);
    rt = Dpml.sub(C_LOG_PI, rt, binaryMode, local);
    yd = Dpml.sub(rt, yd, binaryMode, local);
    flags.raise(local.bits());
    BidConvert.fromBinary128To128(
        yd.highBits(), yd.lowBits(), mode, flags, out);
  }

  static Binary128 positiveBinaryLgamma(
      long hi,
      long lo,
      org.bidfp.binary128.RoundingMode mode,
      org.bidfp.binary128.StatusFlags flags) {
    long[] hiPart = new long[2];
    long[] loPart = new long[2];
    BidBinary128Convert.toBinary128TwoPart(hi, lo, hiPart, loPart);
    return interpolatedLgamma(
        Binary128.fromRawBits(hiPart[0], hiPart[1]),
        Binary128.fromRawBits(loPart[0], loPart[1]),
        mode,
        flags);
  }

  static Binary128[] positiveBinaryLgammaTwoPart(
      long hi,
      long lo,
      org.bidfp.binary128.StatusFlags flags) {
    long[] hiPart = new long[2];
    long[] loPart = new long[2];
    BidBinary128Convert.toBinary128TwoPart(hi, lo, hiPart, loPart);
    return Dpml.positiveLgammaTwoPart(
        Binary128.fromRawBits(hiPart[0], hiPart[1]),
        Binary128.fromRawBits(loPart[0], loPart[1]),
        flags);
  }

  private static Binary128 interpolatedLgamma(
      Binary128 xHi,
      Binary128 xLo,
      org.bidfp.binary128.RoundingMode mode,
      org.bidfp.binary128.StatusFlags flags) {
    Binary128 xUp = Binary128.fromRawBits(
        xHi.highBits() + (xHi.lowBits() == -1L ? 1L : 0L),
        xHi.lowBits() + 1L);
    Binary128 y = Dpml.lgamma(xHi, mode, flags);
    Binary128 z = Dpml.lgamma(xUp, mode, flags);
    Binary128 slope = Dpml.sub(z, y, mode, flags);
    Binary128 width = Dpml.sub(xUp, xHi, mode, flags);
    Binary128 fraction = Dpml.div(xLo, width, mode, flags);
    return Dpml.add(y, Dpml.mul(fraction, slope, mode, flags), mode, flags);
  }
}
