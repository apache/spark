/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

import org.bidfp.binary128.Binary128;

/** Intel {@code bid128_tgamma.c}: poles, exp(lgamma), odd-interval sign. */
final class Bid128Tgamma {
  private static final Bid128 NAN =
      Bid128.fromRawBits(0x7c00_0000_0000_0000L, 0L);
  private static final Bid128 INF =
      Bid128.fromRawBits(Bid128.MASK_INFINITY, 0L);
  private static final Bid128 ZERO =
      Bid128.fromRawBits(0L, 0L);
  private static final Bid128 HALF =
      Bid128.fromRawBits(0x303e_0000_0000_0000L, 5L);
  private static final Bid128 SIXTEEN =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 16L);
  private static final Bid128 THREE_THOUSAND =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 3000L);
  private static final Bid128 SHIFTER =
      Bid128.fromRawBits(0x3040_629b_8c89_1b26L, 0x7182_b614_0000_0000L);

  private Bid128Tgamma() {
  }

  static void tgamma(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (x.isZero()) {
      flags.raise(StatusFlags.DIVIDE_BY_ZERO);
      out[0] = Bid128.MASK_INFINITY ^ (hi & Bid128.MASK_SIGN);
      out[1] = 0L;
      return;
    }
    if (x.isInfinite()) {
      if (x.isSigned()) {
        flags.raise(StatusFlags.INVALID);
        DecNum.store128(NAN, out);
      } else {
        DecNum.store128(INF, out);
      }
      return;
    }
    Bid128 tiny = Bid128.fromRawBits(0x3018_0000_0000_0000L, 1L);
    if (Bid128.fromRawBits(hi & ~Bid128.MASK_SIGN, lo)
        .quietLess(tiny, new StatusFlags())) {
      Bid128Raw.div(
          Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
          hi, lo, mode, flags, out);
      Bid128Raw.sub(
          out[0], out[1], Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
          mode, flags, out);
      return;
    }
    long[] xFrac = null;
    if (x.quietLessEqual(ZERO, new StatusFlags())) {
      long[] xInt = new long[2];
      xFrac = new long[2];
      Bid128Raw.roundIntegralNearestEven(hi, lo, new StatusFlags(), xInt);
      Bid128Raw.sub(hi, lo, xInt[0], xInt[1], mode, flags, xFrac);
      if (Bid128.fromRawBits(xFrac[0], xFrac[1]).isZero()) {
        flags.raise(StatusFlags.INVALID);
        DecNum.store128(NAN, out);
        return;
      }
    }
    if (x.isSigned()
        && xFrac != null
        && Bid128.fromRawBits(xFrac[0] & ~Bid128.MASK_SIGN, xFrac[1])
            .quietLess(tiny, new StatusFlags())) {
      Bid128Raw.div(
          Bid128Libm.ONE.highBits() | Bid128.MASK_SIGN, Bid128Libm.ONE.lowBits(),
          xFrac[0], xFrac[1], mode, flags, out);
      Bid128Raw.sub(
          out[0], out[1], Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
          mode, flags, out);
      return;
    }
    if (!x.isSigned()
        && x.quietGreaterEqual(SIXTEEN, new StatusFlags())
        && x.quietLess(THREE_THOUSAND, new StatusFlags())) {
      org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
      Binary128[] lgamma = Bid128Lgamma.positiveBinaryLgammaTwoPart(
          hi, lo, local);
      flags.raise(local.bits());
      Bid128Exp.expBinaryTwoPart(
          lgamma[0], lgamma[1], mode, flags, out);
      return;
    }
    if (!x.isSigned()
        && x.quietGreaterEqual(HALF, new StatusFlags())
        && x.quietLess(SIXTEEN, new StatusFlags())) {
      org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
      org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
      Binary128 lgamma = Bid128Lgamma.positiveBinaryLgamma(
          hi, lo, binaryMode, local);
      flags.raise(local.bits());
      Bid128Exp.expBinary(lgamma, mode, flags, out);
      return;
    }
    long[] y = new long[2];
    Bid128Raw.lgamma(hi, lo, mode, flags, y);
    Bid128Exp.exp(y[0], y[1], mode, flags, out);
    if (Bid128.fromRawBits(out[0], out[1]).isNaN() || !x.isSigned()) {
      return;
    }
    long[] xInt = new long[2];
    Bid128Raw.roundIntegralZero(hi, lo, new StatusFlags(), xInt);
    int e = (int) ((xInt[0] >>> 49) & 0x3fff);
    if (e <= 6176) {
      if (e < 6176) {
        Bid128Raw.add(
            SHIFTER.highBits(), SHIFTER.lowBits(),
            xInt[0], xInt[1], mode, flags, xInt);
      }
      if ((xInt[1] & 1L) == 0L) {
        out[0] ^= Bid128.MASK_SIGN;
      }
    }
  }

}
