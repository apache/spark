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

/** Intel {@code bid128_log1p.c}. */
final class Bid128Log1p {
  private static final Bid128 MINUS_HALF =
      Bid128.fromRawBits(0xb03e_0000_0000_0000L, 5L);
  private static final Bid128 TEN_POW_4464 =
      Bid128.fromRawBits(0x5320_0000_0000_0000L, 1L);
  private static final Bid128 TEN_POW_N4464 =
      Bid128.fromRawBits(0x0d60_0000_0000_0000L, 1L);
  private static final Bid128 NAN =
      Bid128.fromRawBits(0x7c00_0000_0000_0000L, 0L);
  private static final Binary128 C_4464_LN10 =
      Binary128.fromRawBits(0x400c_4135_eb39_29fbL, 0xa719_f2c9_46d2_d728L);

  private Bid128Log1p() {
  }

  static void log1p(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    StatusFlags cmp = new StatusFlags();
    if (x.quietLess(MINUS_HALF, cmp)) {
      long[] y = new long[2];
      Bid128Raw.add(
          hi, lo, Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
          mode, flags, y);
      if (Bid128.fromRawBits(y[0], y[1]).isSigned()) {
        flags.raise(StatusFlags.INVALID);
        DecNum.store128(NAN, out);
        return;
      }
      BidTranscendental.unary128(y[0], y[1], mode, flags, Dpml::log, out);
      return;
    }
    if (x.quietGreater(TEN_POW_4464, cmp)) {
      long[] scaled = new long[2];
      Bid128Raw.mul(
          hi, lo, TEN_POW_N4464.highBits(), TEN_POW_N4464.lowBits(),
          mode, flags, scaled);
      long[] packed = new long[2];
      BidConvert.toBinary128From128(scaled[0], scaled[1], mode, flags, packed);
      Binary128 xd = Binary128.fromRawBits(packed[0], packed[1]);
      org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
      org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
      Binary128 yd = Dpml.log(xd, binaryMode, local);
      yd = Dpml.add(yd, C_4464_LN10, binaryMode, local);
      flags.raise(local.bits());
      BidConvert.fromBinary128To128(yd.highBits(), yd.lowBits(), mode, flags, out);
      return;
    }
    Bid128 abs = Bid128.fromRawBits(hi & ~Bid128.MASK_SIGN, lo);
    if (abs.quietLess(TEN_POW_N4464, cmp)) {
      Bid128Raw.fma(hi, lo, hi ^ Bid128.MASK_SIGN, lo, hi, lo, mode, flags, out);
      return;
    }
    BidTranscendental.unary128(hi, lo, mode, flags, Dpml::log1p, out);
  }
}
