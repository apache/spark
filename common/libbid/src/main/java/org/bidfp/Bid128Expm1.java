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

/** Intel {@code bid128_expm1.c}. */
final class Bid128Expm1 {
  private static final Binary128 ONE_E_M40 =
      Binary128.fromRawBits(0x3f7a_16c2_6277_7579L, 0xc58c_4647_5896_767bL);
  private static final Binary128 ONE =
      Binary128.fromRawBits(0x3fff_0000_0000_0000L, 0L);

  private Bid128Expm1() {
  }

  static void expm1(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    long[] packed = new long[2];
    BidConvert.toBinary128From128(hi, lo, mode, flags, packed);
    Binary128 xd = Binary128.fromRawBits(packed[0], packed[1]);
    Binary128 abs = xd.isSigned() ? xd.negate() : xd;
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    if (Bid128Libm.lessEqual(abs, ONE_E_M40)) {
      Bid128 x = Bid128.fromRawBits(hi, lo);
      if (x.isZero()) {
        Bid128Raw.mul(
            hi, lo, Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
            mode, flags, out);
      } else {
        Bid128Raw.fma(hi, lo, hi, lo, hi, lo, mode, flags, out);
      }
      return;
    }
    if (Bid128Libm.lessEqual(xd, ONE)) {
      Binary128 yd = Dpml.expm1(xd, binaryMode, local);
      flags.raise(local.bits());
      BidConvert.fromBinary128To128(yd.highBits(), yd.lowBits(), mode, flags, out);
      return;
    }
    long[] exp = new long[2];
    Bid128Exp.exp(hi, lo, mode, flags, exp);
    Bid128Raw.sub(
        exp[0], exp[1],
        Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
        mode, flags, out);
  }
}
