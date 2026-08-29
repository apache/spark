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

/** Intel {@code bid128_asinh.c}: huge-exponent ln(2x) rewrite. */
final class Bid128Asinh {
  private static final Binary128 LN10 =
      Binary128.fromRawBits(0x4000_26bb_1bbb_5551L, 0x582d_d4ad_ac57_05a6L);

  private Bid128Asinh() {
  }

  static void asinh(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (x.isInfinite()) {
      out[0] = (hi & Bid128.MASK_SIGN) | Bid128.MASK_INFINITY;
      out[1] = 0L;
      return;
    }
    if (x.isZero()) {
      out[0] = hi;
      out[1] = lo;
      return;
    }
    if (Bid128Libm.tinyOddFma(hi, lo, mode, flags, out)) {
      return;
    }
    int exponent = x.biasedExponent();
    if (exponent > 6176 + 34) {
      UInt128 coeff = x.coefficient();
      Bid128 xn = Bid128.rawFinite(false, 6176, coeff.high(), coeff.low());
      long[] packed = new long[2];
      BidConvert.toBinary128From128(
          xn.highBits(), xn.lowBits(), mode, flags, packed);
      org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
      org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
      Binary128 xq = Binary128.fromRawBits(packed[0], packed[1]);
      xq = Dpml.add(xq, xq, binaryMode, local);
      long[] expBid = new long[2];
      Bid128Raw.fromInt32(exponent - 6176, expBid);
      BidConvert.toBinary128From128(
          expBid[0], expBid[1], RoundingMode.TIES_TO_EVEN, new StatusFlags(),
          packed);
      Binary128 yq = Binary128.fromRawBits(packed[0], packed[1]);
      Binary128 rq = Dpml.mul(yq, LN10, binaryMode, local);
      Binary128 rt = Dpml.log(xq, binaryMode, local);
      rq = Dpml.add(rq, rt, binaryMode, local);
      flags.raise(local.bits());
      BidConvert.fromBinary128To128(
          rq.highBits(), rq.lowBits(), mode, flags, out);
      out[0] |= hi & Bid128.MASK_SIGN;
      return;
    }
    BidTranscendental.unary128(hi, lo, mode, flags, Dpml::asinh, out);
  }
}
