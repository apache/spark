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

/** Intel {@code bid128_exp2.c}: 25000 clamps, 2-part + 11000 shift. */
final class Bid128Exp2 {
  private static final Bid128 EXP2_11000 =
      Bid128.fromRawBits(0x49dc_6965_e972_d2c8L, 0x910b_e340_7d25_b9c8L);
  private static final Bid128 EXP2_M11000 =
      Bid128.fromRawBits(0x161e_e6a2_f56f_0580L, 0x0555_ddab_03e9_e679L);
  private static final Bid128 ZERO =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 0L);
  private static final Bid128 INF =
      Bid128.fromRawBits(Bid128.MASK_INFINITY, 0L);
  private static final Bid128 ONE =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 1L);
  private static final Bid128 C_25000 =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 0x61a8L);
  private static final Bid128 C_N25000 =
      Bid128.fromRawBits(0xb040_0000_0000_0000L, 0x61a8L);
  private static final Bid128 TEN_POW_N6000 =
      Bid128.fromRawBits(0x0160_0000_0000_0000L, 1L);
  private static final Binary128 F128_11000 =
      Binary128.fromRawBits(0x400c_57c0_0000_0000L, 0L);
  private static final Binary128 F128_NEG_11000 =
      Binary128.fromRawBits(0xc00c_57c0_0000_0000L, 0L);
  private static final Binary128 LN2 =
      Binary128.fromRawBits(0x3ffe_62e4_2fef_a39eL, 0xf357_93c7_6730_07e6L);

  private Bid128Exp2() {
  }

  static void exp2(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (x.isInfinite()) {
      DecNum.store128(x.isSigned() ? ZERO : INF, out);
      return;
    }
    if (x.isZero()) {
      DecNum.store128(ONE, out);
      return;
    }
    if (x.quietGreater(C_25000, new StatusFlags())) {
      Bid128Raw.mul(
          EXP2_11000.highBits(), EXP2_11000.lowBits(),
          EXP2_11000.highBits(), EXP2_11000.lowBits(),
          mode, flags, out);
      return;
    }
    if (x.quietLess(C_N25000, new StatusFlags())) {
      Bid128Raw.mul(
          TEN_POW_N6000.highBits(), TEN_POW_N6000.lowBits(),
          TEN_POW_N6000.highBits(), TEN_POW_N6000.lowBits(),
          mode, flags, out);
      return;
    }
    long[] nq = new long[2];
    long[] mq = new long[2];
    BidBinary128Convert.toBinary128TwoPart(hi, lo, nq, mq);
    Binary128 high = Binary128.fromRawBits(nq[0], nq[1]);
    Binary128 low = Binary128.fromRawBits(mq[0], mq[1]);
    if (high.isZero()) {
      long[] ln2 = new long[2];
      BidConvert.fromBinary128To128(
          LN2.highBits(), LN2.lowBits(), RoundingMode.TIES_TO_EVEN,
          new StatusFlags(), ln2);
      Bid128Raw.fma(
          hi, lo, ln2[0], ln2[1],
          ONE.highBits(), ONE.lowBits(), mode, flags, out);
      return;
    }
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    if (Bid128Libm.greater(high, F128_11000)) {
      high = Dpml.sub(high, F128_11000, binaryMode, local);
      Binary128 exp = combine(high, low, binaryMode, local);
      flags.raise(local.bits());
      BidConvert.fromBinary128To128(
          exp.highBits(), exp.lowBits(), mode, flags, out);
      Bid128Raw.mul(
          out[0], out[1], EXP2_11000.highBits(), EXP2_11000.lowBits(),
          mode, flags, out);
    } else if (Bid128Libm.less(high, F128_NEG_11000)) {
      high = Dpml.add(high, F128_11000, binaryMode, local);
      Binary128 exp = combine(high, low, binaryMode, local);
      flags.raise(local.bits());
      BidConvert.fromBinary128To128(
          exp.highBits(), exp.lowBits(), mode, flags, out);
      Bid128Raw.mul(
          out[0], out[1], EXP2_M11000.highBits(), EXP2_M11000.lowBits(),
          mode, flags, out);
    } else {
      Binary128 exp = combine(high, low, binaryMode, local);
      flags.raise(local.bits());
      BidConvert.fromBinary128To128(
          exp.highBits(), exp.lowBits(), mode, flags, out);
    }
  }

  private static Binary128 combine(
      Binary128 nq,
      Binary128 mq,
      org.bidfp.binary128.RoundingMode mode,
      org.bidfp.binary128.StatusFlags status) {
    Binary128 rq = Dpml.exp2(nq, mode, status);
    Binary128 rt = Dpml.mul(rq, LN2, mode, status);
    rt = Dpml.mul(rt, mq, mode, status);
    return Dpml.add(rq, rt, mode, status);
  }
}
