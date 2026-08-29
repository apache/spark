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

/** Intel {@code bid128_log{,10,2}.c}: 10^(+/-4464) scale and near-1 correction. */
final class Bid128Log {
  enum Kind { LN, LOG10, LOG2 }

  private static final Bid128 TEN_POW_4464 =
      Bid128.fromRawBits(0x5320_0000_0000_0000L, 1L);
  private static final Bid128 TEN_POW_N4464 =
      Bid128.fromRawBits(0x0d60_0000_0000_0000L, 1L);
  private static final Bid128 NEG_INF =
      Bid128.fromRawBits(0xf800_0000_0000_0000L, 0L);
  private static final Bid128 NAN =
      Bid128.fromRawBits(0x7c00_0000_0000_0000L, 0L);
  private static final Binary128 C_ONE =
      Binary128.fromRawBits(0x3fff_0000_0000_0000L, 0L);
  private static final Binary128 C_HALF =
      Binary128.fromRawBits(0x3ffe_0000_0000_0000L, 0L);
  private static final Binary128 C_4464_LN10 =
      Binary128.fromRawBits(0x400c_4135_eb39_29fbL, 0xa719_f2c9_46d2_d728L);
  private static final Binary128 C_INV_LOG10 =
      Binary128.fromRawBits(0x3ffd_bcb7_b152_6e50L, 0xe32a_6ab7_555f_5a68L);
  private static final Binary128 C_4464 =
      Binary128.fromRawBits(0x400b_1700_0000_0000L, 0L);
  private static final Binary128 C_4464_LOG2_10 =
      Binary128.fromRawBits(0x400c_cf68_b235_3912L, 0x08f6_437d_d460_7b55L);
  private static final Binary128 INV_LN2 =
      Binary128.fromRawBits(0x3fff_7154_7652_b82fL, 0xe177_7d0f_fda0_d23aL);

  private Bid128Log() {
  }

  static void log(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    evaluate(hi, lo, mode, flags, Kind.LN, out);
  }

  static void log10(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    evaluate(hi, lo, mode, flags, Kind.LOG10, out);
  }

  static void log2(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    evaluate(hi, lo, mode, flags, Kind.LOG2, out);
  }

  private static void evaluate(
      long hi, long lo, RoundingMode mode, StatusFlags flags, Kind kind,
      long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (x.isZero()) {
      flags.raise(StatusFlags.DIVIDE_BY_ZERO);
      DecNum.store128(NEG_INF, out);
      return;
    }
    if (x.isSigned()) {
      flags.raise(StatusFlags.INVALID);
      DecNum.store128(NAN, out);
      return;
    }
    StatusFlags cmp = new StatusFlags();
    if (x.quietGreater(TEN_POW_4464, cmp)) {
      long[] scaled = new long[2];
      Bid128Raw.mul(
          hi, lo,
          TEN_POW_N4464.highBits(), TEN_POW_N4464.lowBits(),
          mode, flags, scaled);
      applyScale(scaled[0], scaled[1], true, kind, mode, flags, out);
      return;
    }
    if (x.quietLess(TEN_POW_N4464, cmp)) {
      long[] scaled = new long[2];
      Bid128Raw.mul(
          hi, lo,
          TEN_POW_4464.highBits(), TEN_POW_4464.lowBits(),
          mode, flags, scaled);
      applyScale(scaled[0], scaled[1], false, kind, mode, flags, out);
      return;
    }
    applyNearOne(hi, lo, kind, mode, flags, out);
  }

  private static void applyScale(
      long hi, long lo, boolean add, Kind kind,
      RoundingMode mode, StatusFlags flags, long[] out) {
    long[] packed = new long[2];
    BidConvert.toBinary128From128(hi, lo, mode, flags, packed);
    Binary128 xq = Binary128.fromRawBits(packed[0], packed[1]);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 rq;
    if (kind == Kind.LOG10) {
      rq = Dpml.log(xq, binaryMode, local);
      rq = Dpml.mul(rq, C_INV_LOG10, binaryMode, local);
      rq = add
          ? Dpml.add(rq, C_4464, binaryMode, local)
          : Dpml.sub(rq, C_4464, binaryMode, local);
    } else if (kind == Kind.LOG2) {
      rq = Dpml.log2(xq, binaryMode, local);
      rq = add
          ? Dpml.add(rq, C_4464_LOG2_10, binaryMode, local)
          : Dpml.sub(rq, C_4464_LOG2_10, binaryMode, local);
    } else {
      rq = Dpml.log(xq, binaryMode, local);
      rq = add
          ? Dpml.add(rq, C_4464_LN10, binaryMode, local)
          : Dpml.sub(rq, C_4464_LN10, binaryMode, local);
    }
    flags.raise(local.bits());
    BidConvert.fromBinary128To128(rq.highBits(), rq.lowBits(), mode, flags, out);
  }

  private static void applyNearOne(
      long hi, long lo, Kind kind, RoundingMode mode, StatusFlags flags,
      long[] out) {
    long[] hiPart = new long[2];
    long[] loPart = new long[2];
    BidBinary128Convert.toBinary128TwoPart(hi, lo, hiPart, loPart);
    Binary128 xq = Binary128.fromRawBits(hiPart[0], hiPart[1]);
    Binary128 xLow = Binary128.fromRawBits(loPart[0], loPart[1]);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 rq = kernel(kind, xq, binaryMode, local);
    Binary128 eBin = Dpml.sub(xq, C_ONE, binaryMode, local);
    Binary128 absE = eBin.isSigned() ? eBin.negate() : eBin;
    if (Bid128Libm.less(absE, C_HALF)) {
      Binary128 rt = Dpml.div(xLow, xq, binaryMode, local);
      if (kind == Kind.LOG2) {
        rt = Dpml.mul(INV_LN2, rt, binaryMode, local);
      }
      rq = Dpml.add(rq, rt, binaryMode, local);
    }
    if (kind == Kind.LOG10) {
      rq = Dpml.mul(rq, C_INV_LOG10, binaryMode, local);
    }
    flags.raise(local.bits());
    BidConvert.fromBinary128To128(rq.highBits(), rq.lowBits(), mode, flags, out);
  }

  private static Binary128 kernel(
      Kind kind,
      Binary128 xq,
      org.bidfp.binary128.RoundingMode binaryMode,
      org.bidfp.binary128.StatusFlags local) {
    if (kind == Kind.LOG10) {
      return Dpml.log(xq, binaryMode, local);
    }
    if (kind == Kind.LOG2) {
      return Dpml.log2(xq, binaryMode, local);
    }
    return Dpml.log(xq, binaryMode, local);
  }
}
