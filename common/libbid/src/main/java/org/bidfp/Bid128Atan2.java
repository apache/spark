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

/**
 * Intel {@code bid128_atan2.c}. Java {@code atan2(y,x)} maps to Intel
 * {@code bid128_atan2(x=y, y=x)}: first operand is the numerator.
 */
final class Bid128Atan2 {
  private static final Bid128 DEC_PI =
      Bid128.fromRawBits(0x2ffe_9ae4_7957_96a7L, 0xbabe_5564_e6f3_9f8fL);
  private static final Bid128 DEC_PI12 =
      Bid128.fromRawBits(0x2ffe_4d72_3cab_cb53L, 0xdd5f_2ab2_7379_cfc7L);
  private static final Bid128 DEC_PI14 =
      Bid128.fromRawBits(0x2ffe_26b9_1e55_e5a9L, 0xeeaf_9559_39bc_e7e4L);
  private static final Bid128 DEC_PI34 =
      Bid128.fromRawBits(0x2ffe_742b_5b01_b0fdL, 0xcc0e_c00b_ad36_b7abL);
  private static final Bid128 TEN_POW_36 =
      Bid128.fromRawBits(0x3088_0000_0000_0000L, 1L);
  private static final Bid128 TEN_POW_M36 =
      Bid128.fromRawBits(0x2ff8_0000_0000_0000L, 1L);

  private Bid128Atan2() {
  }

  static void atan2(
      long yh, long yl, long xh, long xl,
      RoundingMode mode, StatusFlags flags, long[] out) {
    long ixh = yh;
    long ixl = yl;
    long iyh = xh;
    long iyl = xl;
    Bid128 ix = Bid128.fromRawBits(ixh, ixl);
    Bid128 iy = Bid128.fromRawBits(iyh, iyl);
    long signX = ixh & Bid128.MASK_SIGN;
    long signY = iyh & Bid128.MASK_SIGN;
    if (ix.isNaN()) {
      if (ix.isSignalingNaN() || iy.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      Bid128Libm.canonNan(ixh, ixl, flags, out);
      return;
    }
    if (ix.isInfinite()) {
      if (iy.isInfinite()) {
        Bid128 pi = iy.isSigned() ? DEC_PI34 : DEC_PI14;
        out[0] = signX ^ pi.highBits();
        out[1] = pi.lowBits();
        return;
      }
      if (!iy.isNaN()) {
        out[0] = signX ^ DEC_PI12.highBits();
        out[1] = DEC_PI12.lowBits();
        return;
      }
    }
    if (ix.isZero() && iy.isFinite() && !iy.isNaN()) {
      if (iy.isSigned()) {
        out[0] = signX ^ DEC_PI.highBits();
        out[1] = DEC_PI.lowBits();
      } else {
        out[0] = signX;
        out[1] = 0L;
      }
      return;
    }
    if (iy.isNaN()) {
      if (iy.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      Bid128Libm.canonNan(iyh, iyl, flags, out);
      return;
    }
    if (iy.isInfinite()) {
      if (iy.isSigned()) {
        out[0] = signX ^ DEC_PI.highBits();
        out[1] = DEC_PI.lowBits();
      } else {
        out[0] = signX;
        out[1] = 0L;
      }
      return;
    }
    if (iy.isZero()) {
      if (ix.isZero()) {
        if (iy.isSigned()) {
          out[0] = signX ^ DEC_PI.highBits();
          out[1] = DEC_PI.lowBits();
        } else {
          out[0] = signX;
          out[1] = 0L;
        }
      } else {
        out[0] = signX ^ DEC_PI12.highBits();
        out[1] = DEC_PI12.lowBits();
      }
      return;
    }
    int saved = flags.bits();
    long[] z = new long[2];
    Bid128Raw.div(ixh, ixl, iyh, iyl, mode, flags, z);
    flags.clear();
    flags.raise(saved);
    Bid128 zabs = Bid128.fromRawBits(z[0] & ~Bid128.MASK_SIGN, z[1]);
    if (zabs.quietGreater(TEN_POW_36, new StatusFlags())) {
      out[0] = signX ^ DEC_PI12.highBits();
      out[1] = DEC_PI12.lowBits();
      return;
    }
    if (zabs.quietLess(TEN_POW_M36, new StatusFlags())) {
      if (iy.isSigned()) {
        out[0] = signX ^ DEC_PI.highBits();
        out[1] = DEC_PI.lowBits();
      } else {
        out[0] = z[0];
        out[1] = z[1];
      }
      return;
    }
    long[] packed = new long[2];
    BidConvert.toBinary128From128(
        zabs.highBits(), zabs.lowBits(), mode, flags, packed);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 rq = Dpml.atan(
        Binary128.fromRawBits(packed[0], packed[1]), binaryMode, local);
    flags.raise(local.bits());
    BidConvert.fromBinary128To128(rq.highBits(), rq.lowBits(), mode, flags, out);
    if (iy.isSigned()) {
      Bid128Raw.sub(
          DEC_PI.highBits(), DEC_PI.lowBits(),
          out[0], out[1], mode, flags, out);
    }
    out[0] |= signX;
  }
}
