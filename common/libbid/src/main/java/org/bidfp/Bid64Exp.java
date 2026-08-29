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

/** Intel {@code bid64_exp.c}: NaN/Inf/0 and |x|>8000 overflow/underflow clamps. */
final class Bid64Exp {
  private static final long ONE = 0x31c0_0000_0000_0001L;
  private static final long ZERO = 0x31c0_0000_0000_0000L;
  private static final long INF = Bid64.MASK_INFINITY;
  private static final Binary128 C_8000 =
      Binary128.fromRawBits(0x400b_f400_0000_0000L, 0L);
  private static final Binary128 C_NEG_8000 =
      Binary128.fromRawBits(0xc00b_f400_0000_0000L, 0L);
  private static final Binary128 C_1E2000;
  private static final Binary128 C_1EM2000;

  static {
    long[] packed = new long[2];
    long[] bid = new long[2];
    StatusFlags flags = new StatusFlags();
    BidConvert.fromString128("1e2000", RoundingMode.TIES_TO_EVEN, flags, bid);
    BidConvert.toBinary128From128(
        bid[0], bid[1], RoundingMode.TIES_TO_EVEN, flags, packed);
    C_1E2000 = Binary128.fromRawBits(packed[0], packed[1]);
    flags = new StatusFlags();
    BidConvert.fromString128("1e-2000", RoundingMode.TIES_TO_EVEN, flags, bid);
    BidConvert.toBinary128From128(
        bid[0], bid[1], RoundingMode.TIES_TO_EVEN, flags, packed);
    C_1EM2000 = Binary128.fromRawBits(packed[0], packed[1]);
  }

  private Bid64Exp() {
  }

  static long exp(long x, RoundingMode mode, StatusFlags flags) {
    return evaluate(x, mode, flags, Kind.EXP, C_8000, C_NEG_8000);
  }

  static long exp2(long x, RoundingMode mode, StatusFlags flags) {
    return evaluate(x, mode, flags, Kind.EXP2, C_12000, C_NEG_12000);
  }

  static long exp10(long x, RoundingMode mode, StatusFlags flags) {
    return evaluate(x, mode, flags, Kind.EXP10, C_12000, C_NEG_12000);
  }

  private enum Kind { EXP, EXP2, EXP10 }

  private static final Binary128 C_12000 =
      Binary128.fromRawBits(0x400c_7700_0000_0000L, 0L);
  private static final Binary128 C_NEG_12000 =
      Binary128.fromRawBits(0xc00c_7700_0000_0000L, 0L);

  private static long evaluate(
      long x, RoundingMode mode, StatusFlags flags, Kind kind,
      Binary128 hiClamp, Binary128 loClamp) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isNaN()) {
      if (value.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      long quiet = x & 0xfc03_ffff_ffff_ffffL;
      if ((quiet & 0x0003_ffff_ffff_ffffL) > 999_999_999_999_999L) {
        quiet &= ~0x0003_ffff_ffff_ffffL;
      }
      return quiet;
    }
    if (value.isZero()) {
      return ONE;
    }
    if (value.isInfinite()) {
      flags.clear();
      return value.isSigned() ? ZERO : INF;
    }
    long[] packed = new long[2];
    BidConvert.toBinary128From64(x, mode, flags, packed);
    Binary128 xd = Binary128.fromRawBits(packed[0], packed[1]);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 rd;
    if (Bid128Libm.greater(xd, hiClamp)) {
      rd = C_1E2000;
    } else if (Bid128Libm.less(xd, loClamp)) {
      rd = C_1EM2000;
    } else {
      if (kind == Kind.EXP2) {
        rd = Dpml.exp2(xd, binaryMode, local);
      } else if (kind == Kind.EXP10) {
        rd = Dpml.exp10(xd, binaryMode, local);
      } else {
        rd = Dpml.exp(xd, binaryMode, local);
      }
      flags.raise(local.bits());
    }
    return BidConvert.fromBinary128To64(rd.highBits(), rd.lowBits(), mode, flags);
  }
}
