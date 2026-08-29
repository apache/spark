/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

/**
 * Intel {@code bid{64,128}_hypot.c}: NaN/Inf/0 shortcuts, then hypot via
 * binary128 {@code sqrt(x^2+y^2)}. BID128 rebiases exponents so the kernel
 * stays in range, then restores the larger operand's exponent.
 */
final class BidHypot {
  private static final long QUIET_MASK64 = 0xfdffffffffffffffL;

  private BidHypot() {
  }

  static long hypot64(long x, long y, RoundingMode mode, StatusFlags flags) {
    if (Bid64Raw.isNaN(x)) {
      if (Bid64Raw.isSignalingNaN(x) || Bid64Raw.isSignalingNaN(y)) {
        flags.raise(StatusFlags.INVALID);
      }
      if (Bid64Raw.isSignalingNaN(x) || !Bid64Raw.isInf(y)) {
        return Bid64Log.canonNan(x, flags);
      }
      return Bid64.MASK_INFINITY;
    }
    if (Bid64Raw.isInf(x) && !Bid64Raw.isSignalingNaN(y)) {
      return Bid64.MASK_INFINITY;
    }
    if (Bid64Raw.isZero(x) && Bid64Raw.isFinite(y) && !Bid64Raw.isNaN(y)) {
      return y & ~Bid64.MASK_SIGN;
    }
    if (Bid64Raw.isNaN(y)) {
      if (Bid64Raw.isSignalingNaN(y)) {
        flags.raise(StatusFlags.INVALID);
      }
      return Bid64Log.canonNan(y, flags);
    }
    if (Bid64Raw.isInf(y)) {
      return Bid64.MASK_INFINITY;
    }
    if (Bid64Raw.isZero(y) && Bid64Raw.isFinite(x)) {
      return x & ~Bid64.MASK_SIGN;
    }
    return BidTranscendental.binary64(x, y, mode, flags, BidTranscendental::hypotKernel);
  }

  static void hypot128(
      long xh, long xl, long yh, long yl,
      RoundingMode mode, StatusFlags flags, long[] out) {
    boolean yGreater = Bid128Raw.quietGreater(
        yh & ~Bid128.MASK_SIGN, yl, xh & ~Bid128.MASK_SIGN, xl, new StatusFlags());
    long ah = yGreater ? yh : xh;
    long al = yGreater ? yl : xl;
    long bh = yGreater ? xh : yh;
    long bl = yGreater ? xl : yl;

    Bid128 a = Bid128.fromRawBits(ah, al);
    Bid128 b = Bid128.fromRawBits(bh, bl);
    if (a.isNaN()) {
      if (a.isSignalingNaN() || b.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      if (a.isSignalingNaN() || !b.isInfinite()) {
        Bid128Libm.canonNan(ah, al, flags, out);
      } else {
        out[0] = Bid128.MASK_INFINITY;
        out[1] = 0L;
      }
      return;
    }
    if (a.isInfinite() && !b.isSignalingNaN()) {
      out[0] = Bid128.MASK_INFINITY;
      out[1] = 0L;
      return;
    }
    if (a.isZero() && b.isFinite() && !b.isNaN()) {
      out[0] = bh & ~Bid128.MASK_SIGN;
      out[1] = bl;
      return;
    }
    if (b.isNaN()) {
      if (b.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      Bid128Libm.canonNan(bh, bl, flags, out);
      return;
    }
    if (b.isInfinite()) {
      out[0] = Bid128.MASK_INFINITY;
      out[1] = 0L;
      return;
    }
    if (b.isZero()) {
      out[0] = ah & ~Bid128.MASK_SIGN;
      out[1] = al;
      return;
    }

    int expA = a.biasedExponent();
    int expB = b.biasedExponent();
    if (expA - expB >= 69) {
      out[0] = ah & ~Bid128.MASK_SIGN;
      out[1] = al;
      return;
    }

    UInt128 coeffA = a.coefficient();
    UInt128 coeffB = b.coefficient();
    Bid128 scaledA = Bid128.finite(false, 6176, coeffA.high(), coeffA.low());
    Bid128 scaledB = Bid128.finite(
        false, 6176 + expB - expA, coeffB.high(), coeffB.low());
    long[] kernel = new long[2];
    BidTranscendental.binary128(
        scaledA.highBits(), scaledA.lowBits(),
        scaledB.highBits(), scaledB.lowBits(),
        mode, flags, BidTranscendental::hypotKernel, kernel);
    Bid128 result = Bid128.fromRawBits(kernel[0], kernel[1]);
    if (!result.isFinite() || result.isNaN()) {
      out[0] = kernel[0];
      out[1] = kernel[1];
      return;
    }
    int restored = result.biasedExponent() + expA - 6176;
    UInt128 coeff = result.coefficient();
    DecNum number = DecNum.ofUnsigned(coeff.high(), coeff.low());
    number.shiftExp(restored - 6176);
    number.packBid128(mode, flags, out);
  }
}
