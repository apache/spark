/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

import java.math.BigInteger;

/**
 * Unpacked binary128 operations ported from Intel DPML {@code dpml_ux_ops.c},
 * {@code dpml_ux_ops_64.c}, and {@code dpml_ux_sqrt.c} (64-bit digit path).
 *
 * <p>{@code PACK} is generalized with the DPML {@code S/K/L/R} bit-vectors so
 * all five IEEE rounding modes are explicit arguments rather than host FPSR.
 */
public final class UxOps {
  private static final int F_EXP_WIDTH = 15;
  private static final int F_EXP_BIAS = 16383;
  private static final int CSHIFT = 64 - F_EXP_WIDTH;
  private static final int MIN_UNBIASED = 1 - F_EXP_BIAS;

  private UxOps() {
  }

  public static Unpacked unpack(Binary128 x) {
    Unpacked u = new Unpacked();
    unpackInto(x, u);
    return u;
  }

  static void unpackInto(Binary128 x, Unpacked u) {
    long high = x.highBits();
    long low = x.lowBits();
    int sign = (high & Binary128.MASK_SIGN) != 0L ? Unpacked.UX_SIGN_BIT : 0;
    int biased = x.biasedExponent();
    long fracHi = high & Binary128.MASK_FRACTION_HIGH;
    if (biased == 0x7fff) {
      if (fracHi == 0L && low == 0L) {
        u.setInf(sign);
        return;
      }
      boolean signaling = (high & 0x0000_8000_0000_0000L) == 0L;
      u.setNaN(signaling);
      u.sign = sign;
      u.fracHi = Unpacked.UX_MSB | (fracHi << F_EXP_WIDTH) | (low >>> CSHIFT);
      u.fracLo = low << F_EXP_WIDTH;
      return;
    }
    if (biased == 0) {
      if (fracHi == 0L && low == 0L) {
        u.setZero(sign);
        return;
      }
      long msd = (fracHi << F_EXP_WIDTH) | (low >>> CSHIFT);
      long lsd = low << F_EXP_WIDTH;
      u.setNorm(sign, MIN_UNBIASED + 1, msd, lsd);
      normalize(u);
      return;
    }
    long msd = Unpacked.UX_MSB | (fracHi << F_EXP_WIDTH) | (low >>> CSHIFT);
    long lsd = low << F_EXP_WIDTH;
    int uxExp = biased - F_EXP_BIAS + 1;
    u.setNorm(sign, uxExp, msd, lsd);
  }

  static int normalize(Unpacked u) {
    if (u.klass != Unpacked.CLASS_NORM) {
      return 0;
    }
    if (u.fracHi < 0) {
      return 0;
    }
    if (u.fracHi == 0L && u.fracLo == 0L) {
      u.setZero(u.sign);
      return 128;
    }
    int shift;
    if (u.fracHi != 0L) {
      shift = Long.numberOfLeadingZeros(u.fracHi);
    } else {
      shift = 64 + Long.numberOfLeadingZeros(u.fracLo);
    }
    if (shift >= 64) {
      u.fracHi = u.fracLo << (shift - 64);
      u.fracLo = 0L;
    } else {
      u.fracHi = (u.fracHi << shift) | (u.fracLo >>> (64 - shift));
      u.fracLo <<= shift;
    }
    u.exponent -= shift;
    return shift;
  }

  public static Binary128 pack(Unpacked u, RoundingMode mode, StatusFlags status) {
    if (u.klass == Unpacked.CLASS_NAN) {
      if (u.signaling) {
        status.raise(StatusFlags.INVALID);
      }
      long fractionHigh = (u.fracHi >>> F_EXP_WIDTH)
          & Binary128.MASK_FRACTION_HIGH;
      long fractionLow = (u.fracHi << CSHIFT) | (u.fracLo >>> F_EXP_WIDTH);
      fractionHigh |= Binary128.QUIET_NAN_BIT;
      return Binary128.fromFields(
          u.sign != 0, 0x7fff, fractionHigh, fractionLow);
    }
    if (u.klass == Unpacked.CLASS_INF) {
      return u.sign != 0 ? Binary128.NEGATIVE_INFINITY : Binary128.POSITIVE_INFINITY;
    }
    if (u.klass == Unpacked.CLASS_ZERO) {
      return u.sign != 0 ? Binary128.NEGATIVE_ZERO : Binary128.ZERO;
    }
    int biased = u.exponent + F_EXP_BIAS - 1;
    if (u.fracHi < 0 && biased > 0 && biased < 0x7ffe) {
      long discarded = u.fracLo & 0x7fffL;
      long fractionHigh = (u.fracHi >>> F_EXP_WIDTH)
          & Binary128.MASK_FRACTION_HIGH;
      long fractionLow = (u.fracHi << CSHIFT) | (u.fracLo >>> F_EXP_WIDTH);
      if (discarded != 0L) {
        status.raise(StatusFlags.INEXACT);
        if (roundIncrement(u.sign != 0, fractionLow, discarded, mode)) {
          fractionLow++;
          if (fractionLow == 0L) {
            fractionHigh++;
          }
        }
      }
      if ((fractionHigh & ~Binary128.MASK_FRACTION_HIGH) != 0L) {
        biased++;
        fractionHigh = 0L;
        fractionLow = 0L;
      }
      return Binary128.fromFields(
          u.sign != 0, biased, fractionHigh, fractionLow);
    }
    BigInteger fraction = Wide.u128(u.fracHi, u.fracLo);
    return IeeeRound.binary128(
        u.sign != 0, fraction, BigInteger.ONE, u.exponent - 128, mode, status);
  }

  private static boolean roundIncrement(
      boolean negative, long fractionLow, long discarded, RoundingMode mode) {
    return switch (mode) {
      case TOWARD_ZERO -> false;
      case TOWARD_POSITIVE -> !negative;
      case TOWARD_NEGATIVE -> negative;
      case TIES_AWAY -> discarded >= 0x4000L;
      case TIES_TO_EVEN -> discarded > 0x4000L
          || (discarded == 0x4000L && (fractionLow & 1L) != 0L);
    };
  }

  public static Binary128 add(
      Binary128 x, Binary128 y, RoundingMode mode, StatusFlags status) {
    return addsub(x, y, false, mode, status);
  }

  public static Binary128 sub(
      Binary128 x, Binary128 y, RoundingMode mode, StatusFlags status) {
    return addsub(x, y, true, mode, status);
  }

  private static Binary128 addsub(
      Binary128 x, Binary128 y, boolean subtract,
      RoundingMode mode, StatusFlags status) {
    raiseDenormal(x, y, status);
    UxScratch.Frame scratch = UxScratch.acquire();
    try {
      Unpacked a = scratch.unpacked(0);
      Unpacked b = scratch.unpacked(1);
      Unpacked r = scratch.unpacked(2);
      unpackInto(x, a);
      unpackInto(y, b);
      if (subtract) {
        negate(b);
      }
      addsubUnpacked(a, b, r, status);
      if (r.klass == Unpacked.CLASS_ZERO && a.sign != b.sign) {
        r.sign = mode == RoundingMode.TOWARD_NEGATIVE ? Unpacked.UX_SIGN_BIT : 0;
      }
      return pack(r, mode, status);
    } finally {
      UxScratch.release(scratch);
    }
  }

  static void addsubUnpacked(Unpacked a, Unpacked b, Unpacked r, StatusFlags status) {
    if (a.klass == Unpacked.CLASS_NAN || b.klass == Unpacked.CLASS_NAN) {
      propagateNaN(a, b, r, status);
      return;
    }
    if (a.klass == Unpacked.CLASS_INF && b.klass == Unpacked.CLASS_INF) {
      if (a.sign != b.sign) {
        status.raise(StatusFlags.INVALID);
        r.setNaN(false);
        return;
      }
      r.setInf(a.sign);
      return;
    }
    if (a.klass == Unpacked.CLASS_INF) {
      r.copyFrom(a);
      return;
    }
    if (b.klass == Unpacked.CLASS_INF) {
      r.copyFrom(b);
      return;
    }
    if (a.klass == Unpacked.CLASS_ZERO && b.klass == Unpacked.CLASS_ZERO) {
      if (a.sign != b.sign) {
        r.setZero(0);
      } else {
        r.setZero(a.sign);
      }
      return;
    }
    if (a.klass == Unpacked.CLASS_ZERO) {
      r.copyFrom(b);
      return;
    }
    if (b.klass == Unpacked.CLASS_ZERO) {
      r.copyFrom(a);
      return;
    }
    normalize(a);
    normalize(b);
    Unpacked x = a;
    Unpacked y = b;
    int sign = x.sign;
    if (x.exponent < y.exponent
        || (x.exponent == y.exponent && Wide.cmp128(x.fracHi, x.fracLo, y.fracHi, y.fracLo) < 0)) {
      x = b;
      y = a;
      sign = x.sign;
    }
    int shift = x.exponent - y.exponent;
    long yHi;
    long yLo;
    long sticky;
    if (shift <= 0) {
      yHi = y.fracHi;
      yLo = y.fracLo;
      sticky = 0L;
    } else if (shift >= 128) {
      yHi = 0L;
      yLo = 0L;
      sticky = 1L;
    } else if (shift >= 64) {
      int lowShift = shift - 64;
      yHi = 0L;
      yLo = lowShift == 0 ? y.fracHi : y.fracHi >>> lowShift;
      long lost = y.fracLo;
      if (lowShift != 0) {
        lost |= y.fracHi & ((1L << lowShift) - 1L);
      }
      sticky = lost == 0L ? 0L : 1L;
    } else {
      yHi = y.fracHi >>> shift;
      yLo = (y.fracHi << (64 - shift)) | (y.fracLo >>> shift);
      sticky = (y.fracLo & ((1L << shift) - 1L)) == 0L ? 0L : 1L;
    }
    boolean sameSign = a.sign == b.sign;
    if (x == b) {
      sameSign = a.sign == b.sign;
    }
    sameSign = (a.sign ^ b.sign) == 0;
    if (sameSign) {
      long sumLo = x.fracLo + yLo;
      long carry = Long.compareUnsigned(sumLo, x.fracLo) < 0 ? 1L : 0L;
      long highBase = x.fracHi + yHi;
      long sumHi = highBase + carry;
      boolean ov = Long.compareUnsigned(highBase, x.fracHi) < 0
          || (carry != 0L && Long.compareUnsigned(sumHi, highBase) < 0);
      r.sign = sign;
      r.klass = Unpacked.CLASS_NORM;
      r.signaling = false;
      if (ov) {
        sticky |= sumLo & 1L;
        r.fracHi = Unpacked.UX_MSB | (sumHi >>> 1);
        r.fracLo = (sumHi << 63) | (sumLo >>> 1);
        r.exponent = x.exponent + 1;
      } else {
        r.fracHi = sumHi;
        r.fracLo = sumLo;
        r.exponent = x.exponent;
      }
      if (sticky != 0L) {
        r.fracLo |= 1L;
      }
    } else {
      long borrow = Long.compareUnsigned(x.fracLo, yLo) < 0 ? 1L : 0L;
      long differenceHi = x.fracHi - yHi - borrow;
      long differenceLo = x.fracLo - yLo;
      if (sticky != 0L) {
        long stickyBorrow = differenceLo == 0L ? 1L : 0L;
        differenceLo--;
        differenceHi -= stickyBorrow;
      }
      r.sign = sign;
      r.fracHi = differenceHi;
      r.fracLo = differenceLo;
      r.exponent = x.exponent;
      r.klass = Unpacked.CLASS_NORM;
      r.signaling = false;
      if (r.fracHi == 0L && r.fracLo == 0L) {
        r.setZero(0);
        return;
      }
      normalize(r);
    }
  }

  public static Binary128 mul(
      Binary128 x, Binary128 y, RoundingMode mode, StatusFlags status) {
    raiseDenormal(x, y, status);
    UxScratch.Frame scratch = UxScratch.acquire();
    try {
      Unpacked a = scratch.unpacked(0);
      Unpacked b = scratch.unpacked(1);
      Unpacked r = scratch.unpacked(2);
      unpackInto(x, a);
      unpackInto(y, b);
      mulUnpacked(a, b, r, status);
      return pack(r, mode, status);
    } finally {
      UxScratch.release(scratch);
    }
  }

  static void mulUnpacked(Unpacked a, Unpacked b, Unpacked r, StatusFlags status) {
    if (a.klass == Unpacked.CLASS_NAN || b.klass == Unpacked.CLASS_NAN) {
      propagateNaN(a, b, r, status);
      return;
    }
    int sign = a.sign ^ b.sign;
    boolean aInf = a.klass == Unpacked.CLASS_INF;
    boolean bInf = b.klass == Unpacked.CLASS_INF;
    boolean aZero = a.klass == Unpacked.CLASS_ZERO;
    boolean bZero = b.klass == Unpacked.CLASS_ZERO;
    if ((aInf && bZero) || (bInf && aZero)) {
      status.raise(StatusFlags.INVALID);
      r.setNaN(false);
      return;
    }
    if (aInf || bInf) {
      r.setInf(sign);
      return;
    }
    if (aZero || bZero) {
      r.setZero(sign);
      return;
    }
    normalize(a);
    normalize(b);
    long p0 = a.fracLo * b.fracLo;
    long p0h = Wide.umulh(a.fracLo, b.fracLo);
    long p1l = a.fracLo * b.fracHi;
    long p1h = Wide.umulh(a.fracLo, b.fracHi);
    long p2l = a.fracHi * b.fracLo;
    long p2h = Wide.umulh(a.fracHi, b.fracLo);
    long p3l = a.fracHi * b.fracHi;
    long p3h = Wide.umulh(a.fracHi, b.fracHi);

    long product1 = p0h + p1l;
    long carry = Long.compareUnsigned(product1, p1l) < 0 ? 1L : 0L;
    long product2 = p1h + carry;
    long product3 = Long.compareUnsigned(product2, p1h) < 0 ? 1L : 0L;
    product1 += p2l;
    carry = Long.compareUnsigned(product1, p2l) < 0 ? 1L : 0L;
    long oldProduct2 = product2;
    product2 += p2h + carry;
    product3 += Long.compareUnsigned(product2, oldProduct2) < 0 ? 1L : 0L;
    oldProduct2 = product2;
    product2 += p3l;
    product3 += Long.compareUnsigned(product2, oldProduct2) < 0 ? 1L : 0L;
    product3 += p3h;

    int exp = a.exponent + b.exponent;
    long hi;
    long lo;
    long sticky;
    if ((product3 & Unpacked.UX_MSB) != 0L) {
      hi = product3;
      lo = product2;
      sticky = product1 | p0;
    } else {
      hi = (product3 << 1) | (product2 >>> 63);
      lo = (product2 << 1) | (product1 >>> 63);
      sticky = (product1 << 1) | p0;
      exp--;
    }
    if (sticky != 0L) {
      lo |= 1L;
    }
    r.setNorm(sign, exp, hi, lo);
  }

  public static Binary128 div(
      Binary128 x, Binary128 y, RoundingMode mode, StatusFlags status) {
    raiseDenormal(x, y, status);
    UxScratch.Frame scratch = UxScratch.acquire();
    try {
      Unpacked a = scratch.unpacked(0);
      Unpacked b = scratch.unpacked(1);
      Unpacked r = scratch.unpacked(2);
      unpackInto(x, a);
      unpackInto(y, b);
      divUnpacked(a, b, r, status, scratch.division);
      return pack(r, mode, status);
    } finally {
      UxScratch.release(scratch);
    }
  }

  static void divUnpacked(Unpacked a, Unpacked b, Unpacked r, StatusFlags status) {
    UxScratch.Frame scratch = UxScratch.acquire();
    try {
      divUnpacked(a, b, r, status, scratch.division);
    } finally {
      UxScratch.release(scratch);
    }
  }

  static void divUnpacked(
      Unpacked a, Unpacked b, Unpacked r, StatusFlags status, long[] division) {
    if (a.klass == Unpacked.CLASS_NAN || b.klass == Unpacked.CLASS_NAN) {
      propagateNaN(a, b, r, status);
      return;
    }
    int sign = a.sign ^ b.sign;
    boolean aInf = a.klass == Unpacked.CLASS_INF;
    boolean bInf = b.klass == Unpacked.CLASS_INF;
    boolean aZero = a.klass == Unpacked.CLASS_ZERO;
    boolean bZero = b.klass == Unpacked.CLASS_ZERO;
    if (aInf && bInf) {
      status.raise(StatusFlags.INVALID);
      r.setNaN(false);
      return;
    }
    if (aZero && bZero) {
      status.raise(StatusFlags.INVALID);
      r.setNaN(false);
      return;
    }
    if (bZero) {
      status.raise(StatusFlags.DIVIDE_BY_ZERO);
      r.setInf(sign);
      return;
    }
    if (aZero) {
      r.setZero(sign);
      return;
    }
    if (aInf) {
      r.setInf(sign);
      return;
    }
    if (bInf) {
      r.setZero(sign);
      return;
    }
    normalize(a);
    normalize(b);
    Wide.divFrac128(a.fracHi, a.fracLo, b.fracHi, b.fracLo, division);
    int exp = a.exponent - b.exponent;
    long hi;
    long lo;
    if (division[0] != 0L) {
      if ((division[2] & 1L) != 0L) {
        division[4] |= 1L;
      }
      hi = (division[0] << 63) | (division[1] >>> 1);
      lo = (division[1] << 63) | (division[2] >>> 1);
      exp++;
    } else {
      hi = division[1];
      lo = division[2];
    }
    if ((division[3] | division[4]) != 0L) {
      lo |= 1L;
    }
    if ((hi & Unpacked.UX_MSB) == 0L) {
      hi = (hi << 1) | (lo >>> 63);
      lo <<= 1;
      exp--;
    }
    r.setNorm(sign, exp, hi, lo);
  }

  public static Binary128 sqrt(Binary128 x, RoundingMode mode, StatusFlags status) {
    raiseDenormal(x, null, status);
    UxScratch.Frame scratch = UxScratch.acquire();
    try {
      Unpacked a = scratch.unpacked(0);
      Unpacked r = scratch.unpacked(1);
      unpackInto(x, a);
      sqrtUnpacked(a, r, status, scratch);
      return pack(r, mode, status);
    } finally {
      UxScratch.release(scratch);
    }
  }

  static void sqrtUnpacked(Unpacked a, Unpacked r, StatusFlags status) {
    UxScratch.Frame scratch = UxScratch.acquire();
    try {
      sqrtUnpacked(a, r, status, scratch);
    } finally {
      UxScratch.release(scratch);
    }
  }

  private static void sqrtUnpacked(
      Unpacked a, Unpacked r, StatusFlags status, UxScratch.Frame scratch) {
    if (a.klass == Unpacked.CLASS_NAN) {
      if (a.signaling) {
        status.raise(StatusFlags.INVALID);
      }
      r.copyFrom(a);
      r.signaling = false;
      return;
    }
    if (a.klass == Unpacked.CLASS_ZERO) {
      r.copyFrom(a);
      return;
    }
    if (a.sign != 0) {
      status.raise(StatusFlags.INVALID);
      r.setNaN(false);
      return;
    }
    if (a.klass == Unpacked.CLASS_INF) {
      r.setInf(0);
      return;
    }
    normalize(a);
    int exp = a.exponent;
    boolean odd = (exp & 1) != 0;
    if (odd) {
      exp--;
    }
    long[] root = scratch.root;
    boolean sticky = Wide.sqrtScaled128(
        a.fracHi, a.fracLo, odd, root, scratch.remainder, scratch.trial);
    int outExp = exp / 2;
    long high;
    long low;
    if (root[0] != 0L) {
      sticky |= (root[2] & 1L) != 0L;
      high = (root[0] << 63) | (root[1] >>> 1);
      low = (root[1] << 63) | (root[2] >>> 1);
      outExp++;
    } else {
      high = root[1];
      low = root[2];
    }
    if (sticky) {
      low |= 1L;
    }
    r.setNorm(0, outExp, high, low);
    normalize(r);
  }

  public static int compare(Binary128 x, Binary128 y, StatusFlags status) {
    raiseDenormal(x, y, status);
    Unpacked a = unpack(x);
    Unpacked b = unpack(y);
    if (a.klass == Unpacked.CLASS_NAN || b.klass == Unpacked.CLASS_NAN) {
      status.raise(StatusFlags.INVALID);
      return 2;
    }
    if (a.klass == Unpacked.CLASS_ZERO && b.klass == Unpacked.CLASS_ZERO) {
      return 0;
    }
    if (a.sign != b.sign) {
      return a.sign != 0 ? -1 : 1;
    }
    int mag;
    if (a.klass != b.klass) {
      if (a.klass == Unpacked.CLASS_INF) {
        mag = 1;
      } else if (b.klass == Unpacked.CLASS_INF) {
        mag = -1;
      } else if (a.klass == Unpacked.CLASS_ZERO) {
        mag = -1;
      } else {
        mag = 1;
      }
    } else if (a.klass == Unpacked.CLASS_INF) {
      mag = 0;
    } else {
      mag = Integer.compare(a.exponent, b.exponent);
      if (mag == 0) {
        mag = Wide.cmp128(a.fracHi, a.fracLo, b.fracHi, b.fracLo);
      }
    }
    return a.sign != 0 ? -mag : mag;
  }

  static void propagateNaN(Unpacked a, Unpacked b, Unpacked r, StatusFlags status) {
    if (a.isSignalingNaN() || b.isSignalingNaN()) {
      status.raise(StatusFlags.INVALID);
    }
    Unpacked selected;
    if (a.isSignalingNaN()) {
      selected = a;
    } else if (b.isSignalingNaN()) {
      selected = b;
    } else {
      selected = a.isNaN() ? a : b;
    }
    r.copyFrom(selected);
    r.signaling = false;
  }

  private static void raiseDenormal(
      Binary128 x, Binary128 y, StatusFlags status) {
    if (x.isSubnormal() || (y != null && y.isSubnormal())) {
      status.raise(StatusFlags.DENORMAL);
    }
  }

  static void negate(Unpacked u) {
    if (u.klass == Unpacked.CLASS_NAN) {
      return;
    }
    u.sign ^= Unpacked.UX_SIGN_BIT;
  }

  static void abs(Unpacked u) {
    if (u.klass == Unpacked.CLASS_NAN) {
      return;
    }
    u.sign = 0;
  }
}
