/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

import org.bidfp.binary128.tables.LgammaX;
import org.bidfp.binary128.tables.LogX;

/**
 * Intel QUAD UX {@code lgamma} and {@code tgamma}.
 *
 * <p>The reduced interval and asymptotic corrections use the rational
 * approximations from {@code dpml_lgamma_x.h}. {@code tgamma} keeps the
 * lgamma result unpacked and applies {@code exp} before a single pack.
 */
public final class DpmlGamma {
  private static final long[] TABLE = LgammaX.TABLE;

  private DpmlGamma() {
  }

  public static Binary128 tgamma(Binary128 x, RoundingMode mode, StatusFlags st) {
    Unpacked a = UxOps.unpack(x);
    if (a.isNaN()) {
      return quietNaN(x, a, st);
    }
    if (a.isInfinite()) {
      if (a.sign != 0) {
        st.raise(StatusFlags.INVALID);
        return Binary128.canonicalNaN(true);
      }
      return Binary128.POSITIVE_INFINITY;
    }
    if (a.isZero()) {
      st.raise(StatusFlags.DIVIDE_BY_ZERO);
      return Binary128.POSITIVE_INFINITY;
    }
    UxOps.normalize(a);
    if (a.sign != 0 && isInteger(a)) {
      st.raise(StatusFlags.DIVIDE_BY_ZERO);
      return Binary128.POSITIVE_INFINITY;
    }

    // Intel's positive exponent > 11 screen avoids doing an exp that must overflow.
    if (a.sign == 0 && a.exponent > 11) {
      return overflow(mode, false, st);
    }

    StatusFlags local = new StatusFlags();
    Unpacked lu = a.exponent < 5 ? reduced(a.copy(), local) : asymptotic(a.copy(), local);
    if (lu.isInfinite() || (lu.klass == Unpacked.CLASS_NORM && lu.exponent >= 14)) {
      return overflow(mode, gammaSignNegative(a), st);
    }
    Unpacked magnitude = DpmlExpHyperKernel.exp(lu, local);
    if (gammaSignNegative(a)) {
      UxOps.negate(magnitude);
    }
    return UxOps.pack(magnitude, mode, st);
  }

  public static Binary128 lgamma(Binary128 x, RoundingMode mode, StatusFlags st) {
    Unpacked a = UxOps.unpack(x);
    if (a.isNaN()) {
      return quietNaN(x, a, st);
    }
    if (a.isInfinite()) {
      if (a.sign != 0) {
        st.raise(StatusFlags.INVALID);
        return Binary128.canonicalNaN(true);
      }
      return Binary128.POSITIVE_INFINITY;
    }
    if (a.isZero()) {
      st.raise(StatusFlags.DIVIDE_BY_ZERO);
      return Binary128.POSITIVE_INFINITY;
    }
    UxOps.normalize(a);
    if (a.sign != 0 && isInteger(a)) {
      st.raise(StatusFlags.DIVIDE_BY_ZERO);
      return Binary128.POSITIVE_INFINITY;
    }

    StatusFlags local = new StatusFlags();
    Unpacked result = a.exponent < 5 ? reduced(a.copy(), local) : asymptotic(a.copy(), local);
    return UxOps.pack(result, mode, st);
  }

  /**
   * Computes positive {@code lgamma(xHigh + xLow)} as a nonoverlapping pair.
   *
   * <p>The split retains the 128-bit UX result that would otherwise lose 15 guard bits
   * when packed directly as binary128.
   */
  static Binary128[] positiveLgammaTwoPart(
      Binary128 xHigh, Binary128 xLow, StatusFlags st) {
    StatusFlags local = new StatusFlags();
    Unpacked argument = new Unpacked();
    UxOps.addsubUnpacked(
        UxOps.unpack(xHigh), UxOps.unpack(xLow), argument, local);
    UxOps.normalize(argument);
    Unpacked result = argument.exponent < 5
        ? reduced(argument.copy(), local)
        : asymptotic(argument.copy(), local);

    Binary128 high = UxOps.pack(
        result, RoundingMode.TIES_TO_EVEN, local);
    Unpacked negativeHigh = UxOps.unpack(high);
    UxOps.negate(negativeHigh);
    Unpacked remainder = new Unpacked();
    UxOps.addsubUnpacked(result, negativeHigh, remainder, local);
    Binary128 low = UxOps.pack(
        remainder, RoundingMode.TIES_TO_EVEN, local);
    st.raise(local);
    return new Binary128[] {high, low};
  }

  private static Unpacked reduced(Unpacked x, StatusFlags st) {
    Unpacked original = x.copy();
    Unpacked one = ux(LgammaX.UX_ONE);
    Unpacked two = KernelEval.fromInt(2);
    Unpacked product = one.copy();
    Unpacked tmp = new Unpacked();
    Unpacked next = new Unpacked();

    while (compare(x, one) < 0) {
      KernelEval.mul(product, x, tmp, st);
      product.copyFrom(tmp);
      KernelEval.add(x, one, next, st);
      x.copyFrom(next);
    }
    while (compare(x, two) > 0) {
      KernelEval.sub(x, one, next, st);
      x.copyFrom(next);
      KernelEval.mul(product, x, tmp, st);
      product.copyFrom(tmp);
    }

    // y = 2*x - 3; w = (y - 1)*(y + 1).
    x.exponent++;
    Unpacked y = new Unpacked();
    KernelEval.sub(x, ux(LgammaX.UX_THREE), y, st);
    Unpacked ym = new Unpacked();
    Unpacked yp = new Unpacked();
    KernelEval.sub(y, one, ym, st);
    KernelEval.add(y, one, yp, st);
    Unpacked w = new Unpacked();
    KernelEval.mul(ym, yp, w, st);

    Unpacked result = new Unpacked();
    if (w.isZero()) {
      result.setZero(0);
    } else {
      Unpacked rational = DpmlGammaRational.reduced(
          y,
          TABLE,
          LgammaX.LGAMMA_P_COEF_ARRAY,
          LgammaX.LGAMMA_P_COEF_ARRAY_DEGREE,
          st);
      KernelEval.mul(w, rational, result, st);
    }

    product.sign = 0;
    if (compare(product, one) != 0) {
      Unpacked lp = log(product, st);
      if (compare(original, one) < 0) {
        KernelEval.sub(result, lp, tmp, st);
      } else {
        KernelEval.add(result, lp, tmp, st);
      }
      result.copyFrom(tmp);
    }
    return result;
  }

  private static Unpacked asymptotic(Unpacked signedX, StatusFlags st) {
    boolean negative = signedX.sign != 0;
    signedX.sign = 0;
    Unpacked logX = log(signedX, st);
    Unpacked half = ux(LgammaX.UX_HALF);
    Unpacked factor = new Unpacked();
    if (negative) {
      KernelEval.add(signedX, half, factor, st);
    } else {
      KernelEval.sub(signedX, half, factor, st);
    }

    Unpacked result = new Unpacked();
    KernelEval.mul(logX, factor, result, st);
    Unpacked tmp = new Unpacked();
    KernelEval.sub(result, signedX, tmp, st);
    result.copyFrom(tmp);
    KernelEval.add(
        result,
        ux(negative ? LgammaX.UX_HALF_LN_TWO_OVER_PI : LgammaX.UX_HALF_LN_TWO_PI),
        tmp,
        st);
    result.copyFrom(tmp);

    Unpacked inverse = new Unpacked();
    KernelEval.div(ux(LgammaX.UX_ONE), signedX, inverse, st);
    Unpacked correction = DpmlGammaRational.phi(
        inverse,
        TABLE,
        LgammaX.LGAMMA_PHI_COEF_ARRAY,
        LgammaX.LGAMMA_PHI_COEF_ARRAY_DEGREE,
        st);
    KernelEval.add(result, correction, tmp, st);
    result.copyFrom(tmp);

    if (negative) {
      UxOps.negate(result);
      Unpacked fraction = fractionalPart(signedX);
      Binary128 angle = UxOps.mul(
          UxOps.pack(fraction, RoundingMode.TIES_TO_EVEN, new StatusFlags()),
          UxOps.pack(ux(LgammaX.UX_PI_OVER_2), RoundingMode.TIES_TO_EVEN,
              new StatusFlags()),
          RoundingMode.TIES_TO_EVEN,
          new StatusFlags());
      // sin(pi*x) from sin((pi/2)*(2*frac)); using frac directly needs pi.
      angle = UxOps.add(angle, angle, RoundingMode.TIES_TO_EVEN, new StatusFlags());
      Binary128 sine = DpmlTrig.sin(angle, RoundingMode.TIES_TO_EVEN,
          new StatusFlags());
      Unpacked absSine = UxOps.unpack(sine);
      absSine.sign = 0;
      Unpacked logSine = log(absSine, st);
      KernelEval.sub(result, logSine, tmp, st);
      result.copyFrom(tmp);
    }
    return result;
  }

  private static Unpacked log(Unpacked value, StatusFlags st) {
    value.sign = 0;
    UxOps.normalize(value);
    return DpmlLogInvHyperKernel.log(
        value, UxTable.readUxFloat(LogX.TABLE, LogX.LN_2), st);
  }

  private static Unpacked ux(int offset) {
    return UxTable.readUxFloat(TABLE, offset);
  }

  private static int compare(Unpacked a, Unpacked b) {
    if (a.sign != b.sign) {
      return a.sign != 0 ? -1 : 1;
    }
    int magnitude = Integer.compare(a.exponent, b.exponent);
    if (magnitude == 0) {
      magnitude = Wide.cmp128(a.fracHi, a.fracLo, b.fracHi, b.fracLo);
    }
    return a.sign == 0 ? magnitude : -magnitude;
  }

  private static boolean isInteger(Unpacked x) {
    if (x.exponent <= 0) {
      return false;
    }
    if (x.exponent >= 128) {
      return true;
    }
    int fractionalBits = 128 - x.exponent;
    if (fractionalBits >= 64) {
      int hiBits = fractionalBits - 64;
      long hiMask = hiBits == 64 ? -1L : (1L << hiBits) - 1L;
      return x.fracLo == 0L && (x.fracHi & hiMask) == 0L;
    }
    long loMask = (1L << fractionalBits) - 1L;
    return (x.fracLo & loMask) == 0L;
  }

  /** Fractional part of a positive, noninteger unpacked value. */
  private static Unpacked fractionalPart(Unpacked x) {
    Unpacked f = x.copy();
    if (x.exponent <= 0) {
      return f;
    }
    int fractionalBits = 128 - x.exponent;
    if (fractionalBits >= 64) {
      int hiBits = fractionalBits - 64;
      long hiMask = hiBits == 64 ? -1L : (1L << hiBits) - 1L;
      f.fracHi &= hiMask;
    } else {
      f.fracHi = 0L;
      f.fracLo &= (1L << fractionalBits) - 1L;
    }
    UxOps.normalize(f);
    return f;
  }

  private static boolean gammaSignNegative(Unpacked x) {
    if (x.sign == 0) {
      return false;
    }
    Unpacked magnitude = x.copy();
    magnitude.sign = 0;
    if (magnitude.exponent <= 0) {
      return true;
    }
    int unitBit = 128 - magnitude.exponent;
    boolean floorOdd;
    if (unitBit >= 64) {
      floorOdd = ((magnitude.fracHi >>> (unitBit - 64)) & 1L) != 0L;
    } else if (unitBit >= 0) {
      floorOdd = ((magnitude.fracLo >>> unitBit) & 1L) != 0L;
    } else {
      floorOdd = false;
    }
    return !floorOdd;
  }

  private static Binary128 overflow(
      RoundingMode mode, boolean negative, StatusFlags st) {
    Unpacked huge = new Unpacked();
    huge.setNorm(negative ? Unpacked.UX_SIGN_BIT : 0,
        Unpacked.UX_INFINITY_EXPONENT - 1, Unpacked.UX_MSB, 0L);
    return UxOps.pack(huge, mode, st);
  }

  private static Binary128 quietNaN(Binary128 x, Unpacked a, StatusFlags st) {
    if (a.signaling) {
      st.raise(StatusFlags.INVALID);
    }
    return Binary128.fromRawBits(
        x.highBits() | Binary128.QUIET_NAN_BIT, x.lowBits());
  }
}
