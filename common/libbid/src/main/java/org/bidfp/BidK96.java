/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language regarding permissions and
 * limitations under the License.
 */
package org.bidfp;

/**
 * Decimal96 specialist ({@code k=3}, p=25): two-limb coefficients, three-limb
 * products, and a private scratch. NaN/Inf are handled here; there is no
 * fallback to {@link BidK160} or {@link BidKScratch}.
 */
final class BidK96 {
  private static final ThreadLocal<Scratch> LOCAL =
      ThreadLocal.withInitial(Scratch::new);
  private static final int PRECISION = 25;
  private static final int BIAS = 1559;
  private static final int MAX_BIASED = 3071;

  private BidK96() {
  }

  static boolean matches(FormatParams format) {
    return format.units() == 3;
  }

  static void add(
      FormatParams format,
      long[] x,
      long[] y,
      boolean negateY,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    Scratch s = LOCAL.get();
    Kind kx = load(format, x, s.xa, s);
    boolean an = s.an;
    int ae = s.ae;
    Kind ky = load(format, y, s.xb, s);
    boolean bn = negateY && ky != Kind.NAN && ky != Kind.SNAN ? !s.an : s.an;
    if (binarySpecial(format, kx, ky, an, bn, true, flags, out)) {
      return;
    }
    if (ae == s.ae && tryAddAligned(format, s.xa, an, s.xb, bn, ae, out)) {
      return;
    }
    addWide(format, an, ae, s.xa, bn, s.ae, s.xb, mode, flags, out, s);
  }

  static void mul(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    Scratch s = LOCAL.get();
    Kind kx = load(format, x, s.xa, s);
    int ae = s.ae;
    boolean an = s.an;
    Kind ky = load(format, y, s.xb, s);
    boolean bn = s.an;
    int be = s.ae;
    if (mulDivSpecial(
        format, kx, ky, an, bn, s.xa.isZero(), s.xb.isZero(), false, flags,
        out)) {
      return;
    }
    boolean negative = an ^ bn;
    if (s.xa.isZero() || s.xb.isZero()) {
      s.xr.clear();
      pack(format, negative, ae + be, s.xr, mode, flags, s, out);
      return;
    }
    s.xr.multiply2(s.xa, s.xb);
    pack(format, negative, ae + be, s.xr, mode, flags, s, out);
  }

  static void div(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    Scratch s = LOCAL.get();
    Kind kx = load(format, x, s.xa, s);
    int ae = s.ae;
    boolean an = s.an;
    Kind ky = load(format, y, s.xb, s);
    boolean bn = s.an;
    int be = s.ae;
    if (mulDivSpecial(
        format, kx, ky, an, bn, s.xa.isZero(), s.xb.isZero(), true, flags,
        out)) {
      return;
    }
    boolean negative = an ^ bn;
    int nd = s.xa.decimalDigits();
    int dd = s.xb.decimalDigits();
    int ratio = nd - dd;
    s.xt.copyFrom(s.xb);
    if (ratio >= 0) {
      s.xt.multiplyPow10(ratio);
      if (s.xa.compare(s.xt) < 0) {
        ratio--;
      }
    } else {
      s.xt.copyFrom(s.xa);
      s.xt.multiplyPow10(-ratio);
      if (s.xt.compare(s.xb) < 0) {
        ratio--;
      }
    }
    int scale = PRECISION + 2 - ratio;
    if (scale < 0) {
      scale = 0;
    }
    if (scale > 0) {
      s.xa.multiplyPow10(scale);
    }
    s.xr.divide(s.xa, s.xb, s.xt);
    pack(
        format, negative, ae - be - scale, s.xr, !s.xa.isZero(), mode, flags,
        s, out);
  }

  static void sqrt(
      FormatParams format,
      long[] x,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    Scratch s = LOCAL.get();
    Kind kx = load(format, x, s.xa, s);
    if (kx == Kind.NAN || kx == Kind.SNAN) {
      BidCodec.packNaN(format, false, false, out);
      return;
    }
    if (kx == Kind.INF) {
      if (s.an) {
        flags.raise(StatusFlags.INVALID);
        BidCodec.packNaN(format, false, false, out);
      } else {
        BidCodec.packInfinity(format, false, out);
      }
      return;
    }
    if (s.an && !s.xa.isZero()) {
      flags.raise(StatusFlags.INVALID);
      BidCodec.packNaN(format, false, false, out);
      return;
    }
    if (s.xa.isZero()) {
      pack(format, s.an, s.ae, s.xa, mode, flags, s, out);
      return;
    }
    int exp = s.ae;
    int extra = PRECISION + 4;
    if ((exp & 1) != 0) {
      s.xa.multiplySmall(10L);
      exp--;
    }
    s.xa.multiplyPow10(2 * extra);
    exp -= 2 * extra;
    boolean remSticky = Wide6.sqrtFloor(s.xa, s.xb, s.xr, s.xt, s.xn);
    pack(format, false, exp / 2, s.xb, remSticky, mode, flags, s, out);
  }

  static void fma(
      FormatParams format,
      long[] x,
      long[] y,
      long[] z,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    Scratch s = LOCAL.get();
    Kind kx = load(format, x, s.xa, s);
    int ae = s.ae;
    boolean an = s.an;
    Kind ky = load(format, y, s.xb, s);
    int be = s.ae;
    boolean bn = s.an;
    Kind kz = load(format, z, s.xt, s);
    int ce = s.ae;
    boolean cn = s.an;
    if (kx == Kind.NAN || ky == Kind.NAN || kz == Kind.NAN
        || kx == Kind.SNAN || ky == Kind.SNAN || kz == Kind.SNAN) {
      BidCodec.packNaN(format, false, false, out);
      return;
    }
    boolean pNeg = an ^ bn;
    if (kx == Kind.INF || ky == Kind.INF) {
      if ((kx == Kind.INF && s.xb.isZero())
          || (ky == Kind.INF && s.xa.isZero())) {
        flags.raise(StatusFlags.INVALID);
        BidCodec.packNaN(format, false, false, out);
        return;
      }
      if (kz == Kind.INF && cn != pNeg) {
        flags.raise(StatusFlags.INVALID);
        BidCodec.packNaN(format, false, false, out);
        return;
      }
      BidCodec.packInfinity(format, pNeg, out);
      return;
    }
    if (kz == Kind.INF) {
      BidCodec.packInfinity(format, cn, out);
      return;
    }
    s.xr.multiply2(s.xa, s.xb);
    s.xa.copyFrom(s.xr);
    addWide(format, pNeg, ae + be, s.xa, cn, ce, s.xt, mode, flags, out, s);
  }

  static int sqlCompare(FormatParams format, long[] x, long[] y) {
    long xh = x[1];
    long yh = y[1];
    if (BidCodec.unpackBiasedExponent(format, xh)
        == BidCodec.unpackBiasedExponent(format, yh)) {
      long x1 = coefficientHigh(format, xh);
      long y1 = coefficientHigh(format, yh);
      long[] max = format.maxCoefficientWords();
      long x0 = x[0];
      long y0 = y[0];
      if (!allowed(x0, x1, max)) {
        x1 = 0L;
        x0 = 0L;
      }
      if (!allowed(y0, y1, max)) {
        y1 = 0L;
        y0 = 0L;
      }
      if ((x0 | x1) == 0L && (y0 | y1) == 0L) {
        return 0;
      }
      boolean xn = (xh & format.signMask()) != 0L;
      boolean yn = (yh & format.signMask()) != 0L;
      if (xn != yn) {
        return xn ? -1 : 1;
      }
      int cmp = Long.compareUnsigned(x1, y1);
      if (cmp == 0) {
        cmp = Long.compareUnsigned(x0, y0);
      }
      return xn ? -cmp : cmp;
    }
    Scratch s = LOCAL.get();
    load(format, x, s.xa, s);
    boolean an = s.an;
    int ae = s.ae;
    load(format, y, s.xb, s);
    if (s.xa.isZero() && s.xb.isZero()) {
      return 0;
    }
    if (an != s.an) {
      return an ? -1 : 1;
    }
    int cmp = compareAbsolute(s.xa, ae, s.xb, s.ae, s);
    return an ? -cmp : cmp;
  }

  static void addUnpacked(
      BidUnpacked x,
      BidUnpacked y,
      boolean negateY,
      RoundingMode mode,
      StatusFlags flags,
      BidUnpacked out) {
    Scratch s = LOCAL.get();
    BidCodec.storeUnpacked(x, s.px);
    BidCodec.storeUnpacked(y, s.py);
    add(x.format(), s.px, s.py, negateY, mode, flags, s.po);
    BidCodec.loadUnpacked(x.format(), s.po, out);
  }

  static void mulUnpacked(
      BidUnpacked x,
      BidUnpacked y,
      RoundingMode mode,
      StatusFlags flags,
      BidUnpacked out) {
    Scratch s = LOCAL.get();
    BidCodec.storeUnpacked(x, s.px);
    BidCodec.storeUnpacked(y, s.py);
    mul(x.format(), s.px, s.py, mode, flags, s.po);
    BidCodec.loadUnpacked(x.format(), s.po, out);
  }

  static void divUnpacked(
      BidUnpacked x,
      BidUnpacked y,
      RoundingMode mode,
      StatusFlags flags,
      BidUnpacked out) {
    Scratch s = LOCAL.get();
    BidCodec.storeUnpacked(x, s.px);
    BidCodec.storeUnpacked(y, s.py);
    div(x.format(), s.px, s.py, mode, flags, s.po);
    BidCodec.loadUnpacked(x.format(), s.po, out);
  }

  static void sqrtUnpacked(
      BidUnpacked x, RoundingMode mode, StatusFlags flags, BidUnpacked out) {
    Scratch s = LOCAL.get();
    BidCodec.storeUnpacked(x, s.px);
    sqrt(x.format(), s.px, mode, flags, s.po);
    BidCodec.loadUnpacked(x.format(), s.po, out);
  }

  static void fmaUnpacked(
      BidUnpacked x,
      BidUnpacked y,
      BidUnpacked z,
      RoundingMode mode,
      StatusFlags flags,
      BidUnpacked out) {
    Scratch s = LOCAL.get();
    BidCodec.storeUnpacked(x, s.px);
    BidCodec.storeUnpacked(y, s.py);
    BidCodec.storeUnpacked(z, s.pz);
    fma(x.format(), s.px, s.py, s.pz, mode, flags, s.po);
    BidCodec.loadUnpacked(x.format(), s.po, out);
  }

  private static void addWide(
      FormatParams format,
      boolean an,
      int ae,
      Wide6 left,
      boolean bn,
      int be,
      Wide6 right,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      Scratch s) {
    if (left.isZero() && right.isZero()) {
      s.xr.clear();
      pack(format, an && bn, Math.min(ae, be), s.xr, mode, flags, s, out);
      return;
    }
    if (left.isZero()) {
      pack(format, bn, be, right, mode, flags, s, out);
      return;
    }
    if (right.isZero()) {
      pack(format, an, ae, left, mode, flags, s, out);
      return;
    }
    Wide6 a = left;
    Wide6 b = right;
    boolean aNeg = an;
    boolean bNeg = bn;
    int aExp = ae;
    int bExp = be;
    boolean twoLimb = ae == be;
    if (ae != be) {
      if (aExp < bExp) {
        a = right;
        b = left;
        aNeg = bn;
        bNeg = an;
        aExp = be;
        bExp = ae;
      }
      int difference = aExp - bExp;
      int aDigits = a.decimalDigits();
      int scale = Math.min(PRECISION + 1 - aDigits, difference);
      if (scale > 0) {
        a.multiplyPow10(scale);
        aExp -= scale;
        difference -= scale;
      }
      if (difference > PRECISION + 2) {
        pack(format, aNeg, aExp, a, !b.isZero(), mode, flags, s, out);
        return;
      }
      if (difference > 0) {
        a.multiplyPow10(difference);
        aExp -= difference;
        twoLimb = false;
      }
    }
    boolean negative;
    if (aNeg == bNeg) {
      if (twoLimb) {
        s.xr.add2(a, b);
      } else {
        s.xr.add(a, b);
      }
      negative = aNeg;
    } else {
      int cmp = a.compare(b);
      if (cmp == 0) {
        s.xr.clear();
        pack(format, false, aExp, s.xr, mode, flags, s, out);
        return;
      }
      if (cmp > 0) {
        if (twoLimb) {
          s.xr.subtract2(a, b);
        } else {
          s.xr.subtract(a, b);
        }
        negative = aNeg;
      } else {
        if (twoLimb) {
          s.xr.subtract2(b, a);
        } else {
          s.xr.subtract(b, a);
        }
        negative = bNeg;
      }
    }
    if (format.coefficientAllowed(s.xr)) {
      BidCodec.packFinite(format, negative, aExp + BIAS, s.xr, out);
      return;
    }
    pack(format, negative, aExp, s.xr, mode, flags, s, out);
  }

  private static int compareAbsolute(
      Wide6 a, int ae, Wide6 b, int be, Scratch s) {
    if (a.isZero()) {
      return b.isZero() ? 0 : -1;
    }
    if (b.isZero()) {
      return 1;
    }
    int diff = ae - be;
    if (diff == 0) {
      return a.compare(b);
    }
    if (diff >= PRECISION) {
      return 1;
    }
    if (diff <= -PRECISION) {
      return -1;
    }
    if (diff > 0) {
      s.xt.copyFrom(a);
      s.xt.multiplyPow10(diff);
      return s.xt.compare(b);
    }
    s.xt.copyFrom(b);
    s.xt.multiplyPow10(-diff);
    return a.compare(s.xt);
  }

  private static void pack(
      FormatParams format,
      boolean negative,
      int exp,
      Wide6 coeff,
      RoundingMode mode,
      StatusFlags flags,
      Scratch s,
      long[] out) {
    pack(format, negative, exp, coeff, false, mode, flags, s, out);
  }

  private static void pack(
      FormatParams format,
      boolean negative,
      int exp,
      Wide6 coeff,
      boolean extraSticky,
      RoundingMode mode,
      StatusFlags flags,
      Scratch s,
      long[] out) {
    if (!extraSticky && tryPackExact(format, negative, exp, coeff, out)) {
      return;
    }
    int extra = roundToPrecision(coeff, negative, extraSticky, mode, s);
    exp += extra;
    if (s.inexact) {
      flags.raise(StatusFlags.INEXACT);
    }
    if (coeff.isZero()) {
      int biased = exp + BIAS;
      if (biased < 0) {
        biased = 0;
      }
      if (biased > MAX_BIASED) {
        biased = MAX_BIASED;
      }
      BidCodec.packFinite(format, negative, biased, coeff, out);
      return;
    }
    int biased = exp + BIAS;
    if (biased < 0) {
      int first = coeff.dividePow10(-biased, s.sticky);
      if (BidRound.shouldIncrement(
          negative, coeff.l0, first, s.sticky[0], mode)) {
        coeff.addSmall(1L);
      }
      if (first != 0 || s.sticky[0]) {
        flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
      }
      BidCodec.packFinite(format, negative, 0, coeff, out);
      return;
    }
    while (biased > MAX_BIASED && coeff.decimalDigits() < PRECISION) {
      coeff.multiplySmall(10L);
      biased--;
    }
    if (biased > MAX_BIASED) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
      if (BidRound.overflowToInfinity(negative, mode)) {
        BidCodec.packInfinity(format, negative, out);
        return;
      }
      BidCodec.packFinite(
          format, negative, MAX_BIASED, format.maxCoefficientWords(), out);
      return;
    }
    if (biased == 0 && coeff.decimalDigits() < PRECISION && s.inexact) {
      flags.raise(StatusFlags.UNDERFLOW);
    }
    BidCodec.packFinite(format, negative, biased, coeff, out);
  }

  private static boolean tryPackExact(
      FormatParams format, boolean negative, int exp, Wide6 coeff, long[] out) {
    int biased = exp + BIAS;
    if (biased < 0 || biased > MAX_BIASED) {
      return false;
    }
    if (!format.coefficientAllowed(coeff)) {
      return false;
    }
    BidCodec.packFinite(format, negative, biased, coeff, out);
    return true;
  }

  private static int roundToPrecision(
      Wide6 coeff,
      boolean negative,
      boolean extraSticky,
      RoundingMode mode,
      Scratch s) {
    s.inexact = extraSticky;
    if (coeff.isZero()) {
      return 0;
    }
    int digits = coeff.decimalDigits();
    if (digits <= PRECISION) {
      if (extraSticky
          && BidRound.shouldIncrement(negative, coeff.l0, 0, true, mode)) {
        coeff.addSmall(1L);
        s.inexact = true;
      }
      return 0;
    }
    int extra = digits - PRECISION;
    int first = coeff.dividePow10(extra, s.sticky);
    s.sticky[0] |= extraSticky;
    boolean inexact = first != 0 || s.sticky[0];
    s.inexact = inexact;
    if (BidRound.shouldIncrement(negative, coeff.l0, first, s.sticky[0], mode)) {
      coeff.addSmall(1L);
      if (coeff.decimalDigits() > PRECISION) {
        coeff.dividePow10(1, s.sticky);
        extra++;
      }
    }
    return extra;
  }

  private static boolean tryAddAligned(
      FormatParams format,
      Wide6 x,
      boolean xn,
      Wide6 y,
      boolean yn,
      int exp,
      long[] out) {
    boolean negative;
    long r0;
    long r1;
    long x0 = x.l0;
    long x1 = x.l1;
    long y0 = y.l0;
    long y1 = y.l1;
    if ((x0 | x1) == 0L) {
      r0 = y0;
      r1 = y1;
      negative = (y0 | y1) == 0L ? xn && yn : yn;
    } else if ((y0 | y1) == 0L) {
      r0 = x0;
      r1 = x1;
      negative = xn;
    } else if (xn == yn) {
      r0 = x0 + y0;
      r1 = x1 + y1 + (Long.compareUnsigned(r0, x0) < 0 ? 1L : 0L);
      negative = xn;
    } else {
      int cmp = Long.compareUnsigned(x1, y1);
      if (cmp == 0) {
        cmp = Long.compareUnsigned(x0, y0);
      }
      if (cmp == 0) {
        r0 = 0L;
        r1 = 0L;
        negative = false;
      } else if (cmp > 0) {
        r0 = x0 - y0;
        r1 = x1 - y1 - (Long.compareUnsigned(x0, y0) < 0 ? 1L : 0L);
        negative = xn;
      } else {
        r0 = y0 - x0;
        r1 = y1 - x1 - (Long.compareUnsigned(y0, x0) < 0 ? 1L : 0L);
        negative = yn;
      }
    }
    long[] max = format.maxCoefficientWords();
    if (!allowed(r0, r1, max)) {
      return false;
    }
    packWords(format, negative, exp + BIAS, r0, r1, out);
    return true;
  }

  private static Kind load(
      FormatParams format, long[] words, Wide6 coefficient, Scratch s) {
    if (BidCodec.isNaN(format, words)) {
      s.an = BidCodec.isSigned(format, words);
      return BidCodec.isSignalingNaN(format, words) ? Kind.SNAN : Kind.NAN;
    }
    if (BidCodec.isInfinite(format, words)) {
      s.an = BidCodec.isSigned(format, words);
      return Kind.INF;
    }
    long hi = words[1];
    coefficient.l0 = words[0];
    coefficient.l1 = coefficientHigh(format, hi);
    coefficient.l2 = 0L;
    coefficient.l3 = 0L;
    coefficient.l4 = 0L;
    coefficient.l5 = 0L;
    if (!format.coefficientAllowed(coefficient)) {
      coefficient.clear();
    }
    s.an = (hi & format.signMask()) != 0L;
    s.ae = BidCodec.unpackBiasedExponent(format, hi) - BIAS;
    return Kind.FINITE;
  }

  private static boolean binarySpecial(
      FormatParams format,
      Kind kx,
      Kind ky,
      boolean xn,
      boolean yn,
      boolean adding,
      StatusFlags flags,
      long[] out) {
    if (kx == Kind.NAN || ky == Kind.NAN || kx == Kind.SNAN || ky == Kind.SNAN) {
      BidCodec.packNaN(format, false, false, out);
      return true;
    }
    if (kx == Kind.INF && ky == Kind.INF) {
      if (adding && xn != yn) {
        flags.raise(StatusFlags.INVALID);
        BidCodec.packNaN(format, false, false, out);
      } else {
        BidCodec.packInfinity(format, xn, out);
      }
      return true;
    }
    if (kx == Kind.INF) {
      BidCodec.packInfinity(format, xn, out);
      return true;
    }
    if (ky == Kind.INF) {
      BidCodec.packInfinity(format, yn, out);
      return true;
    }
    return false;
  }

  private static boolean mulDivSpecial(
      FormatParams format,
      Kind kx,
      Kind ky,
      boolean xn,
      boolean yn,
      boolean xZero,
      boolean yZero,
      boolean dividing,
      StatusFlags flags,
      long[] out) {
    if (kx == Kind.NAN || ky == Kind.NAN || kx == Kind.SNAN || ky == Kind.SNAN) {
      BidCodec.packNaN(format, false, false, out);
      return true;
    }
    boolean negative = xn ^ yn;
    if (dividing) {
      if (ky == Kind.INF) {
        if (kx == Kind.INF) {
          flags.raise(StatusFlags.INVALID);
          BidCodec.packNaN(format, false, false, out);
        } else {
          BidCodec.packFinite(format, negative, BIAS, new long[] {0L, 0L}, out);
        }
        return true;
      }
      if (yZero) {
        if (xZero) {
          flags.raise(StatusFlags.INVALID);
          BidCodec.packNaN(format, false, false, out);
        } else {
          if (kx != Kind.INF) {
            flags.raise(StatusFlags.DIVIDE_BY_ZERO);
          }
          BidCodec.packInfinity(format, negative, out);
        }
        return true;
      }
      if (kx == Kind.INF) {
        BidCodec.packInfinity(format, negative, out);
        return true;
      }
      return false;
    }
    if (kx == Kind.INF || ky == Kind.INF) {
      if ((kx == Kind.INF && yZero) || (ky == Kind.INF && xZero)) {
        flags.raise(StatusFlags.INVALID);
        BidCodec.packNaN(format, false, false, out);
      } else {
        BidCodec.packInfinity(format, negative, out);
      }
      return true;
    }
    return false;
  }

  private static long coefficientHigh(FormatParams format, long hi) {
    if ((hi & format.steeringMask()) == format.steeringMask()) {
      return (hi & format.form2CoeffLastMask()) | format.form2ImplicitMask();
    }
    return hi & format.form1CoeffLastMask();
  }

  private static boolean allowed(long low, long high, long[] max) {
    int cmp = Long.compareUnsigned(high, max[1]);
    return cmp < 0 || (cmp == 0 && Long.compareUnsigned(low, max[0]) <= 0);
  }

  private static void packWords(
      FormatParams format,
      boolean negative,
      int exponent,
      long low,
      long high,
      long[] out) {
    out[0] = low;
    long exp = (long) exponent & format.expMask();
    if ((high & ~format.form1CoeffLastMask()) == 0L) {
      out[1] = (high & format.form1CoeffLastMask())
          | (exp << format.form1ExpShift());
    } else {
      out[1] = (high & format.form2CoeffLastMask())
          | (exp << format.form2ExpShift())
          | format.steeringMask();
    }
    if (negative) {
      out[1] |= format.signMask();
    }
  }

  private enum Kind {
    FINITE,
    INF,
    NAN,
    SNAN
  }

  private static final class Scratch {
    final Wide6 xa = new Wide6();
    final Wide6 xb = new Wide6();
    final Wide6 xr = new Wide6();
    final Wide6 xt = new Wide6();
    final Wide6 xn = new Wide6();
    final boolean[] sticky = new boolean[1];
    final long[] px = new long[2];
    final long[] py = new long[2];
    final long[] pz = new long[2];
    final long[] po = new long[2];
    boolean an;
    boolean bn;
    boolean inexact;
    int ae;
    int be;
  }
}
