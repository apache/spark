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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bidfp;

/**
 * Unrolled Wide8 kernels for Decimal224 and Decimal256 ({@code k=7..8}).
 * Bid64/Bid128 stay specialized; other k values keep narrower kernels or
 * {@link BidKKnuth}.
 */
final class BidK256 {
  private BidK256() {
  }

  static boolean matches(FormatParams format) {
    return format.units() >= 7;
  }

  static void add(
      FormatParams format,
      boolean an,
      int ae,
      UIntLimbs ac,
      boolean bn,
      int be,
      UIntLimbs bc,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    frame.ya.copyFrom(ac);
    frame.yb.copyFrom(bc);
    addFromWide(format, an, ae, bn, be, mode, flags, out, frame);
  }

  static void addFromWide(
      FormatParams format,
      boolean an,
      int ae,
      boolean bn,
      int be,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    addWide(format, an, ae, frame.ya, bn, be, frame.yb, mode, flags, out, frame);
  }

  static void mul(
      FormatParams format,
      boolean an,
      int ae,
      UIntLimbs ac,
      boolean bn,
      int be,
      UIntLimbs bc,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    frame.ya.copyFrom(ac);
    frame.yb.copyFrom(bc);
    mulFromWide(format, an, ae, bn, be, mode, flags, out, frame);
  }

  static void mulFromWide(
      FormatParams format,
      boolean an,
      int ae,
      boolean bn,
      int be,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    if (format.units() == 7) {
      frame.ya.store(frame.w0);
      frame.yb.store(frame.w1);
      BidKKnuth.mulFromWork(
          format, an, ae, bn, be, mode, flags, out, frame);
      return;
    }
    boolean negative = an ^ bn;
    if (frame.ya.isZero() || frame.yb.isZero()) {
      frame.yr.clear();
      pack(format, negative, ae + be, frame.yr, mode, flags, frame, out);
      return;
    }
    frame.yr.multiply4(frame.ya, frame.yb);
    pack(format, negative, ae + be, frame.yr, mode, flags, frame, out);
  }

  static void div(
      FormatParams format,
      boolean an,
      int ae,
      UIntLimbs ac,
      boolean bn,
      int be,
      UIntLimbs bc,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    frame.ya.copyFrom(ac);
    frame.yb.copyFrom(bc);
    divFromWide(format, an, ae, bn, be, mode, flags, out, frame);
  }

  static void divFromWide(
      FormatParams format,
      boolean an,
      int ae,
      boolean bn,
      int be,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    boolean negative = an ^ bn;
    int p = format.precision();
    int nd = frame.ya.decimalDigits();
    int dd = frame.yb.decimalDigits();
    int ratio = nd - dd;
    frame.yt.copyFrom(frame.yb);
    if (ratio >= 0) {
      frame.yt.multiplyPow10(ratio);
      if (frame.ya.compare(frame.yt) < 0) {
        ratio--;
      }
    } else {
      frame.yt.copyFrom(frame.ya);
      frame.yt.multiplyPow10(-ratio);
      if (frame.yt.compare(frame.yb) < 0) {
        ratio--;
      }
    }
    int scale = p + 2 - ratio;
    if (scale < 0) {
      scale = 0;
    }
    if (scale > 0) {
      frame.ya.multiplyPow10(scale);
    }
    frame.yr.divide(frame.ya, frame.yb, frame.yt);
    boolean remSticky = !frame.ya.isZero();
    pack(
        format, negative, ae - be - scale, frame.yr, remSticky, mode, flags,
        frame, out);
  }

  static void sqrt(
      FormatParams format,
      int exp,
      UIntLimbs coeff,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    frame.za.copyFrom(coeff);
    int extra = format.precision() + 4;
    if ((exp & 1) != 0) {
      frame.za.multiplySmall(10L);
      exp--;
    }
    frame.za.multiplyPow10(2 * extra);
    exp -= 2 * extra;
    boolean remSticky = Wide12.sqrtFloor(
        frame.za, frame.zb, frame.zr, frame.zt, frame.zw);
    frame.yr.copyFrom(frame.zb);
    pack(format, false, exp / 2, frame.yr, remSticky, mode, flags, frame, out);
  }

  static void fma(
      FormatParams format,
      boolean an,
      int ae,
      UIntLimbs ac,
      boolean bn,
      int be,
      UIntLimbs bc,
      boolean cn,
      int ce,
      UIntLimbs cc,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    frame.ya.copyFrom(ac);
    frame.yb.copyFrom(bc);
    frame.yt.copyFrom(cc);
    fmaFromWide(format, an, ae, bn, be, cn, ce, mode, flags, out, frame);
  }

  static void fmaFromWide(
      FormatParams format,
      boolean an,
      int ae,
      boolean bn,
      int be,
      boolean cn,
      int ce,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    boolean pNeg = an ^ bn;
    int productExp = ae + be;
    int difference = productExp - ce;
    if (difference > 0 && difference <= format.precision() + 2) {
      frame.ya.store(frame.w0);
      frame.yb.store(frame.w1);
      frame.yt.store(frame.w4);
      BidKKnuth.fmaFromWork(
          format, an, ae, bn, be, cn, ce, mode, flags, out, frame);
      return;
    }
    frame.yr.multiply4(frame.ya, frame.yb);
    frame.ya.copyFrom(frame.yr);
    addWide(
        format, pNeg, productExp, frame.ya, cn, ce, frame.yt, mode, flags, out,
        frame);
  }

  static void quantize(
      FormatParams format,
      boolean an,
      int ae,
      UIntLimbs ac,
      int targetExp,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    frame.ya.copyFrom(ac);
    quantizeFromWide(format, an, ae, targetExp, mode, flags, out, frame);
  }

  static void quantizeFromWide(
      FormatParams format,
      boolean an,
      int ae,
      int targetExp,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    int p = format.precision();
    if (frame.ya.isZero()) {
      pack(format, an, targetExp, frame.ya, mode, flags, frame, out);
      return;
    }
    int delta = ae - targetExp;
    if (delta > 0) {
      int digits = frame.ya.decimalDigits();
      if (delta > Wide8.POW10_MAX || digits + delta > p) {
        flags.raise(StatusFlags.INVALID);
        BidEmit.nan(format, false, false, out, frame);
        return;
      }
      frame.ya.multiplyPow10(delta);
      pack(format, an, targetExp, frame.ya, mode, flags, frame, out);
      return;
    }
    if (delta < 0) {
      int first = frame.ya.dividePow10(-delta, frame.sticky);
      if (BidRound.shouldIncrement(
          an, frame.ya.l0, first, frame.sticky[0], mode)) {
        frame.ya.addSmall(1L);
      }
      if (first != 0 || frame.sticky[0]) {
        flags.raise(StatusFlags.INEXACT);
      }
      pack(format, an, targetExp, frame.ya, mode, flags, frame, out);
      return;
    }
    pack(format, an, ae, frame.ya, mode, flags, frame, out);
  }

  static void convert(
      FormatParams dest,
      boolean negative,
      int exp,
      UIntLimbs coeff,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    frame.ya.copyFrom(coeff);
    convertFromWide(dest, negative, exp, mode, flags, out, frame);
  }

  static void convertFromWide(
      FormatParams dest,
      boolean negative,
      int exp,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    pack(dest, negative, exp, frame.ya, mode, flags, frame, out);
  }

  static int sqlCompare(
      FormatParams format, long[] x, long[] y, BidKScratch.Frame frame) {
    BidKKnuth.unpackLeftWide8(format, x, frame, new StatusFlags());
    BidKKnuth.unpackRightWide8(format, y, frame, new StatusFlags());
    if (frame.ya.isZero() && frame.yb.isZero()) {
      return 0;
    }
    if (frame.an != frame.bn) {
      return frame.an ? -1 : 1;
    }
    int cmp = compareAbsolute(
        frame.ya, frame.ae, frame.yb, frame.be, format.precision(), frame);
    return frame.an ? -cmp : cmp;
  }

  static String toString(FormatParams format, long[] words, BidKScratch.Frame frame) {
    if (BidCodec.isNaN(format, words)) {
      String sign = BidCodec.isSigned(format, words) ? "-" : "+";
      return sign + (BidCodec.isSignalingNaN(format, words) ? "SNaN" : "NaN");
    }
    if (BidCodec.isInfinite(format, words)) {
      return (BidCodec.isSigned(format, words) ? "-" : "+") + "Inf";
    }
    BidKKnuth.unpackLeftWide8(format, words, frame, new StatusFlags());
    String digits = frame.ya.toDecimalString();
    return (frame.an ? "-" : "+") + digits + "E" + frame.ae;
  }

  private static void addWide(
      FormatParams format,
      boolean an,
      int ae,
      Wide8 left,
      boolean bn,
      int be,
      Wide8 right,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    if (left.isZero() && right.isZero()) {
      frame.yr.clear();
      pack(format, an && bn, Math.min(ae, be), frame.yr, mode, flags, frame, out);
      return;
    }
    if (left.isZero()) {
      pack(format, bn, be, right, mode, flags, frame, out);
      return;
    }
    if (right.isZero()) {
      pack(format, an, ae, left, mode, flags, frame, out);
      return;
    }
    if (ae == be) {
      packAligned(format, an, ae, left, bn, right, mode, flags, out, frame);
      return;
    }
    Wide8 a = left;
    Wide8 b = right;
    boolean aNeg = an;
    boolean bNeg = bn;
    int aExp = ae;
    int bExp = be;
    if (aExp < bExp) {
      a = right;
      b = left;
      aNeg = bn;
      bNeg = an;
      aExp = be;
      bExp = ae;
    }
    int p = format.precision();
    int difference = aExp - bExp;
    int aDigits = a.decimalDigits();
    int scale = Math.min(p + 1 - aDigits, difference);
    if (scale > 0) {
      a.multiplyPow10(scale);
      aExp -= scale;
      difference -= scale;
    }
    if (difference > p + 2) {
      pack(format, aNeg, aExp, a, !b.isZero(), mode, flags, frame, out);
      return;
    }
    boolean narrow = difference == 0;
    if (difference > 0) {
      a.multiplyPow10(difference);
      aExp -= difference;
    }
    boolean negative;
    if (aNeg == bNeg) {
      if (narrow) {
        frame.yr.add4(a, b);
      } else {
        frame.yr.add(a, b);
      }
      negative = aNeg;
    } else {
      int cmp = a.compare(b);
      if (cmp == 0) {
        frame.yr.clear();
        pack(format, false, aExp, frame.yr, mode, flags, frame, out);
        return;
      }
      if (cmp > 0) {
        if (narrow) {
          frame.yr.subtract4(a, b);
        } else {
          frame.yr.subtract(a, b);
        }
        negative = aNeg;
      } else {
        if (narrow) {
          frame.yr.subtract4(b, a);
        } else {
          frame.yr.subtract(b, a);
        }
        negative = bNeg;
      }
    }
    pack(format, negative, aExp, frame.yr, mode, flags, frame, out);
  }

  private static void packAligned(
      FormatParams format,
      boolean an,
      int ae,
      Wide8 left,
      boolean bn,
      Wide8 right,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    boolean negative;
    if (an == bn) {
      frame.yr.add4(left, right);
      negative = an;
    } else {
      int cmp = left.compare(right);
      if (cmp == 0) {
        frame.yr.clear();
        pack(format, false, ae, frame.yr, mode, flags, frame, out);
        return;
      }
      if (cmp > 0) {
        frame.yr.subtract4(left, right);
        negative = an;
      } else {
        frame.yr.subtract4(right, left);
        negative = bn;
      }
    }
    if (format.coefficientAllowed(frame.yr)) {
      BidEmit.finite(
          format, negative, ae + format.bias(), frame.yr, out, frame);
    } else {
      pack(format, negative, ae, frame.yr, mode, flags, frame, out);
    }
  }

  static int compareAbsolute(
      Wide8 a, int ae, Wide8 b, int be, int precision, BidKScratch.Frame frame) {
    if (a.isZero()) {
      return b.isZero() ? 0 : -1;
    }
    if (b.isZero()) {
      return 1;
    }
    if (ae == be) {
      return a.compare(b);
    }
    int diff = ae - be;
    if (diff >= precision) {
      return 1;
    }
    if (diff <= -precision) {
      return -1;
    }
    if (diff > 0) {
      frame.yt.copyFrom(a);
      frame.yt.multiplyPow10(diff);
      return frame.yt.compare(b);
    }
    frame.yt.copyFrom(b);
    frame.yt.multiplyPow10(-diff);
    return a.compare(frame.yt);
  }

  private static void pack(
      FormatParams format,
      boolean negative,
      int exp,
      Wide8 coeff,
      RoundingMode mode,
      StatusFlags flags,
      BidKScratch.Frame frame,
      long[] out) {
    pack(format, negative, exp, coeff, false, mode, flags, frame, out);
  }

  private static boolean tryPackExact(
      FormatParams format,
      boolean negative,
      int exp,
      Wide8 coeff,
      long[] out,
      BidKScratch.Frame frame) {
    int biased = exp + format.bias();
    if (biased < 0 || biased > format.maxBiasedExponent()) {
      return false;
    }
    if (!format.coefficientAllowed(coeff)) {
      return false;
    }
    BidEmit.finite(format, negative, biased, coeff, out, frame);
    return true;
  }

  private static void pack(
      FormatParams format,
      boolean negative,
      int exp,
      Wide8 coeff,
      boolean extraSticky,
      RoundingMode mode,
      StatusFlags flags,
      BidKScratch.Frame frame,
      long[] out) {
    if (!extraSticky && tryPackExact(format, negative, exp, coeff, out, frame)) {
      return;
    }
    int p = format.precision();
    int extra = roundToPrecision(coeff, p, negative, extraSticky, mode, frame);
    exp += extra;
    if (frame.inexact[0]) {
      flags.raise(StatusFlags.INEXACT);
    }
    int bias = format.bias();
    int maxBiased = format.maxBiasedExponent();
    if (coeff.isZero()) {
      int biased = exp + bias;
      if (biased < 0) {
        biased = 0;
      }
      if (biased > maxBiased) {
        biased = maxBiased;
      }
      BidEmit.finite(format, negative, biased, coeff, out, frame);
      return;
    }
    int biased = exp + bias;
    if (biased < 0) {
      int first = coeff.dividePow10(-biased, frame.sticky);
      if (BidRound.shouldIncrement(
          negative, coeff.l0, first, frame.sticky[0], mode)) {
        coeff.addSmall(1L);
      }
      if (first != 0 || frame.sticky[0]) {
        flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
      }
      BidEmit.finite(format, negative, 0, coeff, out, frame);
      return;
    }
    while (biased > maxBiased && coeff.decimalDigits() < p) {
      coeff.multiplySmall(10L);
      biased--;
    }
    if (biased > maxBiased) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
      if (BidRound.overflowToInfinity(negative, mode)) {
        BidEmit.infinity(format, negative, out, frame);
        return;
      }
      BidEmit.finite(
          format, negative, maxBiased, format.maxCoefficientWords(), out, frame);
      return;
    }
    if (biased == 0 && coeff.decimalDigits() < p && frame.inexact[0]) {
      flags.raise(StatusFlags.UNDERFLOW);
    }
    BidEmit.finite(format, negative, biased, coeff, out, frame);
  }

  private static int roundToPrecision(
      Wide8 coeff,
      int p,
      boolean negative,
      boolean extraSticky,
      RoundingMode mode,
      BidKScratch.Frame frame) {
    frame.inexact[0] = extraSticky;
    if (coeff.isZero()) {
      return 0;
    }
    int digits = coeff.decimalDigits();
    if (digits <= p) {
      if (extraSticky
          && BidRound.shouldIncrement(negative, coeff.l0, 0, true, mode)) {
        coeff.addSmall(1L);
        frame.inexact[0] = true;
      }
      return 0;
    }
    int extra = digits - p;
    int first = coeff.dividePow10(extra, frame.sticky);
    frame.sticky[0] |= extraSticky;
    boolean inexact = first != 0 || frame.sticky[0];
    frame.inexact[0] = inexact;
    if (BidRound.shouldIncrement(negative, coeff.l0, first, frame.sticky[0], mode)) {
      coeff.addSmall(1L);
      if (coeff.decimalDigits() > p) {
        coeff.dividePow10(1, frame.sticky);
        extra++;
      }
    }
    return extra;
  }
}
