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
 * Shared N-limb BID-k arithmetic. Coefficients are binary integers in
 * {@link LimbMath} workspaces; rounding uses the five IEEE modes. Specialized
 * {@link Bid64} / {@link Bid128} kernels are unchanged.
 */
final class BidKKnuth {
  private BidKKnuth() {
  }

  static BidKRaw.Kind unpack(
      FormatParams format,
      long[] words,
      UIntLimbs coeff,
      boolean[] negative,
      int[] exp,
      StatusFlags flags) {
    boolean signed = BidCodec.isSigned(format, words);
    negative[0] = signed;
    if (BidCodec.isNaN(format, words)) {
      coeff.clear();
      exp[0] = 0;
      if (BidCodec.isSignalingNaN(format, words)) {
        flags.raise(StatusFlags.INVALID);
        return BidKRaw.Kind.SNAN;
      }
      return BidKRaw.Kind.NAN;
    }
    if (BidCodec.isInfinite(format, words)) {
      coeff.clear();
      exp[0] = 0;
      return BidKRaw.Kind.INF;
    }
    BidCodec.unpackCoefficient(format, words, coeff);
    if (!format.coefficientAllowed(coeff)) {
      coeff.clear();
    }
    exp[0] = BidCodec.unpackBiasedExponent(format, words) - format.bias();
    return BidKRaw.Kind.FINITE;
  }

  static BidKRaw.Kind unpackLeft(
      FormatParams format, long[] words, BidKScratch.Frame frame, StatusFlags flags) {
    BidKRaw.Kind kind =
        unpack(format, words, frame.ac, frame.tmpNeg, frame.tmpExp, flags);
    frame.an = frame.tmpNeg[0];
    frame.ae = frame.tmpExp[0];
    return kind;
  }

  static BidKRaw.Kind unpackRight(
      FormatParams format, long[] words, BidKScratch.Frame frame, StatusFlags flags) {
    BidKRaw.Kind kind =
        unpack(format, words, frame.bc, frame.tmpNeg, frame.tmpExp, flags);
    frame.bn = frame.tmpNeg[0];
    frame.be = frame.tmpExp[0];
    return kind;
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
    LimbMath.copyLimbs(frame.w0, ac);
    LimbMath.copyLimbs(frame.w1, bc);
    addWide(format, an, ae, frame.w0, bn, be, frame.w1, mode, flags, out, frame);
  }

  static void addWide(
      FormatParams format,
      boolean an,
      int ae,
      long[] left,
      boolean bn,
      int be,
      long[] right,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    if (left != frame.w0) {
      LimbMath.copy(frame.w0, left);
    }
    if (right != frame.w1) {
      LimbMath.copy(frame.w1, right);
    }
    if (LimbMath.isZero(frame.w0) && LimbMath.isZero(frame.w1)) {
      boolean negative = an && bn;
      pack(format, negative, Math.min(ae, be), frame.w0, mode, flags, frame, out);
      return;
    }
    if (LimbMath.isZero(frame.w0)) {
      pack(format, bn, be, frame.w1, mode, flags, frame, out);
      return;
    }
    if (LimbMath.isZero(frame.w1)) {
      pack(format, an, ae, frame.w0, mode, flags, frame, out);
      return;
    }
    boolean aNeg = an;
    int aExp = ae;
    long[] a = frame.w0;
    boolean bNeg = bn;
    int bExp = be;
    long[] b = frame.w1;
    if (aExp < bExp) {
      aNeg = bn;
      aExp = be;
      a = frame.w1;
      bNeg = an;
      bExp = ae;
      b = frame.w0;
    }
    int p = format.precision();
    int difference = aExp - bExp;
    int aDigits = LimbMath.decimalDigits(a);
    int scale = Math.min(p + 1 - aDigits, difference);
    if (scale > 0) {
      LimbMath.multiplyPow10(a, scale, frame.w2);
      aExp -= scale;
      difference -= scale;
    }
    if (difference == 0) {
      addAligned(aNeg, a, bNeg, b, frame.w2, frame.w3);
      pack(format, resultNegative(aNeg, a, bNeg, b, frame), aExp, frame.w2,
          mode, flags, frame, out);
      return;
    }
    if (difference > p + 2) {
      boolean sticky = !LimbMath.isZero(b);
      packWithSticky(format, aNeg, aExp, a, sticky, mode, flags, frame, out);
      return;
    }
    LimbMath.multiplyPow10(a, difference, frame.w2);
    aExp -= difference;
    addAligned(aNeg, a, bNeg, b, frame.w2, frame.w3);
    pack(format, resultNegative(aNeg, a, bNeg, b, frame), aExp, frame.w2,
        mode, flags, frame, out);
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
    LimbMath.copyLimbs(frame.w0, ac);
    LimbMath.copyLimbs(frame.w1, bc);
    boolean negative = an ^ bn;
    if (LimbMath.isZero(frame.w0) || LimbMath.isZero(frame.w1)) {
      LimbMath.clear(frame.w2);
      pack(format, negative, ae + be, frame.w2, mode, flags, frame, out);
      return;
    }
    LimbMath.multiply(frame.w2, frame.w0, frame.w1);
    pack(format, negative, ae + be, frame.w2, mode, flags, frame, out);
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
    LimbMath.copyLimbs(frame.w0, ac);
    LimbMath.copyLimbs(frame.w1, bc);
    boolean negative = an ^ bn;
    int p = format.precision();
    int nd = LimbMath.decimalDigits(frame.w0);
    int dd = LimbMath.decimalDigits(frame.w1);
    int ratio = nd - dd;
    if (ratio >= 0) {
      LimbMath.copy(frame.w3, frame.w1);
      LimbMath.multiplyPow10(frame.w3, ratio, frame.w4);
      if (LimbMath.compare(frame.w0, frame.w3) < 0) {
        ratio--;
      }
    } else {
      LimbMath.copy(frame.w3, frame.w0);
      LimbMath.multiplyPow10(frame.w3, -ratio, frame.w4);
      if (LimbMath.compare(frame.w3, frame.w1) < 0) {
        ratio--;
      }
    }
    int scale = p + 2 - ratio;
    if (scale < 0) {
      scale = 0;
    }
    if (scale > 0) {
      LimbMath.multiplyPow10(frame.w0, scale, frame.w4);
    }
    LimbMath.divide(
        frame.w2, frame.w3, frame.w0, frame.w1,
        frame.d0, frame.d1, frame.d2, frame.d3);
    boolean remSticky = !LimbMath.isZero(frame.w3);
    int exp = ae - be - scale;
    if (remSticky) {
      packWithSticky(format, negative, exp, frame.w2, true, mode, flags, frame, out);
    } else {
      pack(format, negative, exp, frame.w2, mode, flags, frame, out);
    }
  }

  static void sqrt(
      FormatParams format,
      int exp,
      UIntLimbs coeff,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      BidKScratch.Frame frame) {
    LimbMath.copyLimbs(frame.w0, coeff);
    int extra = format.precision() + 4;
    if ((exp & 1) != 0) {
      LimbMath.multiplySmall(frame.w0, 10L);
      exp--;
    }
    LimbMath.multiplyPow10(frame.w0, 2 * extra, frame.w4);
    exp -= 2 * extra;
    LimbMath.sqrtFloor(
        frame.w2, frame.w3, frame.w0, frame.w1, frame.w4, frame.w5,
        frame.d0, frame.d1, frame.d2, frame.d3);
    boolean remSticky = !LimbMath.isZero(frame.w3);
    if (remSticky) {
      packWithSticky(
          format, false, exp / 2, frame.w2, true, mode, flags, frame, out);
    } else {
      pack(format, false, exp / 2, frame.w2, mode, flags, frame, out);
    }
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
    LimbMath.copyLimbs(frame.w0, ac);
    LimbMath.copyLimbs(frame.w1, bc);
    boolean pNeg = an ^ bn;
    LimbMath.multiply(frame.w2, frame.w0, frame.w1);
    int pExp = ae + be;
    LimbMath.copyLimbs(frame.w1, cc);
    LimbMath.copy(frame.w0, frame.w2);
    addWide(format, pNeg, pExp, frame.w0, cn, ce, frame.w1, mode, flags, out, frame);
  }

  private static void addAligned(
      boolean aNeg, long[] a, boolean bNeg, long[] b, long[] dest, long[] tmp) {
    if (aNeg == bNeg) {
      LimbMath.add(dest, a, b);
    } else if (LimbMath.compare(a, b) >= 0) {
      LimbMath.subtract(dest, a, b);
    } else {
      LimbMath.subtract(dest, b, a);
    }
  }

  private static boolean resultNegative(
      boolean aNeg, long[] a, boolean bNeg, long[] b, BidKScratch.Frame frame) {
    if (aNeg == bNeg) {
      return aNeg;
    }
    int cmp = LimbMath.compare(a, b);
    if (cmp == 0) {
      return false;
    }
    return cmp > 0 ? aNeg : bNeg;
  }

  private static void packWithSticky(
      FormatParams format,
      boolean negative,
      int exp,
      long[] coeff,
      boolean sticky,
      RoundingMode mode,
      StatusFlags flags,
      BidKScratch.Frame frame,
      long[] out) {
    int p = format.precision();
    int digits = LimbMath.decimalDigits(coeff);
    if (digits < p && sticky) {
      LimbMath.multiplyPow10(coeff, 1, frame.w4);
      exp--;
      digits++;
      int first = 0;
      if (BidRound.shouldIncrement(negative, coeff[0], first, true, mode)) {
        LimbMath.addSmall(coeff, 1L);
      }
      flags.raise(StatusFlags.INEXACT);
      pack(format, negative, exp, coeff, mode, flags, frame, out);
      return;
    }
    if (digits >= p && sticky) {
      frame.sticky[0] = true;
      int extra = digits - p;
      int first = 0;
      if (extra > 0) {
        first = LimbMath.dividePow10(coeff, extra, frame.sticky);
        frame.sticky[0] = true;
        exp += extra;
      } else {
        first = 0;
      }
      if (BidRound.shouldIncrement(
          negative, coeff[0], extra > 0 ? first : 0, true, mode)) {
        LimbMath.addSmall(coeff, 1L);
      }
      flags.raise(StatusFlags.INEXACT);
      if (LimbMath.decimalDigits(coeff) > p) {
        LimbMath.dividePow10(coeff, 1, frame.sticky);
        exp++;
      }
      packRounded(format, negative, exp, coeff, mode, flags, frame, out);
      return;
    }
    pack(format, negative, exp, coeff, mode, flags, frame, out);
  }

  static void pack(
      FormatParams format,
      boolean negative,
      int exp,
      long[] coeff,
      RoundingMode mode,
      StatusFlags flags,
      BidKScratch.Frame frame,
      long[] out) {
    int p = format.precision();
    int extra = roundToPrecision(coeff, p, negative, mode, frame);
    exp += extra;
    if (frame.inexact[0]) {
      flags.raise(StatusFlags.INEXACT);
    }
    packRounded(format, negative, exp, coeff, mode, flags, frame, out);
  }

  private static void packRounded(
      FormatParams format,
      boolean negative,
      int exp,
      long[] coeff,
      RoundingMode mode,
      StatusFlags flags,
      BidKScratch.Frame frame,
      long[] out) {
    int p = format.precision();
    int bias = format.bias();
    int maxBiased = format.maxBiasedExponent();
    if (LimbMath.isZero(coeff)) {
      int biased = exp + bias;
      if (biased < 0) {
        biased = 0;
      }
      if (biased > maxBiased) {
        biased = maxBiased;
      }
      frame.coeff.clear();
      BidCodec.packFinite(format, negative, biased, frame.coeff, out);
      return;
    }
    int biased = exp + bias;
    if (biased < 0) {
      int shift = -biased;
      int first = LimbMath.dividePow10(coeff, shift, frame.sticky);
      if (BidRound.shouldIncrement(
          negative, coeff[0], first, frame.sticky[0], mode)) {
        LimbMath.addSmall(coeff, 1L);
      }
      if (first != 0 || frame.sticky[0]) {
        flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
      }
      LimbMath.toUIntLimbs(coeff, frame.coeff);
      BidCodec.packFinite(format, negative, 0, frame.coeff, out);
      return;
    }
    while (biased > maxBiased && LimbMath.decimalDigits(coeff) < p) {
      LimbMath.multiplySmall(coeff, 10L);
      biased--;
    }
    if (biased > maxBiased) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
      if (BidRound.overflowToInfinity(negative, mode)) {
        BidCodec.packInfinity(format, negative, out);
        return;
      }
      frame.coeff.copyFrom(format.maxCoefficientWords());
      BidCodec.packFinite(format, negative, maxBiased, frame.coeff, out);
      return;
    }
    if (biased == 0 && LimbMath.decimalDigits(coeff) < p && frame.inexact[0]) {
      flags.raise(StatusFlags.UNDERFLOW);
    }
    LimbMath.toUIntLimbs(coeff, frame.coeff);
    BidCodec.packFinite(format, negative, biased, frame.coeff, out);
  }

  /** Rounds {@code coeff} to {@code p} digits. Returns the exponent increase. */
  static int roundToPrecision(
      long[] coeff,
      int p,
      boolean negative,
      RoundingMode mode,
      BidKScratch.Frame frame) {
    frame.inexact[0] = false;
    if (LimbMath.isZero(coeff)) {
      return 0;
    }
    int digits = LimbMath.decimalDigits(coeff);
    if (digits <= p) {
      return 0;
    }
    int extra = digits - p;
    int first = LimbMath.dividePow10(coeff, extra, frame.sticky);
    boolean inexact = first != 0 || frame.sticky[0];
    frame.inexact[0] = inexact;
    if (BidRound.shouldIncrement(negative, coeff[0], first, frame.sticky[0], mode)) {
      LimbMath.addSmall(coeff, 1L);
      if (LimbMath.decimalDigits(coeff) > p) {
        LimbMath.dividePow10(coeff, 1, frame.sticky);
        extra++;
      }
    }
    return extra;
  }
}
