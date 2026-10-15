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
 * Generic BID-k arithmetic. Finite add/mul/div/sqrt/fma, compare, and
 * toString use Knuth limbs (Decimal160 add/mul/FMA/compare use
 * {@link BidK160}). Parse still uses {@link DecNum}.
 *
 * <p>{@link Bid64} and {@link Bid128} remain the specialized hot paths. This
 * kernel covers Decimal96 (k=3) through Decimal256 (k=8).
 */
final class BidKRaw {
  private BidKRaw() {
  }

  static void add(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    addOrSub(format, x, y, mode, flags, out, false);
  }

  static void sub(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    addOrSub(format, x, y, mode, flags, out, true);
  }

  private static void addOrSub(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      boolean negateY) {
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      Kind kx;
      Kind ky;
      if (BidK160.matches(format)) {
        kx = BidKKnuth.unpackLeftWide(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide(format, y, frame, flags);
      } else {
        kx = BidKKnuth.unpackLeftWork(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWork(format, y, frame, flags);
      }
      if (negateY && ky != Kind.NAN && ky != Kind.SNAN) {
        frame.bn = !frame.bn;
      }
      if (propagateSpecial(
          format, kx, ky, frame.an, frame.bn, true, flags, out)) {
        return;
      }
      if (BidK160.matches(format)) {
        BidK160.addFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be, mode, flags, out,
            frame);
        return;
      }
      BidKKnuth.addFromWork(
          format, frame.an, frame.ae, frame.bn, frame.be, mode, flags, out,
          frame);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static void mul(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      Kind kx;
      Kind ky;
      boolean aZero;
      boolean bZero;
      if (BidK160.matches(format)) {
        kx = BidKKnuth.unpackLeftWide(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide(format, y, frame, flags);
        aZero = frame.xa.isZero();
        bZero = frame.xb.isZero();
      } else {
        kx = BidKKnuth.unpackLeftWork(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWork(format, y, frame, flags);
        aZero = LimbMath.isZero(frame.w0);
        bZero = LimbMath.isZero(frame.w1);
      }
      if (mulDivSpecial(
          format, kx, ky, frame.an, frame.bn,
          aZero, bZero, false, flags, out, frame)) {
        return;
      }
      if (BidK160.matches(format)) {
        BidK160.mulFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be, mode, flags, out,
            frame);
        return;
      }
      BidKKnuth.mulFromWork(
          format, frame.an, frame.ae, frame.bn, frame.be, mode, flags, out,
          frame);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static void div(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      Kind kx;
      Kind ky;
      boolean aZero;
      boolean bZero;
      if (BidK160.matches(format)) {
        kx = BidKKnuth.unpackLeftWide(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide(format, y, frame, flags);
        aZero = frame.xa.isZero();
        bZero = frame.xb.isZero();
      } else {
        kx = BidKKnuth.unpackLeftWork(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWork(format, y, frame, flags);
        aZero = LimbMath.isZero(frame.w0);
        bZero = LimbMath.isZero(frame.w1);
      }
      if (mulDivSpecial(
          format, kx, ky, frame.an, frame.bn,
          aZero, bZero, true, flags, out, frame)) {
        return;
      }
      if (BidK160.matches(format)) {
        BidK160.divFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be, mode, flags, out,
            frame);
        return;
      }
      BidKKnuth.divFromWork(
          format, frame.an, frame.ae, frame.bn, frame.be, mode, flags, out,
          frame);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static void sqrt(
      FormatParams format,
      long[] x,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      Kind kx = BidKKnuth.unpackLeftWork(format, x, frame, flags);
      if (kx == Kind.NAN || kx == Kind.SNAN) {
        BidCodec.packNaN(format, false, false, out);
        return;
      }
      if (kx == Kind.INF) {
        if (frame.an) {
          flags.raise(StatusFlags.INVALID);
          BidCodec.packNaN(format, false, false, out);
        } else {
          BidCodec.packInfinity(format, false, out);
        }
        return;
      }
      if (frame.an && !LimbMath.isZero(frame.w0)) {
        flags.raise(StatusFlags.INVALID);
        BidCodec.packNaN(format, false, false, out);
        return;
      }
      if (LimbMath.isZero(frame.w0)) {
        BidKKnuth.pack(
            format, frame.an, frame.ae, frame.w0, mode, flags, frame, out);
        return;
      }
      BidKKnuth.sqrtFromWork(format, frame.ae, mode, flags, out, frame);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static void fma(
      FormatParams format,
      long[] x,
      long[] y,
      long[] z,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      boolean wide = BidK160.matches(format);
      Kind kx;
      Kind ky;
      boolean aZero;
      boolean bZero;
      if (wide) {
        kx = BidKKnuth.unpackLeftWide(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide(format, y, frame, flags);
        aZero = frame.xa.isZero();
        bZero = frame.xb.isZero();
      } else {
        kx = BidKKnuth.unpackLeftWork(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWork(format, y, frame, flags);
        aZero = LimbMath.isZero(frame.w0);
        bZero = LimbMath.isZero(frame.w1);
      }
      if (kx == Kind.NAN || ky == Kind.NAN || kx == Kind.SNAN || ky == Kind.SNAN) {
        BidCodec.packNaN(format, false, false, out);
        return;
      }
      if (kx == Kind.INF || ky == Kind.INF) {
        if ((kx == Kind.INF && bZero) || (ky == Kind.INF && aZero)) {
          flags.raise(StatusFlags.INVALID);
          BidCodec.packNaN(format, false, false, out);
          return;
        }
        Kind kz = unpackAddend(format, z, wide, frame, flags);
        boolean productNeg = frame.an ^ frame.bn;
        if (kz == Kind.INF && frame.tmpNeg[0] != productNeg) {
          flags.raise(StatusFlags.INVALID);
          BidCodec.packNaN(format, false, false, out);
          return;
        }
        BidCodec.packInfinity(format, productNeg, out);
        return;
      }
      Kind kz = unpackAddend(format, z, wide, frame, flags);
      if (kz == Kind.NAN || kz == Kind.SNAN) {
        BidCodec.packNaN(format, false, false, out);
        return;
      }
      if (kz == Kind.INF) {
        BidCodec.packInfinity(format, frame.tmpNeg[0], out);
        return;
      }
      if (wide) {
        BidK160.fmaFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be,
            frame.tmpNeg[0], frame.tmpExp[0], mode, flags, out, frame);
        return;
      }
      BidKKnuth.fmaFromWork(
          format, frame.an, frame.ae, frame.bn, frame.be,
          frame.tmpNeg[0], frame.tmpExp[0], mode, flags, out, frame);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static void quantize(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      Kind kx;
      Kind ky;
      if (BidK160.matches(format)) {
        kx = BidKKnuth.unpackLeftWide(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide(format, y, frame, flags);
      } else {
        kx = BidKKnuth.unpackLeftWork(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWork(format, y, frame, flags);
      }
      if (kx == Kind.NAN || ky == Kind.NAN || kx == Kind.SNAN || ky == Kind.SNAN) {
        BidCodec.packNaN(format, false, false, out);
        return;
      }
      if (kx == Kind.INF || ky == Kind.INF) {
        if (kx == Kind.INF && ky == Kind.INF) {
          BidCodec.copyWords(format, x, out);
        } else {
          flags.raise(StatusFlags.INVALID);
          BidCodec.packNaN(format, false, false, out);
        }
        return;
      }
      if (BidK160.matches(format)) {
        BidK160.quantizeFromWide(
            format, frame.an, frame.ae, frame.be, mode, flags, out, frame);
        return;
      }
      BidKKnuth.quantizeFromWork(
          format, frame.an, frame.ae, frame.be, mode, flags, out, frame);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static void convert(
      FormatParams src,
      long[] x,
      FormatParams dest,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      Kind kx;
      if (BidK160.matches(src)) {
        kx = BidKKnuth.unpackLeftWide(src, x, frame, flags);
      } else {
        kx = BidKKnuth.unpackLeftWork(src, x, frame, flags);
      }
      if (kx == Kind.NAN || kx == Kind.SNAN) {
        BidCodec.packNaN(dest, frame.an, false, out);
        if (kx == Kind.SNAN) {
          flags.raise(StatusFlags.INVALID);
        }
        return;
      }
      if (kx == Kind.INF) {
        BidCodec.packInfinity(dest, frame.an, out);
        return;
      }
      if (BidK160.matches(src)) {
        BidK160.convertFromWide(
            dest, frame.an, frame.ae, mode, flags, out, frame);
        return;
      }
      BidKKnuth.convertFromWork(
          dest, frame.an, frame.ae, mode, flags, out, frame);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static void parse(
      FormatParams format,
      String text,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    String value = text.trim();
    boolean negative = value.startsWith("-");
    if (negative || value.startsWith("+")) {
      value = value.substring(1);
    }
    String upper = value.toUpperCase();
    if (upper.equals("INF") || upper.equals("INFINITY")) {
      BidCodec.packInfinity(format, negative, out);
      return;
    }
    if (upper.equals("NAN") || upper.equals("QNAN")) {
      BidCodec.packNaN(format, negative, false, out);
      return;
    }
    if (upper.equals("SNAN")) {
      BidCodec.packNaN(format, negative, true, out);
      return;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      parseFinite(value, negative, frame.a);
      pack(format, frame.a, mode, flags, frame.coeff, out);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static String toString(FormatParams format, long[] words) {
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      if (BidK160.matches(format)) {
        return BidK160.toString(format, words, frame);
      }
      return BidKKnuth.toString(format, words, frame);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static int sqlCompare(FormatParams format, long[] x, long[] y) {
    if (BidCodec.isNaN(format, x)) {
      return BidCodec.isNaN(format, y) ? 0 : 1;
    }
    if (BidCodec.isNaN(format, y)) {
      return -1;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      if (BidK160.matches(format)) {
        return BidK160.sqlCompare(format, x, y, frame);
      }
      return BidKKnuth.sqlCompare(format, x, y, frame);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static void requireLibm(FormatParams format) {
    if (format.units() > 4) {
      throw new UnsupportedOperationException(
          "k>128 libm is out of scope unless a binary256 engine is funded");
    }
  }

  enum Kind {
    FINITE,
    INF,
    NAN,
    SNAN
  }

  private static Kind unpackAddend(
      FormatParams format,
      long[] z,
      boolean wide,
      BidKScratch.Frame frame,
      StatusFlags flags) {
    if (wide) {
      return BidKKnuth.unpack(
          format, z, frame.xt, frame.tmpNeg, frame.tmpExp, flags);
    }
    return BidKKnuth.unpack(
        format, z, frame.w4, frame.tmpNeg, frame.tmpExp, flags);
  }

  private static Kind unpack(
      FormatParams format,
      long[] words,
      DecNum dest,
      UIntLimbs coeff,
      StatusFlags flags) {
    boolean negative = BidCodec.isSigned(format, words);
    if (BidCodec.isNaN(format, words)) {
      dest.clear();
      dest.setNegative(negative);
      if (BidCodec.isSignalingNaN(format, words)) {
        flags.raise(StatusFlags.INVALID);
        return Kind.SNAN;
      }
      return Kind.NAN;
    }
    if (BidCodec.isInfinite(format, words)) {
      dest.clear();
      dest.setNegative(negative);
      return Kind.INF;
    }
    BidCodec.unpackCoefficient(format, words, coeff);
    if (!format.coefficientAllowed(coeff)) {
      coeff.clear();
    }
    int unbiased = BidCodec.unpackBiasedExponent(format, words) - format.bias();
    dest.setCoefficient(negative, coeff, unbiased);
    return Kind.FINITE;
  }

  static void pack(
      FormatParams format,
      DecNum number,
      RoundingMode mode,
      StatusFlags flags,
      UIntLimbs coeff,
      long[] out) {
    int p = format.precision();
    int bias = format.bias();
    int maxBiased = format.maxBiasedExponent();
    boolean negative = number.isNegative();
    if (!number.isZero() && number.exp() + bias < 0) {
      DecNum probe = new DecNum();
      probe.copyFrom(number);
      probe.roundToDigits(p, mode, new StatusFlags());
      if (probe.exp() + bias < 0) {
        int shift = Math.max(-(number.exp() + bias), number.digitCount() - p);
        DecNum scaled = new DecNum();
        scaled.copyFrom(number);
        boolean[] sticky = {false};
        int first = scaled.dividePow10(shift, sticky);
        if (BidRound.shouldIncrement(
            negative, scaled.leastDigitParity(), first, sticky[0], mode)) {
          scaled.addOne();
          if (scaled.digitCount() > p) {
            scaled.dividePow10(1, sticky);
          }
        }
        if (first != 0 || sticky[0]) {
          flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
        }
        scaled.toUIntLimbs(coeff);
        BidCodec.packFinite(format, negative, scaled.exp() + bias, coeff, out);
        return;
      }
    }
    boolean roundedInexact = roundKeepingParity(number, p, mode, flags);
    int biased = number.exp() + bias;
    if (number.isZero()) {
      if (biased < 0) {
        biased = 0;
      }
      if (biased > maxBiased) {
        biased = maxBiased;
      }
      coeff.clear();
      BidCodec.packFinite(format, negative, biased, coeff, out);
      return;
    }
    if (biased < 0) {
      int shift = -biased;
      boolean[] sticky = {false};
      DecNum scaled = new DecNum();
      scaled.copyFrom(number);
      scaled.shiftExp(-scaled.exp());
      int first = scaled.dividePow10(shift, sticky);
      if (BidRound.shouldIncrement(
          negative, scaled.leastDigitParity(), first, sticky[0], mode)) {
        scaled.addOne();
      }
      if (first != 0 || sticky[0]) {
        flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
      }
      scaled.toUIntLimbs(coeff);
      BidCodec.packFinite(format, negative, 0, coeff, out);
      return;
    }
    while (biased > maxBiased && number.digitCount() < p) {
      number.multiplyBy10();
      number.shiftExp(-1);
      biased--;
    }
    if (biased > maxBiased) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
      if (BidRound.overflowToInfinity(negative, mode)) {
        BidCodec.packInfinity(format, negative, out);
        return;
      }
      coeff.copyFrom(format.maxCoefficientWords());
      BidCodec.packFinite(format, negative, maxBiased, coeff, out);
      return;
    }
    if (biased == 0 && number.digitCount() < p && roundedInexact) {
      flags.raise(StatusFlags.UNDERFLOW);
    }
    number.toUIntLimbs(coeff);
    BidCodec.packFinite(format, negative, biased, coeff, out);
  }

  private static boolean roundKeepingParity(
      DecNum number, int precision, RoundingMode mode, StatusFlags flags) {
    int digits = number.digitCount();
    if (number.isZero() || digits <= precision) {
      return false;
    }
    boolean[] sticky = {false};
    int first = number.dividePow10(digits - precision, sticky);
    boolean inexact = first != 0 || sticky[0];
    if (BidRound.shouldIncrement(
        number.isNegative(), number.leastDigitParity(), first, sticky[0], mode)) {
      number.addOne();
      if (number.digitCount() > precision) {
        number.dividePow10(1, sticky);
      }
    }
    if (inexact) {
      flags.raise(StatusFlags.INEXACT);
    }
    return inexact;
  }

  private static boolean propagateSpecial(
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
      long[] out,
      BidKScratch.Frame frame) {
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
          LimbMath.clear(frame.w0);
          BidKKnuth.pack(
              format, negative, 0, frame.w0, RoundingMode.TIES_TO_EVEN, flags,
              frame, out);
        }
        return true;
      }
      if (yZero) {
        if (xZero || kx == Kind.INF) {
          if (xZero) {
            flags.raise(StatusFlags.INVALID);
            BidCodec.packNaN(format, false, false, out);
          } else {
            BidCodec.packInfinity(format, negative, out);
          }
        } else {
          flags.raise(StatusFlags.DIVIDE_BY_ZERO);
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

  private static void parseFinite(String value, boolean negative, DecNum dest) {
    dest.clear();
    dest.setNegative(negative);
    int exponent = 0;
    int expMark = -1;
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      if (ch == 'e' || ch == 'E') {
        expMark = i;
        break;
      }
    }
    String digits = expMark >= 0 ? value.substring(0, expMark) : value;
    if (expMark >= 0) {
      exponent = Integer.parseInt(value.substring(expMark + 1));
    }
    int dot = digits.indexOf('.');
    String whole = dot >= 0 ? digits.substring(0, dot) + digits.substring(dot + 1) : digits;
    int frac = dot >= 0 ? digits.length() - dot - 1 : 0;
    for (int i = 0; i < whole.length(); i++) {
      char ch = whole.charAt(i);
      if (ch < '0' || ch > '9') {
        throw new NumberFormatException(value);
      }
      dest.multiplyBy10();
      dest.addDigit(ch - '0');
    }
    dest.shiftExp(exponent - frac);
  }
}
