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

import java.math.BigInteger;

/**
 * Generic BID-k arithmetic using {@link DecNum} as the coefficient backend.
 *
 * <p>{@link Bid64} and {@link Bid128} remain the specialized hot paths. This
 * kernel covers Decimal96 and Decimal160 through Decimal256.
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
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      Kind kx = unpack(format, x, frame.a, frame.coeff, flags);
      Kind ky = unpack(format, y, frame.b, frame.coeff, flags);
      if (propagateSpecial(format, kx, ky, frame.a, frame.b, true, flags, out)) {
        return;
      }
      addFinite(frame.a, frame.b, frame.r);
      pack(format, frame.r, mode, flags, frame.coeff, out);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static void sub(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    long[] negated = BidCodec.cloneWords(format, y);
    if (BidCodec.isSigned(format, negated)) {
      BidCodec.clearBit(negated, format.signBit());
    } else {
      BidCodec.setBit(negated, format.signBit());
    }
    add(format, x, negated, mode, flags, out);
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
      Kind kx = unpack(format, x, frame.a, frame.coeff, flags);
      Kind ky = unpack(format, y, frame.b, frame.coeff, flags);
      if (mulDivSpecial(format, kx, ky, frame.a, frame.b, false, flags, out)) {
        return;
      }
      frame.r.copyFrom(frame.a);
      frame.r.multiply(frame.b);
      pack(format, frame.r, mode, flags, frame.coeff, out);
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
      Kind kx = unpack(format, x, frame.a, frame.coeff, flags);
      Kind ky = unpack(format, y, frame.b, frame.coeff, flags);
      if (mulDivSpecial(format, kx, ky, frame.a, frame.b, true, flags, out)) {
        return;
      }
      divideFinite(format, frame.a, frame.b, frame.r);
      pack(format, frame.r, mode, flags, frame.coeff, out);
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
      Kind kx = unpack(format, x, frame.a, frame.coeff, flags);
      if (kx == Kind.NAN || kx == Kind.SNAN) {
        BidCodec.packNaN(format, false, false, out);
        return;
      }
      if (kx == Kind.INF) {
        if (frame.a.isNegative()) {
          flags.raise(StatusFlags.INVALID);
          BidCodec.packNaN(format, false, false, out);
        } else {
          BidCodec.packInfinity(format, false, out);
        }
        return;
      }
      if (frame.a.isNegative() && !frame.a.isZero()) {
        flags.raise(StatusFlags.INVALID);
        BidCodec.packNaN(format, false, false, out);
        return;
      }
      if (frame.a.isZero()) {
        pack(format, frame.a, mode, flags, frame.coeff, out);
        return;
      }
      sqrtFinite(format, frame.a, frame.r);
      pack(format, frame.r, mode, flags, frame.coeff, out);
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
      Kind kx = unpack(format, x, frame.a, frame.coeff, flags);
      Kind ky = unpack(format, y, frame.b, frame.coeff, flags);
      if (kx == Kind.NAN || ky == Kind.NAN || kx == Kind.SNAN || ky == Kind.SNAN) {
        BidCodec.packNaN(format, false, false, out);
        return;
      }
      if (kx == Kind.INF || ky == Kind.INF) {
        if ((kx == Kind.INF && frame.b.isZero())
            || (ky == Kind.INF && frame.a.isZero())) {
          flags.raise(StatusFlags.INVALID);
          BidCodec.packNaN(format, false, false, out);
          return;
        }
        Kind kz = unpack(format, z, frame.r, frame.coeff, flags);
        boolean productNeg = frame.a.isNegative() ^ frame.b.isNegative();
        if (kz == Kind.INF && frame.r.isNegative() != productNeg) {
          flags.raise(StatusFlags.INVALID);
          BidCodec.packNaN(format, false, false, out);
          return;
        }
        BidCodec.packInfinity(format, productNeg, out);
        return;
      }
      frame.r.copyFrom(frame.a);
      frame.r.multiply(frame.b);
      Kind kz = unpack(format, z, frame.b, frame.coeff, flags);
      if (kz == Kind.NAN || kz == Kind.SNAN) {
        BidCodec.packNaN(format, false, false, out);
        return;
      }
      if (kz == Kind.INF) {
        BidCodec.packInfinity(format, frame.b.isNegative(), out);
        return;
      }
      addFinite(frame.r, frame.b, frame.a);
      pack(format, frame.a, mode, flags, frame.coeff, out);
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
      Kind kx = unpack(format, x, frame.a, frame.coeff, flags);
      Kind ky = unpack(format, y, frame.b, frame.coeff, flags);
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
      int targetExp = frame.b.exp();
      int delta = frame.a.exp() - targetExp;
      if (delta > 0) {
        frame.a.multiplyPow10(delta);
        frame.a.shiftExp(-delta);
      } else if (delta < 0) {
        int first = frame.a.dividePow10(-delta, frame.sticky);
        if (BidRound.shouldIncrement(
            frame.a.isNegative(),
            frame.a.leastDigitParity(),
            first,
            frame.sticky[0],
            mode)) {
          frame.a.addOne();
        }
        if (first != 0 || frame.sticky[0]) {
          flags.raise(StatusFlags.INEXACT);
        }
      }
      pack(format, frame.a, mode, flags, frame.coeff, out);
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
      Kind kx = unpack(src, x, frame.a, frame.coeff, flags);
      if (kx == Kind.NAN || kx == Kind.SNAN) {
        BidCodec.packNaN(dest, frame.a.isNegative(), false, out);
        if (kx == Kind.SNAN) {
          flags.raise(StatusFlags.INVALID);
        }
        return;
      }
      if (kx == Kind.INF) {
        BidCodec.packInfinity(dest, frame.a.isNegative(), out);
        return;
      }
      pack(dest, frame.a, mode, flags, frame.coeff, out);
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
    if (BidCodec.isNaN(format, words)) {
      String sign = BidCodec.isSigned(format, words) ? "-" : "+";
      return sign + (BidCodec.isSignalingNaN(format, words) ? "SNaN" : "NaN");
    }
    if (BidCodec.isInfinite(format, words)) {
      return (BidCodec.isSigned(format, words) ? "-" : "+") + "Inf";
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      unpack(format, words, frame.a, frame.coeff, new StatusFlags());
      String digits = frame.a.isZero() ? "0" : frame.a.toDigits();
      return (frame.a.isNegative() ? "-" : "+") + digits + "E" + frame.a.exp();
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
      unpack(format, x, frame.a, frame.coeff, new StatusFlags());
      unpack(format, y, frame.b, frame.coeff, new StatusFlags());
      if (frame.a.isZero() && frame.b.isZero()) {
        return 0;
      }
      boolean an = frame.a.isNegative();
      boolean bn = frame.b.isNegative();
      if (an != bn) {
        return an ? -1 : 1;
      }
      int cmp = frame.a.compareAbsolute(frame.b);
      return an ? -cmp : cmp;
    } finally {
      BidKScratch.release(frame);
    }
  }

  static void requireLibm(FormatParams format) {
    if (format.packedBits() > 128) {
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

  private static void addFinite(DecNum x, DecNum y, DecNum out) {
    if (x.isZero()) {
      out.copyFrom(y);
      return;
    }
    if (y.isZero()) {
      out.copyFrom(x);
      return;
    }
    out.copyFrom(x);
    if (x.isNegative() == y.isNegative()) {
      out.addAbsolute(y);
      out.setNegative(x.isNegative());
      return;
    }
    int cmp = out.compareAbsolute(y);
    if (cmp == 0) {
      boolean negative = x.isNegative() && y.isNegative();
      out.clear();
      out.setNegative(negative);
      return;
    }
    if (cmp < 0) {
      DecNum tmp = new DecNum();
      tmp.copyFrom(y);
      tmp.subtractAbsolute(out);
      tmp.setNegative(y.isNegative());
      out.copyFrom(tmp);
    } else {
      out.subtractAbsolute(y);
      out.setNegative(x.isNegative());
    }
  }

  private static void divideFinite(FormatParams format, DecNum num, DecNum den, DecNum out) {
    int extra = format.precision() + 8
        + Math.max(0, den.digitCount() - num.digitCount());
    BigInteger numerator = num.toBigIntegerAbsolute().multiply(BigInteger.TEN.pow(extra));
    BigInteger denominator = den.toBigIntegerAbsolute();
    BigInteger[] qr = numerator.divideAndRemainder(denominator);
    out.clear();
    out.setFromBigIntegerAbsolute(qr[0]);
    out.setNegative(num.isNegative() ^ den.isNegative());
    if (qr[1].signum() != 0) {
      out.multiplyBy10();
      out.addOne();
    }
    int adj = qr[1].signum() != 0 ? extra + 1 : extra;
    out.shiftExp(num.exp() - den.exp() - adj);
  }

  private static void sqrtFinite(FormatParams format, DecNum input, DecNum out) {
    DecNum work = new DecNum();
    work.copyFrom(input);
    work.setNegative(false);
    int extra = format.precision() + 4;
    if ((work.exp() & 1) != 0) {
      work.multiplyBy10();
      work.shiftExp(-1);
    }
    work.multiplyPow10(2 * extra);
    work.shiftExp(-2 * extra);
    DecNum.Sqrt sqrt = DecNum.sqrtFloor(work);
    out.copyFrom(sqrt.root());
    int expAdj = 0;
    if (!sqrt.remainder().isZero()) {
      out.multiplyBy10();
      out.addOne();
      expAdj = -1;
    }
    out.shiftExp(work.exp() / 2 + expAdj);
  }

  private static boolean propagateSpecial(
      FormatParams format,
      Kind kx,
      Kind ky,
      DecNum x,
      DecNum y,
      boolean adding,
      StatusFlags flags,
      long[] out) {
    if (kx == Kind.NAN || ky == Kind.NAN || kx == Kind.SNAN || ky == Kind.SNAN) {
      BidCodec.packNaN(format, false, false, out);
      return true;
    }
    if (kx == Kind.INF && ky == Kind.INF) {
      if (adding && x.isNegative() != y.isNegative()) {
        flags.raise(StatusFlags.INVALID);
        BidCodec.packNaN(format, false, false, out);
      } else {
        BidCodec.packInfinity(format, x.isNegative(), out);
      }
      return true;
    }
    if (kx == Kind.INF) {
      BidCodec.packInfinity(format, x.isNegative(), out);
      return true;
    }
    if (ky == Kind.INF) {
      BidCodec.packInfinity(format, y.isNegative(), out);
      return true;
    }
    return false;
  }

  private static boolean mulDivSpecial(
      FormatParams format,
      Kind kx,
      Kind ky,
      DecNum x,
      DecNum y,
      boolean dividing,
      StatusFlags flags,
      long[] out) {
    if (kx == Kind.NAN || ky == Kind.NAN || kx == Kind.SNAN || ky == Kind.SNAN) {
      BidCodec.packNaN(format, false, false, out);
      return true;
    }
    boolean negative = x.isNegative() ^ y.isNegative();
    if (dividing) {
      if (ky == Kind.INF) {
        if (kx == Kind.INF) {
          flags.raise(StatusFlags.INVALID);
          BidCodec.packNaN(format, false, false, out);
        } else {
          DecNum zero = new DecNum();
          zero.setNegative(negative);
          BidKScratch.Frame frame = BidKScratch.acquire();
          try {
            pack(format, zero, RoundingMode.TIES_TO_EVEN, flags, frame.coeff, out);
          } finally {
            BidKScratch.release(frame);
          }
        }
        return true;
      }
      if (y.isZero()) {
        if (x.isZero() || kx == Kind.INF) {
          if (x.isZero()) {
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
      if ((kx == Kind.INF && y.isZero()) || (ky == Kind.INF && x.isZero())) {
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
