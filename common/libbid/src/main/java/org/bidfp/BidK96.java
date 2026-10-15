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

/** Fixed two-limb entry points for finite Decimal96 arithmetic. */
final class BidK96 {
  private BidK96() {
  }

  static boolean matches(FormatParams format) {
    return format.units() == 3;
  }

  static boolean add(
      FormatParams format,
      long[] x,
      long[] y,
      boolean negateY,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    if (!BidCodec.isFinite(format, x) || !BidCodec.isFinite(format, y)) {
      return false;
    }
    if (tryAddAligned(format, x, y, negateY, out)) {
      return true;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      load(format, x, frame.xa, frame.tmpNeg, frame.tmpExp);
      frame.an = frame.tmpNeg[0];
      frame.ae = frame.tmpExp[0];
      load(format, y, frame.xb, frame.tmpNeg, frame.tmpExp);
      frame.bn = negateY ? !frame.tmpNeg[0] : frame.tmpNeg[0];
      frame.be = frame.tmpExp[0];
      BidK160.addFromWide(
          format, frame.an, frame.ae, frame.bn, frame.be, mode, flags, out,
          frame);
      return true;
    } finally {
      BidKScratch.release(frame);
    }
  }

  static boolean mul(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    if (!BidCodec.isFinite(format, x) || !BidCodec.isFinite(format, y)) {
      return false;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      loadBinary(format, x, y, frame);
      BidK160.mulFromWide(
          format, frame.an, frame.ae, frame.bn, frame.be, mode, flags, out,
          frame);
      return true;
    } finally {
      BidKScratch.release(frame);
    }
  }

  static boolean div(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    if (!BidCodec.isFinite(format, x) || !BidCodec.isFinite(format, y)) {
      return false;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      loadBinary(format, x, y, frame);
      if (frame.xa.isZero() || frame.xb.isZero()) {
        return false;
      }
      BidK160.divFromWide(
          format, frame.an, frame.ae, frame.bn, frame.be, mode, flags, out,
          frame);
      return true;
    } finally {
      BidKScratch.release(frame);
    }
  }

  static boolean sqrt(
      FormatParams format,
      long[] x,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    if (!BidCodec.isFinite(format, x) || BidCodec.isSigned(format, x)) {
      return false;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      load(format, x, frame.xa, frame.tmpNeg, frame.tmpExp);
      if (frame.xa.isZero()) {
        return false;
      }
      frame.coeff.set3(frame.xa.l0, frame.xa.l1, 0L);
      BidK160.sqrt(
          format, frame.tmpExp[0], frame.coeff, mode, flags, out, frame);
      return true;
    } finally {
      BidKScratch.release(frame);
    }
  }

  static boolean fma(
      FormatParams format,
      long[] x,
      long[] y,
      long[] z,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    if (!BidCodec.isFinite(format, x)
        || !BidCodec.isFinite(format, y)
        || !BidCodec.isFinite(format, z)) {
      return false;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      loadBinary(format, x, y, frame);
      load(format, z, frame.xt, frame.tmpNeg, frame.tmpExp);
      BidK160.fmaFromWide(
          format, frame.an, frame.ae, frame.bn, frame.be,
          frame.tmpNeg[0], frame.tmpExp[0], mode, flags, out, frame);
      return true;
    } finally {
      BidKScratch.release(frame);
    }
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
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      loadBinary(format, x, y, frame);
      if (frame.xa.isZero() && frame.xb.isZero()) {
        return 0;
      }
      if (frame.an != frame.bn) {
        return frame.an ? -1 : 1;
      }
      int cmp = BidK160.compareAbsolute(
          frame.xa, frame.ae, frame.xb, frame.be, format.precision(), frame);
      return frame.an ? -cmp : cmp;
    } finally {
      BidKScratch.release(frame);
    }
  }

  private static void loadBinary(
      FormatParams format, long[] x, long[] y, BidKScratch.Frame frame) {
    load(format, x, frame.xa, frame.tmpNeg, frame.tmpExp);
    frame.an = frame.tmpNeg[0];
    frame.ae = frame.tmpExp[0];
    load(format, y, frame.xb, frame.tmpNeg, frame.tmpExp);
    frame.bn = frame.tmpNeg[0];
    frame.be = frame.tmpExp[0];
  }

  private static boolean tryAddAligned(
      FormatParams format, long[] x, long[] y, boolean negateY, long[] out) {
    long xh = x[1];
    long yh = y[1];
    int exponent = BidCodec.unpackBiasedExponent(format, xh);
    if (exponent != BidCodec.unpackBiasedExponent(format, yh)) {
      return false;
    }
    long x0 = x[0];
    long x1 = coefficientHigh(format, xh);
    long y0 = y[0];
    long y1 = coefficientHigh(format, yh);
    long[] max = format.maxCoefficientWords();
    if (!allowed(x0, x1, max)) {
      x0 = 0L;
      x1 = 0L;
    }
    if (!allowed(y0, y1, max)) {
      y0 = 0L;
      y1 = 0L;
    }
    boolean xn = (xh & format.signMask()) != 0L;
    boolean yn = ((yh & format.signMask()) != 0L) ^ negateY;
    boolean negative;
    long r0;
    long r1;
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
    if (!allowed(r0, r1, max)) {
      return false;
    }
    pack(format, negative, exponent, r0, r1, out);
    return true;
  }

  private static void load(
      FormatParams format,
      long[] words,
      Wide6 coefficient,
      boolean[] negative,
      int[] exponent) {
    long hi = words[1];
    coefficient.l0 = words[0];
    if ((hi & format.steeringMask()) == format.steeringMask()) {
      coefficient.l1 =
          (hi & format.form2CoeffLastMask()) | format.form2ImplicitMask();
    } else {
      coefficient.l1 = hi & format.form1CoeffLastMask();
    }
    coefficient.l2 = 0L;
    coefficient.l3 = 0L;
    coefficient.l4 = 0L;
    coefficient.l5 = 0L;
    if (!format.coefficientAllowed(coefficient)) {
      coefficient.clear();
    }
    negative[0] = (hi & format.signMask()) != 0L;
    exponent[0] = BidCodec.unpackBiasedExponent(format, hi) - format.bias();
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

  private static void pack(
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
}
