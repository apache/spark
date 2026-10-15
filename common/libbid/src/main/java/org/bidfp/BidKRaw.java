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
 * Generic BID-k arithmetic. {@code k=2,3,4} dispatch to {@link BidKSpecial}
 * (Bid64, Bid128, Decimal96 via Bid128). Wider finite add/mul/div/sqrt/fma
 * use Knuth limbs (Decimal160 Wide6, Decimal256 Wide8). Parse uses
 * {@link DecNum}.
 *
 * <p>{@link BidUnpacked} is the compute form: kernels write
 * {@link BidKScratch.Frame#unpackedOut} through {@link BidEmit} and skip
 * IEEE pack. Packed words remain the interchange encoding.
 *
 * <p>{@link Bid64} and {@link Bid128} remain the specialized hot paths. This
 * kernel covers Decimal96 ({@code k=3}) through Decimal256 ({@code k=8}).
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
    if (BidKSpecial.narrow(format)) {
      if (negateY) {
        BidKSpecial.sub(format, x, y, mode, flags, out);
      } else {
        BidKSpecial.add(format, x, y, mode, flags, out);
      }
      return;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      Kind kx;
      Kind ky;
      if (BidK160.matches(format)) {
        kx = BidKKnuth.unpackLeftWide(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide(format, y, frame, flags);
      } else if (BidK256.matches(format)) {
        kx = BidKKnuth.unpackLeftWide8(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide8(format, y, frame, flags);
      } else {
        kx = BidKKnuth.unpackLeftWork(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWork(format, y, frame, flags);
      }
      if (negateY && ky != Kind.NAN && ky != Kind.SNAN) {
        frame.bn = !frame.bn;
      }
      if (propagateSpecial(
          format, kx, ky, frame.an, frame.bn, true, flags, out, frame)) {
        return;
      }
      if (BidK160.matches(format)) {
        BidK160.addFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be, mode, flags, out,
            frame);
        return;
      }
      if (BidK256.matches(format)) {
        BidK256.addFromWide(
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
    if (BidKSpecial.narrow(format)) {
      BidKSpecial.mul(format, x, y, mode, flags, out);
      return;
    }
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
      } else if (BidK256.matches(format)) {
        kx = BidKKnuth.unpackLeftWide8(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide8(format, y, frame, flags);
        aZero = frame.ya.isZero();
        bZero = frame.yb.isZero();
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
      if (BidK256.matches(format)) {
        BidK256.mulFromWide(
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
    if (BidKSpecial.narrow(format)) {
      BidKSpecial.div(format, x, y, mode, flags, out);
      return;
    }
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
      } else if (BidK256.matches(format)) {
        kx = BidKKnuth.unpackLeftWide8(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide8(format, y, frame, flags);
        aZero = frame.ya.isZero();
        bZero = frame.yb.isZero();
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
      if (BidK256.matches(format)) {
        BidK256.divFromWide(
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
    if (BidKSpecial.narrow(format)) {
      BidKSpecial.sqrt(format, x, mode, flags, out);
      return;
    }
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
    if (BidKSpecial.narrow(format)) {
      BidKSpecial.fma(format, x, y, z, mode, flags, out);
      return;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      boolean wide160 = BidK160.matches(format);
      boolean wide256 = BidK256.matches(format);
      Kind kx;
      Kind ky;
      boolean aZero;
      boolean bZero;
      if (wide160) {
        kx = BidKKnuth.unpackLeftWide(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide(format, y, frame, flags);
        aZero = frame.xa.isZero();
        bZero = frame.xb.isZero();
      } else if (wide256) {
        kx = BidKKnuth.unpackLeftWide8(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide8(format, y, frame, flags);
        aZero = frame.ya.isZero();
        bZero = frame.yb.isZero();
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
        Kind kz = unpackAddend(format, z, frame, flags);
        boolean productNeg = frame.an ^ frame.bn;
        if (kz == Kind.INF && frame.tmpNeg[0] != productNeg) {
          flags.raise(StatusFlags.INVALID);
          BidCodec.packNaN(format, false, false, out);
          return;
        }
        BidCodec.packInfinity(format, productNeg, out);
        return;
      }
      Kind kz = unpackAddend(format, z, frame, flags);
      if (kz == Kind.NAN || kz == Kind.SNAN) {
        BidCodec.packNaN(format, false, false, out);
        return;
      }
      if (kz == Kind.INF) {
        BidCodec.packInfinity(format, frame.tmpNeg[0], out);
        return;
      }
      if (wide160) {
        BidK160.fmaFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be,
            frame.tmpNeg[0], frame.tmpExp[0], mode, flags, out, frame);
        return;
      }
      if (wide256) {
        BidK256.fmaFromWide(
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
    if (BidKSpecial.narrow(format)) {
      BidKSpecial.quantize(format, x, y, mode, flags, out);
      return;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      Kind kx;
      Kind ky;
      if (BidK160.matches(format)) {
        kx = BidKKnuth.unpackLeftWide(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide(format, y, frame, flags);
      } else if (BidK256.matches(format)) {
        kx = BidKKnuth.unpackLeftWide8(format, x, frame, flags);
        ky = BidKKnuth.unpackRightWide8(format, y, frame, flags);
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
      if (BidK256.matches(format)) {
        BidK256.quantizeFromWide(
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
      } else if (BidK256.matches(src)) {
        kx = BidKKnuth.unpackLeftWide8(src, x, frame, flags);
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
      if (BidK256.matches(src)) {
        BidK256.convertFromWide(
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
      if (BidK256.matches(format)) {
        return BidK256.toString(format, words, frame);
      }
      return BidKKnuth.toString(format, words, frame);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static void addUnpacked(
      BidUnpacked x,
      BidUnpacked y,
      boolean negateY,
      RoundingMode mode,
      StatusFlags flags,
      BidUnpacked out) {
    FormatParams format = requireSameFormat(x, y, out);
    if (BidKSpecial.narrow(format)) {
      BidKSpecial.addUnpacked(x, y, negateY, mode, flags, out);
      return;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    frame.unpackedOut = out;
    try {
      Kind kx = x.rawKind();
      Kind ky = y.rawKind();
      loadBinary(x, y, frame);
      if (negateY && ky != Kind.NAN && ky != Kind.SNAN) {
        frame.bn = !frame.bn;
      }
      if (propagateSpecial(
          format, kx, ky, frame.an, frame.bn, true, flags, frame.words, frame)) {
        return;
      }
      if (BidK160.matches(format)) {
        BidK160.addFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be, mode, flags,
            frame.words, frame);
        return;
      }
      if (BidK256.matches(format)) {
        BidK256.addFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be, mode, flags,
            frame.words, frame);
        return;
      }
      BidKKnuth.addFromWork(
          format, frame.an, frame.ae, frame.bn, frame.be, mode, flags,
          frame.words, frame);
    } finally {
      frame.unpackedOut = null;
      BidKScratch.release(frame);
    }
  }

  static void mulUnpacked(
      BidUnpacked x,
      BidUnpacked y,
      RoundingMode mode,
      StatusFlags flags,
      BidUnpacked out) {
    FormatParams format = requireSameFormat(x, y, out);
    if (BidKSpecial.narrow(format)) {
      BidKSpecial.mulUnpacked(x, y, mode, flags, out);
      return;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    frame.unpackedOut = out;
    try {
      Kind kx = x.rawKind();
      Kind ky = y.rawKind();
      loadBinary(x, y, frame);
      boolean aZero = x.isFinite() && x.c0() == 0L && x.c1() == 0L
          && x.c2() == 0L && x.c3() == 0L;
      boolean bZero = y.isFinite() && y.c0() == 0L && y.c1() == 0L
          && y.c2() == 0L && y.c3() == 0L;
      if (mulDivSpecial(
          format, kx, ky, frame.an, frame.bn, aZero, bZero, false, flags,
          frame.words, frame)) {
        return;
      }
      if (BidK160.matches(format)) {
        BidK160.mulFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be, mode, flags,
            frame.words, frame);
        return;
      }
      if (BidK256.matches(format)) {
        BidK256.mulFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be, mode, flags,
            frame.words, frame);
        return;
      }
      BidKKnuth.mulFromWork(
          format, frame.an, frame.ae, frame.bn, frame.be, mode, flags,
          frame.words, frame);
    } finally {
      frame.unpackedOut = null;
      BidKScratch.release(frame);
    }
  }

  static void divUnpacked(
      BidUnpacked x,
      BidUnpacked y,
      RoundingMode mode,
      StatusFlags flags,
      BidUnpacked out) {
    FormatParams format = requireSameFormat(x, y, out);
    if (BidKSpecial.narrow(format)) {
      BidKSpecial.divUnpacked(x, y, mode, flags, out);
      return;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    frame.unpackedOut = out;
    try {
      Kind kx = x.rawKind();
      Kind ky = y.rawKind();
      loadBinary(x, y, frame);
      boolean aZero = x.isFinite() && x.c0() == 0L && x.c1() == 0L
          && x.c2() == 0L && x.c3() == 0L;
      boolean bZero = y.isFinite() && y.c0() == 0L && y.c1() == 0L
          && y.c2() == 0L && y.c3() == 0L;
      if (mulDivSpecial(
          format, kx, ky, frame.an, frame.bn, aZero, bZero, true, flags,
          frame.words, frame)) {
        return;
      }
      if (BidK160.matches(format)) {
        BidK160.divFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be, mode, flags,
            frame.words, frame);
        return;
      }
      if (BidK256.matches(format)) {
        BidK256.divFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be, mode, flags,
            frame.words, frame);
        return;
      }
      BidKKnuth.divFromWork(
          format, frame.an, frame.ae, frame.bn, frame.be, mode, flags,
          frame.words, frame);
    } finally {
      frame.unpackedOut = null;
      BidKScratch.release(frame);
    }
  }

  static void sqrtUnpacked(
      BidUnpacked x, RoundingMode mode, StatusFlags flags, BidUnpacked out) {
    FormatParams format = requireSameFormat(x, out);
    if (BidKSpecial.narrow(format)) {
      BidKSpecial.sqrtUnpacked(x, mode, flags, out);
      return;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    frame.unpackedOut = out;
    try {
      Kind kx = x.rawKind();
      frame.an = x.negative();
      frame.ae = x.exponent();
      loadWork(frame.w0, x);
      if (kx == Kind.NAN || kx == Kind.SNAN) {
        BidEmit.nan(format, false, false, frame.words, frame);
        return;
      }
      if (kx == Kind.INF) {
        if (frame.an) {
          flags.raise(StatusFlags.INVALID);
          BidEmit.nan(format, false, false, frame.words, frame);
        } else {
          BidEmit.infinity(format, false, frame.words, frame);
        }
        return;
      }
      if (frame.an && !LimbMath.isZero(frame.w0)) {
        flags.raise(StatusFlags.INVALID);
        BidEmit.nan(format, false, false, frame.words, frame);
        return;
      }
      if (LimbMath.isZero(frame.w0)) {
        BidKKnuth.pack(
            format, frame.an, frame.ae, frame.w0, mode, flags, frame,
            frame.words);
        return;
      }
      frame.coeff.set4(x.c0(), x.c1(), x.c2(), x.c3());
      if (BidK160.matches(format)) {
        BidK160.sqrt(
            format, frame.ae, frame.coeff, mode, flags, frame.words, frame);
        return;
      }
      if (BidK256.matches(format)) {
        BidK256.sqrt(
            format, frame.ae, frame.coeff, mode, flags, frame.words, frame);
        return;
      }
      BidKKnuth.sqrtFromWork(format, frame.ae, mode, flags, frame.words, frame);
    } finally {
      frame.unpackedOut = null;
      BidKScratch.release(frame);
    }
  }

  static void fmaUnpacked(
      BidUnpacked x,
      BidUnpacked y,
      BidUnpacked z,
      RoundingMode mode,
      StatusFlags flags,
      BidUnpacked out) {
    FormatParams format = requireSameFormat(x, y, out);
    if (z.format() != format) {
      throw new IllegalArgumentException("format mismatch");
    }
    if (BidKSpecial.narrow(format)) {
      BidKSpecial.fmaUnpacked(x, y, z, mode, flags, out);
      return;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    frame.unpackedOut = out;
    try {
      Kind kx = x.rawKind();
      Kind ky = y.rawKind();
      loadBinary(x, y, frame);
      boolean aZero = x.isFinite() && coeffZero(x);
      boolean bZero = y.isFinite() && coeffZero(y);
      if (kx == Kind.NAN || ky == Kind.NAN || kx == Kind.SNAN || ky == Kind.SNAN) {
        BidEmit.nan(format, false, false, frame.words, frame);
        return;
      }
      if (kx == Kind.INF || ky == Kind.INF) {
        if ((kx == Kind.INF && bZero) || (ky == Kind.INF && aZero)) {
          flags.raise(StatusFlags.INVALID);
          BidEmit.nan(format, false, false, frame.words, frame);
          return;
        }
        boolean productNeg = frame.an ^ frame.bn;
        if (z.kind() == BidUnpacked.Kind.INF && z.negative() != productNeg) {
          flags.raise(StatusFlags.INVALID);
          BidEmit.nan(format, false, false, frame.words, frame);
          return;
        }
        BidEmit.infinity(format, productNeg, frame.words, frame);
        return;
      }
      if (z.isNaN()) {
        BidEmit.nan(format, false, false, frame.words, frame);
        return;
      }
      if (z.kind() == BidUnpacked.Kind.INF) {
        BidEmit.infinity(format, z.negative(), frame.words, frame);
        return;
      }
      frame.tmpNeg[0] = z.negative();
      frame.tmpExp[0] = z.exponent();
      if (BidK160.matches(format)) {
        loadWide6(frame.xt, z);
        BidK160.fmaFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be,
            z.negative(), z.exponent(), mode, flags, frame.words, frame);
        return;
      }
      if (BidK256.matches(format)) {
        loadWide8(frame.yt, z);
        BidK256.fmaFromWide(
            format, frame.an, frame.ae, frame.bn, frame.be,
            z.negative(), z.exponent(), mode, flags, frame.words, frame);
        return;
      }
      loadWork(frame.w4, z);
      BidKKnuth.fmaFromWork(
          format, frame.an, frame.ae, frame.bn, frame.be,
          z.negative(), z.exponent(), mode, flags, frame.words, frame);
    } finally {
      frame.unpackedOut = null;
      BidKScratch.release(frame);
    }
  }

  static int sqlCompareUnpacked(BidUnpacked x, BidUnpacked y) {
    FormatParams format = requireSameFormat(x, y);
    if (x.isNaN()) {
      return y.isNaN() ? 0 : 1;
    }
    if (y.isNaN()) {
      return -1;
    }
    if (x.kind() == BidUnpacked.Kind.INF || y.kind() == BidUnpacked.Kind.INF) {
      if (x.kind() == BidUnpacked.Kind.INF && y.kind() == BidUnpacked.Kind.INF) {
        if (x.negative() == y.negative()) {
          return 0;
        }
        return x.negative() ? -1 : 1;
      }
      if (x.kind() == BidUnpacked.Kind.INF) {
        return x.negative() ? -1 : 1;
      }
      return y.negative() ? 1 : -1;
    }
    BidKScratch.Frame frame = BidKScratch.acquire();
    try {
      loadBinary(x, y, frame);
      if (BidK160.matches(format)) {
        if (frame.xa.isZero() && frame.xb.isZero()) {
          return 0;
        }
        if (frame.an != frame.bn) {
          return frame.an ? -1 : 1;
        }
        int cmp = BidK160.compareAbsolute(
            frame.xa, frame.ae, frame.xb, frame.be, format.precision(), frame);
        return frame.an ? -cmp : cmp;
      }
      if (BidK256.matches(format)) {
        if (frame.ya.isZero() && frame.yb.isZero()) {
          return 0;
        }
        if (frame.an != frame.bn) {
          return frame.an ? -1 : 1;
        }
        int cmp = BidK256.compareAbsolute(
            frame.ya, frame.ae, frame.yb, frame.be, format.precision(), frame);
        return frame.an ? -cmp : cmp;
      }
      if (LimbMath.isZero(frame.w0) && LimbMath.isZero(frame.w1)) {
        return 0;
      }
      if (frame.an != frame.bn) {
        return frame.an ? -1 : 1;
      }
      int cmp = BidKKnuth.compareAbsolute(
          frame.w0, frame.ae, frame.w1, frame.be, format.precision(), frame);
      return frame.an ? -cmp : cmp;
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
      if (BidK256.matches(format)) {
        return BidK256.sqlCompare(format, x, y, frame);
      }
      return BidKKnuth.sqlCompare(format, x, y, frame);
    } finally {
      BidKScratch.release(frame);
    }
  }

  static void requireLibm(FormatParams format) {
    if (format.units() > 4) {
      throw new UnsupportedOperationException(
          "k>4 libm is out of scope unless a binary256 engine is funded");
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
      BidKScratch.Frame frame,
      StatusFlags flags) {
    if (BidK160.matches(format)) {
      return BidKKnuth.unpack(
          format, z, frame.xt, frame.tmpNeg, frame.tmpExp, flags);
    }
    if (BidK256.matches(format)) {
      return BidKKnuth.unpack(
          format, z, frame.yt, frame.tmpNeg, frame.tmpExp, flags);
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
      long[] out,
      BidKScratch.Frame frame) {
    if (kx == Kind.NAN || ky == Kind.NAN || kx == Kind.SNAN || ky == Kind.SNAN) {
      BidEmit.nan(format, false, false, out, frame);
      return true;
    }
    if (kx == Kind.INF && ky == Kind.INF) {
      if (adding && xn != yn) {
        flags.raise(StatusFlags.INVALID);
        BidEmit.nan(format, false, false, out, frame);
      } else {
        BidEmit.infinity(format, xn, out, frame);
      }
      return true;
    }
    if (kx == Kind.INF) {
      BidEmit.infinity(format, xn, out, frame);
      return true;
    }
    if (ky == Kind.INF) {
      BidEmit.infinity(format, yn, out, frame);
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
      BidEmit.nan(format, false, false, out, frame);
      return true;
    }
    boolean negative = xn ^ yn;
    if (dividing) {
      if (ky == Kind.INF) {
        if (kx == Kind.INF) {
          flags.raise(StatusFlags.INVALID);
          BidEmit.nan(format, false, false, out, frame);
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
            BidEmit.nan(format, false, false, out, frame);
          } else {
            BidEmit.infinity(format, negative, out, frame);
          }
        } else {
          flags.raise(StatusFlags.DIVIDE_BY_ZERO);
          BidEmit.infinity(format, negative, out, frame);
        }
        return true;
      }
      if (kx == Kind.INF) {
        BidEmit.infinity(format, negative, out, frame);
        return true;
      }
      return false;
    }
    if (kx == Kind.INF || ky == Kind.INF) {
      if ((kx == Kind.INF && yZero) || (ky == Kind.INF && xZero)) {
        flags.raise(StatusFlags.INVALID);
        BidEmit.nan(format, false, false, out, frame);
      } else {
        BidEmit.infinity(format, negative, out, frame);
      }
      return true;
    }
    return false;
  }

  private static FormatParams requireSameFormat(BidUnpacked x, BidUnpacked y) {
    if (x.format() != y.format()) {
      throw new IllegalArgumentException("format mismatch");
    }
    return x.format();
  }

  private static FormatParams requireSameFormat(
      BidUnpacked x, BidUnpacked y, BidUnpacked out) {
    FormatParams format = requireSameFormat(x, y);
    if (out.format() != format) {
      throw new IllegalArgumentException("format mismatch");
    }
    return format;
  }

  private static boolean coeffZero(BidUnpacked value) {
    return (value.c0() | value.c1() | value.c2() | value.c3()) == 0L;
  }

  private static void loadBinary(
      BidUnpacked x, BidUnpacked y, BidKScratch.Frame frame) {
    frame.an = x.negative();
    frame.ae = x.exponent();
    frame.bn = y.negative();
    frame.be = y.exponent();
    FormatParams format = x.format();
    if (BidK160.matches(format)) {
      loadWide6(frame.xa, x);
      loadWide6(frame.xb, y);
    } else if (BidK256.matches(format)) {
      loadWide8(frame.ya, x);
      loadWide8(frame.yb, y);
    } else {
      loadWork(frame.w0, x);
      loadWork(frame.w1, y);
    }
  }

  private static void loadWide6(Wide6 dest, BidUnpacked value) {
    dest.clear();
    dest.l0 = value.c0();
    dest.l1 = value.c1();
    dest.l2 = value.c2();
  }

  private static void loadWide8(Wide8 dest, BidUnpacked value) {
    dest.clear();
    dest.l0 = value.c0();
    dest.l1 = value.c1();
    dest.l2 = value.c2();
    dest.l3 = value.c3();
  }

  private static void loadWork(long[] dest, BidUnpacked value) {
    LimbMath.clear(dest);
    dest[0] = value.c0();
    dest[1] = value.c1();
    dest[2] = value.c2();
    dest[3] = value.c3();
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
