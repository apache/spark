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
 * BidK finite arithmetic for {@code k=2,3,4}: Decimal64/128 specialists, and
 * Decimal96 via Bid128 then round back to {@code p=25}.
 */
final class BidKSpecial {
  private static final ThreadLocal<Scratch> LOCAL =
      ThreadLocal.withInitial(Scratch::new);

  private BidKSpecial() {
  }

  static boolean narrow(FormatParams format) {
    int k = format.units();
    return k >= 2 && k <= 4;
  }

  static void add(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    binary(format, x, y, mode, flags, out, Op.ADD);
  }

  static void sub(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    binary(format, x, y, mode, flags, out, Op.SUB);
  }

  static void mul(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    binary(format, x, y, mode, flags, out, Op.MUL);
  }

  static void div(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    binary(format, x, y, mode, flags, out, Op.DIV);
  }

  static void quantize(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    binary(format, x, y, mode, flags, out, Op.QUANTIZE);
  }

  static void sqrt(
      FormatParams format,
      long[] x,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    int k = format.units();
    if (k == 2) {
      out[0] = Bid64Raw.sqrt(x[0], mode, flags);
      return;
    }
    Scratch s = LOCAL.get();
    if (k == 4) {
      Bid128Raw.sqrt(x[1], x[0], mode, flags, s.r);
      copyFromBid128(s.r, out);
      return;
    }
    widen(format, x, s.x, mode, flags);
    Bid128Raw.sqrt(s.x[1], s.x[0], mode, flags, s.r);
    copyFromBid128(s.r, s.y);
    BidKRaw.convert(
        FormatParams.DECIMAL128, s.y, format, mode, flags, out);
  }

  static void fma(
      FormatParams format,
      long[] x,
      long[] y,
      long[] z,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    int k = format.units();
    if (k == 2) {
      out[0] = Bid64Raw.fma(x[0], y[0], z[0], mode, flags);
      return;
    }
    Scratch s = LOCAL.get();
    if (k == 4) {
      Bid128Raw.fma(
          x[1], x[0], y[1], y[0], z[1], z[0], mode, flags, s.r);
      copyFromBid128(s.r, out);
      return;
    }
    widen(format, x, s.x, mode, flags);
    widen(format, y, s.y, mode, flags);
    widen(format, z, s.z, mode, flags);
    Bid128Raw.fma(
        s.x[1], s.x[0], s.y[1], s.y[0], s.z[1], s.z[0], mode, flags, s.r);
    copyFromBid128(s.r, s.x);
    BidKRaw.convert(
        FormatParams.DECIMAL128, s.x, format, mode, flags, out);
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
    FormatParams format = x.format();
    if (negateY) {
      sub(format, s.px, s.py, mode, flags, s.po);
    } else {
      add(format, s.px, s.py, mode, flags, s.po);
    }
    BidCodec.loadUnpacked(format, s.po, out);
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
      BidUnpacked x,
      RoundingMode mode,
      StatusFlags flags,
      BidUnpacked out) {
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

  private static void binary(
      FormatParams format,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out,
      Op op) {
    int k = format.units();
    if (k == 2) {
      switch (op) {
        case ADD -> out[0] = Bid64Raw.add(x[0], y[0], mode, flags);
        case SUB -> out[0] = Bid64Raw.sub(x[0], y[0], mode, flags);
        case MUL -> out[0] = Bid64Raw.mul(x[0], y[0], mode, flags);
        case DIV -> out[0] = Bid64Raw.div(x[0], y[0], mode, flags);
        case QUANTIZE -> out[0] = Bid64Raw.quantize(x[0], y[0], mode, flags);
      }
      return;
    }
    Scratch s = LOCAL.get();
    long xh;
    long xl;
    long yh;
    long yl;
    if (k == 4) {
      xh = x[1];
      xl = x[0];
      yh = y[1];
      yl = y[0];
    } else {
      widen(format, x, s.x, mode, flags);
      widen(format, y, s.y, mode, flags);
      xh = s.x[1];
      xl = s.x[0];
      yh = s.y[1];
      yl = s.y[0];
    }
    switch (op) {
      case ADD -> Bid128Raw.add(xh, xl, yh, yl, mode, flags, s.r);
      case SUB -> Bid128Raw.sub(xh, xl, yh, yl, mode, flags, s.r);
      case MUL -> Bid128Raw.mul(xh, xl, yh, yl, mode, flags, s.r);
      case DIV -> Bid128Raw.div(xh, xl, yh, yl, mode, flags, s.r);
      case QUANTIZE -> Bid128Raw.quantize(xh, xl, yh, yl, mode, flags, s.r);
    }
    if (k == 4) {
      copyFromBid128(s.r, out);
      return;
    }
    copyFromBid128(s.r, s.z);
    BidKRaw.convert(
        FormatParams.DECIMAL128, s.z, format, mode, flags, out);
  }

  private static void widen(
      FormatParams src,
      long[] words,
      long[] dest128,
      RoundingMode mode,
      StatusFlags flags) {
    BidKRaw.convert(
        src, words, FormatParams.DECIMAL128, mode, flags, dest128);
  }

  /** Bid128 kernels store {@code [high, low]}; BidK stores {@code [low, high]}. */
  private static void copyFromBid128(long[] hiLo, long[] bidk) {
    bidk[0] = hiLo[1];
    bidk[1] = hiLo[0];
  }

  private enum Op {
    ADD,
    SUB,
    MUL,
    DIV,
    QUANTIZE
  }

  private static final class Scratch {
    final long[] x = new long[2];
    final long[] y = new long[2];
    final long[] z = new long[2];
    final long[] r = new long[2];
    final long[] px = new long[2];
    final long[] py = new long[2];
    final long[] pz = new long[2];
    final long[] po = new long[2];
  }
}
