/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   * Neither the name of Intel Corporation nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.bidfp;

import org.bidfp.binary128.Binary128;
import org.bidfp.binary128.Dpml;

/** Intel {@code bid128_asin.c} / {@code acos.c} / {@code atanh.c}. */
final class Bid128InvTrig {
  private static final Binary128 C_1EM40 =
      Binary128.fromRawBits(0x3f7a_16c2_6277_7579L, 0xc58c_4647_5896_767bL);
  private static final Binary128 C_7_10 =
      Binary128.fromRawBits(0x3ffe_6666_6666_6666L, 0x6666_6666_6666_6666L);
  private static final Binary128 C_ONE =
      Binary128.fromRawBits(0x3fff_0000_0000_0000L, 0L);
  private static final Binary128 C_ZERO = Binary128.fromRawBits(0L, 0L);
  private static final Binary128 C_PI =
      Binary128.fromRawBits(0x4000_921f_b544_42d1L, 0x8469_898c_c517_01b8L);
  private static final Binary128 C_HALF =
      Binary128.fromRawBits(0x3ffe_0000_0000_0000L, 0L);
  private static final Bid128 MINUS_ONE =
      Bid128.fromRawBits(0xb040_0000_0000_0000L, 1L);
  private static final Bid128 PI2_HI =
      Bid128.fromRawBits(0x2ffe_4d72_3cab_cb53L, 0xdd5f_2ab2_7379_cfc7L);
  private static final Bid128 PI2_LO =
      Bid128.fromRawBits(0x2fba_d9f8_afb5_01d4L, 0x0492_b413_8a16_2883L);
  private static final Bid128 NAN =
      Bid128.fromRawBits(0x7c00_0000_0000_0000L, 0L);

  private Bid128InvTrig() {
  }

  static void asin(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    long[] packed = new long[2];
    BidConvert.toBinary128From128(hi, lo, mode, flags, packed);
    Binary128 xd = Binary128.fromRawBits(packed[0], packed[1]);
    Binary128 abs = xd.abs();
    if (Bid128Libm.less(abs, C_1EM40)) {
      Bid128Raw.fma(
          hi, lo, Bid128Libm.TEN_PM40_POS, 1L, hi, lo, mode, flags, out);
      return;
    }
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    if (Bid128Libm.lessEqual(abs, C_7_10)) {
      Binary128 yd = Dpml.asin(xd, binaryMode, local);
      flags.raise(local.bits());
      BidConvert.fromBinary128To128(yd.highBits(), yd.lowBits(), mode, flags, out);
      return;
    }
    if (Bid128Libm.greater(abs, C_ONE)) {
      flags.raise(StatusFlags.INVALID);
      DecNum.store128(NAN, out);
      return;
    }
    long[] t = new long[2];
    Bid128Raw.fma(
        hi, lo, hi, lo, MINUS_ONE.highBits(), MINUS_ONE.lowBits(),
        mode, flags, t);
    BidConvert.toBinary128From128(t[0], t[1], mode, flags, packed);
    Binary128 td = Binary128.fromRawBits(packed[0], packed[1]).negate();
    Binary128 yd = Dpml.sqrt(td, binaryMode, local);
    yd = Dpml.acos(yd, binaryMode, local);
    if (Bid128Libm.less(xd, C_ZERO)) {
      yd = yd.negate();
    }
    flags.raise(local.bits());
    BidConvert.fromBinary128To128(yd.highBits(), yd.lowBits(), mode, flags, out);
  }

  static void acos(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    long[] packed = new long[2];
    BidConvert.toBinary128From128(hi, lo, mode, flags, packed);
    Binary128 xd = Binary128.fromRawBits(packed[0], packed[1]);
    Binary128 abs = xd.abs();
    if (Bid128Libm.less(abs, C_1EM40)) {
      Bid128Raw.add(
          PI2_HI.highBits(), PI2_HI.lowBits(),
          PI2_LO.highBits(), PI2_LO.lowBits(),
          mode, flags, out);
      return;
    }
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    if (Bid128Libm.lessEqual(abs, C_7_10)) {
      Binary128 yd = Dpml.acos(xd, binaryMode, local);
      flags.raise(local.bits());
      BidConvert.fromBinary128To128(yd.highBits(), yd.lowBits(), mode, flags, out);
      return;
    }
    if (Bid128Libm.greater(abs, C_ONE)) {
      flags.raise(StatusFlags.INVALID);
      DecNum.store128(NAN, out);
      return;
    }
    if (!Bid128Libm.less(xd, C_ONE)) {
      out[0] = 0L;
      out[1] = 0L;
      return;
    }
    long[] t = new long[2];
    Bid128Raw.fma(
        hi, lo, hi, lo, MINUS_ONE.highBits(), MINUS_ONE.lowBits(),
        mode, flags, t);
    BidConvert.toBinary128From128(t[0], t[1], mode, flags, packed);
    Binary128 td = Binary128.fromRawBits(packed[0], packed[1]).negate();
    Binary128 yd = Dpml.sqrt(td, binaryMode, local);
    yd = Dpml.asin(yd, binaryMode, local);
    if (Bid128Libm.less(xd, C_ZERO)) {
      yd = Dpml.sub(C_PI, yd, binaryMode, local);
    }
    flags.raise(local.bits());
    BidConvert.fromBinary128To128(yd.highBits(), yd.lowBits(), mode, flags, out);
  }

  static void atanh(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (x.isInfinite()) {
      flags.raise(StatusFlags.INVALID);
      DecNum.store128(NAN, out);
      return;
    }
    if (x.isZero()) {
      out[0] = hi;
      out[1] = lo;
      return;
    }
    if (x.biasedExponent() <= 6176 - 51) {
      out[0] = hi;
      out[1] = lo;
      return;
    }
    Bid128 abs = Bid128.fromRawBits(hi & ~Bid128.MASK_SIGN, lo);
    long[] oneMx = new long[2];
    Bid128Raw.sub(
        Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
        abs.highBits(), abs.lowBits(), mode, flags, oneMx);
    if (Bid128.fromRawBits(oneMx[0], oneMx[1]).isSigned()) {
      flags.raise(StatusFlags.INVALID);
      DecNum.store128(NAN, out);
      return;
    }
    if (oneMx[1] == 0L && (oneMx[0] << 15) == 0L) {
      flags.raise(StatusFlags.DIVIDE_BY_ZERO);
      out[0] = (hi & Bid128.MASK_SIGN) | Bid128.MASK_INFINITY;
      out[1] = 0L;
      return;
    }
    long[] tmp = new long[2];
    long[] y = new long[2];
    Bid128Raw.div(
        abs.highBits(), abs.lowBits(), oneMx[0], oneMx[1], mode, flags, tmp);
    Bid128Raw.add(tmp[0], tmp[1], tmp[0], tmp[1], mode, flags, y);
    long[] packed = new long[2];
    BidConvert.toBinary128From128(y[0], y[1], mode, flags, packed);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 rq = Dpml.log1p(
        Binary128.fromRawBits(packed[0], packed[1]), binaryMode, local);
    rq = Dpml.mul(rq, C_HALF, binaryMode, local);
    flags.raise(local.bits());
    BidConvert.fromBinary128To128(rq.highBits(), rq.lowBits(), mode, flags, out);
    out[0] ^= hi & Bid128.MASK_SIGN;
  }
}
