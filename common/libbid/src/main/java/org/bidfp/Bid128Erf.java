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

/** Intel {@code bid128_erf.c} / {@code bid128_erfc.c} specials. */
final class Bid128Erf {
  private static final Bid128 TWO_OVER_SQRT_PI =
      Bid128.fromRawBits(0x2ffe_37a2_25ba_a150L, 0xf009_a099_f5c1_b689L);
  private static final Bid128 ONE =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 1L);
  private static final Bid128 TEN_POW_N6000 =
      Bid128.fromRawBits(0x0160_0000_0000_0000L, 1L);
  private static final Binary128 C_1EM2000 =
      Binary128.fromRawBits(0x260b_1ad5_6d71_2a5dL, 0x7f02_384e_5ded_39beL);
  private static final Binary128 C_1EM40 =
      Binary128.fromRawBits(0x3f7a_16c2_6277_7579L, 0xc58c_4647_5896_767bL);
  private static final Binary128 C_105 =
      Binary128.fromRawBits(0x4005_a400_0000_0000L, 0L);
  private static final Binary128 C_120 =
      Binary128.fromRawBits(0x4005_e000_0000_0000L, 0L);
  private static final Binary128 C_2_SQRT_PI =
      Binary128.fromRawBits(0x3fff_20dd_7504_29b6L, 0xd11a_e3a9_14fe_d7feL);
  private static final Binary128 C_1_SQRT_PI =
      Binary128.fromRawBits(0x3ffe_20dd_7504_29b6L, 0xd11a_e3a9_14fe_d7feL);
  private static final Binary128 C_ONE =
      Binary128.fromRawBits(0x3fff_0000_0000_0000L, 0L);
  private static final Binary128[] ERFC_ASYM = {
      Binary128.fromRawBits(0x4019_2684_1857_e3ffL, 0xfff9_20c8_098a_1091L),
      Binary128.fromRawBits(0xc015_99c2_ea37_8000L, 0L),
      Binary128.fromRawBits(0x4012_3832_fb98_0000L, 0L),
      Binary128.fromRawBits(0xc00f_06e7_9080_0000L, 0L),
      Binary128.fromRawBits(0x400b_eee1_1000_0000L, 0L),
      Binary128.fromRawBits(0xc009_07ef_8000_0000L, 0L),
      Binary128.fromRawBits(0x4006_44d8_0000_0000L, 0L),
      Binary128.fromRawBits(0xc003_d880_0000_0000L, 0L),
      Binary128.fromRawBits(0x4001_a400_0000_0000L, 0L),
      Binary128.fromRawBits(0xbfff_e000_0000_0000L, 0L),
      Binary128.fromRawBits(0x3ffe_8000_0000_0000L, 0L),
      Binary128.fromRawBits(0xbffe_0000_0000_0000L, 0L)
  };

  private Bid128Erf() {
  }

  static void erf(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    long[] packed = new long[2];
    BidConvert.toBinary128From128(hi, lo, mode, flags, packed);
    Binary128 xd = Binary128.fromRawBits(packed[0], packed[1]);
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (x.isFinite() && !x.isZero()
        && x.biasedExponent() - 6176 < -500
        && Bid128Libm.less(xd.abs(), C_1EM2000)) {
      Bid128Raw.mul(
          TWO_OVER_SQRT_PI.highBits(), TWO_OVER_SQRT_PI.lowBits(),
          hi, lo, mode, flags, out);
      return;
    }
    BidTranscendental.unary128(hi, lo, mode, flags, Dpml::erf, out);
  }

  static void erfc(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (x.isZero()) {
      DecNum.store128(ONE, out);
      return;
    }
    long[] packed = new long[2];
    BidConvert.toBinary128From128(hi, lo, mode, flags, packed);
    Binary128 xd = Binary128.fromRawBits(packed[0], packed[1]);
    if (Bid128Libm.less(xd.abs(), C_1EM40)) {
      Bid128Raw.sub(
          ONE.highBits(), ONE.lowBits(), hi, lo, mode, flags, out);
      return;
    }
    if (x.isSigned()) {
      BidTranscendental.unary128(hi, lo, mode, flags, Dpml::erfc, out);
      return;
    }
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    if (Bid128Libm.less(xd, C_105)) {
      long[] hiPart = new long[2];
      long[] loPart = new long[2];
      BidBinary128Convert.toBinary128TwoPart(hi, lo, hiPart, loPart);
      Binary128 high = Binary128.fromRawBits(hiPart[0], hiPart[1]);
      Binary128 low = Binary128.fromRawBits(loPart[0], loPart[1]);
      Binary128 rt = Dpml.mul(high, high, binaryMode, local);
      rt = rt.negate();
      rt = Dpml.exp(rt, binaryMode, local);
      rt = Dpml.mul(C_2_SQRT_PI, rt, binaryMode, local);
      rt = Dpml.mul(rt, low, binaryMode, local);
      Binary128 rd = Dpml.erfc(high, binaryMode, local);
      Binary128 yd = Dpml.sub(rd, rt, binaryMode, local);
      flags.raise(local.bits());
      BidConvert.fromBinary128To128(
          yd.highBits(), yd.lowBits(), mode, flags, out);
      return;
    }
    if (Bid128Libm.greater(xd, C_120)) {
      Bid128Raw.mul(
          TEN_POW_N6000.highBits(), TEN_POW_N6000.lowBits(),
          TEN_POW_N6000.highBits(), TEN_POW_N6000.lowBits(),
          mode, flags, out);
      return;
    }
    long[] x2Hi = new long[2];
    long[] x2Lo = new long[2];
    long[] yHi = new long[2];
    Bid128Raw.mul(hi, lo, hi, lo, mode, flags, x2Hi);
    x2Hi[0] ^= Bid128.MASK_SIGN;
    Bid128Raw.fma(hi, lo, hi, lo, x2Hi[0], x2Hi[1], mode, flags, x2Lo);
    x2Lo[0] ^= Bid128.MASK_SIGN;
    Bid128Exp.exp(x2Hi[0], x2Hi[1], mode, flags, yHi);
    Bid128Raw.fma(
        yHi[0], yHi[1], x2Lo[0], x2Lo[1], yHi[0], yHi[1], mode, flags, yHi);
    Binary128 xdi = Dpml.div(C_ONE, xd, binaryMode, local);
    Binary128 xi2 = Dpml.mul(xdi, xdi, binaryMode, local);
    Binary128 pd = ERFC_ASYM[0];
    for (int i = 1; i < ERFC_ASYM.length; i++) {
      pd = Dpml.mul(xi2, pd, binaryMode, local);
      pd = Dpml.add(ERFC_ASYM[i], pd, binaryMode, local);
    }
    pd = Dpml.mul(xi2, pd, binaryMode, local);
    pd = Dpml.add(C_ONE, pd, binaryMode, local);
    Binary128 rt = Dpml.mul(xdi, C_1_SQRT_PI, binaryMode, local);
    pd = Dpml.mul(rt, pd, binaryMode, local);
    flags.raise(local.bits());
    long[] yLo = new long[2];
    BidConvert.fromBinary128To128(pd.highBits(), pd.lowBits(), mode, flags, yLo);
    Bid128Raw.mul(yHi[0], yHi[1], yLo[0], yLo[1], mode, flags, out);
  }
}
