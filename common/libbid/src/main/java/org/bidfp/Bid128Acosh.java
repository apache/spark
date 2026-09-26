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

/** Intel {@code bid128_acosh.c}: near-1 asinh(sqrt(x*x-1)) and huge-x log. */
final class Bid128Acosh {
  private static final Bid128 NEAR_ONE =
      Bid128.fromRawBits(0x3036_0000_0000_0000L, 103125L);
  private static final Bid128 NEG_ONE =
      Bid128.fromRawBits(0xb040_0000_0000_0000L, 1L);
  private static final Binary128 LN10 =
      Binary128.fromRawBits(0x4000_26bb_1bbb_5551L, 0x582d_d4ad_ac57_05a6L);

  private Bid128Acosh() {
  }

  static void acosh(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (x.isInfinite()) {
      if (x.isSigned()) {
        flags.raise(StatusFlags.INVALID);
        out[0] = Bid128.MASK_NAN;
        out[1] = 0L;
      } else {
        out[0] = Bid128.MASK_INFINITY;
        out[1] = 0L;
      }
      return;
    }
    if (x.quietLess(NEAR_ONE, new StatusFlags())) {
      if (Bid128Libm.ONE.quietGreater(x, new StatusFlags())) {
        flags.raise(StatusFlags.INVALID);
        out[0] = Bid128.MASK_NAN;
        out[1] = 0L;
        return;
      }
      long[] z2 = new long[2];
      long[] z = new long[2];
      Bid128Raw.fma(
          hi, lo, hi, lo, NEG_ONE.highBits(), NEG_ONE.lowBits(),
          mode, flags, z2);
      Bid128Raw.sqrt(z2[0], z2[1], mode, flags, z);
      BidTranscendental.unary128(z[0], z[1], mode, flags, Dpml::asinh, out);
      return;
    }
    int exponent = x.biasedExponent();
    if (exponent > 6176 + 34) {
      UInt128 coeff = x.coefficient();
      Bid128 xn = Bid128.rawFinite(false, 6176, coeff.high(), coeff.low());
      long[] packed = new long[2];
      BidConvert.toBinary128From128(
          xn.highBits(), xn.lowBits(), mode, flags, packed);
      org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
      org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
      Binary128 xq = Binary128.fromRawBits(packed[0], packed[1]);
      xq = Dpml.add(xq, xq, binaryMode, local);
      long[] expBid = new long[2];
      Bid128Raw.fromInt32(exponent - 6176, expBid);
      BidConvert.toBinary128From128(
          expBid[0], expBid[1], RoundingMode.TIES_TO_EVEN, new StatusFlags(),
          packed);
      Binary128 yq = Binary128.fromRawBits(packed[0], packed[1]);
      Binary128 rq = Dpml.log(xq, binaryMode, local);
      Binary128 rt = Dpml.mul(yq, LN10, binaryMode, local);
      rq = Dpml.add(rq, rt, binaryMode, local);
      flags.raise(local.bits());
      BidConvert.fromBinary128To128(
          rq.highBits(), rq.lowBits(), mode, flags, out);
      return;
    }
    BidTranscendental.unary128(hi, lo, mode, flags, Dpml::acosh, out);
  }
}
