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

/** Intel {@code bid128_tgamma.c}: poles, exp(lgamma), odd-interval sign. */
final class Bid128Tgamma {
  private static final Bid128 NAN =
      Bid128.fromRawBits(0x7c00_0000_0000_0000L, 0L);
  private static final Bid128 INF =
      Bid128.fromRawBits(Bid128.MASK_INFINITY, 0L);
  private static final Bid128 ZERO =
      Bid128.fromRawBits(0L, 0L);
  private static final Bid128 HALF =
      Bid128.fromRawBits(0x303e_0000_0000_0000L, 5L);
  private static final Bid128 SIXTEEN =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 16L);
  private static final Bid128 THREE_THOUSAND =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 3000L);
  private static final Bid128 SHIFTER =
      Bid128.fromRawBits(0x3040_629b_8c89_1b26L, 0x7182_b614_0000_0000L);

  private Bid128Tgamma() {
  }

  static void tgamma(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (x.isZero()) {
      flags.raise(StatusFlags.DIVIDE_BY_ZERO);
      out[0] = Bid128.MASK_INFINITY ^ (hi & Bid128.MASK_SIGN);
      out[1] = 0L;
      return;
    }
    if (x.isInfinite()) {
      if (x.isSigned()) {
        flags.raise(StatusFlags.INVALID);
        DecNum.store128(NAN, out);
      } else {
        DecNum.store128(INF, out);
      }
      return;
    }
    Bid128 tiny = Bid128.fromRawBits(0x3018_0000_0000_0000L, 1L);
    if (Bid128.fromRawBits(hi & ~Bid128.MASK_SIGN, lo)
        .quietLess(tiny, new StatusFlags())) {
      Bid128Raw.div(
          Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
          hi, lo, mode, flags, out);
      Bid128Raw.sub(
          out[0], out[1], Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
          mode, flags, out);
      return;
    }
    long[] xFrac = null;
    if (x.quietLessEqual(ZERO, new StatusFlags())) {
      long[] xInt = new long[2];
      xFrac = new long[2];
      Bid128Raw.roundIntegralNearestEven(hi, lo, new StatusFlags(), xInt);
      Bid128Raw.sub(hi, lo, xInt[0], xInt[1], mode, flags, xFrac);
      if (Bid128.fromRawBits(xFrac[0], xFrac[1]).isZero()) {
        flags.raise(StatusFlags.INVALID);
        DecNum.store128(NAN, out);
        return;
      }
    }
    if (x.isSigned()
        && xFrac != null
        && Bid128.fromRawBits(xFrac[0] & ~Bid128.MASK_SIGN, xFrac[1])
            .quietLess(tiny, new StatusFlags())) {
      Bid128Raw.div(
          Bid128Libm.ONE.highBits() | Bid128.MASK_SIGN, Bid128Libm.ONE.lowBits(),
          xFrac[0], xFrac[1], mode, flags, out);
      Bid128Raw.sub(
          out[0], out[1], Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
          mode, flags, out);
      return;
    }
    if (!x.isSigned()
        && x.quietGreaterEqual(SIXTEEN, new StatusFlags())
        && x.quietLess(THREE_THOUSAND, new StatusFlags())) {
      org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
      Binary128[] lgamma = Bid128Lgamma.positiveBinaryLgammaTwoPart(
          hi, lo, local);
      flags.raise(local.bits());
      Bid128Exp.expBinaryTwoPart(
          lgamma[0], lgamma[1], mode, flags, out);
      return;
    }
    if (!x.isSigned()
        && x.quietGreaterEqual(HALF, new StatusFlags())
        && x.quietLess(SIXTEEN, new StatusFlags())) {
      org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
      org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
      Binary128 lgamma = Bid128Lgamma.positiveBinaryLgamma(
          hi, lo, binaryMode, local);
      flags.raise(local.bits());
      Bid128Exp.expBinary(lgamma, mode, flags, out);
      return;
    }
    long[] y = new long[2];
    Bid128Raw.lgamma(hi, lo, mode, flags, y);
    Bid128Exp.exp(y[0], y[1], mode, flags, out);
    if (Bid128.fromRawBits(out[0], out[1]).isNaN() || !x.isSigned()) {
      return;
    }
    long[] xInt = new long[2];
    Bid128Raw.roundIntegralZero(hi, lo, new StatusFlags(), xInt);
    int e = (int) ((xInt[0] >>> 49) & 0x3fff);
    if (e <= 6176) {
      if (e < 6176) {
        Bid128Raw.add(
            SHIFTER.highBits(), SHIFTER.lowBits(),
            xInt[0], xInt[1], mode, flags, xInt);
      }
      if ((xInt[1] & 1L) == 0L) {
        out[0] ^= Bid128.MASK_SIGN;
      }
    }
  }

}
