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

/** Intel {@code bid128_expm1.c}. */
final class Bid128Expm1 {
  private static final Binary128 ONE_E_M40 =
      Binary128.fromRawBits(0x3f7a_16c2_6277_7579L, 0xc58c_4647_5896_767bL);
  private static final Binary128 ONE =
      Binary128.fromRawBits(0x3fff_0000_0000_0000L, 0L);

  private Bid128Expm1() {
  }

  static void expm1(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    long[] packed = new long[2];
    BidConvert.toBinary128From128(hi, lo, mode, flags, packed);
    Binary128 xd = Binary128.fromRawBits(packed[0], packed[1]);
    Binary128 abs = xd.isSigned() ? xd.negate() : xd;
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    if (Bid128Libm.lessEqual(abs, ONE_E_M40)) {
      Bid128 x = Bid128.fromRawBits(hi, lo);
      if (x.isZero()) {
        Bid128Raw.mul(
            hi, lo, Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
            mode, flags, out);
      } else {
        Bid128Raw.fma(hi, lo, hi, lo, hi, lo, mode, flags, out);
      }
      return;
    }
    if (Bid128Libm.lessEqual(xd, ONE)) {
      Binary128 yd = Dpml.expm1(xd, binaryMode, local);
      flags.raise(local.bits());
      BidConvert.fromBinary128To128(yd.highBits(), yd.lowBits(), mode, flags, out);
      return;
    }
    long[] exp = new long[2];
    Bid128Exp.exp(hi, lo, mode, flags, exp);
    Bid128Raw.sub(
        exp[0], exp[1],
        Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
        mode, flags, out);
  }
}
