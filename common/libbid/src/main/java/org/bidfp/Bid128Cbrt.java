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

/** Intel {@code bid128_cbrt.c}: rebias exponent by 1/3 before the kernel. */
final class Bid128Cbrt {
  private Bid128Cbrt() {
  }

  static void cbrt(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (x.isInfinite()) {
      out[0] = (hi & Bid128.MASK_SIGN) | Bid128.MASK_INFINITY;
      out[1] = 0L;
      return;
    }
    if (x.isZero()) {
      out[0] = hi;
      out[1] = lo;
      return;
    }
    int exponent = x.biasedExponent();
    int iexpon = exponent + 1;
    int k = (iexpon * 0x5556) >> 16;
    int j = iexpon - 3 * k;
    k -= (1 + 6176) / 3;
    UInt128 coeff = x.coefficient();
    Bid128 tmp = Bid128.rawFinite(x.isSigned(), j + 6176, coeff.high(), coeff.low());
    long[] packed = new long[2];
    BidConvert.toBinary128From128(
        tmp.highBits(), tmp.lowBits(), mode, flags, packed);
    Binary128 xq = Binary128.fromRawBits(packed[0], packed[1]);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 rq = Dpml.cbrt(xq, binaryMode, local);
    flags.raise(local.bits());
    BidConvert.fromBinary128To128(rq.highBits(), rq.lowBits(), mode, flags, out);
    out[0] += ((long) k) << 49;
  }
}
