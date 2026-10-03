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

/** Intel {@code bid64_tgamma.c}: 0/Inf, 8000 clamp, reflection at poles. */
final class Bid64Tgamma {
  private static final long NAN = 0x7c00_0000_0000_0000L;
  private static final long INF = Bid64.MASK_INFINITY;
  private static final Binary128 C_HALF =
      Binary128.fromRawBits(0x3ffe_0000_0000_0000L, 0L);
  private static final Binary128 C_8000 =
      Binary128.fromRawBits(0x400b_f400_0000_0000L, 0L);
  private static final Binary128 C_ONE =
      Binary128.fromRawBits(0x3fff_0000_0000_0000L, 0L);
  private static final Binary128 PI =
      Binary128.fromRawBits(0x4000_921f_b544_42d1L, 0x8469_898c_c517_01b8L);
  private static final Binary128 C_1E2000;

  static {
    long[] packed = new long[2];
    long[] bid = new long[2];
    StatusFlags flags = new StatusFlags();
    BidConvert.fromString128("1e2000", RoundingMode.TIES_TO_EVEN, flags, bid);
    BidConvert.toBinary128From128(
        bid[0], bid[1], RoundingMode.TIES_TO_EVEN, flags, packed);
    C_1E2000 = Binary128.fromRawBits(packed[0], packed[1]);
  }

  private Bid64Tgamma() {
  }

  static long tgamma(long x, RoundingMode mode, StatusFlags flags) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isNaN()) {
      return Bid64Log.canonNan(x, flags);
    }
    if (value.isZero()) {
      flags.raise(StatusFlags.DIVIDE_BY_ZERO);
      return INF ^ (x & Bid64.MASK_SIGN);
    }
    if (value.isInfinite()) {
      if (value.isSigned()) {
        flags.raise(StatusFlags.INVALID);
        return NAN;
      }
      return INF;
    }
    if (value.biasedExponent() - 398 <= -20) {
      long reciprocal = Bid64Raw.div(Bid64Log.ONE, x, mode, flags);
      return Bid64Raw.sub(reciprocal, Bid64Log.ONE, mode, flags);
    }
    long[] packed = new long[2];
    BidConvert.toBinary128From64(x, mode, flags, packed);
    Binary128 xd = Binary128.fromRawBits(packed[0], packed[1]);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    if (!Bid128Libm.less(xd, C_HALF)) {
      Binary128 yd;
      if (!Bid128Libm.less(xd, C_8000)) {
        yd = C_1E2000;
      } else {
        yd = Dpml.tgamma(xd, binaryMode, local);
        flags.raise(local.bits());
      }
      return BidConvert.fromBinary128To64(yd.highBits(), yd.lowBits(), mode, flags);
    }
    long xInt = Bid64Raw.roundIntegralNearestEven(x, new StatusFlags());
    long xFrac = Bid64Raw.sub(x, xInt, mode, flags);
    if (Bid64.fromRawBits(xFrac).isZero()) {
      flags.raise(StatusFlags.INVALID);
      return NAN;
    }
    long[] fracPacked = new long[2];
    BidConvert.toBinary128From64(xFrac, mode, flags, fracPacked);
    Binary128 fd = Binary128.fromRawBits(fracPacked[0], fracPacked[1]);
    Binary128 rt = Dpml.sub(C_ONE, xd, binaryMode, local);
    Binary128 yd = Dpml.mul(PI, fd, binaryMode, local);
    yd = Dpml.sin(yd, binaryMode, local);
    rt = Dpml.tgamma(rt, binaryMode, local);
    yd = Dpml.mul(yd, rt, binaryMode, local);
    yd = Dpml.div(PI, yd, binaryMode, local);
    flags.raise(local.bits());
    int e = ((xInt & (3L << 61)) == (3L << 61))
        ? (int) ((xInt >>> 51) & 0x3ff)
        : (int) ((xInt >>> 53) & 0x3ff);
    e &= 0x3ff;
    if (e <= 398) {
      if (e < 398) {
        xInt = Bid64Raw.add(0x31c0_0000_0001_0000L, xInt, mode, flags);
      }
      if ((xInt & 1L) != 0L) {
        yd = yd.negate();
      }
    }
    return BidConvert.fromBinary128To64(
        yd.highBits(), yd.lowBits(), mode, flags);
  }
}
