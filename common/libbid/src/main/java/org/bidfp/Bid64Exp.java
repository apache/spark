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

/** Intel {@code bid64_exp.c}: NaN/Inf/0 and |x|>8000 overflow/underflow clamps. */
final class Bid64Exp {
  private static final long ONE = 0x31c0_0000_0000_0001L;
  private static final long ZERO = 0x31c0_0000_0000_0000L;
  private static final long INF = Bid64.MASK_INFINITY;
  private static final Binary128 C_8000 =
      Binary128.fromRawBits(0x400b_f400_0000_0000L, 0L);
  private static final Binary128 C_NEG_8000 =
      Binary128.fromRawBits(0xc00b_f400_0000_0000L, 0L);
  private static final Binary128 C_1E2000;
  private static final Binary128 C_1EM2000;

  static {
    long[] packed = new long[2];
    long[] bid = new long[2];
    StatusFlags flags = new StatusFlags();
    BidConvert.fromString128("1e2000", RoundingMode.TIES_TO_EVEN, flags, bid);
    BidConvert.toBinary128From128(
        bid[0], bid[1], RoundingMode.TIES_TO_EVEN, flags, packed);
    C_1E2000 = Binary128.fromRawBits(packed[0], packed[1]);
    flags = new StatusFlags();
    BidConvert.fromString128("1e-2000", RoundingMode.TIES_TO_EVEN, flags, bid);
    BidConvert.toBinary128From128(
        bid[0], bid[1], RoundingMode.TIES_TO_EVEN, flags, packed);
    C_1EM2000 = Binary128.fromRawBits(packed[0], packed[1]);
  }

  private Bid64Exp() {
  }

  static long exp(long x, RoundingMode mode, StatusFlags flags) {
    return evaluate(x, mode, flags, Kind.EXP, C_8000, C_NEG_8000);
  }

  static long exp2(long x, RoundingMode mode, StatusFlags flags) {
    return evaluate(x, mode, flags, Kind.EXP2, C_12000, C_NEG_12000);
  }

  static long exp10(long x, RoundingMode mode, StatusFlags flags) {
    return evaluate(x, mode, flags, Kind.EXP10, C_12000, C_NEG_12000);
  }

  private enum Kind { EXP, EXP2, EXP10 }

  private static final Binary128 C_12000 =
      Binary128.fromRawBits(0x400c_7700_0000_0000L, 0L);
  private static final Binary128 C_NEG_12000 =
      Binary128.fromRawBits(0xc00c_7700_0000_0000L, 0L);

  private static long evaluate(
      long x, RoundingMode mode, StatusFlags flags, Kind kind,
      Binary128 hiClamp, Binary128 loClamp) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isNaN()) {
      if (value.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      long quiet = x & 0xfc03_ffff_ffff_ffffL;
      if ((quiet & 0x0003_ffff_ffff_ffffL) > 999_999_999_999_999L) {
        quiet &= ~0x0003_ffff_ffff_ffffL;
      }
      return quiet;
    }
    if (value.isZero()) {
      return ONE;
    }
    if (value.isInfinite()) {
      return value.isSigned() ? ZERO : INF;
    }
    long[] packed = new long[2];
    BidConvert.toBinary128From64(x, mode, flags, packed);
    Binary128 xd = Binary128.fromRawBits(packed[0], packed[1]);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 rd;
    if (Bid128Libm.greater(xd, hiClamp)) {
      rd = C_1E2000;
    } else if (Bid128Libm.less(xd, loClamp)) {
      rd = C_1EM2000;
    } else {
      if (kind == Kind.EXP2) {
        rd = Dpml.exp2(xd, binaryMode, local);
      } else if (kind == Kind.EXP10) {
        rd = Dpml.exp10(xd, binaryMode, local);
      } else {
        rd = Dpml.exp(xd, binaryMode, local);
      }
      flags.raise(local.bits());
    }
    return BidConvert.fromBinary128To64(rd.highBits(), rd.lowBits(), mode, flags);
  }
}
