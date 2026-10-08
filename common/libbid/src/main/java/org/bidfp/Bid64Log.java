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

/** Intel {@code bid64_log{,10,2}.c}: domain, divide-by-zero, near-1 correction. */
final class Bid64Log {
  enum Kind { LN, LOG10, LOG2 }

  static final long ONE = 0x31c0_0000_0000_0001L;
  static final long NEG_INF = 0xf800_0000_0000_0000L;
  static final long NAN = 0x7c00_0000_0000_0000L;
  private static final Binary128 C_ONE =
      Binary128.fromRawBits(0x3fff_0000_0000_0000L, 0L);
  private static final Binary128 C_HALF =
      Binary128.fromRawBits(0x3ffe_0000_0000_0000L, 0L);
  private static final Binary128 LN10 =
      Binary128.fromRawBits(0x4000_26bb_1bbb_5551L, 0x582d_d4ad_ac57_05a6L);
  private static final Binary128 INV_LN2 =
      Binary128.fromRawBits(0x3fff_7154_7652_b82fL, 0xe177_7d0f_fda0_d23aL);

  private Bid64Log() {
  }

  static long canonNan(long x, StatusFlags flags) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isSignalingNaN()) {
      flags.raise(StatusFlags.INVALID);
    }
    long quiet = x & 0xfc03_ffff_ffff_ffffL;
    if ((quiet & 0x0003_ffff_ffff_ffffL) > 999_999_999_999_999L) {
      quiet &= ~0x0003_ffff_ffff_ffffL;
    }
    return quiet;
  }

  static long log(long x, RoundingMode mode, StatusFlags flags) {
    return evaluate(x, mode, flags, Kind.LN);
  }

  static long log10(long x, RoundingMode mode, StatusFlags flags) {
    return evaluate(x, mode, flags, Kind.LOG10);
  }

  static long log2(long x, RoundingMode mode, StatusFlags flags) {
    return evaluate(x, mode, flags, Kind.LOG2);
  }

  private static long evaluate(
      long x, RoundingMode mode, StatusFlags flags, Kind kind) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isNaN()) {
      return canonNan(x, flags);
    }
    if (value.isZero()) {
      flags.raise(StatusFlags.DIVIDE_BY_ZERO);
      return NEG_INF;
    }
    if (value.isSigned()) {
      flags.raise(StatusFlags.INVALID);
      return NAN;
    }
    return logKernel(x, mode, flags, kind);
  }

  static long logKernel(long x, RoundingMode mode, StatusFlags flags, Kind kind) {
    long[] packed = new long[2];
    BidConvert.toBinary128From64(x, mode, flags, packed);
    Binary128 xd = Binary128.fromRawBits(packed[0], packed[1]);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 rd = kernel(kind, xd, binaryMode, local);
    Binary128 eBin = Dpml.sub(xd, C_ONE, binaryMode, local);
    Binary128 absE = eBin.isSigned() ? eBin.negate() : eBin;
    if (Bid128Libm.less(absE, C_HALF)) {
      long e = Bid64Raw.sub(x, ONE, mode, flags);
      long[] tmpPacked = new long[2];
      BidConvert.toBinary128From64(e, mode, flags, tmpPacked);
      Binary128 tmpE = Binary128.fromRawBits(tmpPacked[0], tmpPacked[1]);
      tmpE = Dpml.sub(eBin, tmpE, binaryMode, local);
      if (kind == Kind.LOG10) {
        Binary128 rt = Dpml.mul(LN10, xd, binaryMode, local);
        tmpE = Dpml.div(tmpE, rt, binaryMode, local);
      } else if (kind == Kind.LOG2) {
        tmpE = Dpml.mul(INV_LN2, tmpE, binaryMode, local);
        tmpE = Dpml.div(tmpE, xd, binaryMode, local);
      } else {
        tmpE = Dpml.div(tmpE, xd, binaryMode, local);
      }
      rd = Dpml.sub(rd, tmpE, binaryMode, local);
    }
    flags.raise(local.bits());
    return BidConvert.fromBinary128To64(rd.highBits(), rd.lowBits(), mode, flags);
  }

  private static Binary128 kernel(
      Kind kind,
      Binary128 xd,
      org.bidfp.binary128.RoundingMode binaryMode,
      org.bidfp.binary128.StatusFlags local) {
    if (kind == Kind.LOG10) {
      return Dpml.log10(xd, binaryMode, local);
    }
    if (kind == Kind.LOG2) {
      return Dpml.log2(xd, binaryMode, local);
    }
    return Dpml.log(xd, binaryMode, local);
  }
}
