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

/** Shared Intel wrapper helpers for BID128 libm. */
final class Bid128Libm {
  static final Bid128 ONE = Bid128.fromRawBits(0x3040_0000_0000_0000L, 1L);

  private Bid128Libm() {
  }

  static boolean lessEqual(Binary128 a, Binary128 b) {
    Binary128 d = Dpml.sub(
        a, b,
        org.bidfp.binary128.RoundingMode.TIES_TO_EVEN,
        new org.bidfp.binary128.StatusFlags());
    return !d.isNaN() && (d.isZero() || d.isSigned());
  }

  static boolean less(Binary128 a, Binary128 b) {
    Binary128 d = Dpml.sub(
        a, b,
        org.bidfp.binary128.RoundingMode.TIES_TO_EVEN,
        new org.bidfp.binary128.StatusFlags());
    return !d.isNaN() && !d.isZero() && d.isSigned();
  }

  static boolean greater(Binary128 a, Binary128 b) {
    Binary128 d = Dpml.sub(
        a, b,
        org.bidfp.binary128.RoundingMode.TIES_TO_EVEN,
        new org.bidfp.binary128.StatusFlags());
    return !d.isNaN() && !d.isZero() && !d.isSigned();
  }

  static final long TEN_PM40_POS = 0x2ff0_0000_0000_0000L;
  static final long TEN_PM40_NEG = 0xaff0_0000_0000_0000L;

  static boolean tinyOddFma(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    return tinyOddFma(hi, lo, TEN_PM40_POS, mode, flags, out);
  }

  static boolean tinyOddFma(
      long hi, long lo, long scaleHi, RoundingMode mode, StatusFlags flags,
      long[] out) {
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (!x.isFinite() || x.isZero()) {
      return false;
    }
    if (x.biasedExponent() - 6176 >= -52) {
      return false;
    }
    Bid128Raw.fma(hi, lo, scaleHi, 1L, hi, lo, mode, flags, out);
    return true;
  }

  static boolean canonNan(long hi, long lo, StatusFlags flags, long[] out) {
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (!x.isNaN()) {
      return false;
    }
    if (x.isSignalingNaN()) {
      flags.raise(StatusFlags.INVALID);
    }
    long quiet = hi & 0xfc00_3fff_ffff_ffffL;
    long payloadLow = lo;
    UInt128 payload = new UInt128(quiet & 0x0000_3fff_ffff_ffffL, payloadLow);
    if (payload.compareTo(PowersOfTen.MAX_33) > 0) {
      quiet &= ~0x0000_3fff_ffff_ffffL;
      payloadLow = 0L;
    }
    out[0] = quiet;
    out[1] = payloadLow;
    return true;
  }
}
