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

/** Intel {@code bid128_exp10.c}: integer exponent scale plus fractional exp10. */
final class Bid128Exp10 {
  private static final Bid128 THRESHOLD_6111 =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 0x17dfL);
  private static final Bid128 THRESHOLD_6400 =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 0x1900L);
  private static final Bid128 ONE =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 1L);
  private static final Bid128 ZERO =
      Bid128.fromRawBits(0x3040_0000_0000_0000L, 0L);
  private static final Bid128 INF =
      Bid128.fromRawBits(Bid128.MASK_INFINITY, 0L);

  private Bid128Exp10() {
  }

  static void exp10(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(hi, lo);
    if (x.isInfinite()) {
      DecNum.store128(x.isSigned() ? ZERO : INF, out);
      return;
    }
    if (x.isZero()) {
      DecNum.store128(ONE, out);
      return;
    }
    long sign = hi & Bid128.MASK_SIGN;
    Bid128 abs = Bid128.fromRawBits(hi & ~Bid128.MASK_SIGN, lo);
    if (THRESHOLD_6111.quietLess(abs, new StatusFlags())) {
      if (THRESHOLD_6400.quietLess(abs, new StatusFlags())) {
        long tmpHi = sign != 0L ? 0x1100_0000_0000_0000L : 0x4f80_0000_0000_0000L;
        Bid128Raw.mul(tmpHi, 1L, tmpHi, 1L, mode, flags, out);
        return;
      }
      scaleLarge(hi, lo, sign, abs, mode, flags, out);
      return;
    }
    scaleSmall(hi, lo, sign, abs, mode, flags, out);
  }

  private static void scaleLarge(
      long hi, long lo, long sign, Bid128 abs,
      RoundingMode mode, StatusFlags flags, long[] out) {
    StatusFlags discard = new StatusFlags();
    int k = Bid128Raw.toInt32(
        abs.highBits(), abs.lowBits(), RoundingMode.TIES_TO_EVEN, discard, false);
    long tmpHi = sign ^ 0xb040_0000_0000_0000L;
    long[] fd = new long[2];
    Bid128Raw.add(hi, lo, tmpHi, k, mode, flags, fd);
    long[] packed = new long[2];
    BidConvert.toBinary128From128(fd[0], fd[1], mode, flags, packed);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 rq = Dpml.exp10(
        Binary128.fromRawBits(packed[0], packed[1]), binaryMode, local);
    flags.raise(local.bits());
    BidConvert.fromBinary128To128(rq.highBits(), rq.lowBits(), mode, flags, out);
    if (sign != 0L) {
      k = -k;
    }
    int k2 = k >> 1;
    k -= k2;
    out[0] += ((long) k2) << 49;
    long scaleHi = 0x3040_0000_0000_0000L + (((long) k) << 49);
    Bid128Raw.mul(out[0], out[1], scaleHi, 1L, mode, flags, out);
  }

  private static void scaleSmall(
      long hi, long lo, long sign, Bid128 abs,
      RoundingMode mode, StatusFlags flags, long[] out) {
    StatusFlags discard = new StatusFlags();
    int k = Bid128Raw.toInt32(
        abs.highBits(), abs.lowBits(), RoundingMode.TIES_TO_EVEN, discard, false);
    long tmpHi = sign ^ 0xb040_0000_0000_0000L;
    long[] fd = new long[2];
    Bid128Raw.add(hi, lo, tmpHi, k, mode, flags, fd);
    long[] packed = new long[2];
    BidConvert.toBinary128From128(fd[0], fd[1], mode, flags, packed);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 rq = Dpml.exp10(
        Binary128.fromRawBits(packed[0], packed[1]), binaryMode, local);
    flags.raise(local.bits());
    BidConvert.fromBinary128To128(rq.highBits(), rq.lowBits(), mode, flags, out);
    long kl = k;
    long scorr = sign >> 63;
    kl = scorr ^ (kl + scorr);
    out[0] += kl << 49;
  }
}
