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

/** Intel {@code bid64_pow.c} specials, integer exponent, near-1 log. */
final class Bid64Pow {
  private static final long ZERO = 0x31c0_0000_0000_0000L;
  private static final long ONE = 0x31c0_0000_0000_0001L;
  private static final long NAN = 0x7c00_0000_0000_0000L;
  private static final long INF = Bid64.MASK_INFINITY;
  private static final Binary128 C_ONE =
      Binary128.fromRawBits(0x3fff_0000_0000_0000L, 0L);
  private static final Binary128 C_HALF =
      Binary128.fromRawBits(0x3ffe_0000_0000_0000L, 0L);

  private Bid64Pow() {
  }

  static long pow(long x, long y, RoundingMode mode, StatusFlags flags) {
    int smallExponent = smallPositiveInteger(y);
    if (smallExponent >= 2
        && Bid64Raw.isFinite(x)
        && !Bid64Raw.isZero(x)) {
      return powSmallInteger(x, smallExponent, mode, flags);
    }
    Bid64 bx = Bid64.fromRawBits(x);
    Bid64 by = Bid64.fromRawBits(y);
    if (bx.isSignalingNaN() || by.isSignalingNaN()) {
      flags.raise(StatusFlags.INVALID);
    }
    if (by.isZero() && !bx.isSignalingNaN()) {
      return ONE;
    }
    if (Bid64.fromRawBits(x).quietEqual(Bid64.fromRawBits(ONE), new StatusFlags())
        && !by.isSignalingNaN()) {
      return ONE;
    }
    if (bx.isNaN()) {
      return Bid64Log.canonNan(x, flags);
    }
    if (by.isNaN()) {
      return Bid64Log.canonNan(y, flags);
    }
    long yInt = Bid64Raw.roundIntegralNearestEven(y, new StatusFlags());
    boolean isInt = Bid64.fromRawBits(yInt).quietEqual(by, new StatusFlags());
    boolean odd = false;
    if (isInt) {
      int e = ((yInt & (3L << 61)) == (3L << 61))
          ? (int) ((yInt >>> 51) & 0x3ff)
          : (int) ((yInt >>> 53) & 0x3ff);
      if (e == 398 && (yInt & 1L) != 0L) {
        odd = true;
      }
    }
    if (by.isInfinite()) {
      long absX = x & ~Bid64.MASK_SIGN;
      if (Bid64.fromRawBits(absX).quietEqual(Bid64.fromRawBits(ONE), new StatusFlags())) {
        return ONE;
      }
      boolean less = Bid64.fromRawBits(absX).quietLess(Bid64.fromRawBits(ONE), new StatusFlags());
      boolean yNeg = by.isSigned();
      if (less) {
        return yNeg ? INF : ZERO;
      }
      return yNeg ? ZERO : INF;
    }
    if (bx.isInfinite()) {
      long result = by.isSigned() ? ZERO : INF;
      if (odd && bx.isSigned()) {
        result ^= Bid64.MASK_SIGN;
      }
      return result;
    }
    if (bx.isZero()) {
      long result;
      if (by.isSigned()) {
        flags.raise(StatusFlags.DIVIDE_BY_ZERO);
        result = INF;
      } else {
        result = ZERO;
      }
      if (odd && bx.isSigned()) {
        result ^= Bid64.MASK_SIGN;
      }
      return result;
    }
    StatusFlags convertFlags = new StatusFlags();
    int exactY = Bid64Raw.toInt32(y, RoundingMode.TIES_TO_EVEN, convertFlags, true);
    if ((convertFlags.bits() & (StatusFlags.INEXACT | StatusFlags.INVALID)) == 0) {
      boolean inexact = false;
      long p;
      if (exactY < 0) {
        StatusFlags divFlags = new StatusFlags();
        p = Bid64Raw.div(ONE, x, mode, divFlags);
        if ((divFlags.bits() & StatusFlags.INEXACT) != 0) {
          inexact = true;
        }
        flags.raise(divFlags.bits());
        exactY = -exactY;
      } else {
        p = x;
      }
      if (!inexact && exactY <= 398) {
        long r = ONE;
        while (exactY != 0) {
          if ((exactY & 1) != 0) {
            r = Bid64Raw.mul(r, p, mode, flags);
          }
          if (exactY > 1) {
            p = Bid64Raw.mul(p, p, mode, flags);
          }
          exactY >>= 1;
        }
        return r;
      }
    }
    long[] packedX = new long[2];
    long[] packedY = new long[2];
    BidConvert.toBinary128From64(x & ~Bid64.MASK_SIGN, mode, flags, packedX);
    BidConvert.toBinary128From64(y, mode, flags, packedY);
    Binary128 xd = Binary128.fromRawBits(packedX[0], packedX[1]);
    Binary128 yd = Binary128.fromRawBits(packedY[0], packedY[1]);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 ld = Dpml.log(xd, binaryMode, local);
    Binary128 eBin = Dpml.sub(xd, C_ONE, binaryMode, local);
    Binary128 absE = eBin.isSigned() ? eBin.negate() : eBin;
    if (Bid128Libm.less(absE, C_HALF)) {
      long e = Bid64Raw.sub(x & ~Bid64.MASK_SIGN, ONE, mode, flags);
      long[] tmpPacked = new long[2];
      BidConvert.toBinary128From64(e, mode, flags, tmpPacked);
      Binary128 tmpE = Binary128.fromRawBits(tmpPacked[0], tmpPacked[1]);
      tmpE = Dpml.sub(eBin, tmpE, binaryMode, local);
      tmpE = Dpml.div(tmpE, xd, binaryMode, local);
      ld = Dpml.sub(ld, tmpE, binaryMode, local);
    }
    Binary128 rd = Dpml.mul(yd, ld, binaryMode, local);
    rd = Dpml.exp(rd, binaryMode, local);
    flags.raise(local.bits());
    long result = BidConvert.fromBinary128To64(
        rd.highBits(), rd.lowBits(), mode, flags);
    if (Bid64Raw.isNaN(result) || (bx.isSigned() && !isInt)) {
      flags.raise(StatusFlags.INVALID);
      return NAN;
    }
    if (odd && bx.isSigned()) {
      result ^= Bid64.MASK_SIGN;
    }
    return result;
  }

  private static int smallPositiveInteger(long value) {
    if (!Bid64Raw.isFinite(value)
        || Bid64.biasedExponentBits(value) != 398
        || Bid64Raw.isSigned(value)) {
      return -1;
    }
    long coefficient = Bid64.significandBits(value);
    return coefficient >= 2L && coefficient <= 5L ? (int) coefficient : -1;
  }

  private static long powSmallInteger(
      long value, int exponent, RoundingMode mode, StatusFlags flags) {
    long square = Bid64Raw.mul(value, value, mode, flags);
    if (exponent == 2) {
      return square;
    }
    if (exponent == 3) {
      return Bid64Raw.mul(square, value, mode, flags);
    }
    long fourth = Bid64Raw.mul(square, square, mode, flags);
    return exponent == 4 ? fourth : Bid64Raw.mul(fourth, value, mode, flags);
  }
}
