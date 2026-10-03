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

import java.math.BigInteger;

import org.bidfp.binary128.Binary128;
import org.bidfp.binary128.Dpml;

/** Intel {@code bid64_sin.c} / {@code cos} / {@code tan} Payne-Hanek reduction. */
final class Bid64Trig {
  private static final long NAN = 0x7c00_0000_0000_0000L;
  private static final Binary128 PI_OVER_2 =
      Binary128.fromRawBits(0x3fff_921f_b544_42d1L, 0x8469_898c_c517_01b8L);

  enum Kind { SIN, COS, TAN }

  private Bid64Trig() {
  }

  static long sin(long x, RoundingMode mode, StatusFlags flags) {
    return evaluate(x, mode, flags, Kind.SIN);
  }

  static long cos(long x, RoundingMode mode, StatusFlags flags) {
    return evaluate(x, mode, flags, Kind.COS);
  }

  static long tan(long x, RoundingMode mode, StatusFlags flags) {
    return evaluate(x, mode, flags, Kind.TAN);
  }

  private static long evaluate(
      long x, RoundingMode mode, StatusFlags flags, Kind kind) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isNaN()) {
      return Bid64Log.canonNan(x, flags);
    }
    if (value.isInfinite()) {
      flags.raise(StatusFlags.INVALID);
      return NAN;
    }
    int sign = (int) (x >>> 63);
    int exponent;
    long coefficient;
    if ((x & (3L << 61)) == (3L << 61)) {
      exponent = (int) ((x >>> 51) & 0x3ff) - 398;
      coefficient = (1L << 53) + (x & ((1L << 51) - 1));
      if (Long.compareUnsigned(coefficient, 9_999_999_999_999_999L) > 0) {
        coefficient = 0L;
      }
    } else {
      exponent = (int) ((x >>> 53) & 0x3ff) - 398;
      coefficient = x & ((1L << 53) - 1);
    }
    if (coefficient == 0L) {
      exponent = -18;
    }
    if (exponent < -17) {
      return BidTranscendental.unary64(x, mode, flags, kernel(kind));
    }
    long[] words = Bid64TrigModuli.WORDS[exponent + 17];
    BigInteger modulus = unsigned(words[2])
        .shiftLeft(128)
        .or(unsigned(words[1]).shiftLeft(64))
        .or(unsigned(words[0]));
    BigInteger product = unsigned(coefficient).multiply(modulus);
    long p0 = word(product, 0);
    long p1 = word(product, 1);
    long p2 = word(product, 2);
    int k = (int) (p2 >>> 62);
    long[] shifted = shift192Left(p2, p1, p0, 2);
    p2 = shifted[0];
    p1 = shifted[1];
    p0 = shifted[2];
    int fractionSign;
    if ((p2 & Long.MIN_VALUE) != 0L) {
      k = (k + 1) & 3;
      p2 = ~p2;
      p1 = ~p1;
      p0 = ~p0;
      fractionSign = 1 - sign;
    } else {
      fractionSign = sign;
    }
    if (sign != 0) {
      k = (-k) & 3;
    }
    int binaryExp;
    if (p2 == 0L) {
      binaryExp = 16382 - 64;
      p2 = p1;
      p1 = p0;
    } else {
      binaryExp = 16382;
    }
    int leading = Long.numberOfLeadingZeros(p2);
    binaryExp -= leading;
    if (leading != 0) {
      long[] n = shift128Left(p2, p1, leading);
      p2 = n[0];
      p1 = n[1];
    }
    long high = ((long) fractionSign << 63)
        | (((long) binaryExp & 0x7fffL) << 48)
        | ((p2 >>> 15) & 0x0000_ffff_ffff_ffffL);
    long low = (p2 << 49) | (p1 >>> 15);
    Binary128 xd = Binary128.fromRawBits(high, low);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    xd = Dpml.mul(PI_OVER_2, xd, binaryMode, local);
    Binary128 yd = apply(kind, k, xd, binaryMode, local);
    flags.raise(local.bits());
    return BidConvert.fromBinary128To64(yd.highBits(), yd.lowBits(), mode, flags);
  }

  private static BidTranscendental.Unary kernel(Kind kind) {
    if (kind == Kind.COS) {
      return Dpml::cos;
    }
    if (kind == Kind.TAN) {
      return Dpml::tan;
    }
    return Dpml::sin;
  }

  private static Binary128 apply(
      Kind kind, int k, Binary128 xd,
      org.bidfp.binary128.RoundingMode mode,
      org.bidfp.binary128.StatusFlags status) {
    if (kind == Kind.COS) {
      if (k == 0) {
        return Dpml.cos(xd, mode, status);
      }
      if (k == 1) {
        return Dpml.sin(xd, mode, status).negate();
      }
      if (k == 2) {
        return Dpml.cos(xd, mode, status).negate();
      }
      return Dpml.sin(xd, mode, status);
    }
    if (kind == Kind.TAN) {
      Binary128 yd = Dpml.tan(xd, mode, status);
      if (k == 1 || k == 3) {
        Binary128 negOne = Binary128.fromRawBits(0xbfff_0000_0000_0000L, 0L);
        return Dpml.div(negOne, yd, mode, status);
      }
      return yd;
    }
    if (k == 0) {
      return Dpml.sin(xd, mode, status);
    }
    if (k == 1) {
      return Dpml.cos(xd, mode, status);
    }
    if (k == 2) {
      return Dpml.sin(xd, mode, status).negate();
    }
    return Dpml.cos(xd, mode, status).negate();
  }

  private static BigInteger unsigned(long value) {
    if (value >= 0L) {
      return BigInteger.valueOf(value);
    }
    return BigInteger.valueOf(value).add(BigInteger.ONE.shiftLeft(64));
  }

  private static long word(BigInteger value, int index) {
    return value.shiftRight(index * 64).longValue();
  }

  private static long[] shift192Left(long hi, long med, long lo, int bits) {
    long newHi = (hi << bits) | (med >>> (64 - bits));
    long newMed = (med << bits) | (lo >>> (64 - bits));
    long newLo = lo << bits;
    return new long[] {newHi, newMed, newLo};
  }

  private static long[] shift128Left(long hi, long lo, int bits) {
    long newHi = (hi << bits) | (lo >>> (64 - bits));
    long newLo = lo << bits;
    return new long[] {newHi, newLo};
  }
}
