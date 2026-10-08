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

/** Intel {@code bid128_sin.c} Payne-Hanek reduction for sin/cos/tan. */
final class Bid128Trig {
  private static final Bid128 NAN =
      Bid128.fromRawBits(0x7c00_0000_0000_0000L, 0L);
  private static final Binary128 PI_OVER_2 =
      Binary128.fromRawBits(0x3fff_921f_b544_42d1L, 0x8469_898c_c517_01b8L);

  enum Kind { SIN, COS, TAN }

  private Bid128Trig() {
  }

  static void sin(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    evaluate(hi, lo, mode, flags, Kind.SIN, out);
  }

  static void cos(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    evaluate(hi, lo, mode, flags, Kind.COS, out);
  }

  static void tan(
      long hi, long lo, RoundingMode mode, StatusFlags flags, long[] out) {
    evaluate(hi, lo, mode, flags, Kind.TAN, out);
  }

  private static void evaluate(
      long hi, long lo, RoundingMode mode, StatusFlags flags, Kind kind,
      long[] out) {
    if (Bid128Libm.canonNan(hi, lo, flags, out)) {
      return;
    }
    Bid128 value = Bid128.fromRawBits(hi, lo);
    if (value.isInfinite()) {
      flags.raise(StatusFlags.INVALID);
      DecNum.store128(NAN, out);
      return;
    }
    int sign = (int) (hi >>> 63);
    int exponent;
    long cHi;
    long cLo;
    if ((hi & (3L << 61)) == (3L << 61)) {
      exponent = 0;
      cHi = 0L;
      cLo = 0L;
    } else {
      exponent = (int) ((hi >>> 49) & 0x3fff) - 6176;
      cHi = hi & ((1L << 49) - 1);
      cLo = lo;
      if (unsignedGreater(cHi, cLo, 542101086242752L, 4003012203950112767L)) {
        cHi = 0L;
        cLo = 0L;
      }
    }
    if (cHi == 0L && cLo == 0L) {
      exponent = -99999;
    }
    if (exponent < -35) {
      if (kind == Kind.COS) {
        if (exponent < -52) {
          Bid128Raw.sub(
              Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
              Bid128Libm.TEN_PM40_POS, 1L, mode, flags, out);
        } else {
          BidTranscendental.unary128(hi, lo, mode, flags, Dpml::cos, out);
        }
        return;
      }
      if (exponent == -99999) {
        Bid128Raw.mul(
            hi, lo, Bid128Libm.ONE.highBits(), Bid128Libm.ONE.lowBits(),
            mode, flags, out);
      } else if (exponent < -52) {
        long scale = kind == Kind.TAN
            ? Bid128Libm.TEN_PM40_POS
            : Bid128Libm.TEN_PM40_NEG;
        Bid128Libm.tinyOddFma(hi, lo, scale, mode, flags, out);
      } else {
        BidTranscendental.unary128(hi, lo, mode, flags, kernel(kind), out);
      }
      return;
    }
    long[] words = Bid128TrigModuli.WORDS[exponent + 35];
    BigInteger modulus = unsigned(words[5])
        .shiftLeft(320)
        .or(unsigned(words[4]).shiftLeft(256))
        .or(unsigned(words[3]).shiftLeft(192))
        .or(unsigned(words[2]).shiftLeft(128))
        .or(unsigned(words[1]).shiftLeft(64))
        .or(unsigned(words[0]));
    BigInteger coeff = unsigned(cHi).shiftLeft(64).or(unsigned(cLo));
    BigInteger product = coeff.multiply(modulus);
    long[] p = new long[8];
    for (int i = 0; i < 8; i++) {
      p[i] = product.shiftRight(i * 64).longValue();
    }
    int k = (int) (p[5] >>> 62);
    shift256Left(p, 2);
    int fractionSign;
    if ((p[5] & Long.MIN_VALUE) != 0L) {
      k = (k + 1) & 3;
      p[5] = ~p[5];
      p[4] = ~p[4];
      p[3] = ~p[3];
      p[2] = ~p[2];
      fractionSign = 1 - sign;
    } else {
      fractionSign = sign;
    }
    if (sign != 0) {
      k = (-k) & 3;
    }
    int binaryExp;
    if (p[5] == 0L) {
      binaryExp = 16382 - 64;
      p[5] = p[4];
      p[4] = p[3];
      p[3] = p[2];
    } else {
      binaryExp = 16382;
    }
    int leading = Long.numberOfLeadingZeros(p[5]);
    binaryExp -= leading;
    if (leading != 0) {
      shift192Left(p, leading);
    }
    long high = ((long) fractionSign << 63)
        | (((long) binaryExp & 0x7fffL) << 48)
        | (p[5] >>> 15 & 0x0000_ffff_ffff_ffffL);
    long low = (p[5] << 49) | (p[4] >>> 15);
    Binary128 xd = Binary128.fromRawBits(high, low);
    org.bidfp.binary128.RoundingMode binaryMode = BidTranscendental.binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    xd = Dpml.mul(PI_OVER_2, xd, binaryMode, local);
    Binary128 yd = apply(kind, k, xd, binaryMode, local);
    flags.raise(local.bits());
    BidConvert.fromBinary128To128(yd.highBits(), yd.lowBits(), mode, flags, out);
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

  private static BidTranscendental.Unary kernel(Kind kind) {
    if (kind == Kind.COS) {
      return Dpml::cos;
    }
    if (kind == Kind.TAN) {
      return Dpml::tan;
    }
    return Dpml::sin;
  }

  private static boolean unsignedGreater(long ah, long al, long bh, long bl) {
    if (Long.compareUnsigned(ah, bh) != 0) {
      return Long.compareUnsigned(ah, bh) > 0;
    }
    return Long.compareUnsigned(al, bl) > 0;
  }

  private static BigInteger unsigned(long value) {
    if (value >= 0L) {
      return BigInteger.valueOf(value);
    }
    return BigInteger.valueOf(value).add(BigInteger.ONE.shiftLeft(64));
  }

  private static void shift256Left(long[] p, int bits) {
    p[5] = (p[5] << bits) | (p[4] >>> (64 - bits));
    p[4] = (p[4] << bits) | (p[3] >>> (64 - bits));
    p[3] = (p[3] << bits) | (p[2] >>> (64 - bits));
    p[2] = p[2] << bits;
  }

  private static void shift192Left(long[] p, int bits) {
    p[5] = (p[5] << bits) | (p[4] >>> (64 - bits));
    p[4] = (p[4] << bits) | (p[3] >>> (64 - bits));
    p[3] = p[3] << bits;
  }
}
