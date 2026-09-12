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
package org.bidfp.binary128;

import org.bidfp.binary128.tables.ErfX;

/** Error function family (DPML {@code dpml_ux_erf.c}). */
public final class DpmlErf {
  private static final long EIGHT_POINT_SEVEN_FIVE_MSD = 0x8c00_0000_0000_0000L;

  private DpmlErf() {
  }

  public static Binary128 erf(Binary128 x, RoundingMode mode, StatusFlags st) {
    return evaluate(x, false, mode, st);
  }

  public static Binary128 erfc(Binary128 x, RoundingMode mode, StatusFlags st) {
    return evaluate(x, true, mode, st);
  }

  private static Binary128 evaluate(
      Binary128 x, boolean complement, RoundingMode mode, StatusFlags status) {
    Unpacked argument = UxOps.unpack(x);
    if (argument.isNaN()) {
      if (argument.signaling) {
        status.raise(StatusFlags.INVALID);
      }
      return Binary128.fromRawBits(
          x.highBits() | Binary128.QUIET_NAN_BIT, x.lowBits());
    }
    if (argument.isZero()) {
      return complement ? Binary128.ONE : x;
    }
    if (argument.isInfinite()) {
      if (!complement) {
        return argument.sign != 0 ? Binary128.ONE.negate() : Binary128.ONE;
      }
      return argument.sign != 0
          ? Binary128.fromRawBits(0x4000_0000_0000_0000L, 0L)
          : Binary128.ZERO;
    }
    if (complement && x.isSubnormal()) {
      status.raise(StatusFlags.DENORMAL);
      return Binary128.ONE;
    }

    int sign = argument.sign;
    argument.sign = 0;
    UxOps.normalize(argument);
    int interval = interval(argument);
    StatusFlags local = new StatusFlags();
    Unpacked primary;

    if (interval == 0) {
      primary = small(argument, local);
    } else if (interval == 1) {
      primary = middle(argument, local);
    } else if (interval == 2 && complement) {
      primary = tail(argument, local);
    } else if (interval == 3 && complement && sign == 0) {
      Unpacked underflow = new Unpacked();
      underflow.setNorm(0, -(1 << 15), Unpacked.UX_MSB, 0L);
      return UxOps.pack(underflow, mode, status);
    } else {
      primary = KernelEval.fromInt(0);
    }

    Unpacked result = adjust(primary, interval, sign, complement, local);
    return UxOps.pack(result, mode, status);
  }

  private static int interval(Unpacked x) {
    if (x.exponent < 4) {
      return x.exponent <= 0 ? 0 : 1;
    }
    if (x.exponent > 4) {
      return x.exponent < 8 ? 2 : 3;
    }
    return Long.compareUnsigned(x.fracHi, EIGHT_POINT_SEVEN_FIVE_MSD) < 0 ? 1 : 2;
  }

  private static Unpacked small(Unpacked x, StatusFlags status) {
    Unpacked result = new Unpacked();
    long flags = UxEval.numeratorFlags(UxEval.SQUARE_TERM | UxEval.POST_MULTIPLY)
        | UxEval.denominatorFlags(UxEval.SQUARE_TERM);
    UxEval.evaluateRational(
        x.copy(),
        ErfX.TABLE,
        ErfX.ERF_COEF_ARRAY,
        ErfX.ERF_COEF_ARRAY_DEGREE,
        flags,
        result,
        status);
    return result;
  }

  private static Unpacked middle(Unpacked x, StatusFlags status) {
    Unpacked numerator = new Unpacked();
    Unpacked denominator = new Unpacked();
    UxEval.evaluatePackedPoly(
        x,
        ErfX.TABLE,
        ErfX.MID_NUM_COEF_ARRAY,
        ErfX.MID_NUM_COEF_ARRAY_DEGREE,
        ErfX.MID_NUM_SCALE_MASK,
        ErfX.MID_NUM_SCALE_BIAS,
        numerator,
        status);
    UxEval.evaluatePackedPoly(
        x,
        ErfX.TABLE,
        ErfX.MID_DEN_COEF_ARRAY,
        ErfX.MID_DEN_COEF_ARRAY_DEGREE,
        ErfX.MID_DEN_SCALE_MASK,
        ErfX.MID_DEN_SCALE_BIAS,
        denominator,
        status);
    Unpacked result = new Unpacked();
    UxOps.divUnpacked(numerator, denominator, result, status);
    return DpmlErfExp.multiplyByNegativeSquare(x, result, status);
  }

  private static Unpacked tail(Unpacked x, StatusFlags status) {
    Unpacked inverse = new Unpacked();
    UxOps.divUnpacked(KernelEval.fromInt(1), x, inverse, status);
    Unpacked result = new Unpacked();
    long flags = UxEval.numeratorFlags(UxEval.SQUARE_TERM | UxEval.POST_MULTIPLY)
        | UxEval.denominatorFlags(UxEval.SQUARE_TERM)
        | UxEval.packScale(3);
    UxEval.evaluateRational(
        inverse,
        ErfX.TABLE,
        ErfX.ERFC_COEF_ARRAY,
        ErfX.ERFC_COEF_ARRAY_DEGREE,
        flags,
        result,
        status);
    return DpmlErfExp.multiplyByNegativeSquare(x, result, status);
  }

  private static Unpacked adjust(
      Unpacked primary,
      int interval,
      int argumentSign,
      boolean complement,
      StatusFlags status) {
    int constant;
    boolean negatePrimary;
    if (!complement) {
      constant = interval == 0 ? 0 : (argumentSign == 0 ? 1 : -1);
      negatePrimary = argumentSign != 0 ? interval == 0 : interval == 1;
    } else {
      constant = argumentSign == 0
          ? (interval == 0 ? 1 : 0)
          : (interval == 0 ? 1 : 2);
      negatePrimary = argumentSign == 0 ? interval == 0 : interval != 0;
    }
    Unpacked term = primary.copy();
    if (negatePrimary) {
      UxOps.negate(term);
    }
    Unpacked result = new Unpacked();
    UxOps.addsubUnpacked(term, KernelEval.fromInt(constant), result, status);
    return result;
  }
}
