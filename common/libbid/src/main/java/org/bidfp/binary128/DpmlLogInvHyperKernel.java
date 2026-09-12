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

import org.bidfp.binary128.tables.LogX;

/** Shared UX primitives used only by the log and inverse-hyperbolic ports. */
final class DpmlLogInvHyperKernel {
  private DpmlLogInvHyperKernel() {
  }

  static Unpacked add(Unpacked a, Unpacked b, StatusFlags status) {
    Unpacked result = new Unpacked();
    UxOps.addsubUnpacked(a, b, result, status);
    return result;
  }

  static Unpacked subtract(Unpacked a, Unpacked b, StatusFlags status) {
    Unpacked negative = b.copy();
    UxOps.negate(negative);
    return add(a, negative, status);
  }

  static Unpacked multiply(Unpacked a, Unpacked b, StatusFlags status) {
    Unpacked result = new Unpacked();
    UxOps.mulUnpacked(a, b, result, status);
    return result;
  }

  static Unpacked divide(Unpacked numerator, Unpacked denominator, StatusFlags status) {
    Unpacked result = new Unpacked();
    UxOps.divUnpacked(numerator, denominator, result, status);
    return result;
  }

  static Unpacked sqrt(Unpacked argument, StatusFlags status) {
    Unpacked result = new Unpacked();
    UxOps.sqrtUnpacked(argument, result, status);
    return result;
  }

  /** Intel UX_LOG_POLY: log((1+z)/(1-z)). */
  static Unpacked logPoly(Unpacked z, StatusFlags status) {
    Unpacked result = new Unpacked();
    UxEval.evaluateRational(
        z,
        LogX.TABLE,
        LogX.LOG2_COEF_ARRAY,
        LogX.LOG2_COEF_ARRAY_DEGREE,
        UxEval.numeratorFlags(UxEval.SQUARE_TERM | UxEval.POST_MULTIPLY),
        result,
        status);
    return multiply(result, UxTable.readUxFloat(LogX.TABLE, LogX.LN_2), status);
  }

  /**
   * Intel UX_LOG. A null scale computes log2; otherwise scale is ln(2) or log10(2).
   */
  static Unpacked log(Unpacked argument, Unpacked scale, StatusFlags status) {
    int m = argument.exponent;
    long threshold = UxTable.word64(LogX.TABLE, LogX.ONE_OVER_SQRT_2);
    if (Long.compareUnsigned(argument.fracHi, threshold) <= 0) {
      m--;
    }

    Unpacked reduced = argument.copy();
    reduced.exponent -= m;
    Unpacked one = UxTable.readUxFloat(LogX.TABLE, LogX.UX_ONE);
    Unpacked sum = add(reduced, one, status);
    Unpacked difference = subtract(reduced, one, status);
    UxOps.abs(difference);
    if (reduced.exponent == 0) {
      difference.sign = Unpacked.UX_SIGN_BIT;
    }
    Unpacked z = divide(difference, sum, status);

    Unpacked polynomial = new Unpacked();
    UxEval.evaluateRational(
        z,
        LogX.TABLE,
        LogX.LOG2_COEF_ARRAY,
        LogX.LOG2_COEF_ARRAY_DEGREE,
        UxEval.numeratorFlags(UxEval.SQUARE_TERM | UxEval.POST_MULTIPLY),
        polynomial,
        status);
    Unpacked result = add(KernelEval.fromInt(m), polynomial, status);
    return scale == null ? result : multiply(result, scale, status);
  }

  static Binary128 quietNaN(Binary128 x, StatusFlags status) {
    if (x.isSignalingNaN()) {
      status.raise(StatusFlags.INVALID);
    }
    return Binary128.fromRawBits(
        x.highBits() | Binary128.QUIET_NAN_BIT, x.lowBits());
  }

  static Binary128 invalidNaN(StatusFlags status) {
    status.raise(StatusFlags.INVALID);
    return Binary128.canonicalNaN(true);
  }

  static void noteDenormal(Binary128 x, StatusFlags status) {
    if (x.isSubnormal()) {
      status.raise(StatusFlags.DENORMAL);
    }
  }
}
