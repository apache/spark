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

/**
 * exp / expm1 / exp2 / exp10 kernels (DPML {@code dpml_ux_exp.c} family).
 * Range reduction and minimax evaluation follow the Intel QUAD UX path.
 */
public final class DpmlExp {
  private DpmlExp() {
  }

  public static Binary128 exp(Binary128 x, RoundingMode mode, StatusFlags st) {
    return expCommon(x, false, mode, st);
  }

  /**
   * Computes {@code exp(xHigh + xLow)} without first rounding the sum to binary128.
   */
  static Binary128 expTwoPart(
      Binary128 xHigh, Binary128 xLow, RoundingMode mode, StatusFlags st) {
    if (!xHigh.isFinite() || xLow.isZero()) {
      return exp(xHigh, mode, st);
    }
    StatusFlags local = new StatusFlags();
    Unpacked argument = new Unpacked();
    UxOps.addsubUnpacked(
        UxOps.unpack(xHigh), UxOps.unpack(xLow), argument, local);
    Unpacked result = DpmlExpHyperKernel.exp(argument, local);
    st.raise(local);
    return UxOps.pack(result, mode, st);
  }

  public static Binary128 exp2(Binary128 x, RoundingMode mode, StatusFlags st) {
    if (x.isNaN()) {
      return DpmlExpHyperKernel.quietNaN(x, st);
    }
    if (x.isInfinite()) {
      return x.isSigned() ? Binary128.ZERO : Binary128.POSITIVE_INFINITY;
    }
    if (x.isZero()) {
      return Binary128.ONE;
    }
    StatusFlags local = new StatusFlags();
    Unpacked result = DpmlExpHyperKernel.exp2(UxOps.unpack(x), local);
    return UxOps.pack(result, mode, st);
  }

  public static Binary128 exp10(Binary128 x, RoundingMode mode, StatusFlags st) {
    return expCommon(x, true, mode, st);
  }

  public static Binary128 expm1(Binary128 x, RoundingMode mode, StatusFlags st) {
    if (x.isNaN()) {
      return DpmlExpHyperKernel.quietNaN(x, st);
    }
    if (x.isInfinite()) {
      return x.isSigned() ? Binary128.fromRawBits(0xbfff_0000_0000_0000L, 0) : x;
    }
    if (x.isZero()) {
      return x;
    }

    StatusFlags local = new StatusFlags();
    DpmlExpHyperKernel.Reduction reduction =
        DpmlExpHyperKernel.reduce(UxOps.unpack(x), false, local);
    Unpacked result;
    if (reduction.scale == 0) {
      result = DpmlExpHyperKernel.expm1Polynomial(reduction.argument, local);
    } else {
      result = DpmlExpHyperKernel.expPolynomial(reduction.argument, false, local);
      result.exponent += (int) reduction.scale;
      Unpacked one = UxOps.unpack(Binary128.ONE);
      result = DpmlExpHyperKernel.sub(result, one, local);
    }
    return UxOps.pack(result, mode, st);
  }

  private static Binary128 expCommon(
      Binary128 x, boolean base10, RoundingMode mode, StatusFlags st) {
    if (x.isNaN()) {
      return DpmlExpHyperKernel.quietNaN(x, st);
    }
    if (x.isInfinite()) {
      return x.isSigned() ? Binary128.ZERO : Binary128.POSITIVE_INFINITY;
    }
    if (x.isZero()) {
      return Binary128.ONE;
    }

    StatusFlags local = new StatusFlags();
    DpmlExpHyperKernel.Reduction reduction =
        DpmlExpHyperKernel.reduce(UxOps.unpack(x), base10, local);
    Unpacked result =
        DpmlExpHyperKernel.expPolynomial(reduction.argument, base10, local);
    result.exponent += (int) reduction.scale;
    return UxOps.pack(result, mode, st);
  }
}
