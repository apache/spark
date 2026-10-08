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

import org.bidfp.binary128.tables.ExpX;
import org.bidfp.binary128.tables.IeeeConstants;

/** Family-local unpacked exponential support used by the Intel erf kernel. */
final class DpmlErfExp {
  private static final int EXP_COEFFICIENTS = 120;
  private static final int EXP_DEGREE = 22;
  private static final int LN2_LOW = 88;
  private static final long LN2_HIGH = 0xb172_17f7_d1cf_79acL;

  private DpmlErfExp() {
  }

  static Unpacked multiplyByNegativeSquare(
      Unpacked x, Unpacked factor, StatusFlags status) {
    Unpacked high = new Unpacked();
    Unpacked low = new Unpacked();
    extendedSquare(x, high, low);
    UxOps.negate(high);

    Unpacked exponential = exp(high, status);
    if (!low.isZero()) {
      Unpacked correction = new Unpacked();
      UxOps.mulUnpacked(low, exponential, correction, status);
      Unpacked corrected = new Unpacked();
      KernelEval.sub(exponential, correction, corrected, status);
      exponential.copyFrom(corrected);
    }

    Unpacked result = new Unpacked();
    UxOps.mulUnpacked(exponential, factor, result, status);
    return result;
  }

  private static void extendedSquare(Unpacked x, Unpacked high, Unpacked low) {
    Unpacked a = x.copy();
    UxOps.normalize(a);
    long[] product = new long[4];
    Wide.mul128x128(a.fracHi, a.fracLo, a.fracHi, a.fracLo, product);
    int exponent = a.exponent + a.exponent;
    high.setNorm(0, exponent, product[0], product[1]);
    UxOps.normalize(high);
    if ((product[2] | product[3]) == 0L) {
      low.setZero(0);
    } else {
      low.setNorm(0, exponent - 128, product[2], product[3]);
      UxOps.normalize(low);
    }
  }

  private static Unpacked exp(Unpacked x, StatusFlags status) {
    Unpacked product = new Unpacked();
    UxOps.mulUnpacked(x, UxOps.unpack(IeeeConstants.LOG2E), product, status);
    int scale = DpmlPowCbrtSupport.nearestInt(product);

    Unpacked integer = KernelEval.fromInt(scale);
    Unpacked ln2High = new Unpacked();
    ln2High.setNorm(0, 0, LN2_HIGH, 0L);
    Unpacked highProduct = new Unpacked();
    UxOps.mulUnpacked(integer, ln2High, highProduct, status);

    Unpacked ln2Low = UxTable.readUxFloat(ExpX.TABLE, LN2_LOW);
    Unpacked lowProduct = new Unpacked();
    UxOps.mulUnpacked(integer, ln2Low, lowProduct, status);

    Unpacked reduced = new Unpacked();
    KernelEval.sub(x, highProduct, reduced, status);
    Unpacked tmp = new Unpacked();
    KernelEval.sub(reduced, lowProduct, tmp, status);
    reduced.copyFrom(tmp);

    Unpacked result = new Unpacked();
    int polynomialFlags = UxEval.STANDARD;
    if (reduced.sign != 0) {
      reduced.sign = 0;
      polynomialFlags |= UxEval.ALTERNATE_SIGN;
    }
    UxEval.evaluateRational(
        reduced,
        ExpX.TABLE,
        EXP_COEFFICIENTS,
        EXP_DEGREE,
        UxEval.numeratorFlags(polynomialFlags),
        result,
        status);
    result.exponent += scale;
    return result;
  }
}
