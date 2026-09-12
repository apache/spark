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

import org.bidfp.binary128.tables.CbrtX;

/**
 * Cube root ported from Intel DPML {@code dpml_ux_cbrt.c}.
 */
public final class DpmlCbrt {
  private DpmlCbrt() {
  }

  public static Binary128 cbrt(Binary128 x, RoundingMode mode, StatusFlags st) {
    Unpacked a = UxOps.unpack(x);
    if (a.isNaN()) {
      return DpmlPowCbrtSupport.quietNaN(x, st);
    }
    if (a.isInfinite() || a.isZero()) {
      return x;
    }
    UxOps.normalize(a);
    int sign = a.sign;
    a.sign = 0;

    // x = f * 2^n, 1 <= f < 2, and n = 3*q + rem. Java's / truncates,
    // so floorDiv/floorMod are required for subnormal and small inputs.
    int n = a.exponent - 1;
    int q = DpmlPowCbrtSupport.floorDiv3(n);
    int rem = DpmlPowCbrtSupport.floorMod3(n);
    double f = DpmlPowCbrtSupport.normalizedFractionAsDouble(a);
    double z = reciprocalCbrtSquaredSeed(f);
    double z2 = z * z;
    double z4 = z2 * z2;
    double f2 = f * f;
    double y0 = tableDouble(CbrtX.POW_CBRT_2_TABLE + 8 * rem)
        * (((tableDouble(CbrtX.FOURTEEN_NINTHS) * f) * z)
        - z4 * ((tableDouble(CbrtX.SEVEN_NINTHS) * f) * f2)
        + (z4 * (z2 * z)) * ((tableDouble(CbrtX.TWO_NINTHS) * f) * (f2 * f2)));

    Unpacked y = UxOps.unpack(Binary128.fromBinary64(y0));
    y.exponent += q;

    // Intel's full UX correction:
    // y <- y/2 * (y^3 + 2*x) / (y^3 + x/2).
    StatusFlags local = new StatusFlags();
    Unpacked y2 = new Unpacked();
    Unpacked y3 = new Unpacked();
    Unpacked twiceX = a.copy();
    Unpacked halfX = a.copy();
    Unpacked numerator = new Unpacked();
    Unpacked denominator = new Unpacked();
    Unpacked correction = new Unpacked();
    UxOps.mulUnpacked(y, y, y2, local);
    UxOps.mulUnpacked(y, y2, y3, local);
    twiceX.exponent++;
    halfX.exponent--;
    UxOps.addsubUnpacked(y3, twiceX, numerator, local);
    UxOps.addsubUnpacked(y3, halfX, denominator, local);
    UxOps.divUnpacked(numerator, denominator, correction, local);
    UxOps.mulUnpacked(y, correction, y2, local);
    y2.exponent--;
    y.copyFrom(y2);
    y.sign = sign;
    return UxOps.pack(y, mode, st);
  }

  private static double reciprocalCbrtSquaredSeed(double f) {
    double c0 = tableDouble(CbrtX.COEFS);
    double c1 = tableDouble(CbrtX.COEFS + 8);
    double c2 = tableDouble(CbrtX.COEFS + 16);
    double c3 = tableDouble(CbrtX.COEFS + 24);
    double c4 = tableDouble(CbrtX.COEFS + 32);
    double c5 = tableDouble(CbrtX.COEFS + 40);
    return c0 + f * (c1 + f * (c2 + f * (c3 + f * (c4 + f * c5))));
  }

  private static double tableDouble(int offset) {
    return Double.longBitsToDouble(CbrtX.TABLE.get(offset >>> 3));
  }
}
