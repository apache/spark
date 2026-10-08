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
 * Bounded facade for the packed DPML operations used by this libbid port.
 *
 * <p>This is the stable entry surface for the emulated {@code bid_f128_*}
 * engine used by BID64/BID128 transcendentals. It is not a complete IEEE
 * binary128 language binding or a replacement for a general-purpose
 * {@code _Float128} math library. Supported operations are exactly those
 * declared here; the wider Intel DPML source tree is not part of this API.
 * Kernels live in package-private {@code Dpml*} classes.
 */
public final class Dpml {
  private Dpml() {
  }

  public static Binary128 add(Binary128 x, Binary128 y, RoundingMode r, StatusFlags s) {
    return UxOps.add(x, y, r, s);
  }

  public static Binary128 sub(Binary128 x, Binary128 y, RoundingMode r, StatusFlags s) {
    return UxOps.sub(x, y, r, s);
  }

  public static Binary128 mul(Binary128 x, Binary128 y, RoundingMode r, StatusFlags s) {
    return UxOps.mul(x, y, r, s);
  }

  public static Binary128 div(Binary128 x, Binary128 y, RoundingMode r, StatusFlags s) {
    return UxOps.div(x, y, r, s);
  }

  public static Binary128 sqrt(Binary128 x, RoundingMode r, StatusFlags s) {
    return UxOps.sqrt(x, r, s);
  }

  public static Binary128 exp(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlExp.exp(x, r, s);
  }

  /**
   * Integration helper for libbid: evaluates {@code exp(xHigh + xLow)}
   * without rounding the sum before range reduction.
   *
   * <p>This method exists to preserve BID128 gamma accuracy and is not a
   * general expansion-arithmetic API.
   */
  public static Binary128 expTwoPart(
      Binary128 xHigh, Binary128 xLow, RoundingMode r, StatusFlags s) {
    return DpmlExp.expTwoPart(xHigh, xLow, r, s);
  }

  public static Binary128 expm1(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlExp.expm1(x, r, s);
  }

  public static Binary128 exp2(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlExp.exp2(x, r, s);
  }

  public static Binary128 exp10(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlExp.exp10(x, r, s);
  }

  public static Binary128 log(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlLog.log(x, r, s);
  }

  public static Binary128 log2(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlLog.log2(x, r, s);
  }

  public static Binary128 log10(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlLog.log10(x, r, s);
  }

  public static Binary128 log1p(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlLog.log1p(x, r, s);
  }

  public static Binary128 pow(Binary128 x, Binary128 y, RoundingMode r, StatusFlags s) {
    return DpmlPow.pow(x, y, r, s);
  }

  public static Binary128 cbrt(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlCbrt.cbrt(x, r, s);
  }

  public static Binary128 sin(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlTrig.sin(x, r, s);
  }

  public static Binary128 cos(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlTrig.cos(x, r, s);
  }

  public static Binary128 tan(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlTrig.tan(x, r, s);
  }

  public static Binary128 asin(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlInvTrig.asin(x, r, s);
  }

  public static Binary128 acos(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlInvTrig.acos(x, r, s);
  }

  public static Binary128 atan(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlInvTrig.atan(x, r, s);
  }

  public static Binary128 atan2(
      Binary128 y, Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlInvTrig.atan2(y, x, r, s);
  }

  public static Binary128 sinh(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlHyper.sinh(x, r, s);
  }

  public static Binary128 cosh(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlHyper.cosh(x, r, s);
  }

  public static Binary128 tanh(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlHyper.tanh(x, r, s);
  }

  public static Binary128 asinh(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlInvHyper.asinh(x, r, s);
  }

  public static Binary128 acosh(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlInvHyper.acosh(x, r, s);
  }

  public static Binary128 atanh(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlInvHyper.atanh(x, r, s);
  }

  public static Binary128 erf(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlErf.erf(x, r, s);
  }

  public static Binary128 erfc(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlErf.erfc(x, r, s);
  }

  public static Binary128 lgamma(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlGamma.lgamma(x, r, s);
  }

  /**
   * Integration helper for libbid: returns a nonoverlapping high/low result
   * for positive {@code lgamma(xHigh + xLow)}.
   *
   * <p>This method retains DPML guard bits needed by BID128 {@code tgamma}
   * and is not a general expansion-arithmetic API.
   */
  public static Binary128[] positiveLgammaTwoPart(
      Binary128 xHigh, Binary128 xLow, StatusFlags s) {
    return DpmlGamma.positiveLgammaTwoPart(xHigh, xLow, s);
  }

  public static Binary128 tgamma(Binary128 x, RoundingMode r, StatusFlags s) {
    return DpmlGamma.tgamma(x, r, s);
  }
}
