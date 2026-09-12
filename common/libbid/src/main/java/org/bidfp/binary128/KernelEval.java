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

import org.bidfp.binary128.tables.TableData;

/**
 * Shared Horner / table-eval helpers for DPML kernel families.
 *
 * <p>Rational and packed-poly evaluation are thin facades over
 * {@link UxEval}; UX table decoding is via {@link UxTable}.
 */
final class KernelEval {
  private KernelEval() {
  }

  // ---- Unpacked arithmetic facades ---------------------------------------

  static Unpacked fromInt(long n) {
    Unpacked u = new Unpacked();
    if (n == 0L) {
      u.setZero(0);
      return u;
    }
    int sign = n < 0L ? Unpacked.UX_SIGN_BIT : 0;
    long mag = n < 0L ? -n : n;
    u.setNorm(sign, 64, mag, 0L);
    UxOps.normalize(u);
    return u;
  }

  static Unpacked unpack(Binary128 x) {
    return UxOps.unpack(x);
  }

  static Binary128 pack(Unpacked u, RoundingMode mode, StatusFlags status) {
    return UxOps.pack(u, mode, status);
  }

  static void add(Unpacked a, Unpacked b, Unpacked r, StatusFlags st) {
    UxOps.addsubUnpacked(a, b, r, st);
  }

  static void sub(Unpacked a, Unpacked b, Unpacked r, StatusFlags st) {
    Unpacked nb = b.copy();
    UxOps.negate(nb);
    UxOps.addsubUnpacked(a, nb, r, st);
  }

  static void mul(Unpacked a, Unpacked b, Unpacked r, StatusFlags st) {
    UxOps.mulUnpacked(a, b, r, st);
  }

  static void div(Unpacked a, Unpacked b, Unpacked r, StatusFlags st) {
    UxOps.divUnpacked(a, b, r, st);
  }

  // ---- UX table decode facades -------------------------------------------

  static Unpacked readUxFloat(TableData table, int byteOffset) {
    return UxTable.readUxFloat(table, byteOffset);
  }

  static void readUxFloat(TableData table, int byteOffset, Unpacked dest) {
    UxTable.readUxFloat(table, byteOffset, dest);
  }

  static void fixed128ToUnpacked(
      TableData table, int byteOffset, Unpacked dest) {
    UxTable.fixed128ToUnpacked(table, byteOffset, dest);
  }

  static long word64(TableData table, int byteOffset) {
    return UxTable.word64(table, byteOffset);
  }

  static double readDouble(TableData table, int byteOffset) {
    return UxTable.readDouble(table, byteOffset);
  }

  static int coefBankBytes(int degree) {
    return UxTable.coefBankBytes(degree);
  }

  static int readCoefScale(TableData table, int coefsOffset, int degree) {
    return UxTable.readCoefScale(table, coefsOffset, degree);
  }

  // ---- Rational / packed-poly facades ------------------------------------

  static void evaluateRational(
      Unpacked argument,
      TableData table,
      int coefsOffset,
      int degree,
      long flags,
      Unpacked result,
      StatusFlags status) {
    UxEval.evaluateRational(
        argument, table, coefsOffset, degree, flags, result, status);
  }

  static void evaluateRational(
      Unpacked argument,
      TableData table,
      int coefsOffset,
      int degree,
      long flags,
      Unpacked[] results,
      StatusFlags status) {
    UxEval.evaluateRational(
        argument, table, coefsOffset, degree, flags, results, status);
  }

  static long packScale(int n) {
    return UxEval.packScale(n);
  }

  static int getScale(long flags) {
    return UxEval.getScale(flags);
  }

  static int numeratorFlags(int n) {
    return UxEval.numeratorFlags(n);
  }

  static int denominatorFlags(int n) {
    return UxEval.denominatorFlags(n);
  }

  static void evaluatePackedPoly(
      Unpacked argument,
      TableData table,
      int coefsOffset,
      int degree,
      long mask,
      int bias,
      Unpacked result,
      StatusFlags status) {
    UxEval.evaluatePackedPoly(
        argument, table, coefsOffset, degree, mask, bias, result, status);
  }

  // ---- Series helpers (debug / fallback) ---------------------------------

  /** exp(r) for |r| modest, Horner of Taylor series. */
  static void expSeries(Unpacked r, Unpacked out, StatusFlags st) {
    Unpacked term = fromInt(1);
    Unpacked acc = fromInt(1);
    Unpacked tmp = new Unpacked();
    Unpacked n = new Unpacked();
    for (int k = 1; k <= 28; k++) {
      mul(term, r, tmp, st);
      n.copyFrom(fromInt(k));
      div(tmp, n, term, st);
      add(acc, term, tmp, st);
      acc.copyFrom(tmp);
    }
    out.copyFrom(acc);
  }

  /** log(1+f) for |f| < 0.5, atanh-style series. */
  static void log1pSeries(Unpacked f, Unpacked out, StatusFlags st) {
    Unpacked two = fromInt(2);
    Unpacked den = new Unpacked();
    Unpacked u = new Unpacked();
    Unpacked tmp = new Unpacked();
    add(two, f, den, st);
    div(f, den, u, st);
    Unpacked u2 = new Unpacked();
    mul(u, u, u2, st);
    Unpacked acc = u.copy();
    Unpacked p = u.copy();
    for (int k = 1; k <= 24; k++) {
      mul(p, u2, tmp, st);
      p.copyFrom(tmp);
      div(p, fromInt(2 * k + 1), tmp, st);
      add(acc, tmp, den, st);
      acc.copyFrom(den);
    }
    mul(acc, two, out, st);
  }

  static int roundToInt(Unpacked x) {
    if (x.klass != Unpacked.CLASS_NORM) {
      return 0;
    }
    Unpacked u = x.copy();
    UxOps.normalize(u);
    int shift = 128 - u.exponent;
    if (shift <= 0) {
      return u.sign != 0 ? Integer.MIN_VALUE / 2 : Integer.MAX_VALUE / 2;
    }
    long[] t = new long[2];
    Wide.shiftRight128Sticky(u.fracHi, u.fracLo, shift, t);
    int n = (int) t[1];
    if (u.sign != 0) {
      n = -n;
    }
    return n;
  }
}
