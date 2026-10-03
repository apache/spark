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

/**
 * Faithful QUAD UX log / log2 / log10 / log1p port from Intel
 * {@code dpml_ux_log.c}.
 */
public final class DpmlLog {
  private DpmlLog() {
  }

  public static Binary128 log(Binary128 x, RoundingMode mode, StatusFlags st) {
    return ordinaryLog(x, mode, st, LogX.LN_2);
  }

  public static Binary128 log2(Binary128 x, RoundingMode mode, StatusFlags st) {
    return ordinaryLog(x, mode, st, -1);
  }

  public static Binary128 log10(Binary128 x, RoundingMode mode, StatusFlags st) {
    return ordinaryLog(x, mode, st, LogX.LOG10_2);
  }

  public static Binary128 log1p(Binary128 x, RoundingMode mode, StatusFlags st) {
    DpmlLogInvHyperKernel.noteDenormal(x, st);
    if (x.isNaN()) {
      return DpmlLogInvHyperKernel.quietNaN(x, st);
    }
    if (x.isInfinite()) {
      return x.isSigned()
          ? DpmlLogInvHyperKernel.invalidNaN(st)
          : Binary128.POSITIVE_INFINITY;
    }
    if (x.isZero() || x.isSubnormal()) {
      return x;
    }

    Unpacked argument = UxOps.unpack(x);
    UxOps.normalize(argument);
    if (argument.sign != 0 && argument.exponent >= 1) {
      if (argument.exponent == 1
          && argument.fracHi == Unpacked.UX_MSB
          && argument.fracLo == 0L) {
        st.raise(StatusFlags.DIVIDE_BY_ZERO);
        return Binary128.NEGATIVE_INFINITY;
      }
      return DpmlLogInvHyperKernel.invalidNaN(st);
    }

    StatusFlags local = new StatusFlags();
    Unpacked result;
    if (isDirectLog1pRange(argument)) {
      Unpacked two = UxTable.readUxFloat(LogX.TABLE, LogX.UX_TWO);
      Unpacked denominator = DpmlLogInvHyperKernel.add(two, argument, local);
      Unpacked reduced = DpmlLogInvHyperKernel.divide(argument, denominator, local);
      result = DpmlLogInvHyperKernel.logPoly(reduced, local);
    } else {
      Unpacked one = UxTable.readUxFloat(LogX.TABLE, LogX.UX_ONE);
      Unpacked sum = DpmlLogInvHyperKernel.add(one, argument, local);
      result = DpmlLogInvHyperKernel.log(
          sum, UxTable.readUxFloat(LogX.TABLE, LogX.LN_2), local);
    }
    return UxOps.pack(result, mode, st);
  }

  private static Binary128 ordinaryLog(
      Binary128 x, RoundingMode mode, StatusFlags status, int scaleOffset) {
    DpmlLogInvHyperKernel.noteDenormal(x, status);
    if (x.isNaN()) {
      return DpmlLogInvHyperKernel.quietNaN(x, status);
    }
    if (x.isZero() && x.isSigned()) {
      return DpmlLogInvHyperKernel.invalidNaN(status);
    }
    if (x.isZero()) {
      status.raise(StatusFlags.DIVIDE_BY_ZERO);
      return Binary128.NEGATIVE_INFINITY;
    }
    if (x.isSigned()) {
      return DpmlLogInvHyperKernel.invalidNaN(status);
    }
    if (x.isInfinite()) {
      return Binary128.POSITIVE_INFINITY;
    }

    StatusFlags local = new StatusFlags();
    Unpacked argument = UxOps.unpack(x);
    UxOps.normalize(argument);
    Unpacked scale = scaleOffset < 0
        ? null : UxTable.readUxFloat(LogX.TABLE, scaleOffset);
    Unpacked result = DpmlLogInvHyperKernel.log(argument, scale, local);
    return UxOps.pack(result, mode, status);
  }

  private static boolean isDirectLog1pRange(Unpacked argument) {
    if (argument.exponent <= -2) {
      return true;
    }
    if (argument.exponent != -1) {
      return false;
    }
    long high = argument.fracHi >>> 2;
    long approximate = argument.sign == 0
        ? Unpacked.UX_MSB + high : Unpacked.UX_MSB - high;
    long low = UxTable.word64(LogX.TABLE, LogX.I_RECIP_SQRT_2);
    long highLimit = UxTable.word64(LogX.TABLE, LogX.I_SQRT_2);
    return Long.compareUnsigned(approximate, low) >= 0
        && Long.compareUnsigned(approximate, highLimit) < 0;
  }
}
