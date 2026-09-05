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

import org.bidfp.binary128.Binary128;
import org.bidfp.binary128.Dpml;

/**
 * BID64/BID128 transcendentals: convert to binary128, DPML kernel, convert
 * back. {@code hypot} is sqrt(x^2+y^2) plus Intel NaN/Inf/0 and BID128
 * exponent rebias (Intel uses host hypot, not a DPML table).
 */
final class BidTranscendental {
  private BidTranscendental() {
  }

  @FunctionalInterface
  interface Unary {
    Binary128 apply(
        Binary128 x,
        org.bidfp.binary128.RoundingMode mode,
        org.bidfp.binary128.StatusFlags status);
  }

  @FunctionalInterface
  interface Binary {
    Binary128 apply(
        Binary128 x,
        Binary128 y,
        org.bidfp.binary128.RoundingMode mode,
        org.bidfp.binary128.StatusFlags status);
  }

  static long unary64(long x, RoundingMode mode, StatusFlags flags, Unary op) {
    long[] packed = new long[2];
    BidConvert.toBinary128From64(x, mode, flags, packed);
    Binary128 result = apply(packed, mode, flags, op);
    return BidConvert.fromBinary128To64(
        result.highBits(), result.lowBits(), mode, flags);
  }

  static void unary128(
      long high, long low, RoundingMode mode, StatusFlags flags, Unary op,
      long[] out) {
    long[] packed = new long[2];
    BidConvert.toBinary128From128(high, low, mode, flags, packed);
    Binary128 result = apply(packed, mode, flags, op);
    BidConvert.fromBinary128To128(
        result.highBits(), result.lowBits(), mode, flags, out);
  }

  static long binary64(
      long x, long y, RoundingMode mode, StatusFlags flags, Binary op) {
    long[] a = new long[2];
    long[] b = new long[2];
    BidConvert.toBinary128From64(x, mode, flags, a);
    BidConvert.toBinary128From64(y, mode, flags, b);
    Binary128 result = apply(a, b, mode, flags, op);
    return BidConvert.fromBinary128To64(
        result.highBits(), result.lowBits(), mode, flags);
  }

  static void binary128(
      long xh, long xl, long yh, long yl,
      RoundingMode mode, StatusFlags flags, Binary op, long[] out) {
    long[] a = new long[2];
    long[] b = new long[2];
    BidConvert.toBinary128From128(xh, xl, mode, flags, a);
    BidConvert.toBinary128From128(yh, yl, mode, flags, b);
    Binary128 result = apply(a, b, mode, flags, op);
    BidConvert.fromBinary128To128(
        result.highBits(), result.lowBits(), mode, flags, out);
  }

  static long hypot64(long x, long y, RoundingMode mode, StatusFlags flags) {
    return BidHypot.hypot64(x, y, mode, flags);
  }

  static void hypot128(
      long xh, long xl, long yh, long yl,
      RoundingMode mode, StatusFlags flags, long[] out) {
    BidHypot.hypot128(xh, xl, yh, yl, mode, flags, out);
  }

  static Binary128 hypotKernel(
      Binary128 x, Binary128 y,
      org.bidfp.binary128.RoundingMode mode,
      org.bidfp.binary128.StatusFlags status) {
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 x2 = Dpml.mul(x, x, mode, local);
    Binary128 y2 = Dpml.mul(y, y, mode, local);
    Binary128 sum = Dpml.add(x2, y2, mode, local);
    Binary128 root = Dpml.sqrt(sum, mode, status);
    status.raise(local.bits());
    return root;
  }

  private static Binary128 apply(
      long[] packed, RoundingMode mode, StatusFlags flags, Unary op) {
    org.bidfp.binary128.RoundingMode binaryMode = binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 result = op.apply(
        Binary128.fromRawBits(packed[0], packed[1]), binaryMode, local);
    flags.raise(local.bits());
    return result;
  }

  private static Binary128 apply(
      long[] a, long[] b, RoundingMode mode, StatusFlags flags, Binary op) {
    org.bidfp.binary128.RoundingMode binaryMode = binaryMode(mode);
    org.bidfp.binary128.StatusFlags local = new org.bidfp.binary128.StatusFlags();
    Binary128 result = op.apply(
        Binary128.fromRawBits(a[0], a[1]),
        Binary128.fromRawBits(b[0], b[1]),
        binaryMode,
        local);
    flags.raise(local.bits());
    return result;
  }

  static org.bidfp.binary128.RoundingMode binaryMode(RoundingMode mode) {
    return org.bidfp.binary128.RoundingMode.fromIntel(mode.toIntel());
  }
}
