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

/** Intel domain NaNs for inverse trig/hyperbolic BID64. */
final class Bid64Domain {
  static final long NAN = 0x7c00_0000_0000_0000L;

  private Bid64Domain() {
  }

  static long invalid(StatusFlags flags) {
    flags.raise(StatusFlags.INVALID);
    return NAN;
  }

  static long asin(long x, RoundingMode mode, StatusFlags flags) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isNaN()) {
      return Bid64Log.canonNan(x, flags);
    }
    if (value.isInfinite()) {
      return invalid(flags);
    }
    long abs = x & ~Bid64.MASK_SIGN;
    if (Bid64.fromRawBits(abs).quietGreater(
        Bid64.fromRawBits(Bid64Log.ONE), new StatusFlags())) {
      return invalid(flags);
    }
    return BidTranscendental.unary64(x, mode, flags, org.bidfp.binary128.Dpml::asin);
  }

  static long acos(long x, RoundingMode mode, StatusFlags flags) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isNaN()) {
      return Bid64Log.canonNan(x, flags);
    }
    if (value.isInfinite()) {
      return invalid(flags);
    }
    long abs = x & ~Bid64.MASK_SIGN;
    if (Bid64.fromRawBits(abs).quietGreater(
        Bid64.fromRawBits(Bid64Log.ONE), new StatusFlags())) {
      return invalid(flags);
    }
    return BidTranscendental.unary64(x, mode, flags, org.bidfp.binary128.Dpml::acos);
  }

  static long acosh(long x, RoundingMode mode, StatusFlags flags) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isNaN()) {
      return Bid64Log.canonNan(x, flags);
    }
    if (value.isInfinite()) {
      if (value.isSigned()) {
        return invalid(flags);
      }
      return Bid64.MASK_INFINITY;
    }
    if (value.quietLess(
        Bid64.fromRawBits(Bid64Log.ONE), new StatusFlags())) {
      return invalid(flags);
    }
    return BidTranscendental.unary64(
        x, mode, flags, org.bidfp.binary128.Dpml::acosh);
  }

  static long atanh(long x, RoundingMode mode, StatusFlags flags) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isNaN()) {
      return Bid64Log.canonNan(x, flags);
    }
    if (value.isInfinite()) {
      return invalid(flags);
    }
    long abs = x & ~Bid64.MASK_SIGN;
    if (Bid64.fromRawBits(abs).quietGreater(
        Bid64.fromRawBits(Bid64Log.ONE), new StatusFlags())) {
      return invalid(flags);
    }
    return BidTranscendental.unary64(x, mode, flags, org.bidfp.binary128.Dpml::atanh);
  }
}
