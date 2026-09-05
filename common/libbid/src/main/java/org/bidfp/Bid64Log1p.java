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

import org.bidfp.binary128.Dpml;

/** Intel {@code bid64_log1p.c}: {@code x < -1/2} uses decimal {@code 1+x}. */
final class Bid64Log1p {
  private static final long MINUS_HALF = 0xb1a0_0000_0000_0005L;
  private static final long ONE = 0x31c0_0000_0000_0001L;
  private static final long NAN = 0x7c00_0000_0000_0000L;

  private Bid64Log1p() {
  }

  static long log1p(long x, RoundingMode mode, StatusFlags flags) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isNaN()) {
      return Bid64Log.canonNan(x, flags);
    }
    if (value.quietLess(Bid64.fromRawBits(MINUS_HALF), new StatusFlags())) {
      long y = Bid64Raw.add(x, ONE, mode, flags);
      if (Bid64.fromRawBits(y).isSigned()) {
        flags.raise(StatusFlags.INVALID);
        return NAN;
      }
      return BidTranscendental.unary64(y, mode, flags, Dpml::log);
    }
    return BidTranscendental.unary64(x, mode, flags, Dpml::log1p);
  }
}
