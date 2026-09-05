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

/** Family-private exact classification and exponent helpers for pow and cbrt. */
final class DpmlPowCbrtSupport {
  private DpmlPowCbrtSupport() {
  }

  static Binary128 quietNaN(Binary128 value, StatusFlags status) {
    if (value.isSignalingNaN()) {
      status.raise(StatusFlags.INVALID);
    }
    return Binary128.fromRawBits(
        value.highBits() | Binary128.QUIET_NAN_BIT, value.lowBits());
  }

  static int compareAbsToOne(Binary128 value) {
    int exponent = value.biasedExponent();
    if (exponent != Binary128.BIAS) {
      return Integer.compare(exponent, Binary128.BIAS);
    }
    return value.fractionHigh() == 0L && value.fractionLow() == 0L ? 0 : 1;
  }

  /**
   * Returns 0 for non-integral, 1 for even integral, and 2 for odd integral.
   * This examines the packed significand, so very large representable values
   * are classified without conversion through a Java primitive.
   */
  static int integerKind(Binary128 value) {
    if (!value.isFinite()) {
      return 0;
    }
    if (value.isZero()) {
      return 1;
    }
    int e = value.biasedExponent() - Binary128.BIAS;
    if (e < 0) {
      return 0;
    }
    if (e > Binary128.SIGNIFICAND_BITS) {
      return 1;
    }
    int fractionalBits = Binary128.SIGNIFICAND_BITS - e;
    long high = value.significandHigh();
    long low = value.significandLow();
    if (fractionalBits < 64) {
      long mask = fractionalBits == 0 ? 0L : (1L << fractionalBits) - 1L;
      if ((low & mask) != 0L) {
        return 0;
      }
      return ((low >>> fractionalBits) & 1L) != 0L ? 2 : 1;
    }
    if (low != 0L) {
      return 0;
    }
    int highFractionalBits = fractionalBits - 64;
    long mask = highFractionalBits == 0
        ? 0L
        : (1L << highFractionalBits) - 1L;
    if ((high & mask) != 0L) {
      return 0;
    }
    return ((high >>> highFractionalBits) & 1L) != 0L ? 2 : 1;
  }

  /** Round a finite UX value to the nearest integer, ties to even. */
  static int nearestInt(Unpacked value) {
    UxScratch.Frame scratch = UxScratch.acquire();
    try {
      Unpacked u = scratch.unpacked(0);
      u.copyFrom(value);
      UxOps.normalize(u);
      if (u.exponent <= 0) {
        return 0;
      }
      if (u.exponent > 31) {
        return u.sign != 0 ? Integer.MIN_VALUE : Integer.MAX_VALUE;
      }
      int shift = 128 - u.exponent;
      int highShift = shift - 64;
      long integer = u.fracHi >>> highShift;
      long lowerMask = (1L << (highShift - 1)) - 1L;
      boolean halfway = (u.fracHi & (1L << (highShift - 1))) != 0L;
      boolean belowHalf = (u.fracHi & lowerMask) != 0L || u.fracLo != 0L;
      if (halfway && (belowHalf || (integer & 1L) != 0L)) {
        integer++;
      }
      int result = (int) integer;
      return u.sign != 0 ? -result : result;
    } finally {
      UxScratch.release(scratch);
    }
  }

  static double normalizedFractionAsDouble(Unpacked value) {
    long bits = 0x3ff0_0000_0000_0000L
        | ((value.fracHi >>> 11) & 0x000f_ffff_ffff_ffffL);
    return Double.longBitsToDouble(bits);
  }

  static int floorDiv3(int value) {
    int quotient = value / 3;
    return value < 0 && value % 3 != 0 ? quotient - 1 : quotient;
  }

  static int floorMod3(int value) {
    return value - 3 * floorDiv3(value);
  }
}
