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
 * IEEE 754 rounding-direction attributes. Codes 0..4 match Intel {@code _IDEC_round}
 * and the DPML {@code R{Z,P,M,N,V}_BIT_VECTOR} packing used by {@link UxOps}.
 */
public enum RoundingMode {
  TIES_TO_EVEN(0),
  TOWARD_NEGATIVE(1),
  TOWARD_POSITIVE(2),
  TOWARD_ZERO(3),
  TIES_AWAY(4);

  private final int intelCode;

  RoundingMode(int intelCode) {
    this.intelCode = intelCode;
  }

  /** Intel RDFP rounding code: 0..4. */
  public int toIntel() {
    return intelCode;
  }

  /** Inverse of {@link #toIntel()}. */
  public static RoundingMode fromIntel(int code) {
    for (RoundingMode mode : values()) {
      if (mode.intelCode == code) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Intel rounding code must be in [0, 4]");
  }

  /**
   * DPML increment bit-vector indexed by {@code 8*S + 4*K + 2*L + R}
   * (see {@code dpml_ux_int.c}).
   */
  int bitVector() {
    switch (this) {
      case TOWARD_ZERO:
        return 0x0000;
      case TOWARD_POSITIVE:
        return 0x00fa;
      case TOWARD_NEGATIVE:
        return 0xfa00;
      case TIES_TO_EVEN:
        return 0xa8a8;
      case TIES_AWAY:
        return 0xaaaa;
      default:
        throw new IllegalStateException();
    }
  }
}
