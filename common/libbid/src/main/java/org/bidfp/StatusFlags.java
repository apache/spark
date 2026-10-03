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

/**
 * Mutable IEEE 754 status flags for an explicit operation context.
 *
 * <p>Instances are thread-confined and must not be shared between threads.
 */
public final class StatusFlags {
  public static final int INVALID = 0x01;
  public static final int DENORMAL = 0x02;
  public static final int DIVIDE_BY_ZERO = 0x04;
  public static final int OVERFLOW = 0x08;
  public static final int UNDERFLOW = 0x10;
  public static final int INEXACT = 0x20;

  private int bits;

  public int bits() {
    return bits;
  }

  public boolean contains(int flag) {
    return (bits & flag) != 0;
  }

  public void raise(int flags) {
    bits |= flags;
  }

  public void clear() {
    bits = 0;
  }

  void clear(int mask) {
    bits &= ~mask;
  }

  /** Accumulates this object's flags into a JNI-style {@code int[1]} out-parameter. */
  public void copyTo(int[] statusOut) {
    if (statusOut != null && statusOut.length > 0) {
      statusOut[0] |= bits;
    }
  }

  static StatusFlags begin() {
    return new StatusFlags();
  }

  static RoundingMode mode(int rounding) {
    return RoundingMode.fromIntel(rounding);
  }
}
