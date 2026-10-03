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
package org.bidfp.binary128.tables;

import org.bidfp.binary128.Binary128;

/**
 * Packed binary128 constants used by DPML-style kernels (IEEE values, not
 * BID encodings).
 */
public final class IeeeConstants {
  private IeeeConstants() {
  }

  public static final Binary128 LN2 =
      Binary128.fromRawBits(0x3ffe62e42fefa39eL, 0xf35793c7673007e6L);
  public static final Binary128 LN10 =
      Binary128.fromRawBits(0x400026bb1bbb5551L, 0x582dd4adac5705a6L);
  public static final Binary128 LOG2E =
      Binary128.fromRawBits(0x3fff71547652b82fL, 0xe1777d0ffda0d23aL);
  public static final Binary128 LOG10E =
      Binary128.fromRawBits(0x3ffdbcb7b1526e50L, 0xe32a6ab7555f5a68L);
  public static final Binary128 PI =
      Binary128.fromRawBits(0x4000921fb54442d1L, 0x8469898cc51701b8L);
  public static final Binary128 TWO_PI =
      Binary128.fromRawBits(0x4001921fb54442d1L, 0x8469898cc51701b8L);
  public static final Binary128 PI_2 =
      Binary128.fromRawBits(0x3fff921fb54442d1L, 0x8469898cc51701b8L);
  public static final Binary128 PI_4 =
      Binary128.fromRawBits(0x3ffe921fb54442d1L, 0x8469898cc51701b8L);
  public static final Binary128 SQRT2 =
      Binary128.fromRawBits(0x3fff6a09e667f3bcL, 0xc908b2fb1366ea95L);
  public static final Binary128 SQRT1_2 =
      Binary128.fromRawBits(0x3ffe6a09e667f3bcL, 0xc908b2fb1366ea95L);
  public static final Binary128 HALF =
      Binary128.fromRawBits(0x3ffe000000000000L, 0L);
  public static final Binary128 TWO =
      Binary128.fromRawBits(0x4000000000000000L, 0L);
}
