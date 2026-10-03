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
 *
 * Generated from Intel RDFP 2.0 Update 4 float128 UX table sources.
 * Do not edit by hand; regenerate with common/libbid/tools/gen_dpml_tables.py.
 */
package org.bidfp.binary128.tables;

/**
 * QUAD UX table from Intel {@code dpml_cons_x.h}.
 * Little-endian memory image as {@code long[]} (160 bytes).
 */
public final class ConsX {
  private ConsX() {
  }

  /** Total table size in bytes (Intel comment offsets). */
  public static final int BYTE_LENGTH = 160;

  public static final int INF = 9;
  public static final int LAST_CONS_INDEX = 10;
  public static final int NINETY = 7;
  public static final int ONE = 1;
  public static final int ONE_EIGHTY = 8;
  public static final int PI = 3;
  public static final int PI_OVER_2 = 4;
  public static final int PI_OVER_4 = 5;
  public static final int THREE_PI_OVER_4 = 6;
  public static final int TWO = 2;
  public static final int ZERO = 0;

  /** Little-endian table words (two u32s per long). */
  public static final TableData TABLE = new TableData(new long[] {
      0x0000000000000000L, 0x0000000000000000L,
      0x0000000000000000L, 0x3FFF000000000000L,
      0x0000000000000000L, 0x4000000000000000L,
      0x8469898CC51701B8L, 0x4000921FB54442D1L,
      0x8469898CC51701B8L, 0x3FFF921FB54442D1L,
      0x8469898CC51701B8L, 0x3FFE921FB54442D1L,
      0x234F272993D1414AL, 0x40002D97C7F3321DL,
      0x0000000000000000L, 0x4005680000000000L,
      0x0000000000000000L, 0x4006680000000000L,
      0x0000000000000000L, 0x7FFF000000000000L
  });
}
