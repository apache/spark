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
 * QUAD UX table from Intel {@code dpml_inv_hyper_x.h}.
 * Little-endian memory image as {@code long[]} (112 bytes).
 */
public final class InvHyperX {
  private InvHyperX() {
  }

  /** Total table size in bytes (Intel comment offsets). */
  public static final int BYTE_LENGTH = 112;

  public static final int ACOSH_CLASS_TO_ACTION_MAP = 8;
  public static final int ASINH_CLASS_TO_ACTION_MAP = 0;
  public static final int ATANH_CLASS_TO_ACTION_MAP = 24;
  public static final int UX_LN2 = 88;
  public static final int UX_ONE = 64;
  public static final int SQRT_2_M1_SQR = 56;
  public static final int SQRT_2_OV_4 = 40;
  public static final int THREE_SQRT_2_OV_4 = 48;

  /** Little-endian table words (two u32s per long). */
  public static final TableData TABLE = new TableData(new long[] {
      0x1410410000410408L, 0x1E79E79E40E50408L,
      0x0000000000000002L, 0x1410410000E79408L,
      0x0000000000000005L, 0xB504F333F9DE6484L,
      0x87C3B666FB66CB63L, 0xAFB0CCC06219B7BAL,
      0x0000000100000000L, 0x8000000000000000L,
      0x0000000000000000L, 0x0000000000000000L,
      0xB17217F7D1CF79ABL, 0xC9E3B39803F2F6AFL
  });
}
