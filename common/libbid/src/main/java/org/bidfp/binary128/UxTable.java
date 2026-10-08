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

import org.bidfp.binary128.tables.TableData;

/**
 * Decode little-endian Intel QUAD UX table blobs ({@link TableData} from
 * {@code dpml_*_x.h} / {@code FourOverPi}).
 *
 * <p>Layout notes (64-bit UX path):
 * <ul>
 *   <li>{@code UX_FLOAT}: 24 bytes = sign(i32) | exp(i32) | MSD(u64) |
 *       LSD(u64)</li>
 *   <li>{@code FIXED_128}: 16 bytes = digits[0]=lo(u64), digits[1]=hi(u64)
 *       (Horner order; opposite of UX fraction indexing)</li>
 * </ul>
 */
final class UxTable {
  static final int UX_FLOAT_BYTES = 24;
  static final int FIXED_128_BYTES = 16;
  /** Trailing scale {@code WORD} after a FIXED_128 coefficient bank. */
  static final int SCALE_WORD_BYTES = 8;

  private UxTable() {
  }

  /** Byte length of one rational/poly coefficient bank including scale. */
  static int coefBankBytes(int degree) {
    return (degree + 1) * FIXED_128_BYTES + SCALE_WORD_BYTES;
  }

  static long word64(TableData table, int byteOffset) {
    requireAligned(byteOffset, 8);
    return table.get(byteOffset >>> 3);
  }

  /** Low 32 bits at {@code byteOffset} (must be 4-byte aligned). */
  static int word32(TableData table, int byteOffset) {
    if ((byteOffset & 3) != 0) {
      throw new IllegalArgumentException("offset " + byteOffset);
    }
    long w = table.get(byteOffset >>> 3);
    return (byteOffset & 7) == 0 ? (int) w : (int) (w >>> 32);
  }

  static double readDouble(TableData table, int byteOffset) {
    return Double.longBitsToDouble(word64(table, byteOffset));
  }

  /**
   * Read {@code FIXED_128}: {@code lo = digits[0]}, {@code hi = digits[1]}.
   */
  static void readFixed128(TableData table, int byteOffset, long[] loHiOut) {
    requireAligned(byteOffset, 8);
    int i = byteOffset >>> 3;
    loHiOut[0] = table.get(i);
    loHiOut[1] = table.get(i + 1);
  }

  /** Trailing signed scale word after {@code degree + 1} FIXED_128 coeffs. */
  static int readCoefScale(TableData table, int coefsOffset, int degree) {
    int scaleOff = coefsOffset + (degree + 1) * FIXED_128_BYTES;
    return (int) word64(table, scaleOff);
  }

  /**
   * Convert FIXED_128 at {@code byteOffset} to an unpacked value with
   * exponent 0 (fraction = hi:lo as a 128-bit significand with MSB weight
   * 2^-1 when normalized).
   */
  static void fixed128ToUnpacked(TableData table, int byteOffset, Unpacked dest) {
    requireAligned(byteOffset, 8);
    int i = byteOffset >>> 3;
    long lo = table.get(i);
    long hi = table.get(i + 1);
    if (hi == 0L && lo == 0L) {
      dest.setZero(0);
      return;
    }
    // FIXED_128 digits[1]=hi -> UX MSD; digits[0]=lo -> UX LSD.
    dest.setNorm(0, 0, hi, lo);
    UxOps.normalize(dest);
  }

  static Unpacked readUxFloat(TableData table, int byteOffset) {
    Unpacked u = new Unpacked();
    readUxFloat(table, byteOffset, u);
    return u;
  }

  static void readUxFloat(TableData table, int byteOffset, Unpacked dest) {
    requireAligned(byteOffset, 8);
    int i = byteOffset >>> 3;
    long head = table.get(i);
    int sign = (int) head;
    int exp = (int) (head >>> 32);
    long msd = table.get(i + 1);
    long lsd = table.get(i + 2);
    if (exp == Unpacked.UX_ZERO_EXPONENT || (msd == 0L && lsd == 0L)) {
      dest.setZero(sign);
      return;
    }
    if (exp == Unpacked.UX_INFINITY_EXPONENT) {
      if ((msd & ~Unpacked.UX_MSB) == 0L && lsd == 0L) {
        dest.setInf(sign);
      } else {
        boolean signaling = (msd & 0x4000_0000_0000_0000L) == 0L;
        dest.setNaN(signaling);
        dest.sign = sign;
      }
      return;
    }
    dest.setNorm(sign, exp, msd, lsd);
  }

  private static void requireAligned(int byteOffset, int align) {
    if (byteOffset < 0 || (byteOffset & (align - 1)) != 0) {
      throw new IllegalArgumentException("bad offset " + byteOffset);
    }
  }
}
