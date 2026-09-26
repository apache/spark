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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Spot-checks generated QUAD UX tables against known Intel byte layouts.
 */
public class DpmlTablesTest {

  @Test
  void consXHasPackedPi() {
    assertEquals(160, ConsX.BYTE_LENGTH);
    assertEquals(20, ConsX.TABLE.length());
    // DATA_4R PI entry: low then high (little-endian 128-bit).
    assertEquals(0x8469898CC51701B8L, ConsX.TABLE.get(2 * ConsX.PI));
    assertEquals(0x4000921FB54442D1L, ConsX.TABLE.get(2 * ConsX.PI + 1));
  }

  @Test
  void logXLn2MsdMatchesIntel() {
    assertEquals(496, LogX.BYTE_LENGTH);
    assertEquals(448, LogX.LN_2);
    assertEquals(17, LogX.LOG2_COEF_ARRAY_DEGREE);
    // UX_FLOAT at 448: sign|exp long, then MSD, then LSD.
    int idx = LogX.LN_2 / 8;
    assertEquals(0L, LogX.TABLE.get(idx));
    assertEquals(0xB17217F7D1CF79ABL, LogX.TABLE.get(idx + 1));
  }

  @Test
  void expXByteLengthAndClassMaps() {
    assertEquals(1352, ExpX.BYTE_LENGTH);
    assertEquals(0, ExpX.EXP_CLASS_TO_ACTION_MAP);
    assertEquals(8, ExpX.EXPM1_CLASS_TO_ACTION_MAP);
    assertEquals(16, ExpX.SINH_CLASS_TO_ACTION_MAP);
    assertEquals(952, ExpX.SINHCOSH_COEF_ARRAY);
    assertEquals(11, ExpX.SINHCOSH_COEF_ARRAY_DEGREE);
  }

  @Test
  void fourOverPiLengthAndLeadDigits() {
    assertEquals(2104, FourOverPi.BYTE_LENGTH);
    assertEquals(263, FourOverPi.LENGTH);
    assertEquals(263, FourOverPi.TABLE.length());
    assertEquals(0L, FourOverPi.TABLE.get(0));
    assertEquals(0L, FourOverPi.TABLE.get(1));
    assertEquals(0x0028BE60DB939105L, FourOverPi.TABLE.get(2));
  }

  @Test
  void remainingFamiliesMatchExpectedSizes() {
    assertEquals(1048, PowX.BYTE_LENGTH);
    assertEquals(104, CbrtX.BYTE_LENGTH);
    assertEquals(1032, TrigX.BYTE_LENGTH);
    assertEquals(1312, InvTrigX.BYTE_LENGTH);
    assertEquals(112, InvHyperX.BYTE_LENGTH);
    assertEquals(1368, ErfX.BYTE_LENGTH);
    assertEquals(968, LgammaX.BYTE_LENGTH);
    assertTrue(PowX.POW2_COEF_ARRAY_DEGREE == 22);
    assertTrue(TrigX.SINCOS_COEF_ARRAY_DEGREE == 13);
  }

  @Test
  void tableLongCountMatchesByteLength() {
    assertEquals(ConsX.BYTE_LENGTH, ConsX.TABLE.length() * 8);
    assertEquals(LogX.BYTE_LENGTH, LogX.TABLE.length() * 8);
    assertEquals(ExpX.BYTE_LENGTH, ExpX.TABLE.length() * 8);
    assertEquals(PowX.BYTE_LENGTH, PowX.TABLE.length() * 8);
    assertEquals(CbrtX.BYTE_LENGTH, CbrtX.TABLE.length() * 8);
    assertEquals(TrigX.BYTE_LENGTH, TrigX.TABLE.length() * 8);
    assertEquals(InvTrigX.BYTE_LENGTH, InvTrigX.TABLE.length() * 8);
    assertEquals(InvHyperX.BYTE_LENGTH, InvHyperX.TABLE.length() * 8);
    assertEquals(ErfX.BYTE_LENGTH, ErfX.TABLE.length() * 8);
    assertEquals(LgammaX.BYTE_LENGTH, LgammaX.TABLE.length() * 8);
    assertEquals(FourOverPi.BYTE_LENGTH, FourOverPi.TABLE.length() * 8);
  }

  @Test
  void tableCopiesCannotMutateKernelData() {
    TableData[] tables = {
        ConsX.TABLE, LogX.TABLE, ExpX.TABLE, PowX.TABLE, CbrtX.TABLE, TrigX.TABLE,
        InvTrigX.TABLE, InvHyperX.TABLE, ErfX.TABLE, LgammaX.TABLE, FourOverPi.TABLE
    };
    for (TableData table : tables) {
      long original = table.get(0);
      long[] copy = table.copy();
      copy[0] = ~original;
      assertEquals(original, table.get(0));
    }
  }
}
