/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in
 * LICENSE-INTEL are met.
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
    assertEquals(20, ConsX.TABLE.length);
    // DATA_4R PI entry: low then high (little-endian 128-bit).
    assertEquals(0x8469898CC51701B8L, ConsX.TABLE[2 * ConsX.PI]);
    assertEquals(0x4000921FB54442D1L, ConsX.TABLE[2 * ConsX.PI + 1]);
  }

  @Test
  void logXLn2MsdMatchesIntel() {
    assertEquals(496, LogX.BYTE_LENGTH);
    assertEquals(448, LogX.LN_2);
    assertEquals(17, LogX.LOG2_COEF_ARRAY_DEGREE);
    // UX_FLOAT at 448: sign|exp long, then MSD, then LSD.
    int idx = LogX.LN_2 / 8;
    assertEquals(0L, LogX.TABLE[idx]);
    assertEquals(0xB17217F7D1CF79ABL, LogX.TABLE[idx + 1]);
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
    assertEquals(263, FourOverPi.TABLE.length);
    assertEquals(0L, FourOverPi.TABLE[0]);
    assertEquals(0L, FourOverPi.TABLE[1]);
    assertEquals(0x0028BE60DB939105L, FourOverPi.TABLE[2]);
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
    assertEquals(ConsX.BYTE_LENGTH, ConsX.TABLE.length * 8);
    assertEquals(LogX.BYTE_LENGTH, LogX.TABLE.length * 8);
    assertEquals(ExpX.BYTE_LENGTH, ExpX.TABLE.length * 8);
    assertEquals(PowX.BYTE_LENGTH, PowX.TABLE.length * 8);
    assertEquals(CbrtX.BYTE_LENGTH, CbrtX.TABLE.length * 8);
    assertEquals(TrigX.BYTE_LENGTH, TrigX.TABLE.length * 8);
    assertEquals(InvTrigX.BYTE_LENGTH, InvTrigX.TABLE.length * 8);
    assertEquals(InvHyperX.BYTE_LENGTH, InvHyperX.TABLE.length * 8);
    assertEquals(ErfX.BYTE_LENGTH, ErfX.TABLE.length * 8);
    assertEquals(LgammaX.BYTE_LENGTH, LgammaX.TABLE.length * 8);
    assertEquals(FourOverPi.BYTE_LENGTH, FourOverPi.TABLE.length * 8);
  }
}
