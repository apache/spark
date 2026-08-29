/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Intel QUAD UX oracle coverage for radian sin, cos, and tan. */
final class DpmlTrigOracleTest {
  @Test
  void allIntelRadianTrigVectorsMatch() throws IOException {
    int checked = 0;
    int huge = 0;
    int maxUlp = 0;
    for (IntelF128Oracle.Case c : IntelF128Oracle.load()) {
      if (!isTrig(c.op)) {
        continue;
      }
      Binary128 got = evaluate(c);
      int ulp = IntelF128Oracle.ulpDistance(c.expected, got);
      maxUlp = Integer.max(maxUlp, ulp);
      assertTrue(ulp <= 1, mismatch(c, got, ulp));
      if (c.x.biasedExponent() == 0x7ffe) {
        huge++;
      }
      if (("sin".equals(c.op) || "cos".equals(c.op)) && !got.isNaN()) {
        assertTrue(
            UxOps.compare(got.abs(), Binary128.ONE, new StatusFlags()) <= 0,
            c.op + " escaped [-1, 1]: " + got);
      }
      checked++;
    }
    assertEquals(48, checked);
    assertEquals(3, huge);
    assertEquals(0, maxUlp);
  }

  @Test
  void infinitiesAreInvalidAndQuietNaN() {
    for (Binary128 infinity :
        new Binary128[] {Binary128.POSITIVE_INFINITY, Binary128.NEGATIVE_INFINITY}) {
      StatusFlags status = new StatusFlags();
      Binary128 result =
          DpmlTrig.sin(infinity, RoundingMode.TIES_TO_EVEN, status);
      assertTrue(result.isNaN());
      assertTrue(status.contains(StatusFlags.INVALID));
    }
  }

  private static boolean isTrig(String op) {
    return "sin".equals(op) || "cos".equals(op) || "tan".equals(op);
  }

  private static Binary128 evaluate(IntelF128Oracle.Case c) {
    StatusFlags status = new StatusFlags();
    RoundingMode mode = RoundingMode.fromIntel(c.rnd);
    switch (c.op) {
      case "sin":
        return DpmlTrig.sin(c.x, mode, status);
      case "cos":
        return DpmlTrig.cos(c.x, mode, status);
      case "tan":
        return DpmlTrig.tan(c.x, mode, status);
      default:
        throw new IllegalArgumentException(c.op);
    }
  }

  private static String mismatch(
      IntelF128Oracle.Case c, Binary128 got, int ulp) {
    return c.op + " x=" + c.x + " expected=" + c.expected
        + " got=" + got + " ulp=" + ulp;
  }
}
