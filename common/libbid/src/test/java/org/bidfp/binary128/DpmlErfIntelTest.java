/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bidfp.binary128;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Intel QUAD UX oracle and interval invariants for erf/erfc. */
final class DpmlErfIntelTest {
  private static final Binary128 NEGATIVE_ONE = Binary128.ONE.negate();
  private static final Binary128 TWO =
      Binary128.fromRawBits(0x4000_0000_0000_0000L, 0L);

  @Test
  void everyIntelOracleVectorIsStrictlyGated() throws IOException {
    int checked = 0;
    int maximumUlp = 0;
    for (IntelF128Oracle.Case c : IntelF128Oracle.load()) {
      if (!"erf".equals(c.op) && !"erfc".equals(c.op)) {
        continue;
      }
      Binary128 got = evaluate(c.op, c.x);
      int ulp = IntelF128Oracle.ulpDistance(c.expected, got);
      assertTrue(ulp <= 1, mismatch(c.op, c.x, c.expected, got, ulp));
      if (ulp > maximumUlp) {
        maximumUlp = ulp;
      }
      assertRange(c.op, got);
      checked++;
    }
    assertEquals(32, checked, "unexpected Intel erf/erfc vector count");
    System.out.println("DpmlErf Intel vectors=" + checked + " maxUlp=" + maximumUlp);
  }

  @Test
  void cancellationAndAsymptoticPointsMatchQuadReferences() {
    Unpacked expMinusOne = DpmlErfExp.multiplyByNegativeSquare(
        UxOps.unpack(Binary128.ONE), KernelEval.fromInt(1), new StatusFlags());
    assertEquals(
        Binary128.fromRawBits(0x3ffd_78b56362cef3L, 0x7c6a_eb7b1e0a4154L),
        UxOps.pack(expMinusOne, RoundingMode.TIES_TO_EVEN, new StatusFlags()));
    check(
        "erf",
        0x4001_0000_0000_0000L,
        0L,
        0x3ffe_ffffff7b9117L,
        0x6216_50cac2bb6806L);
    check(
        "erfc",
        0x4001_0000_0000_0000L,
        0L,
        0x3fe5_08ddd13bd35eL,
        0x6a7a_892ff39b1fa3L);
    check(
        "erf",
        0x4002_4000_0000_0000L,
        0L,
        0x3fff_000000000000L,
        0L);
    check(
        "erfc",
        0x4002_4000_0000_0000L,
        0L,
        0x3f6a_7d8a7f2a8a2cL,
        0xf9d3_7388c15c764dL);
  }

  @Test
  void specialsAndUnderflowFlagsFollowIntelActions() {
    Binary128 signaling =
        Binary128.fromRawBits(0x7fff_0000_0000_0001L, 0x0123_4567_89ab_cdefL);
    StatusFlags invalid = new StatusFlags();
    Binary128 quiet = DpmlErf.erf(signaling, RoundingMode.TIES_TO_EVEN, invalid);
    assertTrue(quiet.isNaN());
    assertTrue(!quiet.isSignalingNaN());
    assertEquals(signaling.lowBits(), quiet.lowBits());
    assertTrue(invalid.contains(StatusFlags.INVALID));

    StatusFlags underflow = new StatusFlags();
    Binary128 at128 = Binary128.fromRawBits(0x4006_0000_0000_0000L, 0L);
    assertEquals(
        Binary128.ZERO,
        DpmlErf.erfc(at128, RoundingMode.TIES_TO_EVEN, underflow));
    assertEquals(StatusFlags.UNDERFLOW | StatusFlags.INEXACT, underflow.bits());

    StatusFlags roundedUp = new StatusFlags();
    assertEquals(
        Binary128.fromRawBits(0L, 1L),
        DpmlErf.erfc(at128, RoundingMode.TOWARD_POSITIVE, roundedUp));
    assertEquals(StatusFlags.UNDERFLOW | StatusFlags.INEXACT, roundedUp.bits());
  }

  @Test
  void symmetryAndComplementInvariantsHoldAcrossIntervals() {
    Binary128[] positive = {
        Binary128.fromRawBits(0x3ffd_555555555555L, 0x5555_555555555555L),
        Binary128.ONE,
        Binary128.fromRawBits(0x4001_000000000000L, 0L),
        Binary128.fromRawBits(0x4002_180000000000L, 0L),
        Binary128.fromRawBits(0x4002_400000000000L, 0L)
    };
    for (Binary128 x : positive) {
      Binary128 positiveErf = evaluate("erf", x);
      Binary128 negativeErf = evaluate("erf", x.negate());
      assertEquals(positiveErf.negate(), negativeErf);

      Binary128 positiveErfc = evaluate("erfc", x);
      Binary128 negativeErfc = evaluate("erfc", x.negate());
      Binary128 reflected = UxOps.sub(
          TWO, positiveErfc, RoundingMode.TIES_TO_EVEN, new StatusFlags());
      assertTrue(IntelF128Oracle.ulpDistance(reflected, negativeErfc) <= 1);

      Binary128 sum = UxOps.add(
          positiveErf, positiveErfc, RoundingMode.TIES_TO_EVEN, new StatusFlags());
      assertTrue(IntelF128Oracle.ulpDistance(Binary128.ONE, sum) <= 1);
    }
  }

  private static void check(
      String operation, long xHigh, long xLow, long expectedHigh, long expectedLow) {
    Binary128 x = Binary128.fromRawBits(xHigh, xLow);
    Binary128 expected = Binary128.fromRawBits(expectedHigh, expectedLow);
    Binary128 got = evaluate(operation, x);
    int ulp = IntelF128Oracle.ulpDistance(expected, got);
    assertEquals(0, ulp, mismatch(operation, x, expected, got, ulp));
    assertRange(operation, got);
  }

  private static Binary128 evaluate(String operation, Binary128 x) {
    StatusFlags status = new StatusFlags();
    if ("erf".equals(operation)) {
      return DpmlErf.erf(x, RoundingMode.TIES_TO_EVEN, status);
    }
    return DpmlErf.erfc(x, RoundingMode.TIES_TO_EVEN, status);
  }

  private static void assertRange(String operation, Binary128 value) {
    if (value.isNaN()) {
      return;
    }
    StatusFlags status = new StatusFlags();
    if ("erf".equals(operation)) {
      assertTrue(UxOps.compare(value, NEGATIVE_ONE, status) >= 0);
      assertTrue(UxOps.compare(value, Binary128.ONE, status) <= 0);
    } else {
      assertTrue(UxOps.compare(value, Binary128.ZERO, status) >= 0);
      assertTrue(UxOps.compare(value, TWO, status) <= 0);
    }
  }

  private static String mismatch(
      String operation,
      Binary128 x,
      Binary128 expected,
      Binary128 got,
      int ulp) {
    return operation + " x=" + x + " expected=" + expected
        + " got=" + got + " ulp=" + ulp;
  }
}
