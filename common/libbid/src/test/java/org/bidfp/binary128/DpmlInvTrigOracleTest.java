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
import org.bidfp.binary128.tables.IeeeConstants;
import org.junit.jupiter.api.Test;

/** Intel QUAD UX oracle coverage dedicated to the inverse-trig family. */
final class DpmlInvTrigOracleTest {
  private static final Binary128 THREE_PI_4 =
      bits(0x4000_2d97_c7f3_321dL, 0x234f_2729_93d1_414aL);

  @Test
  void unaryIntelVectorsStayWithinOneUlp() throws IOException {
    int checked = 0;
    int maxUlp = 0;
    for (IntelF128Oracle.Case c : IntelF128Oracle.load()) {
      if (!isUnaryInverseTrig(c.op)) {
        continue;
      }
      StatusFlags status = new StatusFlags();
      Binary128 got = eval(c.op, c.x, status);
      int ulp = IntelF128Oracle.ulpDistance(c.expected, got);
      assertTrue(ulp <= 1, mismatch(c, got, ulp));
      if (isSpecial(c.x, c.expected)) {
        assertEquals(c.expected, got, mismatch(c, got, ulp));
      }
      maxUlp = Integer.max(maxUlp, ulp);
      checked++;
    }
    assertEquals(48, checked);
    System.out.println("DpmlInvTrig Intel unary vectors: max ULP = " + maxUlp);
  }

  @Test
  void atan2HasIntelSignedZeroAndInfiniteQuadrants() {
    checkAtan2(Binary128.ZERO, Binary128.ONE, Binary128.ZERO);
    checkAtan2(Binary128.NEGATIVE_ZERO, Binary128.ONE, Binary128.NEGATIVE_ZERO);
    checkAtan2(Binary128.ZERO, Binary128.fromRawBits(0xbfff_0000_0000_0000L, 0L),
        IeeeConstants.PI);
    checkAtan2(Binary128.NEGATIVE_ZERO,
        Binary128.fromRawBits(0xbfff_0000_0000_0000L, 0L),
        IeeeConstants.PI.negate());
    checkAtan2(Binary128.POSITIVE_INFINITY, Binary128.POSITIVE_INFINITY,
        IeeeConstants.PI_4);
    checkAtan2(Binary128.POSITIVE_INFINITY, Binary128.NEGATIVE_INFINITY, THREE_PI_4);
    checkAtan2(Binary128.NEGATIVE_INFINITY, Binary128.POSITIVE_INFINITY,
        IeeeConstants.PI_4.negate());
    checkAtan2(Binary128.NEGATIVE_INFINITY, Binary128.NEGATIVE_INFINITY,
        THREE_PI_4.negate());
    checkAtan2(Binary128.ONE, Binary128.ZERO, IeeeConstants.PI_2);
    checkAtan2(Binary128.ONE, Binary128.NEGATIVE_INFINITY, IeeeConstants.PI);
  }

  @Test
  void invalidDomainsAndSignalingNanRaiseInvalid() {
    Binary128 two = bits(0x4000_0000_0000_0000L, 0L);
    StatusFlags asinStatus = new StatusFlags();
    assertEquals(Binary128.canonicalNaN(true),
        DpmlInvTrig.asin(two, RoundingMode.TIES_TO_EVEN, asinStatus));
    assertTrue(asinStatus.contains(StatusFlags.INVALID));

    Binary128 signaling = bits(0x7fff_0000_0000_0001L, 0L);
    StatusFlags atanStatus = new StatusFlags();
    assertTrue(DpmlInvTrig.atan(signaling, RoundingMode.TIES_TO_EVEN, atanStatus).isNaN());
    assertTrue(atanStatus.contains(StatusFlags.INVALID));
  }

  @Test
  void specialCasesPreserveIntelFlags() {
    StatusFlags infinityStatus = new StatusFlags();
    DpmlInvTrig.atan(
        Binary128.POSITIVE_INFINITY, RoundingMode.TIES_TO_EVEN, infinityStatus);
    assertEquals(0, infinityStatus.bits());

    StatusFlags zeroStatus = new StatusFlags();
    DpmlInvTrig.acos(Binary128.ZERO, RoundingMode.TIES_TO_EVEN, zeroStatus);
    assertEquals(0, zeroStatus.bits());

    Binary128 subnormal = bits(0L, 1L);
    StatusFlags denormalStatus = new StatusFlags();
    assertEquals(subnormal,
        DpmlInvTrig.asin(subnormal, RoundingMode.TIES_TO_EVEN, denormalStatus));
    assertEquals(StatusFlags.DENORMAL, denormalStatus.bits());

    StatusFlags bothZeroStatus = new StatusFlags();
    assertEquals(Binary128.ZERO, DpmlInvTrig.atan2(
        Binary128.ZERO, Binary128.ZERO, RoundingMode.TIES_TO_EVEN, bothZeroStatus));
    assertEquals(0, bothZeroStatus.bits());
  }

  private static Binary128 eval(String op, Binary128 x, StatusFlags status) {
    switch (op) {
      case "asin":
        return DpmlInvTrig.asin(x, RoundingMode.TIES_TO_EVEN, status);
      case "acos":
        return DpmlInvTrig.acos(x, RoundingMode.TIES_TO_EVEN, status);
      case "atan":
        return DpmlInvTrig.atan(x, RoundingMode.TIES_TO_EVEN, status);
      default:
        throw new IllegalArgumentException(op);
    }
  }

  private static boolean isUnaryInverseTrig(String op) {
    return "asin".equals(op) || "acos".equals(op) || "atan".equals(op);
  }

  private static boolean isSpecial(Binary128 x, Binary128 expected) {
    return x.isZero() || x.isSubnormal() || x.isInfinite() || x.isNaN()
        || expected.isZero() || expected.isNaN() || expected.isInfinite();
  }

  private static void checkAtan2(Binary128 y, Binary128 x, Binary128 expected) {
    StatusFlags status = new StatusFlags();
    Binary128 got =
        DpmlInvTrig.atan2(y, x, RoundingMode.TIES_TO_EVEN, status);
    assertEquals(expected, got, "atan2(" + y + ", " + x + ")");
  }

  private static Binary128 bits(long high, long low) {
    return Binary128.fromRawBits(high, low);
  }

  private static String mismatch(
      IntelF128Oracle.Case c, Binary128 got, int ulp) {
    return c.op + "(" + c.x + ") expected=" + c.expected
        + " got=" + got + " ulp=" + ulp;
  }
}
