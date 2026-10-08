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

/** Intel QUAD UX oracle coverage specific to {@link DpmlGamma}. */
final class DpmlGammaOracleTest {
  private static final int MAX_ULP = 1;

  @Test
  void everyIntelLgammaVectorIsStrictlyGated() throws IOException {
    gateIntelVectors("lgamma");
  }

  @Test
  void everyIntelTgammaVectorIsStrictlyGated() throws IOException {
    gateIntelVectors("tgamma");
  }

  private static void gateIntelVectors(String operation) throws IOException {
    int checked = 0;
    int maxUlp = 0;
    String worst = "";
    for (IntelF128Oracle.Case c : IntelF128Oracle.load()) {
      if (!operation.equals(c.op)) {
        continue;
      }
      StatusFlags status = new StatusFlags();
      Binary128 got = "lgamma".equals(c.op)
          ? DpmlGamma.lgamma(c.x, RoundingMode.TIES_TO_EVEN, status)
          : DpmlGamma.tgamma(c.x, RoundingMode.TIES_TO_EVEN, status);
      int ulp = IntelF128Oracle.ulpDistance(c.expected, got);
      if (ulp > maxUlp) {
        maxUlp = ulp;
        worst = mismatch(c, got, ulp);
      }
      checked++;
    }
    assertEquals(16, checked);
    assertTrue(maxUlp <= MAX_ULP, "max ulp=" + maxUlp + " " + worst);
  }

  @Test
  void exactValuesPolesSignsAndSpecials() {
    assertExact(Binary128.ONE, false, Binary128.ZERO);
    assertExact(Binary128.ONE, true, Binary128.ONE);
    assertExact(bits(0x4000_0000_0000_0000L, 0L), false, Binary128.ZERO);
    assertExact(bits(0x4000_0000_0000_0000L, 0L), true, Binary128.ONE);
    Binary128 three = bits(0x4000_8000_0000_0000L, 0L);
    assertExact(three, true, bits(0x4000_0000_0000_0000L, 0L));

    Binary128 minusOne = bits(0xbfff_0000_0000_0000L, 0L);
    StatusFlags lgFlags = new StatusFlags();
    assertEquals(Binary128.POSITIVE_INFINITY,
        DpmlGamma.lgamma(minusOne, RoundingMode.TIES_TO_EVEN, lgFlags));
    assertTrue(lgFlags.contains(StatusFlags.DIVIDE_BY_ZERO));

    StatusFlags tgFlags = new StatusFlags();
    assertEquals(Binary128.POSITIVE_INFINITY,
        DpmlGamma.tgamma(minusOne, RoundingMode.TIES_TO_EVEN, tgFlags));
    assertTrue(tgFlags.contains(StatusFlags.DIVIDE_BY_ZERO));

    Binary128 minusHalf = bits(0xbffe_0000_0000_0000L, 0L);
    Binary128 minusThreeHalves = bits(0xbfff_8000_0000_0000L, 0L);
    assertTrue(DpmlGamma.tgamma(
        minusHalf, RoundingMode.TIES_TO_EVEN, new StatusFlags()).isSigned());
    assertTrue(!DpmlGamma.tgamma(
        minusThreeHalves, RoundingMode.TIES_TO_EVEN, new StatusFlags()).isSigned());

    StatusFlags zeroFlags = new StatusFlags();
    assertEquals(Binary128.POSITIVE_INFINITY,
        DpmlGamma.lgamma(Binary128.NEGATIVE_ZERO, RoundingMode.TIES_TO_EVEN, zeroFlags));
    assertTrue(zeroFlags.contains(StatusFlags.DIVIDE_BY_ZERO));

    StatusFlags infFlags = new StatusFlags();
    Binary128 negInf = DpmlGamma.lgamma(
        Binary128.NEGATIVE_INFINITY, RoundingMode.TIES_TO_EVEN, infFlags);
    assertTrue(negInf.isNaN() && negInf.isSigned());
    assertTrue(infFlags.contains(StatusFlags.INVALID));
  }

  private static void assertExact(Binary128 x, boolean gamma, Binary128 expected) {
    Binary128 got = gamma
        ? DpmlGamma.tgamma(x, RoundingMode.TIES_TO_EVEN, new StatusFlags())
        : DpmlGamma.lgamma(x, RoundingMode.TIES_TO_EVEN, new StatusFlags());
    assertEquals(expected, got);
  }

  private static Binary128 bits(long high, long low) {
    return Binary128.fromRawBits(high, low);
  }

  private static String mismatch(IntelF128Oracle.Case c, Binary128 got, int ulp) {
    return c.op + " x=" + c.x + " expected=" + c.expected + " got=" + got
        + " ulp=" + ulp;
  }
}
