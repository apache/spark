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
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Intel QUAD oracle coverage for the log and inverse-hyperbolic UX families. */
final class DpmlLogInvHyperOracleTest {
  // Intel's 128-bit UX evaluation retains 15 guard bits before the only pack.
  private static final int MAX_ULP = 1;

  @Test
  void allIntelVectorsMatch() throws IOException {
    Map<String, Integer> counts = new LinkedHashMap<>();
    Map<String, Integer> maxima = new LinkedHashMap<>();
    for (IntelF128Oracle.Case vector : IntelF128Oracle.load()) {
      if (!isOwned(vector.op)) {
        continue;
      }
      Binary128 actual = evaluate(vector);
      counts.merge(vector.op, 1, Integer::sum);
      if (isSpecial(vector.expected)) {
        assertEquals(vector.expected, actual, mismatch(vector, actual));
      } else {
        int ulp = IntelF128Oracle.ulpDistance(vector.expected, actual);
        maxima.merge(vector.op, ulp, Integer::max);
        assertTrue(ulp <= MAX_ULP, mismatch(vector, actual) + " ulp=" + ulp);
      }
    }
    for (String operation : operations()) {
      assertTrue(counts.getOrDefault(operation, 0) > 0, "no vectors for " + operation);
      assertTrue(maxima.containsKey(operation), "no finite vectors for " + operation);
    }
  }

  @Test
  void domainFlagsFollowIntelActions() {
    StatusFlags status = new StatusFlags();
    assertEquals(Binary128.NEGATIVE_INFINITY,
        DpmlLog.log(Binary128.ZERO, RoundingMode.TIES_TO_EVEN, status));
    assertEquals(StatusFlags.DIVIDE_BY_ZERO, status.bits());

    status.clear();
    assertEquals(Binary128.canonicalNaN(true),
        DpmlLog.log(Binary128.NEGATIVE_ZERO, RoundingMode.TIES_TO_EVEN, status));
    assertEquals(StatusFlags.INVALID, status.bits());

    status.clear();
    Binary128 negativeOne = Binary128.fromRawBits(0xbfff_0000_0000_0000L, 0L);
    assertEquals(Binary128.NEGATIVE_INFINITY,
        DpmlLog.log1p(negativeOne, RoundingMode.TIES_TO_EVEN, status));
    assertEquals(StatusFlags.DIVIDE_BY_ZERO, status.bits());

    status.clear();
    Binary128 negativeTwo = Binary128.fromRawBits(0xc000_0000_0000_0000L, 0L);
    assertEquals(Binary128.canonicalNaN(true),
        DpmlLog.log1p(negativeTwo, RoundingMode.TIES_TO_EVEN, status));
    assertEquals(StatusFlags.INVALID, status.bits());

    status.clear();
    assertEquals(Binary128.canonicalNaN(true),
        DpmlInvHyper.acosh(Binary128.ZERO, RoundingMode.TIES_TO_EVEN, status));
    assertEquals(StatusFlags.INVALID, status.bits());

    status.clear();
    assertEquals(Binary128.POSITIVE_INFINITY,
        DpmlInvHyper.atanh(Binary128.ONE, RoundingMode.TIES_TO_EVEN, status));
    assertEquals(StatusFlags.DIVIDE_BY_ZERO, status.bits());

    status.clear();
    assertEquals(Binary128.canonicalNaN(true),
        DpmlInvHyper.atanh(negativeTwo, RoundingMode.TIES_TO_EVEN, status));
    assertEquals(StatusFlags.INVALID, status.bits());

    status.clear();
    Binary128 minimumSubnormal = Binary128.fromRawBits(0L, 1L);
    assertEquals(minimumSubnormal,
        DpmlInvHyper.asinh(minimumSubnormal, RoundingMode.TIES_TO_EVEN, status));
    assertEquals(StatusFlags.DENORMAL, status.bits());
  }

  private static Binary128 evaluate(IntelF128Oracle.Case vector) {
    StatusFlags status = new StatusFlags();
    RoundingMode mode = RoundingMode.fromIntel(vector.rnd);
    switch (vector.op) {
      case "log":
        return DpmlLog.log(vector.x, mode, status);
      case "log2":
        return DpmlLog.log2(vector.x, mode, status);
      case "log10":
        return DpmlLog.log10(vector.x, mode, status);
      case "log1p":
        return DpmlLog.log1p(vector.x, mode, status);
      case "asinh":
        return DpmlInvHyper.asinh(vector.x, mode, status);
      case "acosh":
        return DpmlInvHyper.acosh(vector.x, mode, status);
      case "atanh":
        return DpmlInvHyper.atanh(vector.x, mode, status);
      default:
        throw new IllegalArgumentException(vector.op);
    }
  }

  private static boolean isOwned(String operation) {
    for (String candidate : operations()) {
      if (candidate.equals(operation)) {
        return true;
      }
    }
    return false;
  }

  private static String[] operations() {
    return new String[] {"log", "log2", "log10", "log1p", "asinh", "acosh", "atanh"};
  }

  private static boolean isSpecial(Binary128 value) {
    return value.isNaN() || value.isInfinite() || value.isZero();
  }

  private static String mismatch(IntelF128Oracle.Case vector, Binary128 actual) {
    return vector.op + " x=" + vector.x + " expected=" + vector.expected
        + " actual=" + actual;
  }
}
