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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Intel QUAD UX oracle gate for the pow and cbrt families. */
final class DpmlPowCbrtOracleTest {
  private static final int MAX_ULP = 2;

  public static void main(String[] args) throws IOException {
    new DpmlPowCbrtOracleTest().powAndCbrtMatchIntel();
  }

  @Test
  void powAndCbrtMatchIntel() throws IOException {
    int checked = 0;
    int maxUlp = 0;
    for (IntelF128Oracle.Case c : IntelF128Oracle.load()) {
      if (!"pow".equals(c.op) && !"cbrt".equals(c.op)) {
        continue;
      }
      StatusFlags status = new StatusFlags();
      RoundingMode mode = RoundingMode.fromIntel(c.rnd);
      Binary128 got = "pow".equals(c.op)
          ? DpmlPow.pow(c.x, c.y, mode, status)
          : DpmlCbrt.cbrt(c.x, mode, status);
      if (isSpecial(c.expected)) {
        assertTrue(c.expected.equals(got), mismatch(c, got));
      } else {
        int ulp = IntelF128Oracle.ulpDistance(c.expected, got);
        maxUlp = Integer.max(maxUlp, ulp);
        assertTrue(ulp <= MAX_ULP, mismatch(c, got) + " ulp=" + ulp);
      }
      checked++;
    }
    assertTrue(checked > 0, "no pow/cbrt vectors");
    System.out.println("Intel pow/cbrt vectors=" + checked + " maxUlp=" + maxUlp);
  }

  private static boolean isSpecial(Binary128 value) {
    return value.isNaN() || value.isInfinite() || value.isZero();
  }

  private static String mismatch(IntelF128Oracle.Case c, Binary128 got) {
    return c.op + " x=" + c.x + (c.y == null ? "" : " y=" + c.y)
        + " expected=" + c.expected + " got=" + got;
  }
}
