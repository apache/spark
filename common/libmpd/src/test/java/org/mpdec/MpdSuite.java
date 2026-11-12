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
package org.mpdec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Random;

import org.junit.jupiter.api.Test;

public class MpdSuite {
  @Test
  public void parseAndFormatBasics() {
    Mpd a = MpdFormat.parse("1.25");
    assertEquals(false, a.isNegative());
    assertEquals(125L, a.data[0]);
    assertEquals(-2, a.exp);
    assertEquals(3, a.digits);

    Mpd z = MpdFormat.parse("-0");
    assertTrue(z.isZero());
    assertTrue(z.isNegative());

    Mpd inf = MpdFormat.parse("-Inf");
    assertTrue(inf.isInfinite());
    assertTrue(inf.isNegative());
  }

  @Test
  public void cohortsPreservedOnAddSameQuantum() {
    MpdContext ctx = MpdContext.ieee(16);
    Mpd a = MpdFormat.parse("1.230");
    Mpd b = MpdFormat.parse("4.560");
    Mpd r = MpdMath.add(a, b, ctx);
    // 1.230 + 4.560 = 5.790
    assertEquals(-3, r.exp);
    assertEquals(4, r.digits);
  }

  @Test
  public void addMulDivMatchBigDecimal() {
    MpdContext ctx = MpdContext.ieee(34);
    MathContext mc = new MathContext(34, RoundingMode.HALF_EVEN);
    String[][] pairs = {
        {"1.5", "2.5"},
        {"9999999999999999", "1"},
        {"1.234567890123456789", "9.876543210987654321"},
        {"-3.5", "2.25"},
        {"1E-10", "2E-10"},
        {"1.0E20", "3.5E-5"}
    };
    for (String[] p : pairs) {
      Mpd a = MpdFormat.parse(p[0]);
      Mpd b = MpdFormat.parse(p[1]);
      BigDecimal ba = new BigDecimal(p[0]);
      BigDecimal bb = new BigDecimal(p[1]);

      assertBd(MpdMath.add(a, b, ctx), ba.add(bb, mc), mc);
      assertBd(MpdMath.sub(a, b, ctx), ba.subtract(bb, mc), mc);
      assertBd(MpdMath.mul(a, b, ctx), ba.multiply(bb, mc), mc);
      if (bb.signum() != 0) {
        assertBd(MpdMath.div(a, b, ctx), ba.divide(bb, mc), mc);
      }
    }
  }

  @Test
  public void randomOpsMatchBigDecimal() {
    MpdContext ctx = MpdContext.ieee(16);
    MathContext mc = new MathContext(16, RoundingMode.HALF_EVEN);
    Random rnd = new Random(1L);
    for (int i = 0; i < 200; i++) {
      BigDecimal ba = randomBd(rnd, 16);
      BigDecimal bb = randomBd(rnd, 16);
      if (bb.signum() == 0) {
        bb = BigDecimal.ONE;
      }
      Mpd a = MpdFormat.parse(ba.toString());
      Mpd b = MpdFormat.parse(bb.toString());
      assertBd(MpdMath.add(a, b, ctx), ba.add(bb, mc), mc);
      assertBd(MpdMath.mul(a, b, ctx), ba.multiply(bb, mc), mc);
      assertBd(MpdMath.div(a, b, ctx), ba.divide(bb, mc), mc);
    }
  }


  @Test
  public void sqrtMatchBigDecimal() {
    MpdContext ctx = MpdContext.ieee(34);
    MathContext mc = new MathContext(34, RoundingMode.HALF_EVEN);
    String[] vals = {
        "1", "2", "1.2345", "9.876543210987654321E20", "1E-20", "100"
    };
    for (String v : vals) {
      Mpd a = MpdFormat.parse(v);
      BigDecimal ba = new BigDecimal(v);
      assertBd(MpdMath.sqrt(a, ctx), ba.sqrt(mc), mc);
    }
  }

  @Test
  public void specials() {
    MpdContext ctx = MpdContext.ieee(16);
    Mpd inf = MpdFormat.parse("Inf");
    Mpd one = MpdFormat.parse("1");
    Mpd r = MpdMath.add(inf, one, ctx);
    assertTrue(r.isInfinite());
    Mpd nan = MpdMath.mul(inf, MpdFormat.parse("0"), ctx);
    assertTrue(nan.isNan());
  }

  private static BigDecimal randomBd(Random rnd, int prec) {
    StringBuilder sb = new StringBuilder();
    if (rnd.nextBoolean()) {
      sb.append('-');
    }
    sb.append(rnd.nextInt(9) + 1);
    int rem = rnd.nextInt(prec);
    for (int i = 0; i < rem; i++) {
      sb.append(rnd.nextInt(10));
    }
    int exp = rnd.nextInt(21) - 10;
    return new BigDecimal(sb.toString() + "E" + exp);
  }

  private static void assertBd(Mpd got, BigDecimal expect, MathContext mc) {
    BigDecimal g = MpdFormat.toBigDecimal(got).round(mc);
    BigDecimal e = expect.round(mc);
    assertEquals(0, g.compareTo(e),
        () -> "got=" + got + " expect=" + e);
  }
}
