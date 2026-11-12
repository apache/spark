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

import org.junit.jupiter.api.Test;

public class MpdFnsSuite {
  private final MpdContext ctx = MpdContext.ieee(16);
  private final MathContext mc = new MathContext(16, RoundingMode.HALF_EVEN);

  private static void assertNum(Mpd got, String expect) {
    BigDecimal g = new BigDecimal(got.toString());
    BigDecimal e = new BigDecimal(expect);
    assertEquals(0, g.compareTo(e), () -> "got=" + got + " expect=" + expect);
  }

  @Test
  public void unaryAbsPlusMinus() {
    Mpd a = MpdFormat.parse("-1.25");
    assertNum(MpdMath.abs(a, ctx), "1.25");
    assertNum(MpdMath.minus(MpdFormat.parse("1.25"), ctx), "-1.25");
    assertNum(MpdMath.plus(MpdFormat.parse("1.25"), ctx), "1.25");
  }

  @Test
  public void minMax() {
    Mpd a = MpdFormat.parse("2");
    Mpd b = MpdFormat.parse("-3");
    assertNum(MpdMath.max(a, b, ctx), "2");
    assertNum(MpdMath.min(a, b, ctx), "-3");
    assertNum(MpdMath.maxMag(a, b, ctx), "-3");
    assertNum(MpdMath.minMag(a, b, ctx), "2");
  }

  @Test
  public void integerRounding() {
    Mpd a = MpdFormat.parse("2.7");
    Mpd b = MpdFormat.parse("-2.7");
    assertNum(MpdMath.trunc(a, ctx), "2");
    assertNum(MpdMath.trunc(b, ctx), "-2");
    assertNum(MpdMath.floor(a, ctx), "2");
    assertNum(MpdMath.floor(b, ctx), "-3");
    assertNum(MpdMath.ceil(a, ctx), "3");
    assertNum(MpdMath.ceil(b, ctx), "-2");
  }

  @Test
  public void quantizeSameReduce() {
    Mpd a = MpdFormat.parse("1.2345");
    Mpd q = MpdFormat.parse("0.01");
    Mpd r = MpdMath.quantize(a, q, ctx);
    assertEquals(-2, r.exp);
    assertTrue(MpdFns.sameQuantum(r, q));
    assertNum(r, "1.23");
    Mpd wide = MpdFormat.parse("1.200");
    Mpd red = MpdMath.reduce(wide, ctx);
    assertNum(red, "1.2");
    assertTrue(red.digits <= 2);
  }

  @Test
  public void remDivintDivmod() {
    Mpd a = MpdFormat.parse("10");
    Mpd b = MpdFormat.parse("3");
    assertNum(MpdMath.divint(a, b, ctx), "3");
    assertNum(MpdMath.rem(a, b, ctx), "1");
    Mpd[] dm = MpdMath.divmod(a, b, ctx);
    assertNum(dm[0], "3");
    assertNum(dm[1], "1");
  }

  @Test
  public void scalebLogb() {
    Mpd a = MpdFormat.parse("1.25");
    Mpd n = MpdFormat.parse("2");
    assertNum(MpdMath.scaleb(a, n, ctx), "125");
    assertNum(MpdMath.logb(a, ctx), "0");
    assertNum(MpdMath.logb(MpdFormat.parse("125"), ctx), "2");
  }

  @Test
  public void expLnLog10Pow() {
    Mpd one = MpdFormat.parse("1");
    assertNum(MpdMath.exp(MpdFormat.parse("0"), ctx), "1");
    assertTrue(MpdMath.ln(one, ctx).isZero());

    Mpd eApprox = MpdMath.exp(one, ctx);
    BigDecimal expected = BigDecimal.valueOf(Math.E).round(mc);
    BigDecimal got = new BigDecimal(eApprox.toString());
    assertTrue(
        expected.subtract(got).abs().compareTo(new BigDecimal("1E-12")) < 0,
        () -> "exp(1)=" + eApprox);

    Mpd log = MpdMath.log10(MpdFormat.parse("10"), ctx);
    assertTrue(
        new BigDecimal(log.toString()).subtract(BigDecimal.ONE).abs()
            .compareTo(new BigDecimal("1E-10")) < 0,
        () -> "log10(10)=" + log);

    assertNum(MpdMath.pow(
        MpdFormat.parse("2"), MpdFormat.parse("3"), ctx), "8");
    assertNum(MpdMath.powmod(
        MpdFormat.parse("2"), MpdFormat.parse("3"),
        MpdFormat.parse("5"), ctx), "3");
  }

  @Test
  public void remNearBasics() {
    assertNum(MpdMath.remNear(
        MpdFormat.parse("5"), MpdFormat.parse("3"), ctx), "-1");
  }

  @Test
  public void powmodIntegerContract() {
    // integral-valued decimals are accepted (2.0 is an integer)
    assertNum(MpdMath.powmod(MpdFormat.parse("2.0"), MpdFormat.parse("3"),
        MpdFormat.parse("5"), ctx), "3");
    assertNum(MpdMath.powmod(MpdFormat.parse("2"), MpdFormat.parse("3.00"),
        MpdFormat.parse("5"), ctx), "3");
    // non-integers are rejected with NaN + Invalid_operation
    Mpd bad = MpdMath.powmod(MpdFormat.parse("2.5"), MpdFormat.parse("3"),
        MpdFormat.parse("5"), ctx);
    assertTrue(bad.isNan());
    MpdContext c2 = MpdContext.ieee(16);
    MpdMath.powmod(MpdFormat.parse("2"), MpdFormat.parse("0.5"),
        MpdFormat.parse("5"), c2);
    assertTrue(c2.status.test(MpdStatus.INVALID_OPERATION));
    // zero modulus rejected
    assertTrue(MpdMath.powmod(MpdFormat.parse("2"), MpdFormat.parse("3"),
        MpdFormat.parse("0"), ctx).isNan());
  }

  @Test
  public void powIntegralDecimalExponent() {
    assertNum(MpdMath.pow(MpdFormat.parse("2"), MpdFormat.parse("3.0"),
        ctx), "8");
    assertNum(MpdMath.pow(MpdFormat.parse("2"), MpdFormat.parse("-2.00"),
        ctx), "0.25");
  }
}
