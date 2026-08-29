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
package org.bidfp;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Random;

/** Tests for Spark-relevant BID64 integer and text conversions. */
public final class Bid64ConversionTest {
  private Bid64ConversionTest() {
  }

  public static void main(String[] args) {
    testLongToBid64();
    testBid64ToLong();
    testRandomLongRounding();
    System.out.println("Bid64ConversionTest: all tests passed");
  }

  private static void testLongToBid64() {
    StatusFlags flags = new StatusFlags();
    Bid64 exact = Bid64.fromLong(9_007_199_254_740_991L, RoundingMode.TIES_TO_EVEN, flags);
    check(!flags.contains(StatusFlags.INEXACT), "16-digit long exact");
    check(exact.toLong(RoundingMode.TIES_TO_EVEN, new StatusFlags())
        == 9_007_199_254_740_991L, "exact long round trip");

    flags.clear();
    Bid64 rounded =
        Bid64.fromLong(9_223_372_036_854_775_807L, RoundingMode.TIES_TO_EVEN, flags);
    check(flags.contains(StatusFlags.INEXACT), "19-digit long rounded");
    check(
        toBigDecimal(rounded).compareTo(new BigDecimal("9.223372036854776E18")) == 0,
        "ties-even long rounding");

    flags.clear();
    Bid64 minimum = Bid64.fromLong(Long.MIN_VALUE, RoundingMode.TOWARD_ZERO, flags);
    check(toBigDecimal(minimum).compareTo(new BigDecimal("-9.223372036854775E18")) == 0,
        "minimum long toward zero");
  }

  private static void testBid64ToLong() {
    check(Bid64.parseExact("1.5").toLong(RoundingMode.TIES_TO_EVEN, new StatusFlags()) == 2,
        "positive tie");
    check(Bid64.parseExact("2.5").toLong(RoundingMode.TIES_TO_EVEN, new StatusFlags()) == 2,
        "even tie");
    check(Bid64.parseExact("-1.1").toLong(RoundingMode.TOWARD_NEGATIVE, new StatusFlags()) == -2,
        "floor");
    check(Bid64.parseExact("-1.9").toLong(RoundingMode.TOWARD_ZERO, new StatusFlags()) == -1,
        "truncate");
    check(Bid64.parseExact("0.1").toLong(RoundingMode.TOWARD_POSITIVE, new StatusFlags()) == 1,
        "ceiling");

    boolean rejected = false;
    StatusFlags flags = new StatusFlags();
    try {
      Bid64.POSITIVE_INFINITY.toLong(RoundingMode.TIES_TO_EVEN, flags);
    } catch (ArithmeticException expected) {
      rejected = true;
    }
    check(rejected && flags.contains(StatusFlags.INVALID), "infinity to long");
  }

  private static void testRandomLongRounding() {
    Random random = new Random(0x1dec64L);
    for (int i = 0; i < 20_000; i++) {
      long value = random.nextLong();
      StatusFlags flags = new StatusFlags();
      Bid64 actual = Bid64.fromLong(value, RoundingMode.TIES_TO_EVEN, flags);
      BigDecimal expected = BigDecimal.valueOf(value).round(MathContext.DECIMAL64);
      if (toBigDecimal(actual).compareTo(expected) != 0) {
        throw new AssertionError(
            "fromLong(" + value + "): expected " + expected + ", actual " + actual);
      }
    }
  }

  private static BigDecimal toBigDecimal(Bid64 value) {
    BigDecimal result =
        BigDecimal.valueOf(value.significand()).scaleByPowerOfTen(value.biasedExponent() - 398);
    return value.isSigned() ? result.negate() : result;
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
