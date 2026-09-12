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

/** Regression tests for bounded parsing and Spark adapter conversions. */
public final class BidParserAdapterTest {
  private BidParserAdapterTest() {
  }

  public static void main(String[] args) {
    testMalformedAndExtremeParsing();
    testExactBoundaryCohorts();
    testTiesAwayBinaryConversion();
    testCoarseRoundToScale();
    testTargetDecimalTypes();
    testRoundingCodes();
    System.out.println("BidParserAdapterTest: all tests passed");
  }

  private static void testMalformedAndExtremeParsing() {
    for (String text : new String[] {"", "+", ".", "abc", "1..0", "1e", "NaNjunk"}) {
      StatusFlags flags = new StatusFlags();
      long value = Bid64Raw.fromString(text, RoundingMode.TIES_TO_EVEN, flags);
      check(Bid64Raw.isNaN(value), "malformed input result: " + text);
      check(flags.contains(StatusFlags.INVALID), "malformed input status: " + text);
    }

    StatusFlags flags = new StatusFlags();
    long overflow = Bid64Raw.fromString(
        "1E2147483647", RoundingMode.TIES_TO_EVEN, flags);
    check(Bid64Raw.isInf(overflow), "huge positive exponent");
    check((flags.bits() & (StatusFlags.OVERFLOW | StatusFlags.INEXACT))
            == (StatusFlags.OVERFLOW | StatusFlags.INEXACT),
        "huge positive exponent status");

    flags.clear();
    long underflow = Bid64Raw.fromString(
        "1E-2147483648", RoundingMode.TIES_TO_EVEN, flags);
    check(Bid64Raw.isZero(underflow), "huge negative exponent");
    check((flags.bits() & (StatusFlags.UNDERFLOW | StatusFlags.INEXACT))
            == (StatusFlags.UNDERFLOW | StatusFlags.INEXACT),
        "huge negative exponent status");

    flags.clear();
    long nearIntegerLimit = Bid64Raw.fromString(
        "1E2147483250", RoundingMode.TIES_TO_EVEN, flags);
    check(Bid64Raw.isInf(nearIntegerLimit), "large exponent before integer limit");
    check((flags.bits() & (StatusFlags.OVERFLOW | StatusFlags.INEXACT))
            == (StatusFlags.OVERFLOW | StatusFlags.INEXACT),
        "large exponent before integer limit status");

    String huge = "1" + "0".repeat(100_000) + "1E-100000";
    flags.clear();
    Bid64Raw.fromString(huge, RoundingMode.TIES_TO_EVEN, flags);
    check(flags.contains(StatusFlags.INEXACT), "bounded long significand");
  }

  private static void testExactBoundaryCohorts() {
    check(Bid64.parseExact("1E+370").quietEqual(
        Bid64.parseExact("10E+369"), new StatusFlags()), "bid64 upper cohort");
    check(Bid64.parseExact("10E-399").quietEqual(
        Bid64.parseExact("1E-398"), new StatusFlags()), "bid64 lower cohort");
    check(Bid128.parseExact("1E+6112").quietEqual(
        Bid128.parseExact("10E+6111"), new StatusFlags()), "bid128 upper cohort");
    check(Bid128.parseExact("10E-6177").quietEqual(
        Bid128.parseExact("1E-6176"), new StatusFlags()), "bid128 lower cohort");
  }

  private static void testTiesAwayBinaryConversion() {
    StatusFlags flags = new StatusFlags();
    double even = Bid64.parseExact("9007199254740993")
        .toDouble(RoundingMode.TIES_TO_EVEN, flags);
    flags.clear();
    double away = Bid64.parseExact("9007199254740993")
        .toDouble(RoundingMode.TIES_AWAY, flags);
    check(even == 9_007_199_254_740_992.0, "binary64 ties-even");
    check(away == 9_007_199_254_740_994.0, "binary64 ties-away");
    check(flags.contains(StatusFlags.INEXACT), "binary64 midpoint status");

    flags.clear();
    float even32 = Bid64.parseExact("16777217")
        .toFloat(RoundingMode.TIES_TO_EVEN, flags);
    flags.clear();
    float away32 = Bid64.parseExact("16777217")
        .toFloat(RoundingMode.TIES_AWAY, flags);
    check(even32 == 16_777_216.0f, "binary32 ties-even");
    check(away32 == 16_777_218.0f, "binary32 ties-away");
    check(flags.contains(StatusFlags.INEXACT), "binary32 midpoint status");

    double negative = Bid64.parseExact("-9007199254740993")
        .toDouble(RoundingMode.TIES_AWAY, new StatusFlags());
    check(negative == -9_007_199_254_740_994.0, "negative ties-away");
  }

  private static void testCoarseRoundToScale() {
    int[] status = {0};
    long zero = DecFloatAdapters.roundToScale64(
        Bid64.parseExact("12345").toRawBits(), 370, 0, status);
    check(Bid64Raw.isZero(zero), "coarse bid64 result");
    check((status[0] & StatusFlags.INEXACT) != 0, "coarse bid64 status");

    status[0] = 0;
    long up = DecFloatAdapters.roundToScale64(
        Bid64.parseExact("12345").toRawBits(), 370, 2, status);
    check(Bid64.fromRawBits(up).quietEqual(
        Bid64.parseExact("1E370"), new StatusFlags()), "coarse directed bid64");

    status[0] = 0;
    long extreme = DecFloatAdapters.roundToScale64(
        Bid64.parseExact("12345").toRawBits(), Long.MAX_VALUE, 0, status);
    check(Bid64Raw.isZero(extreme), "extreme coarse bid64 result");

    long negativeZero = Bid64.parseExact("-0").toRawBits();
    check(DecFloatAdapters.roundToScale64(negativeZero, Long.MAX_VALUE, 0, status)
        == negativeZero, "signed zero preserved");
    long nan = Bid64.fromRawBits(Bid64.MASK_SIGN | Bid64.MASK_NAN | 7).toRawBits();
    check(DecFloatAdapters.roundToScale64(nan, Long.MAX_VALUE, 0, status) == nan,
        "NaN payload preserved");

    Bid128 input = Bid128.parseExact("12345");
    long[] out = new long[2];
    status[0] = 0;
    DecFloatAdapters.roundToScale128(
        input.highBits(), input.lowBits(), 6112, 0, out, status);
    check(Bid128Raw.isZero(out[0], out[1]), "coarse bid128 result");
    check((status[0] & StatusFlags.INEXACT) != 0, "coarse bid128 status");
  }

  private static void testTargetDecimalTypes() {
    long[] unscaled = new long[2];
    int[] status = {0};
    int scale = DecFloatAdapters.toDecimal64(
        Bid64.parseExact("1E-350").toRawBits(), 38, 38, 0, unscaled, status);
    check(scale == 38 && unscaled[0] == 0L && unscaled[1] == 0L,
        "tiny value rounded to Decimal(38,38)");
    check((status[0] & StatusFlags.INEXACT) != 0, "tiny Decimal status");

    status[0] = 0;
    DecFloatAdapters.toDecimal64(
        Bid64.parseExact("1").toRawBits(), 38, 39, 0, unscaled, status);
    check((status[0] & StatusFlags.INVALID) != 0, "illegal Decimal scale");

    status[0] = 0;
    DecFloatAdapters.toDecimal64(
        Bid64.parseExact("1E100").toRawBits(), 38, 0, 0, unscaled, status);
    check((status[0] & StatusFlags.INVALID) != 0, "Decimal precision overflow");

    status[0] = 0;
    DecFloatAdapters.toDecimal64(
        Bid64.parseExact("12345").toRawBits(), 38, -400, 2, unscaled, status);
    check((status[0] & StatusFlags.INVALID) != 0, "coarse negative Decimal scale");

    Bid128 negativeLowZero = Bid128.finite(true, 6176, 1L, 0L);
    status[0] = 0;
    scale = DecFloatAdapters.toDecimal128(
        negativeLowZero.highBits(),
        negativeLowZero.lowBits(),
        38,
        0,
        0,
        unscaled,
        status);
    check(scale == 0 && unscaled[0] == -1L && unscaled[1] == 0L,
        "negative decimal128 low-zero coefficient");

    status[0] = 0;
    Bid128 coarse128 = Bid128.parseExact("12345");
    DecFloatAdapters.toDecimal128(
        coarse128.highBits(),
        coarse128.lowBits(),
        38,
        -6200,
        2,
        unscaled,
        status);
    check((status[0] & StatusFlags.INVALID) != 0, "coarse negative Decimal128 scale");
  }

  private static void testRoundingCodes() {
    for (int code = 0; code <= 4; code++) {
      check(RoundingMode.fromIntel(code).toIntel() == code, "decimal rounding code");
      check(org.bidfp.binary128.RoundingMode.fromIntel(code).toIntel() == code,
          "binary128 rounding code");
    }
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new IllegalStateException(message);
    }
  }
}
