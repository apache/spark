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

import java.io.IOException;
import java.util.List;

/** Intel-vector and smoke tests for the raw BID64/BID128 surface. */
public final class BidRawApiTest {
  private BidRawApiTest() {
  }

  public static void main(String[] args) throws IOException {
    testRawMatchesObjectAdd();
    testCompatAdd();
    testFromInt64Vectors();
    testFromInt64ToBid128Vectors();
    testToInt64Vectors();
    testFromStringVectors();
    testToStringVectors();
    testToBid128Vectors();
    testToBid64Vectors();
    testAdapters();
    testRoundIntegralZero();
    testQuantizeSmoke();
    testNextUpZero();
    testDpdRoundTrip();
    testFmaFinite();
    testRoundIntegralVectors();
    testNextUpVectors();
    testQuantizeVectors();
    testBinary64Vectors();
    testBinary64ToBid128Vectors();
    testBinary32Vectors();
    testBid64ToBinary64Vectors();
    testBid128ToBinary64Vectors();
    testToBinary32Vectors();
    testDpdVectors();
    System.out.println("BidRawApiTest: all tests passed");
  }

  private static void testRawMatchesObjectAdd() {
    StatusFlags flags = new StatusFlags();
    long x = Bid64.parseExact("1.25").toRawBits();
    long y = Bid64.parseExact("2.5").toRawBits();
    long raw = Bid64Raw.add(x, y, RoundingMode.TIES_TO_EVEN, flags);
    long object = Bid64.fromRawBits(x)
        .add(Bid64.fromRawBits(y), RoundingMode.TIES_TO_EVEN, new StatusFlags())
        .toRawBits();
    check(raw == object, "raw add matches object");
  }

  private static void testCompatAdd() {
    long x = Bid64.parseExact("10").toRawBits();
    long y = Bid64.parseExact("3").toRawBits();
    long sum = DecFloat16Compat.bid64Add(x, y, 0);
    check(Bid64Raw.quietEqual(sum, Bid64.parseExact("13").toRawBits(), new StatusFlags()),
        "compat add");
    check(DecFloat16Compat.bid64Compare(x, y) > 0, "dbr compare");
    check(DecFloatAdapters.equals64(
        Bid64.parseExact("-0").toRawBits(),
        Bid64.parseExact("0").toRawBits()), "signed zeros equal");
  }

  private static void testFromInt64Vectors() throws IOException {
    List<String> lines = IntelVectors.lines("bid64_from_int64");
    int tested = 0;
    for (String line : lines) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5 || !IntelVectors.isHexPayload(tokens[3])) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long input = Long.parseLong(tokens[2]);
      long expected = IntelVectors.hex64(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Raw.fromInt64(input, mode, flags);
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "from_int64 %s: expected [0x%016x] %02x, actual [0x%016x] %02x",
            line, expected, expectedFlags, actual, flags.bits()));
      }
      tested++;
    }
    if (tested < 20) {
      throw new AssertionError("too few from_int64 hex vectors: " + tested);
    }
  }

  private static void testToBid128Vectors() throws IOException {
    List<String> lines = IntelVectors.lines("bid64_to_bid128");
    int tested = 0;
    for (String line : lines) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5 || !IntelVectors.isHexPayload(tokens[3])) {
        continue;
      }
      long input = IntelVectors.hex64(tokens[2]);
      long[] expected;
      if (IntelVectors.isHexPayload(tokens[3])) {
        expected = IntelVectors.hex128(tokens[3]);
      } else {
        Bid128 value = Bid128.parseExact(tokens[3]);
        expected = new long[] {value.highBits(), value.lowBits()};
      }
      int expectedFlags = IntelVectors.flags(tokens[4]);
      long[] actual = new long[2];
      StatusFlags flags = new StatusFlags();
      Bid64Raw.toBid128(input, actual, flags);
      if (actual[0] != expected[0] || actual[1] != expected[1]
          || flags.bits() != expectedFlags) {
        throw new AssertionError("bid64_to_bid128 " + line);
      }
      tested++;
    }
    if (tested < 50) {
      throw new AssertionError("too few bid64_to_bid128 vectors: " + tested);
    }
  }

  private static void testFromInt64ToBid128Vectors() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid128_from_int64")) {
      String[] tokens = IntelVectors.tokens(line);
      long input = Long.parseLong(tokens[2]);
      long[] expected = IntelVectors.hex128(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      long[] actual = new long[2];
      StatusFlags flags = new StatusFlags();
      Bid128Raw.fromInt64(input, IntelVectors.mode(tokens[1]), flags, actual);
      if (actual[0] != expected[0] || actual[1] != expected[1]
          || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "bid128_from_int64 %s actual [0x%016x%016x] %02x",
            line, actual[0], actual[1], flags.bits()));
      }
      tested++;
    }
    check(tested == 20, "unexpected bid128_from_int64 vector count");
  }

  private static void testToInt64Vectors() throws IOException {
    int tested = 0;
    for (String suffix : new String[] {"int", "xint"}) {
      for (String line : IntelVectors.lines("bid64_to_int64_" + suffix)) {
        String[] tokens = IntelVectors.tokens(line);
        long input = parse64Operand(tokens[2]);
        long expected = Long.parseLong(tokens[3]);
        int expectedFlags = IntelVectors.flags(tokens[4]);
        StatusFlags flags = new StatusFlags();
        long actual = suffix.equals("int")
            ? Bid64Raw.toInt64Int(input, flags)
            : Bid64Raw.toInt64Xint(input, flags);
        if (actual != expected || flags.bits() != expectedFlags) {
          throw new AssertionError(
              line + ": actual " + actual + "/" + flags.bits());
        }
        tested++;
      }
      for (String line : IntelVectors.lines("bid128_to_int64_" + suffix)) {
        String[] tokens = IntelVectors.tokens(line);
        long[] input;
        if (IntelVectors.isHexPayload(tokens[2])) {
          input = IntelVectors.hex128(tokens[2]);
        } else if (tokens[2].equalsIgnoreCase("QNaN")) {
          input = new long[] {
            Bid128.QUIET_NAN.highBits(),
            Bid128.QUIET_NAN.lowBits()
          };
        } else {
          Bid128 value = Bid128.parseExact(tokens[2]);
          input = new long[] {value.highBits(), value.lowBits()};
        }
        long expected = Long.parseLong(tokens[3]);
        int expectedFlags = IntelVectors.flags(tokens[4]);
        StatusFlags flags = new StatusFlags();
        long actual = suffix.equals("int")
            ? Bid128Raw.toInt64Int(input[0], input[1], flags)
            : Bid128Raw.toInt64Xint(input[0], input[1], flags);
        if (actual != expected || flags.bits() != expectedFlags) {
          throw new AssertionError(
              line + ": actual " + actual + "/" + flags.bits());
        }
        tested++;
      }
    }
    check(tested == 1351, "unexpected to_int64 truncation vector count");
  }

  private static void testFromStringVectors() throws IOException {
    int bid64 = 0;
    for (String line : IntelVectors.lines("bid64_from_string")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long expected = parse64Operand(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Raw.fromString(tokens[2], mode, flags);
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "bid64_from_string %s actual [0x%016x] %02x",
            line, actual, flags.bits()));
      }
      bid64++;
    }
    int bid128 = 0;
    for (String line : IntelVectors.lines("bid128_from_string")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] expected;
      if (IntelVectors.isHexPayload(tokens[3])) {
        expected = IntelVectors.hex128(tokens[3]);
      } else {
        Bid128 value = Bid128.parseExact(tokens[3]);
        expected = new long[] {value.highBits(), value.lowBits()};
      }
      int expectedFlags = IntelVectors.flags(tokens[4]);
      long[] actual = new long[2];
      StatusFlags flags = new StatusFlags();
      Bid128Raw.fromString(tokens[2], mode, flags, actual);
      if (actual[0] != expected[0] || actual[1] != expected[1]
          || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "bid128_from_string %s actual [0x%016x%016x] %02x",
            line, actual[0], actual[1], flags.bits()));
      }
      bid128++;
    }
    check(bid64 == 73 && bid128 == 70, "unexpected from_string vector counts");
  }

  private static void testToStringVectors() throws IOException {
    int bid64 = 0;
    for (String line : IntelVectors.lines("bid64_to_string")) {
      String[] tokens = IntelVectors.tokens(line);
      String actual = Bid64Raw.toString(IntelVectors.hex64(tokens[2]));
      if (!actual.equals(tokens[3])) {
        throw new AssertionError(line + ": actual " + actual);
      }
      bid64++;
    }
    int bid128 = 0;
    for (String line : IntelVectors.lines("bid128_to_string")) {
      String[] tokens = IntelVectors.tokens(line);
      long[] input = IntelVectors.hex128(tokens[2]);
      String actual = Bid128Raw.toString(input[0], input[1]);
      if (!actual.equals(tokens[3])) {
        throw new AssertionError(line + ": actual " + actual);
      }
      bid128++;
    }
    check(bid64 == 12 && bid128 == 31, "unexpected to_string vector counts");
  }

  private static void testAdapters() {
    long nan = Bid64.QUIET_NAN.toRawBits();
    check(DecFloatAdapters.compare64(nan, Bid64.parseExact("1").toRawBits()) > 0,
        "nan greatest");
    check(DecFloatAdapters.canonicalize64(Bid64.parseExact("150").toRawBits())
            == Bid64.parseExact("15E1").toRawBits()
            || DecFloatAdapters.equals64(
                DecFloatAdapters.canonicalize64(Bid64.parseExact("150").toRawBits()),
                Bid64.parseExact("15E1").toRawBits()),
        "canonicalize cohort");
    check(DecFloatAdapters.sign64(Bid64.parseExact("-4").toRawBits())
            == Bid64.parseExact("-1").toRawBits(),
        "sign");
    check(DecFloat16Compat.bid64IsInf(Bid64.POSITIVE_INFINITY.toRawBits()), "is inf");
    long[] decimal128 = new long[2];
    int[] status = {0};
    DecFloatAdapters.fromDecimal128(-1L, -5L, 0, 0, decimal128, status);
    check(Bid128.fromRawBits(decimal128[0], decimal128[1])
        .quietEqual(Bid128.parseExact("-5"), new StatusFlags()), "negative decimal128 input");
    long[] unscaled = new long[2];
    int scale = DecFloatAdapters.toDecimal128(
        decimal128[0], decimal128[1], unscaled, status);
    check(scale == 0 && unscaled[0] == -1L && unscaled[1] == -5L,
        "negative decimal128 round trip");
    StatusFlags flags = new StatusFlags();
    check(Bid64.parseExact("5").remainder(Bid64.parseExact("2"), flags)
            .quietEqual(Bid64.parseExact("1"), new StatusFlags()),
        "object remainder");
  }

  private static void testToBid64Vectors() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid128_to_bid64")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] input;
      if (IntelVectors.isHexPayload(tokens[2])) {
        input = IntelVectors.hex128(tokens[2]);
      } else {
        Bid128 value = Bid128.parseExact(tokens[2]);
        input = new long[] {value.highBits(), value.lowBits()};
      }
      long expected = IntelVectors.hex64(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      if (line.contains("underflow_before_only")) {
        expectedFlags &= ~StatusFlags.UNDERFLOW;
      }
      StatusFlags flags = new StatusFlags();
      long actual = Bid128Raw.toBid64(input[0], input[1], mode, flags);
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "bid128_to_bid64 %s actual [0x%016x] %02x",
            line, actual, flags.bits()));
      }
      tested++;
    }
    check(tested == 60, "unexpected bid128_to_bid64 vector count");
  }

  private static void testRoundIntegralZero() {
    StatusFlags flags = new StatusFlags();
    long actual = Bid64Raw.roundIntegralZero(
        Bid64.parseExact("1.9").toRawBits(), flags);
    check(Bid64Raw.quietEqual(actual, Bid64.parseExact("1").toRawBits(), new StatusFlags()),
        "round toward zero");
    long floor = Bid64Raw.roundIntegralNegative(
        Bid64.parseExact("-1.1").toRawBits(), new StatusFlags());
    check(Bid64Raw.quietEqual(floor, Bid64.parseExact("-2").toRawBits(), new StatusFlags()),
        "floor");
  }

  private static void testQuantizeSmoke() {
    StatusFlags flags = new StatusFlags();
    long x = Bid64.parseExact("1.234").toRawBits();
    long y = Bid64.parseExact("0.01").toRawBits();
    long q = Bid64Raw.quantize(x, y, RoundingMode.TIES_TO_EVEN, flags);
    check(Bid64Raw.sameQuantum(q, y), "quantize quantum");
  }

  private static void testNextUpZero() {
    StatusFlags flags = new StatusFlags();
    long next = Bid64Raw.nextUp(Bid64.POSITIVE_ZERO.toRawBits(), flags);
    check(next == 1L, "nextup(+0) is min positive");
  }

  private static void testDpdRoundTrip() {
    long x = Bid64.parseExact("1234567890123456").toRawBits();
    long dpd = Bid64Raw.toDpd(x);
    long back = Bid64Raw.fromDpd(dpd);
    check(Bid64Raw.quietEqual(x, back, new StatusFlags()), "dpd round trip");
  }

  private static void testFmaFinite() {
    StatusFlags flags = new StatusFlags();
    long x = Bid64.parseExact("2").toRawBits();
    long y = Bid64.parseExact("3").toRawBits();
    long z = Bid64.parseExact("4").toRawBits();
    long result = Bid64Raw.fma(x, y, z, RoundingMode.TIES_TO_EVEN, flags);
    check(Bid64Raw.quietEqual(result, Bid64.parseExact("10").toRawBits(), new StatusFlags()),
        "fma 2*3+4");
  }

  private static void testRoundIntegralVectors() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_round_integral_zero")) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5 || !IntelVectors.isHexPayload(tokens[2])) {
        continue;
      }
      long input = IntelVectors.hex64(tokens[2]);
      long expected = IntelVectors.hex64(tokens[3]);
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Raw.roundIntegralZero(input, flags);
      if (actual != expected) {
        throw new AssertionError("round_integral_zero " + line
            + " actual " + Long.toHexString(actual));
      }
      tested++;
    }
    if (tested < 10) {
      throw new AssertionError("too few round_integral_zero vectors: " + tested);
    }
  }

  private static void testNextUpVectors() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_nextup")) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5) {
        continue;
      }
      long input = parse64Operand(tokens[2]);
      long expected = IntelVectors.hex64(tokens[3]);
      long actual = Bid64Raw.nextUp(input, new StatusFlags());
      if (actual != expected) {
        throw new AssertionError("nextup " + line + " actual " + Long.toHexString(actual));
      }
      tested++;
    }
    if (tested < 10) {
      throw new AssertionError("too few nextup vectors: " + tested);
    }
  }

  private static void testQuantizeVectors() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_quantize")) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 6 || !IntelVectors.isHexPayload(tokens[2])
          || !IntelVectors.isHexPayload(tokens[4])) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long x = IntelVectors.hex64(tokens[2]);
      long y = parse64Operand(tokens[3]);
      long expected = IntelVectors.hex64(tokens[4]);
      int expectedFlags = IntelVectors.flags(tokens[5]);
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Raw.quantize(x, y, mode, flags);
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "quantize %s actual [0x%016x] %02x", line, actual, flags.bits()));
      }
      tested++;
    }
    if (tested < 5) {
      throw new AssertionError("too few quantize hex vectors: " + tested);
    }
  }

  private static void testBinary64Vectors() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("binary64_to_bid64")) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      double input = IntelVectors.isHexPayload(tokens[2])
          ? Double.longBitsToDouble(IntelVectors.hex64(tokens[2]))
          : Double.parseDouble(tokens[2]);
      long expected = IntelVectors.hex64(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Raw.fromBinary64(input, mode, flags);
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "binary64_to_bid64 %s actual [0x%016x] %02x", line, actual, flags.bits()));
      }
      tested++;
    }
    if (tested != 1529) {
      throw new AssertionError("unexpected binary64_to_bid64 vector count: " + tested);
    }
  }

  private static void testDpdVectors() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid_dpd_to_bid64")) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5) {
        continue;
      }
      long input = IntelVectors.hex64(tokens[2]);
      if ((input & Bid64.MASK_INFINITY) == Bid64.MASK_INFINITY) {
        continue;
      }
      long expected = IntelVectors.hex64(tokens[3]);
      long actual = Bid64Raw.fromDpd(input);
      if (actual != expected) {
        throw new AssertionError("dpd " + line + " actual " + Long.toHexString(actual));
      }
      tested++;
    }
    if (tested < 10) {
      throw new AssertionError("too few dpd vectors: " + tested);
    }
  }

  private static void testToBinary32Vectors() throws IOException {
    int bid64 = 0;
    for (String line : IntelVectors.lines("bid64_to_binary32")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long input = parse64Operand(tokens[2]);
      int expected = (int) IntelVectors.hex64(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      StatusFlags flags = new StatusFlags();
      int actual = Float.floatToRawIntBits(Bid64Raw.toBinary32(input, mode, flags));
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "bid64_to_binary32 %s actual [0x%08x] %02x",
            line, actual, flags.bits()));
      }
      bid64++;
    }
    int bid128 = 0;
    for (String line : IntelVectors.lines("bid128_to_binary32")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] input;
      if (IntelVectors.isHexPayload(tokens[2])) {
        input = IntelVectors.hex128(tokens[2]);
      } else {
        Bid128 value = Bid128.parseExact(tokens[2]);
        input = new long[] {value.highBits(), value.lowBits()};
      }
      int expected = (int) IntelVectors.hex64(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      StatusFlags flags = new StatusFlags();
      int actual = Float.floatToRawIntBits(
          Bid128Raw.toBinary32(input[0], input[1], mode, flags));
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "bid128_to_binary32 %s actual [0x%08x] %02x",
            line, actual, flags.bits()));
      }
      bid128++;
    }
    check(bid64 == 1927 && bid128 == 1957, "unexpected to_binary32 vector counts");
  }

  private static void testBinary64ToBid128Vectors() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("binary64_to_bid128")) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      double input = IntelVectors.isHexPayload(tokens[2])
          ? Double.longBitsToDouble(IntelVectors.hex64(tokens[2]))
          : Double.parseDouble(tokens[2]);
      long[] expected;
      if (IntelVectors.isHexPayload(tokens[3])) {
        expected = IntelVectors.hex128(tokens[3]);
      } else {
        Bid128 value = Bid128.parseExact(tokens[3]);
        expected = new long[] {value.highBits(), value.lowBits()};
      }
      int expectedFlags = IntelVectors.flags(tokens[4]);
      long[] actual = new long[2];
      StatusFlags flags = new StatusFlags();
      Bid128Raw.fromBinary64(input, mode, flags, actual);
      if (actual[0] != expected[0] || actual[1] != expected[1]
          || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "binary64_to_bid128 %s actual [0x%016x%016x] %02x",
            line, actual[0], actual[1], flags.bits()));
      }
      tested++;
    }
    if (tested != 1550) {
      throw new AssertionError("unexpected binary64_to_bid128 vector count: " + tested);
    }
  }

  private static void testBid64ToBinary64Vectors() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_to_binary64")) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long input = parse64Operand(tokens[2]);
      long expected = IntelVectors.hex64(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      StatusFlags flags = new StatusFlags();
      long actual = Double.doubleToRawLongBits(Bid64Raw.toBinary64(input, mode, flags));
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "bid64_to_binary64 %s actual [0x%016x] %02x",
            line, actual, flags.bits()));
      }
      tested++;
    }
    if (tested != 1756) {
      throw new AssertionError("unexpected bid64_to_binary64 vector count: " + tested);
    }
  }

  private static void testBinary32Vectors() throws IOException {
    int bid64 = 0;
    for (String line : IntelVectors.lines("binary32_to_bid64")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      float input = tokens[2].startsWith("[")
          ? Float.intBitsToFloat((int) IntelVectors.hex64(tokens[2]))
          : Float.parseFloat(tokens[2]);
      long expected = IntelVectors.hex64(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Raw.fromBinary32(input, mode, flags);
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "binary32_to_bid64 %s actual [0x%016x] %02x",
            line, actual, flags.bits()));
      }
      bid64++;
    }
    int bid128 = 0;
    for (String line : IntelVectors.lines("binary32_to_bid128")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      float input = tokens[2].startsWith("[")
          ? Float.intBitsToFloat((int) IntelVectors.hex64(tokens[2]))
          : Float.parseFloat(tokens[2]);
      long[] expected;
      if (IntelVectors.isHexPayload(tokens[3])) {
        expected = IntelVectors.hex128(tokens[3]);
      } else {
        Bid128 value = Bid128.parseExact(tokens[3]);
        expected = new long[] {value.highBits(), value.lowBits()};
      }
      int expectedFlags = IntelVectors.flags(tokens[4]);
      long[] actual = new long[2];
      StatusFlags flags = new StatusFlags();
      Bid128Raw.fromBinary32(input, mode, flags, actual);
      if (actual[0] != expected[0] || actual[1] != expected[1]
          || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "binary32_to_bid128 %s actual [0x%016x%016x] %02x",
            line, actual[0], actual[1], flags.bits()));
      }
      bid128++;
    }
    check(bid64 == 1599 && bid128 == 1616, "unexpected binary32 vector counts");
  }

  private static void testBid128ToBinary64Vectors() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid128_to_binary64")) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] input;
      if (IntelVectors.isHexPayload(tokens[2])) {
        input = IntelVectors.hex128(tokens[2]);
      } else {
        Bid128 value = Bid128.parseExact(tokens[2]);
        input = new long[] {value.highBits(), value.lowBits()};
      }
      long expected = IntelVectors.hex64(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      StatusFlags flags = new StatusFlags();
      long actual = Double.doubleToRawLongBits(
          Bid128Raw.toBinary64(input[0], input[1], mode, flags));
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "bid128_to_binary64 %s actual [0x%016x] %02x",
            line, actual, flags.bits()));
      }
      tested++;
    }
    if (tested != 1816) {
      throw new AssertionError("unexpected bid128_to_binary64 vector count: " + tested);
    }
  }

  private static long parse64Operand(String token) {
    if (IntelVectors.isHexPayload(token) && token.contains("[")) {
      return IntelVectors.hex64(token);
    }
    StatusFlags flags = new StatusFlags();
    return Bid64Raw.fromString(token, RoundingMode.TIES_TO_EVEN, flags);
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
