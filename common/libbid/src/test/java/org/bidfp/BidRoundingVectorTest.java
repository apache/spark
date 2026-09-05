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

/** Runs all Intel BID64/BID128 round-integral vectors. */
public final class BidRoundingVectorTest {
  private static final String[] OPERATIONS = {
    "round_integral_zero",
    "round_integral_negative",
    "round_integral_positive",
    "round_integral_nearest_even",
    "round_integral_nearest_away",
    "round_integral_exact"
  };

  private BidRoundingVectorTest() {
  }

  public static void main(String[] args) throws IOException {
    int bid64 = test64();
    int bid128 = test128();
    int quantize = testQuantize();
    int sqrt = testSqrt();
    System.out.println(
        "BidRoundingVectorTest: all tests passed (" + bid64 + " BID64, "
            + bid128 + " BID128 round, " + quantize + " quantize, "
            + sqrt + " sqrt)");
  }

  private static int test64() throws IOException {
    int tested = 0;
    for (String operation : OPERATIONS) {
      for (String line : IntelVectors.lines("bid64_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long input = operand64(tokens[2]);
        long expected = IntelVectors.hex64(tokens[3]);
        int expectedFlags = IntelVectors.flags(tokens[4]);
        StatusFlags flags = new StatusFlags();
        long actual = round64(operation, input, IntelVectors.mode(tokens[1]), flags);
        if (actual != expected || flags.bits() != expectedFlags) {
          throw new IllegalStateException(String.format(
              "%s actual [0x%016x] %02x", line, actual, flags.bits()));
        }
        tested++;
      }
    }
    return tested;
  }

  private static int test128() throws IOException {
    int tested = 0;
    for (String operation : OPERATIONS) {
      for (String line : IntelVectors.lines("bid128_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long[] input = operand128(tokens[2]);
        long[] expected = operand128(tokens[3]);
        int expectedFlags = IntelVectors.flags(tokens[4]);
        StatusFlags flags = new StatusFlags();
        long[] actual = new long[2];
        round128(operation, input, IntelVectors.mode(tokens[1]), flags, actual);
        if (actual[0] != expected[0] || actual[1] != expected[1]
            || flags.bits() != expectedFlags) {
          throw new IllegalStateException(String.format(
              "%s actual [0x%016x%016x] %02x",
              line, actual[0], actual[1], flags.bits()));
        }
        tested++;
      }
    }
    return tested;
  }

  private static int testQuantize() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_quantize")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long x = operand64(tokens[2]);
      long y = operand64(tokens[3]);
      long expected = operand64(tokens[4]);
      int expectedFlags = IntelVectors.flags(tokens[5]);
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Raw.quantize(x, y, mode, flags);
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new IllegalStateException(String.format(
            "%s actual [0x%016x] %02x", line, actual, flags.bits()));
      }
      tested++;
    }
    for (String line : IntelVectors.lines("bid128_quantize")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] x = operand128(tokens[2]);
      long[] y = operand128(tokens[3]);
      long[] expected = operand128(tokens[4]);
      int expectedFlags = IntelVectors.flags(tokens[5]);
      StatusFlags flags = new StatusFlags();
      long[] actual = new long[2];
      Bid128Raw.quantize(x[0], x[1], y[0], y[1], mode, flags, actual);
      if (actual[0] != expected[0] || actual[1] != expected[1]
          || flags.bits() != expectedFlags) {
        throw new IllegalStateException(String.format(
            "%s actual [0x%016x%016x] %02x",
            line, actual[0], actual[1], flags.bits()));
      }
      tested++;
    }
    return tested;
  }

  private static int testSqrt() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_sqrt")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long input = operand64(tokens[2]);
      long expected = operand64(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Raw.sqrt(input, mode, flags);
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new IllegalStateException(String.format(
            "%s actual [0x%016x] %02x", line, actual, flags.bits()));
      }
      tested++;
    }
    for (String line : IntelVectors.lines("bid128_sqrt")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] input = operand128(tokens[2]);
      long[] expected = operand128(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      StatusFlags flags = new StatusFlags();
      long[] actual = new long[2];
      Bid128Raw.sqrt(input[0], input[1], mode, flags, actual);
      if (actual[0] != expected[0] || actual[1] != expected[1]
          || flags.bits() != expectedFlags) {
        throw new IllegalStateException(String.format(
            "%s actual [0x%016x%016x] %02x",
            line, actual[0], actual[1], flags.bits()));
      }
      tested++;
    }
    return tested;
  }

  private static long round64(
      String operation, long input, RoundingMode mode, StatusFlags flags) {
    switch (operation) {
      case "round_integral_zero":
        return Bid64Raw.roundIntegralZero(input, flags);
      case "round_integral_negative":
        return Bid64Raw.roundIntegralNegative(input, flags);
      case "round_integral_positive":
        return Bid64Raw.roundIntegralPositive(input, flags);
      case "round_integral_nearest_even":
        return Bid64Raw.roundIntegralNearestEven(input, flags);
      case "round_integral_nearest_away":
        return Bid64Raw.roundIntegralNearestAway(input, flags);
      case "round_integral_exact":
        return Bid64Raw.roundIntegralExact(input, mode, flags);
      default:
        throw new IllegalStateException(operation);
    }
  }

  private static void round128(
      String operation,
      long[] input,
      RoundingMode mode,
      StatusFlags flags,
      long[] result) {
    switch (operation) {
      case "round_integral_zero":
        Bid128Raw.roundIntegralZero(input[0], input[1], flags, result);
        break;
      case "round_integral_negative":
        Bid128Raw.roundIntegralNegative(input[0], input[1], flags, result);
        break;
      case "round_integral_positive":
        Bid128Raw.roundIntegralPositive(input[0], input[1], flags, result);
        break;
      case "round_integral_nearest_even":
        Bid128Raw.roundIntegralNearestEven(input[0], input[1], flags, result);
        break;
      case "round_integral_nearest_away":
        Bid128Raw.roundIntegralNearestAway(input[0], input[1], flags, result);
        break;
      case "round_integral_exact":
        Bid128Raw.roundIntegralExact(input[0], input[1], mode, flags, result);
        break;
      default:
        throw new IllegalStateException(operation);
    }
  }

  private static long operand64(String token) {
    if (IntelVectors.isHexPayload(token)) {
      return IntelVectors.hex64(token);
    }
    if (token.equalsIgnoreCase("QNaN")) {
      return Bid64.QUIET_NAN.toRawBits();
    }
    if (token.toUpperCase().endsWith("NAN") || token.toUpperCase().endsWith("INF")
        || token.toUpperCase().endsWith("INFINITY")) {
      return Bid64.parseExact(token).toRawBits();
    }
    return Bid64Raw.fromString(token, RoundingMode.TIES_TO_EVEN, new StatusFlags());
  }

  private static long[] operand128(String token) {
    if (IntelVectors.isHexPayload(token)) {
      return IntelVectors.hex128(token);
    }
    if (token.equalsIgnoreCase("QNaN")) {
      return new long[] {Bid128.QUIET_NAN.highBits(), Bid128.QUIET_NAN.lowBits()};
    }
    if (token.toUpperCase().endsWith("NAN") || token.toUpperCase().endsWith("INF")
        || token.toUpperCase().endsWith("INFINITY")) {
      Bid128 special = Bid128.parseExact(token);
      return new long[] {special.highBits(), special.lowBits()};
    }
    long[] result = new long[2];
    Bid128Raw.fromString(token, RoundingMode.TIES_TO_EVEN, new StatusFlags(), result);
    return result;
  }
}
