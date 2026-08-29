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

/** Runs all Intel BID64/BID128 scale, logb, and quantum vectors. */
public final class BidScaleVectorTest {
  private BidScaleVectorTest() {
  }

  public static void main(String[] args) throws IOException {
    int tested = testScale64() + testScale128();
    tested += testIntegerResults();
    tested += testDecimalResults();
    System.out.println("BidScaleVectorTest: all tests passed (" + tested + " vectors)");
  }

  private static int testScale64() throws IOException {
    int tested = 0;
    for (String operation : new String[] {"scalbn", "ldexp", "scalbln"}) {
      for (String line : IntelVectors.lines("bid64_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long input = operand64(tokens[2]);
        long amount = Long.parseLong(tokens[3]);
        long expected = operand64(tokens[4]);
        int expectedFlags = IntelVectors.flags(tokens[5]);
        StatusFlags flags = new StatusFlags();
        long actual;
        if (operation.equals("scalbn")) {
          actual = Bid64Raw.scalbn(
              input, (int) amount, IntelVectors.mode(tokens[1]), flags);
        } else if (operation.equals("ldexp")) {
          actual = Bid64Raw.ldexp(
              input, (int) amount, IntelVectors.mode(tokens[1]), flags);
        } else {
          actual = Bid64Raw.scalbln(
              input, amount, IntelVectors.mode(tokens[1]), flags);
        }
        check64(line, expected, expectedFlags, actual, flags.bits());
        tested++;
      }
    }
    return tested;
  }

  private static int testScale128() throws IOException {
    int tested = 0;
    for (String operation : new String[] {"scalbn", "ldexp", "scalbln"}) {
      for (String line : IntelVectors.lines("bid128_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long[] input = operand128(tokens[2]);
        long amount = Long.parseLong(tokens[3]);
        long[] expected = operand128(tokens[4]);
        int expectedFlags = IntelVectors.flags(tokens[5]);
        StatusFlags flags = new StatusFlags();
        long[] actual = new long[2];
        if (operation.equals("scalbn")) {
          Bid128Raw.scalbn(
              input[0], input[1], (int) amount,
              IntelVectors.mode(tokens[1]), flags, actual);
        } else if (operation.equals("ldexp")) {
          Bid128Raw.ldexp(
              input[0], input[1], (int) amount,
              IntelVectors.mode(tokens[1]), flags, actual);
        } else {
          Bid128Raw.scalbln(
              input[0], input[1], amount,
              IntelVectors.mode(tokens[1]), flags, actual);
        }
        check128(line, expected, expectedFlags, actual, flags.bits());
        tested++;
      }
    }
    return tested;
  }

  private static int testIntegerResults() throws IOException {
    int tested = 0;
    for (String operation : new String[] {"ilogb", "quantexp", "llquantexp"}) {
      for (String line : IntelVectors.lines("bid64_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long input = operand64(tokens[2]);
        long expected = expectedInteger(tokens[3], operation);
        int expectedFlags = IntelVectors.flags(tokens[4]);
        StatusFlags flags = new StatusFlags();
        long actual = operation.equals("ilogb")
            ? Bid64Raw.ilogb(input, flags)
            : operation.equals("quantexp")
                ? Bid64Raw.quantexp(input, flags)
                : Bid64Raw.llquantexp(input, flags);
        checkInteger(line, expected, expectedFlags, actual, flags.bits());
        tested++;
      }
      for (String line : IntelVectors.lines("bid128_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long[] input = operand128(tokens[2]);
        long expected = expectedInteger(tokens[3], operation);
        int expectedFlags = IntelVectors.flags(tokens[4]);
        StatusFlags flags = new StatusFlags();
        long actual = operation.equals("ilogb")
            ? Bid128Raw.ilogb(input[0], input[1], flags)
            : operation.equals("quantexp")
                ? Bid128Raw.quantexp(input[0], input[1], flags)
                : Bid128Raw.llquantexp(input[0], input[1], flags);
        checkInteger(line, expected, expectedFlags, actual, flags.bits());
        tested++;
      }
    }
    return tested;
  }

  private static int testDecimalResults() throws IOException {
    int tested = 0;
    for (String operation : new String[] {"logb", "quantum"}) {
      for (String line : IntelVectors.lines("bid64_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long input = operand64(tokens[2]);
        long expected = operand64(tokens[3]);
        int expectedFlags = IntelVectors.flags(tokens[4]);
        StatusFlags flags = new StatusFlags();
        long actual = operation.equals("logb")
            ? Bid64Raw.logb(input, flags)
            : Bid64Raw.quantum(input);
        check64(line, expected, expectedFlags, actual, flags.bits());
        tested++;
      }
      for (String line : IntelVectors.lines("bid128_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long[] input = operand128(tokens[2]);
        long[] expected = operand128(tokens[3]);
        int expectedFlags = IntelVectors.flags(tokens[4]);
        StatusFlags flags = new StatusFlags();
        long[] actual = new long[2];
        if (operation.equals("logb")) {
          Bid128Raw.logb(input[0], input[1], flags, actual);
        } else {
          Bid128Raw.quantum(input[0], input[1], actual);
        }
        check128(line, expected, expectedFlags, actual, flags.bits());
        tested++;
      }
    }
    return tested;
  }

  private static long operand64(String token) {
    if (IntelVectors.isHexPayload(token)) {
      return IntelVectors.hex64(token);
    }
    if (token.equalsIgnoreCase("QNaN")) {
      return Bid64.QUIET_NAN.toRawBits();
    }
    if (isSpecial(token)) {
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
    if (isSpecial(token)) {
      Bid128 value = Bid128.parseExact(token);
      return new long[] {value.highBits(), value.lowBits()};
    }
    long[] result = new long[2];
    Bid128Raw.fromString(token, RoundingMode.TIES_TO_EVEN, new StatusFlags(), result);
    return result;
  }

  private static boolean isSpecial(String token) {
    String upper = token.toUpperCase();
    return upper.endsWith("NAN") || upper.endsWith("INF") || upper.endsWith("INFINITY");
  }

  private static long expectedInteger(String token, String operation) {
    if (!token.startsWith("[")) {
      return Long.parseLong(token);
    }
    long bits = IntelVectors.hex64(token);
    return operation.equals("llquantexp") ? bits : (int) bits;
  }

  private static void check64(
      String line, long expected, int expectedFlags, long actual, int actualFlags) {
    if (actual != expected || actualFlags != expectedFlags) {
      throw new AssertionError(String.format(
          "%s actual [0x%016x] %02x", line, actual, actualFlags));
    }
  }

  private static void check128(
      String line, long[] expected, int expectedFlags, long[] actual, int actualFlags) {
    if (actual[0] != expected[0] || actual[1] != expected[1]
        || actualFlags != expectedFlags) {
      throw new AssertionError(String.format(
          "%s actual [0x%016x%016x] %02x",
          line, actual[0], actual[1], actualFlags));
    }
  }

  private static void checkInteger(
      String line, long expected, int expectedFlags, long actual, int actualFlags) {
    if (actual != expected || actualFlags != expectedFlags) {
      throw new AssertionError(
          line + ": actual " + actual + "/" + actualFlags);
    }
  }
}
