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

/** Intel vectors for sign, classification, and total-order utilities. */
public final class BidUtilityVectorTest {
  private static final String[] UNARY = {"abs", "copy", "negate"};
  private static final String[] PREDICATES = {
      "isCanonical", "isFinite", "isInf", "isNaN", "isNormal",
      "isSignaling", "isSigned", "isSubnormal", "isZero"
  };
  private static final String[] RELATIONS = {
      "sameQuantum", "totalOrder", "totalOrderMag"
  };

  private BidUtilityVectorTest() {
  }

  public static void main(String[] args) throws IOException {
    int count64 = test64();
    int count128 = test128();
    System.out.printf(
        "BidUtilityVectorTest: all tests passed (%d BID64, %d BID128 vectors)%n",
        count64, count128);
  }

  private static int test64() throws IOException {
    int tested = 0;
    for (String operation : UNARY) {
      for (String line : IntelVectors.lines("bid64_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long input = operand64(tokens[2]);
        long actual = switch (operation) {
          case "abs" -> Bid64Raw.abs(input);
          case "copy" -> Bid64Raw.copy(input);
          default -> Bid64Raw.negate(input);
        };
        assert64(line, actual, operand64(tokens[3]), tokens[4]);
        tested++;
      }
    }
    for (String line : IntelVectors.lines("bid64_copySign")) {
      String[] tokens = IntelVectors.tokens(line);
      assert64(
          line,
          Bid64Raw.copySign(operand64(tokens[2]), operand64(tokens[3])),
          operand64(tokens[4]),
          tokens[5]);
      tested++;
    }
    tested += classes64();
    tested += predicates64();
    tested += relations64();
    return tested;
  }

  private static int classes64() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_class")) {
      String[] tokens = IntelVectors.tokens(line);
      int actual = Bid64Raw.classify(operand64(tokens[2])).ordinal();
      if (actual != Integer.parseInt(tokens[3]) || IntelVectors.flags(tokens[4]) != 0) {
        throw new AssertionError(line + " actual " + actual);
      }
      tested++;
    }
    return tested;
  }

  private static int predicates64() throws IOException {
    int tested = 0;
    for (String operation : PREDICATES) {
      for (String line : IntelVectors.lines("bid64_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long input = operand64(tokens[2]);
        boolean actual = switch (operation) {
          case "isCanonical" -> Bid64Raw.isCanonical(input);
          case "isFinite" -> Bid64Raw.isFinite(input);
          case "isInf" -> Bid64Raw.isInf(input);
          case "isNaN" -> Bid64Raw.isNaN(input);
          case "isNormal" -> Bid64Raw.isNormal(input);
          case "isSignaling" -> Bid64Raw.isSignalingNaN(input);
          case "isSigned" -> Bid64Raw.isSigned(input);
          case "isSubnormal" -> Bid64Raw.isSubnormal(input);
          default -> Bid64Raw.isZero(input);
        };
        assertBoolean(line, actual, tokens[3], tokens[4]);
        tested++;
      }
    }
    return tested;
  }

  private static int relations64() throws IOException {
    int tested = 0;
    for (String operation : RELATIONS) {
      for (String line : IntelVectors.lines("bid64_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long x = operand64(tokens[2]);
        long y = operand64(tokens[3]);
        boolean actual = switch (operation) {
          case "sameQuantum" -> Bid64Raw.sameQuantum(x, y);
          case "totalOrder" -> Bid64Raw.totalOrder(x, y);
          default -> Bid64Raw.totalOrderMag(x, y);
        };
        assertBoolean(line, actual, tokens[4], tokens[5]);
        tested++;
      }
    }
    return tested;
  }

  private static int test128() throws IOException {
    int tested = 0;
    for (String operation : UNARY) {
      for (String line : IntelVectors.lines("bid128_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long[] input = operand128(tokens[2]);
        long[] actual = new long[2];
        switch (operation) {
          case "abs" -> Bid128Raw.abs(input[0], input[1], actual);
          case "copy" -> Bid128Raw.copy(input[0], input[1], actual);
          default -> Bid128Raw.negate(input[0], input[1], actual);
        }
        assert128(line, actual, operand128(tokens[3]), tokens[4]);
        tested++;
      }
    }
    for (String line : IntelVectors.lines("bid128_copySign")) {
      String[] tokens = IntelVectors.tokens(line);
      long[] input = operand128(tokens[2]);
      long[] sign = operand128(tokens[3]);
      long[] actual = new long[2];
      Bid128Raw.copySign(input[0], input[1], sign[0], actual);
      assert128(line, actual, operand128(tokens[4]), tokens[5]);
      tested++;
    }
    tested += classes128();
    tested += predicates128();
    tested += relations128();
    return tested;
  }

  private static int classes128() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid128_class")) {
      String[] tokens = IntelVectors.tokens(line);
      long[] input = operand128(tokens[2]);
      int actual = Bid128Raw.classify(input[0], input[1]).ordinal();
      if (actual != Integer.parseInt(tokens[3]) || IntelVectors.flags(tokens[4]) != 0) {
        throw new AssertionError(line + " actual " + actual);
      }
      tested++;
    }
    return tested;
  }

  private static int predicates128() throws IOException {
    int tested = 0;
    for (String operation : PREDICATES) {
      for (String line : IntelVectors.lines("bid128_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long[] input = operand128(tokens[2]);
        boolean actual = switch (operation) {
          case "isCanonical" -> Bid128Raw.isCanonical(input[0], input[1]);
          case "isFinite" -> Bid128Raw.isFinite(input[0], input[1]);
          case "isInf" -> Bid128Raw.isInf(input[0], input[1]);
          case "isNaN" -> Bid128Raw.isNaN(input[0], input[1]);
          case "isNormal" -> Bid128Raw.isNormal(input[0], input[1]);
          case "isSignaling" -> Bid128Raw.isSignalingNaN(input[0], input[1]);
          case "isSigned" -> Bid128Raw.isSigned(input[0], input[1]);
          case "isSubnormal" -> Bid128Raw.isSubnormal(input[0], input[1]);
          default -> Bid128Raw.isZero(input[0], input[1]);
        };
        assertBoolean(line, actual, tokens[3], tokens[4]);
        tested++;
      }
    }
    return tested;
  }

  private static int relations128() throws IOException {
    int tested = 0;
    for (String operation : RELATIONS) {
      for (String line : IntelVectors.lines("bid128_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long[] x = operand128(tokens[2]);
        long[] y = operand128(tokens[3]);
        boolean actual = switch (operation) {
          case "sameQuantum" -> Bid128Raw.sameQuantum(x[0], x[1], y[0], y[1]);
          case "totalOrder" -> Bid128Raw.totalOrder(x[0], x[1], y[0], y[1]);
          default -> Bid128Raw.totalOrderMag(x[0], x[1], y[0], y[1]);
        };
        assertBoolean(line, actual, tokens[4], tokens[5]);
        tested++;
      }
    }
    return tested;
  }

  private static void assert64(String line, long actual, long expected, String flags) {
    if (actual != expected || IntelVectors.flags(flags) != 0) {
      throw new AssertionError(String.format("%s actual [0x%016x]", line, actual));
    }
  }

  private static void assert128(
      String line, long[] actual, long[] expected, String flags) {
    if (actual[0] != expected[0] || actual[1] != expected[1]
        || IntelVectors.flags(flags) != 0) {
      throw new AssertionError(String.format(
          "%s actual [0x%016x%016x]", line, actual[0], actual[1]));
    }
  }

  private static void assertBoolean(
      String line, boolean actual, String expected, String flags) {
    if (actual != !expected.equals("0") || IntelVectors.flags(flags) != 0) {
      throw new AssertionError(line + " actual " + actual);
    }
  }

  private static long operand64(String token) {
    if (IntelVectors.isHexPayload(token)) {
      return IntelVectors.hex64(token);
    }
    if (token.equalsIgnoreCase("QNaN")) {
      return Bid64.QUIET_NAN.toRawBits();
    }
    return Bid64.parseExact(token).toRawBits();
  }

  private static long[] operand128(String token) {
    if (IntelVectors.isHexPayload(token)) {
      return IntelVectors.hex128(token);
    }
    if (token.equalsIgnoreCase("QNaN")) {
      return new long[] {Bid128.QUIET_NAN.highBits(), Bid128.QUIET_NAN.lowBits()};
    }
    Bid128 value = Bid128.parseExact(token);
    return new long[] {value.highBits(), value.lowBits()};
  }
}
