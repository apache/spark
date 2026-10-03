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

/** Runs all Intel BID64/BID128 quiet and signaling comparison vectors. */
public final class BidComparisonVectorTest {
  private static final String[] PREDICATES = {
    "quiet_equal",
    "quiet_greater",
    "quiet_greater_equal",
    "quiet_greater_unordered",
    "quiet_less",
    "quiet_less_equal",
    "quiet_less_unordered",
    "quiet_not_equal",
    "quiet_not_greater",
    "quiet_not_less",
    "quiet_ordered",
    "quiet_unordered",
    "signaling_greater",
    "signaling_greater_equal",
    "signaling_greater_unordered",
    "signaling_less",
    "signaling_less_equal",
    "signaling_less_unordered",
    "signaling_not_greater",
    "signaling_not_less"
  };

  private BidComparisonVectorTest() {
  }

  public static void main(String[] args) throws IOException {
    int bid64 = test64();
    int bid128 = test128();
    System.out.println(
        "BidComparisonVectorTest: all tests passed (" + bid64 + " BID64, "
            + bid128 + " BID128)");
  }

  private static int test64() throws IOException {
    int tested = 0;
    for (String predicate : PREDICATES) {
      String operation = "bid64_" + predicate;
      List<String> lines = IntelVectors.lines(operation);
      for (String line : lines) {
        String[] tokens = IntelVectors.tokens(line);
        long x = operand64(tokens[2]);
        long y = operand64(tokens[3]);
        boolean expected = Integer.parseInt(tokens[4]) != 0;
        int expectedFlags = IntelVectors.flags(tokens[5]);
        StatusFlags flags = new StatusFlags();
        boolean actual = predicate64(predicate, x, y, flags);
        check(line, expected, expectedFlags, actual, flags.bits());
        tested++;
      }
    }
    return tested;
  }

  private static int test128() throws IOException {
    int tested = 0;
    for (String predicate : PREDICATES) {
      String operation = "bid128_" + predicate;
      List<String> lines = IntelVectors.lines(operation);
      for (String line : lines) {
        String[] tokens = IntelVectors.tokens(line);
        long[] x = operand128(tokens[2]);
        long[] y = operand128(tokens[3]);
        boolean expected = Integer.parseInt(tokens[4]) != 0;
        int expectedFlags = IntelVectors.flags(tokens[5]);
        StatusFlags flags = new StatusFlags();
        boolean actual = predicate128(predicate, x, y, flags);
        check(line, expected, expectedFlags, actual, flags.bits());
        tested++;
      }
    }
    return tested;
  }

  private static boolean predicate64(
      String predicate, long x, long y, StatusFlags flags) {
    switch (predicate) {
      case "quiet_equal":
        return Bid64Raw.quietEqual(x, y, flags);
      case "quiet_greater":
        return Bid64Raw.quietGreater(x, y, flags);
      case "quiet_greater_equal":
        return Bid64Raw.quietGreaterEqual(x, y, flags);
      case "quiet_greater_unordered":
        return Bid64Raw.quietGreaterUnordered(x, y, flags);
      case "quiet_less":
        return Bid64Raw.quietLess(x, y, flags);
      case "quiet_less_equal":
        return Bid64Raw.quietLessEqual(x, y, flags);
      case "quiet_less_unordered":
        return Bid64Raw.quietLessUnordered(x, y, flags);
      case "quiet_not_equal":
        return Bid64Raw.quietNotEqual(x, y, flags);
      case "quiet_not_greater":
        return Bid64Raw.quietNotGreater(x, y, flags);
      case "quiet_not_less":
        return Bid64Raw.quietNotLess(x, y, flags);
      case "quiet_ordered":
        return Bid64Raw.quietOrdered(x, y, flags);
      case "quiet_unordered":
        return Bid64Raw.quietUnordered(x, y, flags);
      case "signaling_greater":
        return Bid64Raw.signalingGreater(x, y, flags);
      case "signaling_greater_equal":
        return Bid64Raw.signalingGreaterEqual(x, y, flags);
      case "signaling_greater_unordered":
        return Bid64Raw.signalingGreaterUnordered(x, y, flags);
      case "signaling_less":
        return Bid64Raw.signalingLess(x, y, flags);
      case "signaling_less_equal":
        return Bid64Raw.signalingLessEqual(x, y, flags);
      case "signaling_less_unordered":
        return Bid64Raw.signalingLessUnordered(x, y, flags);
      case "signaling_not_greater":
        return Bid64Raw.signalingNotGreater(x, y, flags);
      case "signaling_not_less":
        return Bid64Raw.signalingNotLess(x, y, flags);
      default:
        throw new IllegalStateException(predicate);
    }
  }

  private static boolean predicate128(
      String predicate, long[] x, long[] y, StatusFlags flags) {
    switch (predicate) {
      case "quiet_equal":
        return Bid128Raw.quietEqual(x[0], x[1], y[0], y[1], flags);
      case "quiet_greater":
        return Bid128Raw.quietGreater(x[0], x[1], y[0], y[1], flags);
      case "quiet_greater_equal":
        return Bid128Raw.quietGreaterEqual(x[0], x[1], y[0], y[1], flags);
      case "quiet_greater_unordered":
        return Bid128Raw.quietGreaterUnordered(x[0], x[1], y[0], y[1], flags);
      case "quiet_less":
        return Bid128Raw.quietLess(x[0], x[1], y[0], y[1], flags);
      case "quiet_less_equal":
        return Bid128Raw.quietLessEqual(x[0], x[1], y[0], y[1], flags);
      case "quiet_less_unordered":
        return Bid128Raw.quietLessUnordered(x[0], x[1], y[0], y[1], flags);
      case "quiet_not_equal":
        return Bid128Raw.quietNotEqual(x[0], x[1], y[0], y[1], flags);
      case "quiet_not_greater":
        return Bid128Raw.quietNotGreater(x[0], x[1], y[0], y[1], flags);
      case "quiet_not_less":
        return Bid128Raw.quietNotLess(x[0], x[1], y[0], y[1], flags);
      case "quiet_ordered":
        return Bid128Raw.quietOrdered(x[0], x[1], y[0], y[1], flags);
      case "quiet_unordered":
        return Bid128Raw.quietUnordered(x[0], x[1], y[0], y[1], flags);
      case "signaling_greater":
        return Bid128Raw.signalingGreater(x[0], x[1], y[0], y[1], flags);
      case "signaling_greater_equal":
        return Bid128Raw.signalingGreaterEqual(x[0], x[1], y[0], y[1], flags);
      case "signaling_greater_unordered":
        return Bid128Raw.signalingGreaterUnordered(x[0], x[1], y[0], y[1], flags);
      case "signaling_less":
        return Bid128Raw.signalingLess(x[0], x[1], y[0], y[1], flags);
      case "signaling_less_equal":
        return Bid128Raw.signalingLessEqual(x[0], x[1], y[0], y[1], flags);
      case "signaling_less_unordered":
        return Bid128Raw.signalingLessUnordered(x[0], x[1], y[0], y[1], flags);
      case "signaling_not_greater":
        return Bid128Raw.signalingNotGreater(x[0], x[1], y[0], y[1], flags);
      case "signaling_not_less":
        return Bid128Raw.signalingNotLess(x[0], x[1], y[0], y[1], flags);
      default:
        throw new IllegalStateException(predicate);
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

  private static void check(
      String line, boolean expected, int expectedFlags, boolean actual, int actualFlags) {
    if (actual != expected || actualFlags != expectedFlags) {
      throw new IllegalStateException(
          line + ": expected " + expected + "/" + expectedFlags
              + ", actual " + actual + "/" + actualFlags);
    }
  }
}
