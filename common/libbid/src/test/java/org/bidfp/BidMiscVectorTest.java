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

/** Intel readtest.in vectors for nearbyint, frexp, and modf. */
public final class BidMiscVectorTest {
  private BidMiscVectorTest() {
  }

  public static void main(String[] args) throws IOException {
    int count64 = nearby64() + frexp64() + modf64() + nextToward64();
    int count128 = nearby128() + frexp128() + modf128() + nextToward128();
    System.out.printf(
        "BidMiscVectorTest: all tests passed (%d BID64, %d BID128 vectors)%n",
        count64, count128);
  }

  private static int nearby64() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_nearbyint")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Raw.nearbyint(operand64(tokens[2], mode), mode, flags);
      assert64(line, actual, operand64(tokens[3], mode), flags.bits(), tokens[4]);
      tested++;
    }
    return tested;
  }

  private static int frexp64() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_frexp")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      int[] exponent = new int[1];
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Raw.frexp(operand64(tokens[2], mode), exponent, flags);
      if (exponent[0] != Integer.parseInt(tokens[3])) {
        throw new AssertionError(line + " exponent " + exponent[0]);
      }
      assert64(line, actual, operand64(tokens[4], mode), flags.bits(), tokens[5]);
      tested++;
    }
    return tested;
  }

  private static int modf64() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_modf")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] integral = new long[1];
      StatusFlags flags = new StatusFlags();
      long fractional = Bid64Raw.modf(operand64(tokens[2], mode), integral, flags);
      if (integral[0] != operand64(tokens[3], mode)
          || fractional != operand64(tokens[4], mode)
          || flags.bits() != IntelVectors.flags(tokens[5])) {
        throw new AssertionError(line);
      }
      tested++;
    }
    return tested;
  }

  private static int nearby128() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid128_nearbyint")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] input = operand128(tokens[2], mode);
      long[] expected = operand128(tokens[3], mode);
      long[] actual = new long[2];
      StatusFlags flags = new StatusFlags();
      Bid128Raw.nearbyint(input[0], input[1], mode, flags, actual);
      assert128(line, actual, expected, flags.bits(), tokens[4]);
      tested++;
    }
    return tested;
  }

  private static int frexp128() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid128_frexp")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] input = operand128(tokens[2], mode);
      long[] expected = operand128(tokens[4], mode);
      long[] actual = new long[2];
      int[] exponent = new int[1];
      StatusFlags flags = new StatusFlags();
      Bid128Raw.frexp(input[0], input[1], exponent, flags, actual);
      if (exponent[0] != Integer.parseInt(tokens[3])) {
        throw new AssertionError(line + " exponent " + exponent[0]);
      }
      assert128(line, actual, expected, flags.bits(), tokens[5]);
      tested++;
    }
    return tested;
  }

  private static int modf128() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid128_modf")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] input = operand128(tokens[2], mode);
      long[] expectedIntegral = operand128(tokens[3], mode);
      long[] expectedFractional = operand128(tokens[4], mode);
      long[] integral = new long[2];
      long[] fractional = new long[2];
      StatusFlags flags = new StatusFlags();
      Bid128Raw.modf(input[0], input[1], integral, flags, fractional);
      if (integral[0] != expectedIntegral[0] || integral[1] != expectedIntegral[1]
          || fractional[0] != expectedFractional[0]
          || fractional[1] != expectedFractional[1]
          || flags.bits() != IntelVectors.flags(tokens[5])) {
        throw new AssertionError(line);
      }
      tested++;
    }
    return tested;
  }

  private static int nextToward64() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_nexttoward")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] target = operand128(tokens[3], mode);
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Raw.nextToward(
          operand64(tokens[2], mode), target[0], target[1], flags);
      assert64(line, actual, operand64(tokens[4], mode), flags.bits(), tokens[5]);
      tested++;
    }
    return tested;
  }

  private static int nextToward128() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid128_nexttoward")) {
      String[] tokens = IntelVectors.tokens(line);
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] input = operand128(tokens[2], mode);
      long[] target = operand128(tokens[3], mode);
      long[] expected = operand128(tokens[4], mode);
      long[] actual = new long[2];
      StatusFlags flags = new StatusFlags();
      Bid128Raw.nextToward(
          input[0], input[1], target[0], target[1], flags, actual);
      assert128(line, actual, expected, flags.bits(), tokens[5]);
      tested++;
    }
    return tested;
  }

  private static void assert64(
      String line, long actual, long expected, int actualFlags, String expectedFlags) {
    if (actual != expected || actualFlags != IntelVectors.flags(expectedFlags)) {
      throw new AssertionError(String.format(
          "%s actual [0x%016x] %02x", line, actual, actualFlags));
    }
  }

  private static void assert128(
      String line, long[] actual, long[] expected, int actualFlags, String expectedFlags) {
    if (actual[0] != expected[0] || actual[1] != expected[1]
        || actualFlags != IntelVectors.flags(expectedFlags)) {
      throw new AssertionError(String.format(
          "%s actual [0x%016x%016x] %02x",
          line, actual[0], actual[1], actualFlags));
    }
  }

  private static long operand64(String token, RoundingMode mode) {
    if (IntelVectors.isHexPayload(token)) {
      if (token.indexOf(',') >= 0) {
        return IntelVectors.hex128(token)[1];
      }
      return IntelVectors.hex64(token);
    }
    if (token.equalsIgnoreCase("QNaN")) {
      return Bid64.QUIET_NAN.toRawBits();
    }
    if (isSpecial(token)) {
      return Bid64.parseExact(token).toRawBits();
    }
    return Bid64Raw.fromString(token, mode, new StatusFlags());
  }

  private static long[] operand128(String token, RoundingMode mode) {
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
    Bid128Raw.fromString(token, mode, new StatusFlags(), result);
    return result;
  }

  private static boolean isSpecial(String token) {
    String upper = token.toUpperCase();
    return upper.endsWith("NAN") || upper.endsWith("INF") || upper.endsWith("INFINITY");
  }
}
