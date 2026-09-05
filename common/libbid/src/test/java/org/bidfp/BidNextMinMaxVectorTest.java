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

/** Runs Intel BID64/BID128 next, min/max, and positive-difference vectors. */
public final class BidNextMinMaxVectorTest {
  private static final int EXPECTED_MALFORMED_VECTORS = 19;
  private static int skippedMalformedVectors;

  private BidNextMinMaxVectorTest() {
  }

  public static void main(String[] args) throws IOException {
    skippedMalformedVectors = 0;
    int tested = test64() + test128();
    if (skippedMalformedVectors != EXPECTED_MALFORMED_VECTORS) {
      throw new IllegalStateException(
          "unexpected malformed vector count: " + skippedMalformedVectors);
    }
    System.out.println(
        "BidNextMinMaxVectorTest: all tests passed (" + tested
            + " vectors, " + skippedMalformedVectors + " malformed upstream vectors skipped)");
  }

  private static int test64() throws IOException {
    int tested = 0;
    for (String operation : new String[] {
      "nextup", "nextdown", "nextafter",
      "minnum", "maxnum", "minnum_mag", "maxnum_mag", "fdim"
    }) {
      for (String line : IntelVectors.lines("bid64_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long x = operand64(tokens[2]);
        int resultIndex = operation.equals("nextup") || operation.equals("nextdown") ? 3 : 4;
        if (!isFlagToken(tokens[resultIndex + 1])) {
          skippedMalformedVectors++;
          continue;
        }
        long y = resultIndex == 3 ? 0L : operand64(tokens[3]);
        long expected = operand64(tokens[resultIndex]);
        int expectedFlags = IntelVectors.flags(tokens[resultIndex + 1]);
        StatusFlags flags = new StatusFlags();
        long actual = apply64(operation, x, y, IntelVectors.mode(tokens[1]), flags);
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
    for (String operation : new String[] {
      "nextup", "nextdown", "nextafter",
      "minnum", "maxnum", "minnum_mag", "maxnum_mag", "fdim"
    }) {
      for (String line : IntelVectors.lines("bid128_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        long[] x = operand128(tokens[2]);
        int resultIndex = operation.equals("nextup") || operation.equals("nextdown") ? 3 : 4;
        if (!isFlagToken(tokens[resultIndex + 1])) {
          skippedMalformedVectors++;
          continue;
        }
        if (tokens[resultIndex].startsWith("[")
            && tokens[resultIndex].replace(",", "").length() != 34) {
          skippedMalformedVectors++;
          continue;
        }
        long[] y = resultIndex == 3 ? new long[2] : operand128(tokens[3]);
        long[] expected = operand128(tokens[resultIndex]);
        int expectedFlags = IntelVectors.flags(tokens[resultIndex + 1]);
        StatusFlags flags = new StatusFlags();
        long[] actual = new long[2];
        apply128(operation, x, y, IntelVectors.mode(tokens[1]), flags, actual);
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

  private static long apply64(
      String operation, long x, long y, RoundingMode mode, StatusFlags flags) {
    switch (operation) {
      case "nextup":
        return Bid64Raw.nextUp(x, flags);
      case "nextdown":
        return Bid64Raw.nextDown(x, flags);
      case "nextafter":
        return Bid64Raw.nextAfter(x, y, flags);
      case "minnum":
        return Bid64Raw.minnum(x, y, flags);
      case "maxnum":
        return Bid64Raw.maxnum(x, y, flags);
      case "minnum_mag":
        return Bid64Raw.minnumMag(x, y, flags);
      case "maxnum_mag":
        return Bid64Raw.maxnumMag(x, y, flags);
      case "fdim":
        return Bid64Raw.fdim(x, y, mode, flags);
      default:
        throw new IllegalStateException(operation);
    }
  }

  private static boolean isFlagToken(String token) {
    return token.length() == 2
        && Character.digit(token.charAt(0), 16) >= 0
        && Character.digit(token.charAt(1), 16) >= 0;
  }

  private static void apply128(
      String operation,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] result) {
    switch (operation) {
      case "nextup":
        Bid128Raw.nextUp(x[0], x[1], flags, result);
        break;
      case "nextdown":
        Bid128Raw.nextDown(x[0], x[1], flags, result);
        break;
      case "nextafter":
        Bid128Raw.nextAfter(x[0], x[1], y[0], y[1], flags, result);
        break;
      case "minnum":
        Bid128Raw.minnum(x[0], x[1], y[0], y[1], flags, result);
        break;
      case "maxnum":
        Bid128Raw.maxnum(x[0], x[1], y[0], y[1], flags, result);
        break;
      case "minnum_mag":
        Bid128Raw.minnumMag(x[0], x[1], y[0], y[1], flags, result);
        break;
      case "maxnum_mag":
        Bid128Raw.maxnumMag(x[0], x[1], y[0], y[1], flags, result);
        break;
      case "fdim":
        Bid128Raw.fdim(x[0], x[1], y[0], y[1], mode, flags, result);
        break;
      default:
        throw new IllegalStateException(operation);
    }
  }

  private static long operand64(String token) {
    if (IntelVectors.isHexPayload(token)) {
      if (token.contains(",")) {
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
}
