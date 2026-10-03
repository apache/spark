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

/** Intel readtest.in vectors for signed and unsigned integer conversions. */
public final class BidIntegerVectorTest {
  private static final String[] SUFFIXES = {
      "int", "xint", "floor", "xfloor", "ceil",
      "xceil", "rnint", "xrnint", "rninta", "xrninta"
  };
  private static final int[] WIDTHS = {8, 16, 32, 64};

  private BidIntegerVectorTest() {
  }

  public static void main(String[] args) throws IOException {
    int count64 = test64() + testRounding64() + testFrom64();
    int count128 = test128() + testRounding128() + testFrom128();
    System.out.printf(
        "BidIntegerVectorTest: all tests passed (%d BID64, %d BID128 vectors)%n",
        count64, count128);
  }

  private static int test64() throws IOException {
    int tested = 0;
    for (boolean signed : new boolean[] {true, false}) {
      for (int width : WIDTHS) {
        for (String suffix : SUFFIXES) {
          String operation = operation("bid64", signed, width, suffix);
          for (String line : IntelVectors.lines(operation)) {
            String[] tokens = IntelVectors.tokens(line);
            RoundingMode mode = mode(suffix);
            long input = operand64(tokens[2], mode);
            long expected = integer(tokens[3], width);
            int expectedFlags = IntelVectors.flags(tokens[4]);
            StatusFlags flags = new StatusFlags();
            long actual = BidConvert.toInt64(
                input, mode, flags, signed, width, suffix.startsWith("x"));
            if (!sameBits(actual, expected, width) || flags.bits() != expectedFlags) {
              throw new IllegalStateException(String.format(
                  "%s actual [0x%016x] %02x", line, actual, flags.bits()));
            }
            tested++;
          }
        }
      }
    }
    return tested;
  }

  private static int test128() throws IOException {
    int tested = 0;
    for (boolean signed : new boolean[] {true, false}) {
      for (int width : WIDTHS) {
        for (String suffix : SUFFIXES) {
          String operation = operation("bid128", signed, width, suffix);
          for (String line : IntelVectors.lines(operation)) {
            String[] tokens = IntelVectors.tokens(line);
            RoundingMode mode = mode(suffix);
            long[] input = operand128(tokens[2], mode);
            long expected = integer(tokens[3], width);
            int expectedFlags = IntelVectors.flags(tokens[4]);
            StatusFlags flags = new StatusFlags();
            long actual = BidConvert.toInt64From128(
                input[0], input[1], mode, flags, signed, width, suffix.startsWith("x"));
            if (!sameBits(actual, expected, width) || flags.bits() != expectedFlags) {
              throw new IllegalStateException(String.format(
                  "%s actual [0x%016x] %02x", line, actual, flags.bits()));
            }
            tested++;
          }
        }
      }
    }
    return tested;
  }

  private static int testRounding64() throws IOException {
    int tested = 0;
    for (String operation : new String[] {"lrint", "llrint", "lround", "llround"}) {
      for (String line : IntelVectors.lines("bid64_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        int width = operation.startsWith("ll") ? 64 : longWidth(tokens);
        RoundingMode mode = operation.endsWith("rint")
            ? IntelVectors.mode(tokens[1]) : RoundingMode.TIES_AWAY;
        boolean exact = operation.endsWith("rint");
        long input = operand64(tokens[2], mode);
        long expected = integer(tokens[3], width);
        int expectedFlags = IntelVectors.flags(tokens[4]);
        StatusFlags flags = new StatusFlags();
        long actual = BidConvert.toInt64(input, mode, flags, true, width, exact);
        if (!sameBits(actual, expected, width) || flags.bits() != expectedFlags) {
          throw new IllegalStateException(String.format(
              "%s actual [0x%016x] %02x", line, actual, flags.bits()));
        }
        tested++;
      }
    }
    return tested;
  }

  private static int testRounding128() throws IOException {
    int tested = 0;
    for (String operation : new String[] {"lrint", "llrint", "lround", "llround"}) {
      for (String line : IntelVectors.lines("bid128_" + operation)) {
        String[] tokens = IntelVectors.tokens(line);
        int width = operation.startsWith("ll") ? 64 : longWidth(tokens);
        RoundingMode mode = operation.endsWith("rint")
            ? IntelVectors.mode(tokens[1]) : RoundingMode.TIES_AWAY;
        boolean exact = operation.endsWith("rint");
        long[] input = operand128(tokens[2], mode);
        long expected = integer(tokens[3], width);
        int expectedFlags = IntelVectors.flags(tokens[4]);
        StatusFlags flags = new StatusFlags();
        long actual = BidConvert.toInt64From128(
            input[0], input[1], mode, flags, true, width, exact);
        if (!sameBits(actual, expected, width) || flags.bits() != expectedFlags) {
          throw new IllegalStateException(String.format(
              "%s actual [0x%016x] %02x", line, actual, flags.bits()));
        }
        tested++;
      }
    }
    return tested;
  }

  private static int testFrom64() throws IOException {
    int tested = 0;
    for (boolean signed : new boolean[] {true, false}) {
      for (int width : WIDTHS) {
        String operation = "bid64_from_" + (signed ? "int" : "uint") + width;
        for (String line : IntelVectors.lines(operation)) {
          String[] tokens = IntelVectors.tokens(line);
          RoundingMode mode = IntelVectors.mode(tokens[1]);
          long value = integer(tokens[2], width);
          if (signed) {
            value = signExtend(value, width);
          }
          StatusFlags flags = new StatusFlags();
          long actual = signed
              ? BidConvert.fromInt64To64(value, mode, flags)
              : BidConvert.fromUInt64To64(value, mode, flags);
          assertFrom64(line, actual, IntelVectors.hex64(tokens[3]), flags.bits(), tokens[4]);
          tested++;
        }
      }
    }
    return tested;
  }

  private static int testFrom128() throws IOException {
    int tested = 0;
    for (boolean signed : new boolean[] {true, false}) {
      for (int width : WIDTHS) {
        String operation = "bid128_from_" + (signed ? "int" : "uint") + width;
        for (String line : IntelVectors.lines(operation)) {
          String[] tokens = IntelVectors.tokens(line);
          RoundingMode mode = IntelVectors.mode(tokens[1]);
          long value = integer(tokens[2], width);
          if (signed) {
            value = signExtend(value, width);
          }
          StatusFlags flags = new StatusFlags();
          long[] actual = new long[2];
          if (signed) {
            BidConvert.fromInt64To128(value, mode, flags, actual);
          } else {
            BidConvert.fromUInt64To128(value, mode, flags, actual);
          }
          long[] expected = IntelVectors.hex128(tokens[3]);
          if (actual[0] != expected[0] || actual[1] != expected[1]
              || flags.bits() != IntelVectors.flags(tokens[4])) {
            throw new IllegalStateException(String.format(
                "%s actual [0x%016x%016x] %02x",
                line, actual[0], actual[1], flags.bits()));
          }
          tested++;
        }
      }
    }
    return tested;
  }

  private static String operation(String type, boolean signed, int width, String suffix) {
    return type + "_to_" + (signed ? "int" : "uint") + width + "_" + suffix;
  }

  private static RoundingMode mode(String suffix) {
    if (suffix.endsWith("floor")) {
      return RoundingMode.TOWARD_NEGATIVE;
    }
    if (suffix.endsWith("ceil")) {
      return RoundingMode.TOWARD_POSITIVE;
    }
    if (suffix.endsWith("rninta")) {
      return RoundingMode.TIES_AWAY;
    }
    if (suffix.endsWith("rnint")) {
      return RoundingMode.TIES_TO_EVEN;
    }
    return RoundingMode.TOWARD_ZERO;
  }

  private static long operand64(String token, RoundingMode mode) {
    if (IntelVectors.isHexPayload(token)) {
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

  private static long integer(String token, int width) {
    if (token.startsWith("[")) {
      String value = token.substring(1, token.length() - 1);
      return Long.parseUnsignedLong(value, 16);
    }
    try {
      return Long.parseLong(token);
    } catch (NumberFormatException exception) {
      return Long.parseUnsignedLong(token);
    }
  }

  private static boolean sameBits(long actual, long expected, int width) {
    long mask = width == 64 ? -1L : (1L << width) - 1;
    return (actual & mask) == (expected & mask);
  }

  private static long signExtend(long value, int width) {
    if (width == 64) {
      return value;
    }
    int shift = 64 - width;
    return value << shift >> shift;
  }

  private static void assertFrom64(
      String line, long actual, long expected, int actualFlags, String expectedFlags) {
    if (actual != expected || actualFlags != IntelVectors.flags(expectedFlags)) {
      throw new IllegalStateException(String.format(
          "%s actual [0x%016x] %02x", line, actual, actualFlags));
    }
  }

  private static int longWidth(String[] tokens) {
    for (String token : tokens) {
      if (token.equals("longintsize=32")) {
        return 32;
      }
    }
    return 64;
  }

  private static boolean isSpecial(String token) {
    String upper = token.toUpperCase();
    return upper.endsWith("NAN") || upper.endsWith("INF") || upper.endsWith("INFINITY");
  }
}
