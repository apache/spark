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

/**
 * Intel {@code readtest.in} add/sub/mul/div for BID64 and BID128. Replays every
 * named line (hex and decimal-text operands) with bit and flag equality, then
 * checks that the object API matches the raw kernel on the same inputs.
 */
public final class BidArithmeticVectorTest {
  private static final int EXPECTED_VECTORS = 2461;

  private BidArithmeticVectorTest() {
  }

  public static void main(String[] args) throws IOException {
    int tested = 0;
    tested += test64("add");
    tested += test64("sub");
    tested += test64("mul");
    tested += test64("div");
    tested += test128("add");
    tested += test128("sub");
    tested += test128("mul");
    tested += test128("div");
    if (tested != EXPECTED_VECTORS) {
      throw new IllegalStateException("unexpected arithmetic vector count: " + tested);
    }
    System.out.println(
        "BidArithmeticVectorTest: all tests passed (" + tested + " vectors)");
  }

  private static int test64(String operation) throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_" + operation)) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 6) {
        throw new IllegalStateException("short vector: " + line);
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long x = IntelVectors.operand64(tokens[2]);
      long y = IntelVectors.operand64(tokens[3]);
      long expected = IntelVectors.operand64(tokens[4]);
      int expectedFlags = IntelVectors.flags(tokens[5], line);
      StatusFlags flags = new StatusFlags();
      long actual = apply64(operation, x, y, mode, flags);
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new IllegalStateException(String.format(
            "%s actual [0x%016x] %02x", line, actual, flags.bits()));
      }
      StatusFlags objectFlags = new StatusFlags();
      long objectBits = applyObject64(operation, x, y, mode, objectFlags);
      if (objectBits != actual || objectFlags.bits() != flags.bits()) {
        throw new IllegalStateException("object!=raw " + line);
      }
      tested++;
    }
    return tested;
  }

  private static int test128(String operation) throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid128_" + operation)) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 6) {
        throw new IllegalStateException("short vector: " + line);
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] x = IntelVectors.operand128(tokens[2]);
      long[] y = IntelVectors.operand128(tokens[3]);
      long[] expected = IntelVectors.operand128(tokens[4]);
      int expectedFlags = IntelVectors.flags(tokens[5], line);
      StatusFlags flags = new StatusFlags();
      long[] actual = new long[2];
      apply128(operation, x, y, mode, flags, actual);
      if (actual[0] != expected[0] || actual[1] != expected[1]
          || flags.bits() != expectedFlags) {
        throw new IllegalStateException(String.format(
            "%s actual [0x%016x%016x] %02x",
            line, actual[0], actual[1], flags.bits()));
      }
      StatusFlags objectFlags = new StatusFlags();
      long[] objectBits = applyObject128(operation, x, y, mode, objectFlags);
      if (objectBits[0] != actual[0] || objectBits[1] != actual[1]
          || objectFlags.bits() != flags.bits()) {
        throw new IllegalStateException("object!=raw " + line);
      }
      tested++;
    }
    return tested;
  }

  private static long apply64(
      String operation, long x, long y, RoundingMode mode, StatusFlags flags) {
    return switch (operation) {
      case "add" -> Bid64Raw.add(x, y, mode, flags);
      case "sub" -> Bid64Raw.sub(x, y, mode, flags);
      case "mul" -> Bid64Raw.mul(x, y, mode, flags);
      case "div" -> Bid64Raw.div(x, y, mode, flags);
      default -> throw new IllegalStateException(operation);
    };
  }

  private static long applyObject64(
      String operation, long x, long y, RoundingMode mode, StatusFlags flags) {
    Bid64 left = Bid64.fromRawBits(x);
    Bid64 right = Bid64.fromRawBits(y);
    Bid64 result = switch (operation) {
      case "add" -> left.add(right, mode, flags);
      case "sub" -> left.subtract(right, mode, flags);
      case "mul" -> left.multiply(right, mode, flags);
      case "div" -> left.divide(right, mode, flags);
      default -> throw new IllegalStateException(operation);
    };
    return result.toRawBits();
  }

  private static void apply128(
      String operation,
      long[] x,
      long[] y,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    switch (operation) {
      case "add" -> Bid128Raw.add(x[0], x[1], y[0], y[1], mode, flags, out);
      case "sub" -> Bid128Raw.sub(x[0], x[1], y[0], y[1], mode, flags, out);
      case "mul" -> Bid128Raw.mul(x[0], x[1], y[0], y[1], mode, flags, out);
      case "div" -> Bid128Raw.div(x[0], x[1], y[0], y[1], mode, flags, out);
      default -> throw new IllegalStateException(operation);
    }
  }

  private static long[] applyObject128(
      String operation, long[] x, long[] y, RoundingMode mode, StatusFlags flags) {
    Bid128 left = Bid128.fromRawBits(x[0], x[1]);
    Bid128 right = Bid128.fromRawBits(y[0], y[1]);
    Bid128 result = switch (operation) {
      case "add" -> left.add(right, mode, flags);
      case "sub" -> left.subtract(right, mode, flags);
      case "mul" -> left.multiply(right, mode, flags);
      case "div" -> left.divide(right, mode, flags);
      default -> throw new IllegalStateException(operation);
    };
    return new long[] {result.highBits(), result.lowBits()};
  }
}
