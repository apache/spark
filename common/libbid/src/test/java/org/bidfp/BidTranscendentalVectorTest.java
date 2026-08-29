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

import java.lang.reflect.Method;

/**
 * Intel {@code readtest.in} transcendental families. Values use Intel relative
 * ULP limits; NaN/Inf require exact bits. Flags check INVALID and DIVBYZERO
 * only ({@code trans_flags_mask = 0x05} in {@code readtest.c}).
 */
public final class BidTranscendentalVectorTest {
  private static final int TRANS_FLAGS = StatusFlags.INVALID | StatusFlags.DIVIDE_BY_ZERO;
  private static final boolean REPORT_ALL = Boolean.getBoolean("bid.trans.reportAll");
  private static final String[] UNARY64 = {
      "exp", "expm1", "exp2", "exp10", "log", "log10", "log2", "log1p",
      "sin", "cos", "tan", "asin", "acos", "atan",
      "sinh", "cosh", "tanh", "asinh", "acosh", "atanh",
      "erf", "erfc", "tgamma", "lgamma", "cbrt"
  };

  private BidTranscendentalVectorTest() {
  }

  public static void main(String[] args) throws Exception {
    StringBuilder failures = new StringBuilder();
    int total = 0;
    for (String op : UNARY64) {
      total += checkUnary64("bid64_" + op, failures);
      total += checkUnary128("bid128_" + op, failures);
    }
    total += checkBinary64("bid64_pow", "pow", failures);
    total += checkBinary64("bid64_hypot", "hypot", failures);
    total += checkBinary64("bid64_atan2", "atan2", failures);
    total += checkBinary128("bid128_pow", "pow", failures);
    total += checkBinary128("bid128_hypot", "hypot", failures);
    total += checkBinary128("bid128_atan2", "atan2", failures);
    if (failures.length() > 0) {
      throw new AssertionError(failures.toString());
    }
    if (total != 5448) {
      throw new AssertionError("unexpected transcendental vector count: " + total);
    }
    System.out.println("BidTranscendentalVectorTest: all tests passed (" + total
        + " vectors)");
  }

  private static int checkUnary64(String operation, StringBuilder failures)
      throws Exception {
    Method method = Bid64Raw.class.getMethod(
        operation.substring(6), long.class, RoundingMode.class, StatusFlags.class);
    int tested = 0;
    boolean reported = false;
    for (String line : IntelVectors.lines(operation)) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long input = parse64(tokens[2]);
      long expected = parse64(tokens[3], mode);
      int expectedFlags = IntelVectors.flags(tokens[4]) & TRANS_FLAGS;
      StatusFlags flags = new StatusFlags();
      long actual = (Long) method.invoke(null, input, mode, flags);
      if ((!reported || REPORT_ALL) && (!accept64(
          actual, expected, mode, IntelVectors.ulp(line))
          || (flags.bits() & TRANS_FLAGS) != expectedFlags)) {
        failures.append(String.format(
            "%s actual [0x%016x] %02x%n", line, actual, flags.bits()));
        reported = true;
      }
      tested++;
    }
    return tested;
  }

  private static int checkUnary128(String operation, StringBuilder failures)
      throws Exception {
    Method method = Bid128Raw.class.getMethod(
        operation.substring(7),
        long.class, long.class, RoundingMode.class, StatusFlags.class, long[].class);
    int tested = 0;
    boolean reported = false;
    for (String line : IntelVectors.lines(operation)) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] input = parse128(tokens[2]);
      long[] expected = parse128(tokens[3], mode);
      int expectedFlags = IntelVectors.flags(tokens[4]) & TRANS_FLAGS;
      long[] actual = new long[2];
      StatusFlags flags = new StatusFlags();
      method.invoke(null, input[0], input[1], mode, flags, actual);
      if ((!reported || REPORT_ALL) && (!accept128(
          actual, expected, mode, IntelVectors.ulp(line))
          || (flags.bits() & TRANS_FLAGS) != expectedFlags)) {
        failures.append(String.format(
            "%s actual [0x%016x%016x] %02x%n",
            line, actual[0], actual[1], flags.bits()));
        reported = true;
      }
      tested++;
    }
    return tested;
  }

  private static int checkBinary64(
      String operation, String methodName, StringBuilder failures)
      throws Exception {
    Method method = Bid64Raw.class.getMethod(
        methodName, long.class, long.class, RoundingMode.class, StatusFlags.class);
    int tested = 0;
    boolean reported = false;
    for (String line : IntelVectors.lines(operation)) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 6) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long x = parse64(tokens[2]);
      long y = parse64(tokens[3]);
      long expected = parse64(tokens[4], mode);
      int expectedFlags = IntelVectors.flags(tokens[5]) & TRANS_FLAGS;
      StatusFlags flags = new StatusFlags();
      long actual = (Long) method.invoke(null, x, y, mode, flags);
      if ((!reported || REPORT_ALL) && (!accept64(
          actual, expected, mode, IntelVectors.ulp(line))
          || (flags.bits() & TRANS_FLAGS) != expectedFlags)) {
        failures.append(String.format(
            "%s actual [0x%016x] %02x%n", line, actual, flags.bits()));
        reported = true;
      }
      tested++;
    }
    return tested;
  }

  private static int checkBinary128(
      String operation, String methodName, StringBuilder failures)
      throws Exception {
    Method method = Bid128Raw.class.getMethod(
        methodName,
        long.class, long.class, long.class, long.class,
        RoundingMode.class, StatusFlags.class, long[].class);
    int tested = 0;
    boolean reported = false;
    for (String line : IntelVectors.lines(operation)) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 6) {
        tested++;
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] x = parse128(tokens[2]);
      long[] y = parse128(tokens[3]);
      long[] expected = parse128(tokens[4], mode);
      int expectedFlags = IntelVectors.flags(tokens[5]) & TRANS_FLAGS;
      long[] actual = new long[2];
      StatusFlags flags = new StatusFlags();
      method.invoke(null, x[0], x[1], y[0], y[1], mode, flags, actual);
      if ((!reported || REPORT_ALL) && (!accept128(
          actual, expected, mode, IntelVectors.ulp(line))
          || (flags.bits() & TRANS_FLAGS) != expectedFlags)) {
        failures.append(String.format(
            "%s actual [0x%016x%016x] %02x%n",
            line, actual[0], actual[1], flags.bits()));
        reported = true;
      }
      tested++;
    }
    return tested;
  }

  private static boolean accept64(
      long actual, long expected, RoundingMode mode, double expectedUlp) {
    if (Bid64Raw.isNaN(expected) || Bid64Raw.isInf(expected)
        || Bid64Raw.isNaN(actual) || Bid64Raw.isInf(actual)) {
      return actual == expected;
    }
    if ((actual & Bid64.MASK_SIGN) != (expected & Bid64.MASK_SIGN)) {
      return Bid64Raw.isZero(actual) && Bid64Raw.isZero(expected);
    }
    Bid64 a = Bid64.fromRawBits(actual);
    Bid64 e = Bid64.fromRawBits(expected);
    long alignedActual = actual;
    long alignedExpected = expected;
    StatusFlags flags = new StatusFlags();
    if (a.biasedExponent() < e.biasedExponent()) {
      alignedActual = Bid64Raw.quantize(actual, expected, mode, flags);
    } else if (e.biasedExponent() < a.biasedExponent()) {
      alignedExpected = Bid64Raw.quantize(expected, actual, mode, flags);
    }
    if (Bid64Raw.isNaN(alignedActual) || Bid64Raw.isNaN(alignedExpected)
        || Bid64.fromRawBits(alignedActual).biasedExponent()
            != Bid64.fromRawBits(alignedExpected).biasedExponent()) {
      return false;
    }
    long m1 = Bid64.significandBits(alignedActual);
    long m2 = Bid64.significandBits(alignedExpected);
    double difference = Math.abs((double) (m1 - m2));
    if (e.quietLess(a, new StatusFlags())) {
      difference = -difference;
    }
    double ulp = difference + expectedUlp;
    return Math.abs(ulp) <= limit64(mode);
  }

  private static boolean accept128(
      long[] actual, long[] expected, RoundingMode mode, double expectedUlp) {
    Bid128 a = Bid128.fromRawBits(actual[0], actual[1]);
    Bid128 e = Bid128.fromRawBits(expected[0], expected[1]);
    if (e.isNaN() || a.isNaN()) {
      return actual[0] == expected[0] && actual[1] == expected[1];
    }
    if (e.isInfinite() || a.isInfinite()) {
      if (actual[0] == expected[0] && actual[1] == expected[1]) {
        return true;
      }
      if (a.isInfinite() && !e.isInfinite()) {
        a = Bid128.fromRawBits(
            (actual[0] & Bid128.MASK_SIGN) | 0x5fff_ed09_bead_87c0L,
            0x378d_8e63_ffff_ffffL);
      } else if (e.isInfinite() && !a.isInfinite()) {
        e = Bid128.fromRawBits(
            (expected[0] & Bid128.MASK_SIGN) | 0x5fff_ed09_bead_87c0L,
            0x378d_8e63_ffff_ffffL);
      } else {
        return false;
      }
    }
    if (a.isSigned() != e.isSigned()) {
      return a.isZero() && e.isZero();
    }
    StatusFlags flags = new StatusFlags();
    Bid128 alignedActual = a;
    Bid128 alignedExpected = e;
    long[] quantized = new long[2];
    if (a.biasedExponent() < e.biasedExponent()) {
      Bid128Raw.quantize(
          a.highBits(), a.lowBits(), e.highBits(), e.lowBits(),
          mode, flags, quantized);
      alignedActual = Bid128.fromRawBits(quantized[0], quantized[1]);
    } else if (e.biasedExponent() < a.biasedExponent()) {
      Bid128Raw.quantize(
          e.highBits(), e.lowBits(), a.highBits(), a.lowBits(),
          mode, flags, quantized);
      alignedExpected = Bid128.fromRawBits(quantized[0], quantized[1]);
    }
    if (alignedActual.isNaN() || alignedExpected.isNaN()
        || alignedActual.biasedExponent() != alignedExpected.biasedExponent()) {
      return false;
    }
    UInt128 mc = alignedActual.coefficient();
    UInt128 me = alignedExpected.coefficient();
    int comparison = mc.compareTo(me);
    UInt128 diff = comparison >= 0 ? mc.subtract(me) : me.subtract(mc);
    if (diff.high() != 0L) {
      return false;
    }
    if (Long.compareUnsigned(diff.low(), 1_000L) > 0) {
      return false;
    }
    double difference = (double) diff.low();
    if (e.quietLess(a, new StatusFlags())) {
      difference = -difference;
    }
    double ulp = difference + expectedUlp;
    return Math.abs(ulp) <= limit128(mode);
  }

  private static double limit64(RoundingMode mode) {
    return mode == RoundingMode.TIES_TO_EVEN || mode == RoundingMode.TIES_AWAY
        ? 0.55 : 1.05;
  }

  private static double limit128(RoundingMode mode) {
    return mode == RoundingMode.TIES_TO_EVEN || mode == RoundingMode.TIES_AWAY
        ? 2.0 : 5.0;
  }

  private static long parse64(String token) {
    return parse64(token, RoundingMode.TIES_TO_EVEN);
  }

  private static long parse64(String token, RoundingMode mode) {
    if (IntelVectors.isHexPayload(token) && token.contains("[")) {
      return IntelVectors.hex64(token);
    }
    return Bid64Raw.fromString(token, mode, new StatusFlags());
  }

  private static long[] parse128(String token) {
    return parse128(token, RoundingMode.TIES_TO_EVEN);
  }

  private static long[] parse128(String token, RoundingMode mode) {
    if (IntelVectors.isHexPayload(token)) {
      return IntelVectors.hex128(token);
    }
    long[] value = new long[2];
    Bid128Raw.fromString(token, mode, new StatusFlags(), value);
    return value;
  }
}
