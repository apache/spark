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

import java.math.BigInteger;
import java.util.Random;

/** Checks that primitive BID64 kernels exactly match their object API wrappers. */
public final class Bid64RawKernelTest {
  private static final int CASES = 100_000;

  private Bid64RawKernelTest() {
  }

  public static void main(String[] args) {
    Random random = new Random(0xb1d6_4a11L);
    RoundingMode[] modes = RoundingMode.values();
    for (int i = 0; i < CASES; i++) {
      long xBits = random.nextLong();
      long yBits = random.nextLong();
      RoundingMode mode = modes[i % modes.length];
      checkAdd(xBits, yBits, mode);
      checkSubtract(xBits, yBits, mode);
      checkMultiply(xBits, yBits, mode);
      checkDivide(xBits, yBits, mode);
    }
    checkUnsignedDivision(random);
    checkSameQuantumAdd(random);
    checkExactMultiply(random);
    checkExactDivide(random);
    checkArithmeticBoundaries();
    System.out.println("Bid64RawKernelTest: all tests passed");
  }

  private static void checkArithmeticBoundaries() {
    long maximum = 9_999_999_999_999_999L;
    StatusFlags flags = new StatusFlags();
    long sum = Bid64Add.addRawBits(
        Bid64.finite(false, 400, maximum).toRawBits(),
        Bid64.finite(false, 400, maximum).toRawBits(),
        RoundingMode.TIES_TO_EVEN,
        flags);
    checkFiniteResult("maximum add", sum, false, 401, 2_000_000_000_000_000L);
    if (flags.bits() != StatusFlags.INEXACT) {
      throw new AssertionError("maximum add must be inexact");
    }

    flags.clear();
    long product = Bid64Multiply.multiplyRawBits(
        Bid64.finite(true, 398, maximum).toRawBits(),
        Bid64.finite(false, 398, 1L).toRawBits(),
        RoundingMode.TIES_TO_EVEN,
        flags);
    checkFiniteResult("maximum exact multiply", product, true, 398, maximum);
    if (flags.bits() != 0) {
      throw new AssertionError("maximum exact multiply raised flags");
    }

    long quotient = Bid64Divide.divideRawBits(
        Bid64.finite(false, 767, maximum).toRawBits(),
        Bid64.finite(false, 398, maximum).toRawBits(),
        RoundingMode.TIES_TO_EVEN,
        flags);
    checkFiniteResult("maximum exact divide", quotient, false, 767, 1L);
    if (flags.bits() != 0) {
      throw new AssertionError("maximum exact divide raised flags");
    }
  }

  private static void checkSameQuantumAdd(Random random) {
    RoundingMode[] modes = RoundingMode.values();
    for (int i = 0; i < 20_000; i++) {
      long left = 5_000_000_000_000_000L
          + random.nextLong(5_000_000_000_000_000L);
      long right = 5_000_000_000_000_000L
          + random.nextLong(5_000_000_000_000_000L);
      boolean negative = random.nextBoolean();
      RoundingMode mode = modes[i % modes.length];
      long sum = left + right;
      long coefficient = sum / 10L;
      long remainder = sum % 10L;
      if (increment(negative, coefficient, remainder, 10L, mode)) {
        coefficient++;
      }
      int expectedFlags = remainder == 0L ? 0 : StatusFlags.INEXACT;
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Add.addRawBits(
          Bid64.finite(negative, 400, left).toRawBits(),
          Bid64.finite(negative, 400, right).toRawBits(),
          mode,
          flags);
      if (flags.bits() != expectedFlags) {
        throw new AssertionError(String.format(
            "same-quantum add flags: expected %02x, actual %02x",
            expectedFlags,
            flags.bits()));
      }
      checkFiniteResult(
          "same-quantum add",
          actual,
          negative,
          401,
          coefficient);
    }
  }

  private static void checkExactMultiply(Random random) {
    for (int i = 0; i < 20_000; i++) {
      long left = random.nextLong(99_999_999L) + 1L;
      long right = random.nextLong(99_999_999L) + 1L;
      boolean negative = random.nextBoolean();
      int leftExponent = 300 + random.nextInt(151);
      int rightExponent = 300 + random.nextInt(151);
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Multiply.multiplyRawBits(
          Bid64.finite(negative, leftExponent, left).toRawBits(),
          Bid64.finite(false, rightExponent, right).toRawBits(),
          RoundingMode.TIES_TO_EVEN,
          flags);
      if (flags.bits() != 0) {
        throw new AssertionError("exact multiply raised flags");
      }
      checkFiniteResult(
          "exact multiply",
          actual,
          negative,
          leftExponent + rightExponent - 398,
          left * right);
    }
  }

  private static void checkExactDivide(Random random) {
    for (int i = 0; i < 20_000; i++) {
      long quotient;
      do {
        quotient = random.nextLong(99_999_999L) + 1L;
      } while (quotient % 10L == 0L);
      long divisor = random.nextLong(99_999_999L) + 1L;
      long dividend = quotient * divisor;
      boolean negative = random.nextBoolean();
      int leftExponent = 300 + random.nextInt(151);
      int rightExponent = 300 + random.nextInt(151);
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Divide.divideRawBits(
          Bid64.finite(negative, leftExponent, dividend).toRawBits(),
          Bid64.finite(false, rightExponent, divisor).toRawBits(),
          RoundingMode.TIES_TO_EVEN,
          flags);
      if (flags.bits() != 0) {
        throw new AssertionError("exact divide raised flags");
      }
      checkFiniteResult(
          "exact divide",
          actual,
          negative,
          leftExponent - rightExponent + 398,
          quotient);
    }
  }

  private static boolean increment(
      boolean negative,
      long coefficient,
      long remainder,
      long divisor,
      RoundingMode mode) {
    switch (mode) {
      case TIES_TO_EVEN:
        return remainder * 2L > divisor
            || remainder * 2L == divisor && (coefficient & 1L) != 0L;
      case TIES_AWAY:
        return remainder * 2L >= divisor;
      case TOWARD_POSITIVE:
        return !negative && remainder != 0L;
      case TOWARD_NEGATIVE:
        return negative && remainder != 0L;
      case TOWARD_ZERO:
        return false;
      default:
        throw new AssertionError(mode);
    }
  }

  private static void checkFiniteResult(
      String operation, long actual, boolean negative, int exponent, long coefficient) {
    long expected = Bid64.finite(negative, exponent, coefficient).toRawBits();
    if (actual != expected) {
      throw new AssertionError(String.format(
          "%s: expected 0x%016x, actual 0x%016x", operation, expected, actual));
    }
  }

  private static void checkUnsignedDivision(Random random) {
    BigInteger mask = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    for (int i = 0; i < CASES; i++) {
      long divisor = random.nextLong(9_999_999_999_999_999L) + 1L;
      long high = random.nextLong(divisor);
      long low = random.nextLong();
      BigInteger numerator = BigInteger.valueOf(high)
          .shiftLeft(64)
          .add(BigInteger.valueOf(low).and(mask));
      long expected = numerator.divide(BigInteger.valueOf(divisor)).longValue();
      long actual = Bid64Divide.divide128By64(high, low, divisor);
      if (actual != expected) {
        throw new AssertionError(String.format(
            "divide128By64(0x%016x%016x, %d): expected 0x%016x, actual 0x%016x",
            high,
            low,
            divisor,
            expected,
            actual));
      }
    }
  }

  private static void checkAdd(long xBits, long yBits, RoundingMode mode) {
    StatusFlags rawFlags = new StatusFlags();
    StatusFlags objectFlags = new StatusFlags();
    long raw = Bid64Add.addRawBits(xBits, yBits, mode, rawFlags);
    long object = Bid64Add.add(
        Bid64.fromRawBits(xBits),
        Bid64.fromRawBits(yBits),
        mode,
        objectFlags).toRawBits();
    check("add", xBits, yBits, raw, object, rawFlags, objectFlags);
  }

  private static void checkSubtract(long xBits, long yBits, RoundingMode mode) {
    StatusFlags rawFlags = new StatusFlags();
    StatusFlags objectFlags = new StatusFlags();
    long raw = Bid64Add.subtractRawBits(xBits, yBits, mode, rawFlags);
    long object = Bid64Add.subtract(
        Bid64.fromRawBits(xBits),
        Bid64.fromRawBits(yBits),
        mode,
        objectFlags).toRawBits();
    check("subtract", xBits, yBits, raw, object, rawFlags, objectFlags);
  }

  private static void checkMultiply(long xBits, long yBits, RoundingMode mode) {
    StatusFlags rawFlags = new StatusFlags();
    StatusFlags objectFlags = new StatusFlags();
    long raw = Bid64Multiply.multiplyRawBits(xBits, yBits, mode, rawFlags);
    long object = Bid64Multiply.multiply(
        Bid64.fromRawBits(xBits),
        Bid64.fromRawBits(yBits),
        mode,
        objectFlags).toRawBits();
    check("multiply", xBits, yBits, raw, object, rawFlags, objectFlags);
  }

  private static void checkDivide(long xBits, long yBits, RoundingMode mode) {
    StatusFlags rawFlags = new StatusFlags();
    StatusFlags objectFlags = new StatusFlags();
    long raw = Bid64Divide.divideRawBits(xBits, yBits, mode, rawFlags);
    long object = Bid64Divide.divide(
        Bid64.fromRawBits(xBits),
        Bid64.fromRawBits(yBits),
        mode,
        objectFlags).toRawBits();
    check("divide", xBits, yBits, raw, object, rawFlags, objectFlags);
  }

  private static void check(
      String operation,
      long xBits,
      long yBits,
      long raw,
      long object,
      StatusFlags rawFlags,
      StatusFlags objectFlags) {
    if (raw != object || rawFlags.bits() != objectFlags.bits()) {
      throw new AssertionError(String.format(
          "%s(0x%016x, 0x%016x): raw [0x%016x] %02x, object [0x%016x] %02x",
          operation,
          xBits,
          yBits,
          raw,
          rawFlags.bits(),
          object,
          objectFlags.bits()));
    }
  }
}
