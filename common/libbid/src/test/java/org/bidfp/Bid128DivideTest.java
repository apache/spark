/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   * Neither the name of Intel Corporation nor the names of its contributors may
 *     be used to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.bidfp;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Random;

/** Special-value and boundary tests for {@link Bid128Divide}. */
public final class Bid128DivideTest {
  private Bid128DivideTest() {
  }

  public static void main(String[] args) {
    testSpecialValues();
    testRoundingModes();
    testAdversarialDivision();
    testRandomDifferential();
    testFlagsAccumulate();
    System.out.println(
        "Bid128DivideTest: all tests passed (2360 differential cases)");
  }

  private static void testSpecialValues() {
    check(Bid128.QUIET_NAN, Bid128.POSITIVE_ZERO, Bid128.POSITIVE_ZERO,
        RoundingMode.TIES_TO_EVEN, StatusFlags.INVALID);
    check(Bid128.POSITIVE_INFINITY, Bid128.finite(false, 6176, 0, 1),
        Bid128.POSITIVE_ZERO, RoundingMode.TIES_TO_EVEN, StatusFlags.DIVIDE_BY_ZERO);
    check(Bid128.QUIET_NAN, Bid128.POSITIVE_INFINITY, Bid128.POSITIVE_INFINITY,
        RoundingMode.TIES_TO_EVEN, StatusFlags.INVALID);
    check(Bid128.NEGATIVE_ZERO, Bid128.finite(true, 6176, 0, 1),
        Bid128.POSITIVE_INFINITY, RoundingMode.TIES_TO_EVEN, 0);
    check(
        Bid128.fromRawBits(0x7c00_0000_0000_0123L, 0x4567L),
        Bid128.fromRawBits(0x7e00_0000_0000_0123L, 0x4567L),
        Bid128.finite(false, 6176, 0, 2),
        RoundingMode.TIES_TO_EVEN,
        StatusFlags.INVALID);
  }

  private static void testRoundingModes() {
    Bid128 one = Bid128.finite(false, 6176, 0, 1);
    Bid128 six = Bid128.finite(false, 6176, 0, 6);
    check(
        raw("2ffc522c4a72414a", "b3eced10aaaaaaab"),
        one,
        six,
        RoundingMode.TIES_TO_EVEN,
        StatusFlags.INEXACT);
    check(
        raw("2ffc522c4a72414a", "b3eced10aaaaaaab"),
        one,
        six,
        RoundingMode.TOWARD_POSITIVE,
        StatusFlags.INEXACT);
    check(
        raw("affc522c4a72414a", "b3eced10aaaaaaab"),
        one.negate(),
        six,
        RoundingMode.TOWARD_NEGATIVE,
        StatusFlags.INEXACT);
    check(
        raw("2ffc522c4a72414a", "b3eced10aaaaaaaa"),
        one,
        six,
        RoundingMode.TOWARD_ZERO,
        StatusFlags.INEXACT);
    check(Bid128.finite(false, 6176, 0, 2),
        Bid128.finite(false, 6176, 0, 6),
        Bid128.finite(false, 6176, 0, 3),
        RoundingMode.TIES_TO_EVEN,
        0);
  }

  private static void testFlagsAccumulate() {
    StatusFlags flags = new StatusFlags();
    flags.raise(StatusFlags.DENORMAL);
    Bid128Divide.divide(
        Bid128.POSITIVE_ZERO,
        Bid128.POSITIVE_ZERO,
        RoundingMode.TIES_TO_EVEN,
        flags);
    int expected = StatusFlags.DENORMAL | StatusFlags.INVALID;
    if (flags.bits() != expected) {
      throw new AssertionError(String.format(
          "accumulated flags: expected %02x, actual %02x", expected, flags.bits()));
    }
  }

  private static void testAdversarialDivision() {
    BigInteger ten33 = BigInteger.TEN.pow(33);
    BigInteger max = BigInteger.TEN.pow(34).subtract(BigInteger.ONE);
    BigInteger[] coefficients = {
      BigInteger.ONE,
      BigInteger.TWO,
      BigInteger.valueOf(3),
      ten33.subtract(BigInteger.ONE),
      ten33,
      max
    };
    for (BigInteger left : coefficients) {
      for (BigInteger right : coefficients) {
        for (RoundingMode mode : RoundingMode.values()) {
          checkOracle(finite(false, 6176, left), finite(false, 6176, right), mode);
          checkOracle(finite(true, 6176, left), finite(false, 6176, right), mode);
        }
      }
    }
  }

  private static void testRandomDifferential() {
    Random random = new Random(0x128_d1f1L);
    BigInteger limit = BigInteger.TEN.pow(34);
    for (int sample = 0; sample < 2_000; sample++) {
      BigInteger left = randomCoefficient(random, limit);
      BigInteger right = randomCoefficient(random, limit);
      int leftExponent = 6176 + random.nextInt(401) - 200;
      int rightExponent = 6176 + random.nextInt(401) - 200;
      checkOracle(
          finite(random.nextBoolean(), leftExponent, left),
          finite(random.nextBoolean(), rightExponent, right),
          RoundingMode.values()[sample % RoundingMode.values().length]);
    }
  }

  private static void checkOracle(Bid128 x, Bid128 y, RoundingMode mode) {
    BigDecimal xd = decimal(x);
    BigDecimal yd = decimal(y);
    BigDecimal expected = xd.divide(yd, new MathContext(34, javaMode(mode)));
    StatusFlags flags = new StatusFlags();
    Bid128 actual = Bid128Divide.divide(x, y, mode, flags);
    if (!actual.equals(encode(expected))) {
      throw new AssertionError(
          "differential divide: expected " + expected + ", actual "
              + actual.toCanonicalString());
    }
    int expectedFlags = isExact(xd, yd, expected) ? 0 : StatusFlags.INEXACT;
    if (flags.bits() != expectedFlags) {
      throw new AssertionError(
          "differential flags: expected " + expectedFlags + ", actual " + flags.bits());
    }
  }

  private static boolean isExact(BigDecimal x, BigDecimal y, BigDecimal rounded) {
    try {
      return x.divide(y).compareTo(rounded) == 0;
    } catch (ArithmeticException nonTerminating) {
      return false;
    }
  }

  private static BigInteger randomCoefficient(Random random, BigInteger limit) {
    BigInteger result;
    do {
      result = new BigInteger(113, random);
    } while (result.signum() == 0 || result.compareTo(limit) >= 0);
    return result;
  }

  private static BigDecimal decimal(Bid128 value) {
    BigInteger coefficient = unsigned(value.highBits() & Bid128.MASK_COEFFICIENT)
        .shiftLeft(64)
        .add(unsigned(value.lowBits()));
    if (value.isSigned()) {
      coefficient = coefficient.negate();
    }
    return new BigDecimal(coefficient, 6176 - value.biasedExponent());
  }

  private static Bid128 encode(BigDecimal value) {
    return finite(
        value.signum() < 0,
        6176 - value.scale(),
        value.unscaledValue().abs());
  }

  private static Bid128 finite(
      boolean negative, int exponent, BigInteger coefficient) {
    BigInteger mask = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    return Bid128.finite(
        negative,
        exponent,
        coefficient.shiftRight(64).longValue(),
        coefficient.and(mask).longValue());
  }

  private static BigInteger unsigned(long value) {
    BigInteger result = BigInteger.valueOf(value & Long.MAX_VALUE);
    return value < 0L ? result.setBit(63) : result;
  }

  private static java.math.RoundingMode javaMode(RoundingMode mode) {
    return switch (mode) {
      case TIES_TO_EVEN -> java.math.RoundingMode.HALF_EVEN;
      case TIES_AWAY -> java.math.RoundingMode.HALF_UP;
      case TOWARD_POSITIVE -> java.math.RoundingMode.CEILING;
      case TOWARD_NEGATIVE -> java.math.RoundingMode.FLOOR;
      case TOWARD_ZERO -> java.math.RoundingMode.DOWN;
    };
  }

  private static void check(
      Bid128 expected,
      Bid128 x,
      Bid128 y,
      RoundingMode mode,
      int expectedFlags) {
    StatusFlags flags = new StatusFlags();
    Bid128 actual = Bid128Divide.divide(x, y, mode, flags);
    if (!actual.equals(expected) || flags.bits() != expectedFlags) {
      throw new AssertionError(String.format(
          "divide(%s, %s, %s): expected %s %02x, actual %s %02x",
          x, y, mode, expected, expectedFlags, actual, flags.bits()));
    }
  }

  private static Bid128 raw(String high, String low) {
    return Bid128.fromRawBits(unsignedHex(high), unsignedHex(low));
  }

  private static long unsignedHex(String value) {
    return Long.parseUnsignedLong(value, 16);
  }
}
