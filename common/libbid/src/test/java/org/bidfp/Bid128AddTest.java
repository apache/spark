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

/** Special-value and finite-oracle tests for {@link Bid128Add}. */
public final class Bid128AddTest {
  private Bid128AddTest() {
  }

  public static void main(String[] args) {
    testSpecialValues();
    testSignedZeros();
    testExactFiniteOracle();
    testUnequalExponentDifferential();
    testFlagsAccumulate();
    System.out.println(
        "Bid128AddTest: all tests passed (10000 differential cases)");
  }

  private static void testSpecialValues() {
    check(
        Bid128.POSITIVE_INFINITY,
        false,
        Bid128.POSITIVE_INFINITY,
        Bid128.parseExact("1"),
        RoundingMode.TIES_TO_EVEN,
        0);
    check(
        Bid128.QUIET_NAN,
        false,
        Bid128.POSITIVE_INFINITY,
        Bid128.NEGATIVE_INFINITY,
        RoundingMode.TIES_TO_EVEN,
        StatusFlags.INVALID);
    check(
        Bid128.fromRawBits(0x7c00_0000_0000_0123L, 0x456L),
        false,
        Bid128.fromRawBits(0x7e00_0000_0000_0123L, 0x456L),
        Bid128.parseExact("1"),
        RoundingMode.TIES_TO_EVEN,
        StatusFlags.INVALID);
  }

  private static void testSignedZeros() {
    Bid128 positive = Bid128.finite(false, 200, 0, 7);
    Bid128 negative = Bid128.finite(true, 200, 0, 7);
    check(
        Bid128.finite(true, 200, 0, 0),
        false,
        positive,
        negative,
        RoundingMode.TOWARD_NEGATIVE,
        0);
    check(
        Bid128.finite(false, 200, 0, 0),
        false,
        positive,
        negative,
        RoundingMode.TIES_TO_EVEN,
        0);
    check(
        Bid128.finite(true, 100, 0, 0),
        false,
        Bid128.finite(true, 300, 0, 0),
        Bid128.finite(false, 100, 0, 0),
        RoundingMode.TOWARD_NEGATIVE,
        0);
  }

  private static void testExactFiniteOracle() {
    Random random = new Random(0x128addL);
    BigInteger limit = BigInteger.TEN.pow(34);
    for (int i = 0; i < 2_000; i++) {
      int exponent = random.nextInt(12_288);
      BigInteger xCoefficient = new BigInteger(112, random).mod(limit);
      BigInteger yCoefficient = new BigInteger(112, random).mod(limit);
      boolean xNegative = random.nextBoolean();
      boolean yNegative = random.nextBoolean();
      BigInteger expected = xNegative ? xCoefficient.negate() : xCoefficient;
      expected = expected.add(yNegative ? yCoefficient.negate() : yCoefficient);
      if (expected.abs().compareTo(limit) >= 0) {
        i--;
        continue;
      }
      Bid128 x = finite(xNegative, exponent, xCoefficient);
      Bid128 y = finite(yNegative, exponent, yCoefficient);
      Bid128 expectedBid = finite(expected.signum() < 0, exponent, expected.abs());
      StatusFlags flags = new StatusFlags();
      Bid128 actual = Bid128Add.add(x, y, RoundingMode.TIES_TO_EVEN, flags);
      if (!actual.equals(expectedBid) || flags.bits() != 0) {
        throw new AssertionError(
            "exact oracle: expected " + expectedBid + ", actual " + actual);
      }
    }
  }

  private static void testFlagsAccumulate() {
    StatusFlags flags = new StatusFlags();
    flags.raise(StatusFlags.DIVIDE_BY_ZERO);
    Bid128Add.add(
        Bid128.POSITIVE_INFINITY,
        Bid128.NEGATIVE_INFINITY,
        RoundingMode.TIES_TO_EVEN,
        flags);
    int expected = StatusFlags.DIVIDE_BY_ZERO | StatusFlags.INVALID;
    if (flags.bits() != expected) {
      throw new AssertionError(
          String.format("accumulated flags: expected %02x, actual %02x", expected, flags.bits()));
    }
  }

  private static void testUnequalExponentDifferential() {
    Random random = new Random(0x128_a11dL);
    BigInteger limit = BigInteger.TEN.pow(34);
    for (int sample = 0; sample < 2_000; sample++) {
      BigInteger left = randomCoefficient(random, limit);
      BigInteger right = randomCoefficient(random, limit);
      int leftExponent = random.nextInt(121) - 60;
      int rightExponent;
      do {
        rightExponent = random.nextInt(121) - 60;
      } while (rightExponent == leftExponent);
      boolean leftNegative = random.nextBoolean();
      boolean rightNegative = random.nextBoolean();
      Bid128 x = finite(leftNegative, 6176 + leftExponent, left);
      Bid128 y = finite(rightNegative, 6176 + rightExponent, right);
      BigDecimal xd = decimal(left, leftExponent, leftNegative);
      BigDecimal yd = decimal(right, rightExponent, rightNegative);
      for (RoundingMode mode : RoundingMode.values()) {
        BigDecimal exact = xd.add(yd);
        BigDecimal rounded = xd.add(yd, new MathContext(34, javaMode(mode)));
        StatusFlags flags = new StatusFlags();
        Bid128 actual = Bid128Add.add(x, y, mode, flags);
        if (!actual.equals(encode(rounded))) {
          throw new AssertionError(
              "differential add: expected " + rounded + ", actual "
                  + actual.toCanonicalString());
        }
        int expectedFlags = exact.compareTo(rounded) == 0 ? 0 : StatusFlags.INEXACT;
        if (flags.bits() != expectedFlags) {
          throw new AssertionError("differential add flags");
        }
      }
    }
  }

  private static BigInteger randomCoefficient(Random random, BigInteger limit) {
    BigInteger result;
    do {
      result = new BigInteger(113, random);
    } while (result.signum() == 0 || result.compareTo(limit) >= 0);
    return result;
  }

  private static BigDecimal decimal(
      BigInteger coefficient, int exponent, boolean negative) {
    return new BigDecimal(negative ? coefficient.negate() : coefficient, -exponent);
  }

  private static Bid128 encode(BigDecimal value) {
    return finite(
        value.signum() < 0,
        6176 - value.scale(),
        value.unscaledValue().abs());
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

  private static Bid128 finite(boolean negative, int exponent, BigInteger coefficient) {
    BigInteger mask = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    return Bid128.finite(
        negative,
        exponent,
        coefficient.shiftRight(64).longValue(),
        coefficient.and(mask).longValue());
  }

  private static void check(
      Bid128 expected,
      boolean subtract,
      Bid128 x,
      Bid128 y,
      RoundingMode mode,
      int expectedFlags) {
    StatusFlags flags = new StatusFlags();
    Bid128 result = subtract
        ? Bid128Add.subtract(x, y, mode, flags)
        : Bid128Add.add(x, y, mode, flags);
    if (!result.equals(expected) || flags.bits() != expectedFlags) {
      throw new AssertionError(String.format(
          "%s(%s, %s, %s): expected %s %02x, actual %s %02x",
          subtract ? "subtract" : "add",
          x,
          y,
          mode,
          expected,
          expectedFlags,
          result,
          flags.bits()));
    }
  }
}
