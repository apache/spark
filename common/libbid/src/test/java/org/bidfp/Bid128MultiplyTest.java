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

/** Special-value and boundary tests for {@link Bid128Multiply}. */
public final class Bid128MultiplyTest {
  private Bid128MultiplyTest() {
  }

  public static void main(String[] args) {
    testSpecialValues();
    testExactFiniteValues();
    testRandomDifferential();
    testPreferredZeroExponent();
    testFlagsAccumulate();
    System.out.println(
        "Bid128MultiplyTest: all tests passed (2500 differential cases)");
  }

  private static void testSpecialValues() {
    check(
        Bid128.QUIET_NAN,
        Bid128.POSITIVE_ZERO,
        Bid128.POSITIVE_INFINITY,
        RoundingMode.TIES_TO_EVEN,
        StatusFlags.INVALID);
    check(
        Bid128.NEGATIVE_INFINITY,
        Bid128.finite(true, 6176, 0L, 1L),
        Bid128.POSITIVE_INFINITY,
        RoundingMode.TIES_TO_EVEN,
        0);
    check(
        Bid128.fromRawBits(0xfc00_0000_0000_0123L, 0x4567L),
        Bid128.fromRawBits(0xfe00_0000_0000_0123L, 0x4567L),
        Bid128.finite(false, 6176, 0L, 2L),
        RoundingMode.TIES_TO_EVEN,
        StatusFlags.INVALID);
  }

  private static void testExactFiniteValues() {
    check(
        Bid128.finite(false, 6176, 0L, 42L),
        Bid128.finite(false, 6176, 0L, 6L),
        Bid128.finite(false, 6176, 0L, 7L),
        RoundingMode.TIES_TO_EVEN,
        0);
    check(
        Bid128.finite(true, 0, 0x0000_314d_c644_8d93L, 0x38c1_5b0a_0000_0000L),
        Bid128.finite(true, 0, 0x0000_314d_c644_8d93L, 0x38c1_5b0a_0000_0000L),
        Bid128.finite(false, 6176, 0L, 1L),
        RoundingMode.TIES_TO_EVEN,
        0);
  }

  private static void testPreferredZeroExponent() {
    Bid128 zero = Bid128Multiply.multiply(
        Bid128.finite(true, 100, 0L, 0L),
        Bid128.finite(false, 200, 0L, 17L),
        RoundingMode.TIES_TO_EVEN,
        new StatusFlags());
    check(zero.isSigned(), "zero sign");
    check(zero.biasedExponent() == 0, "clamped preferred zero exponent");
  }

  private static void testRandomDifferential() {
    Random random = new Random(0x128_6d75L);
    BigInteger limit = BigInteger.TEN.pow(34);
    for (int sample = 0; sample < 500; sample++) {
      BigInteger xCoefficient = randomCoefficient(random, limit);
      BigInteger yCoefficient = randomCoefficient(random, limit);
      int xExponent = random.nextInt(81) - 40;
      int yExponent = random.nextInt(81) - 40;
      boolean xNegative = random.nextBoolean();
      boolean yNegative = random.nextBoolean();
      Bid128 x = finite(xNegative, 6176 + xExponent, xCoefficient);
      Bid128 y = finite(yNegative, 6176 + yExponent, yCoefficient);
      BigDecimal xd = decimal(xCoefficient, xExponent, xNegative);
      BigDecimal yd = decimal(yCoefficient, yExponent, yNegative);
      for (RoundingMode mode : RoundingMode.values()) {
        MathContext context = new MathContext(34, javaMode(mode));
        BigDecimal exact = xd.multiply(yd);
        BigDecimal rounded = xd.multiply(yd, context);
        StatusFlags flags = new StatusFlags();
        Bid128 actual = Bid128Multiply.multiply(x, y, mode, flags);
        check(actual.equals(encode(rounded)), "random differential result");
        int expectedFlags = exact.compareTo(rounded) == 0 ? 0 : StatusFlags.INEXACT;
        check(flags.bits() == expectedFlags, "random differential flags");
      }
    }
  }

  private static void testFlagsAccumulate() {
    StatusFlags flags = new StatusFlags();
    flags.raise(StatusFlags.DIVIDE_BY_ZERO);
    Bid128Multiply.multiply(
        Bid128.POSITIVE_ZERO,
        Bid128.POSITIVE_INFINITY,
        RoundingMode.TIES_TO_EVEN,
        flags);
    int expected = StatusFlags.DIVIDE_BY_ZERO | StatusFlags.INVALID;
    check(flags.bits() == expected, "status flags accumulate");
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

  private static Bid128 finite(
      boolean negative, int exponent, BigInteger coefficient) {
    BigInteger mask = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    return Bid128.finite(
        negative,
        exponent,
        coefficient.shiftRight(64).longValue(),
        coefficient.and(mask).longValue());
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
    Bid128 actual = Bid128Multiply.multiply(x, y, mode, flags);
    if (!actual.equals(expected) || flags.bits() != expectedFlags) {
      throw new AssertionError(String.format(
          "multiply(%s, %s, %s): expected %s %02x, actual %s %02x",
          x, y, mode, expected, expectedFlags, actual, flags.bits()));
    }
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
