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

import java.math.BigInteger;
import java.util.Random;

/** Special-value and finite-oracle tests for {@link Bid64Add}. */
public final class Bid64AddTest {
  private Bid64AddTest() {
  }

  public static void main(String[] args) {
    testSpecialValues();
    testSignedZeros();
    testExactFiniteOracle();
    testFlagsAccumulate();
    System.out.println("Bid64AddTest: all tests passed");
  }

  private static void testSpecialValues() {
    check(
        Bid64.POSITIVE_INFINITY.toRawBits(),
        false,
        Bid64.POSITIVE_INFINITY,
        Bid64.finite(false, 398, 1L),
        RoundingMode.TIES_TO_EVEN,
        0);
    check(
        Bid64.QUIET_NAN.toRawBits(),
        false,
        Bid64.POSITIVE_INFINITY,
        Bid64.NEGATIVE_INFINITY,
        RoundingMode.TIES_TO_EVEN,
        StatusFlags.INVALID);
    check(
        0x7c00_0000_0000_0123L,
        false,
        Bid64.fromRawBits(0x7e00_0000_0000_0123L),
        Bid64.finite(false, 398, 1L),
        RoundingMode.TIES_TO_EVEN,
        StatusFlags.INVALID);
    check(
        0x7c00_0000_0000_0456L,
        true,
        Bid64.finite(false, 398, 1L),
        Bid64.fromRawBits(0x7e00_0000_0000_0456L),
        RoundingMode.TIES_TO_EVEN,
        StatusFlags.INVALID);
  }

  private static void testSignedZeros() {
    check(
        Bid64.finite(true, 200, 0L).toRawBits(),
        false,
        Bid64.finite(false, 200, 7L),
        Bid64.finite(true, 200, 7L),
        RoundingMode.TOWARD_NEGATIVE,
        0);
    check(
        Bid64.finite(false, 200, 0L).toRawBits(),
        false,
        Bid64.finite(false, 200, 7L),
        Bid64.finite(true, 200, 7L),
        RoundingMode.TOWARD_ZERO,
        0);
    check(
        Bid64.finite(true, 100, 0L).toRawBits(),
        false,
        Bid64.finite(true, 300, 0L),
        Bid64.finite(false, 100, 0L),
        RoundingMode.TOWARD_NEGATIVE,
        0);
  }

  private static void testExactFiniteOracle() {
    Random random = new Random(0x64addL);
    for (int i = 0; i < 2_000; i++) {
      int exponent = random.nextInt(768);
      long xCoefficient = random.nextLong(5_000_000_000_000_000L);
      long yCoefficient = random.nextLong(5_000_000_000_000_000L);
      boolean xNegative = random.nextBoolean();
      boolean yNegative = random.nextBoolean();
      BigInteger expected = BigInteger.valueOf(xCoefficient);
      if (xNegative) {
        expected = expected.negate();
      }
      BigInteger addend = BigInteger.valueOf(yCoefficient);
      if (yNegative) {
        addend = addend.negate();
      }
      expected = expected.add(addend);
      boolean expectedNegative = expected.signum() < 0;
      long expectedCoefficient = expected.abs().longValueExact();
      if (expectedCoefficient > 9_999_999_999_999_999L) {
        i--;
        continue;
      }

      StatusFlags flags = new StatusFlags();
      Bid64 actual = Bid64Add.add(
          Bid64.finite(xNegative, exponent, xCoefficient),
          Bid64.finite(yNegative, exponent, yCoefficient),
          RoundingMode.TIES_TO_EVEN,
          flags);
      Bid64 expectedBid = Bid64.finite(expectedNegative, exponent, expectedCoefficient);
      if (!actual.equals(expectedBid) || flags.bits() != 0) {
        throw new AssertionError(
            "exact oracle: expected " + expectedBid + ", actual " + actual);
      }
    }
  }

  private static void testFlagsAccumulate() {
    StatusFlags flags = new StatusFlags();
    flags.raise(StatusFlags.DIVIDE_BY_ZERO);
    Bid64Add.add(
        Bid64.POSITIVE_INFINITY,
        Bid64.NEGATIVE_INFINITY,
        RoundingMode.TIES_TO_EVEN,
        flags);
    int expected = StatusFlags.DIVIDE_BY_ZERO | StatusFlags.INVALID;
    if (flags.bits() != expected) {
      throw new AssertionError(
          String.format("accumulated flags: expected %02x, actual %02x", expected, flags.bits()));
    }
  }

  private static void check(
      long expected,
      boolean subtract,
      Bid64 x,
      Bid64 y,
      RoundingMode mode,
      int expectedFlags) {
    StatusFlags flags = new StatusFlags();
    Bid64 result = subtract
        ? Bid64Add.subtract(x, y, mode, flags)
        : Bid64Add.add(x, y, mode, flags);
    if (result.toRawBits() != expected || flags.bits() != expectedFlags) {
      throw new AssertionError(String.format(
          "%s(%s, %s, %s): expected [0x%016x] %02x, actual [0x%016x] %02x",
          subtract ? "subtract" : "add",
          x,
          y,
          mode,
          expected,
          expectedFlags,
          result.toRawBits(),
          flags.bits()));
    }
  }
}
