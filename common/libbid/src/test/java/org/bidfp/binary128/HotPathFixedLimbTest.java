/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   * Neither the name of Intel Corporation nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
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
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.bidfp.binary128;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.Random;
import org.bidfp.binary128.tables.FourOverPi;
import org.bidfp.binary128.tables.TrigX;
import org.junit.jupiter.api.Test;

/** Differential checks for fixed-limb replacements of former BigInteger hot paths. */
final class HotPathFixedLimbTest {
  private static final BigInteger FOUR_OVER_PI = tableInteger();
  private static final int TABLE_BITS = FourOverPi.LENGTH * Long.SIZE;
  private static final int FOUR_OVER_PI_BINARY_POINT =
      TABLE_BITS - FourOverPi.FOUR_OV_PI_ZERO_PAD_LEN - 1;

  @Test
  void integerClassificationMatchesBigIntegerReference() {
    Random random = new Random(0x1a2b_3c4d_5e6f_7081L);
    for (int i = 0; i < 20_000; i++) {
      int biased = 1 + random.nextInt(0x7ffe);
      long high = ((long) biased << 48) | (random.nextLong() & 0x0000_ffff_ffff_ffffL);
      if (random.nextBoolean()) {
        high |= Long.MIN_VALUE;
      }
      Binary128 value = Binary128.fromRawBits(high, random.nextLong());
      assertEquals(referenceIntegerKind(value), DpmlPowCbrtSupport.integerKind(value));
    }
  }

  @Test
  void nearestIntegerMatchesBigIntegerReferenceIncludingTies() {
    Random random = new Random(0x6e65_6172_6573_7431L);
    for (int i = 0; i < 20_000; i++) {
      Unpacked value = new Unpacked();
      value.setNorm(
          random.nextBoolean() ? Unpacked.UX_SIGN_BIT : 0,
          random.nextInt(48) - 8,
          random.nextLong() | Long.MIN_VALUE,
          random.nextLong());
      assertEquals(referenceNearestInt(value), DpmlPowCbrtSupport.nearestInt(value));
    }

    for (int integer = 0; integer < 32; integer++) {
      Unpacked tie = unpackedIntegerAndHalf(integer);
      int expected = (integer & 1) == 0 ? integer : integer + 1;
      assertEquals(expected, DpmlPowCbrtSupport.nearestInt(tie));
      tie.sign = Unpacked.UX_SIGN_BIT;
      assertEquals(-expected, DpmlPowCbrtSupport.nearestInt(tie));
    }
  }

  @Test
  void payneHanekReductionMatchesFullTableBigIntegerReference() {
    Random random = new Random(0x7061_796e_6568_616eL);
    for (int i = 0; i < 5_000; i++) {
      Unpacked argument = new Unpacked();
      argument.setNorm(
          random.nextBoolean() ? Unpacked.UX_SIGN_BIT : 0,
          random.nextInt(16_385),
          random.nextLong() | Long.MIN_VALUE,
          random.nextLong());
      assertReductionMatchesReference(argument, random.nextInt(8) - 4);
    }
  }

  @Test
  void hugeAngleNeighborsPreserveOctantAndStickyBits() {
    long[] highFractions = {
        0xffff_ffff_ffff_ffffL,
        0xffff_ffff_ffff_fffeL,
        0x8000_0000_0000_0000L,
        0x8000_0000_0000_0001L
    };
    long[] lowFractions = {0L, 1L, -2L, -1L};
    for (int sign : new int[] {0, Unpacked.UX_SIGN_BIT}) {
      for (long high : highFractions) {
        for (long low : lowFractions) {
          Unpacked argument = new Unpacked();
          argument.setNorm(sign, 16_384, high, low);
          for (int octant = -4; octant <= 4; octant++) {
            assertReductionMatchesReference(argument, octant);
          }
        }
      }
    }
  }

  @Test
  void maxUxExponentDoesNotWalkOffFourOverPiTable() {
    Unpacked argument = new Unpacked();
    argument.setNorm(0, 16_384, Long.MIN_VALUE, 1L);
    assertReductionMatchesReference(argument, 0);
  }

  private static void assertReductionMatchesReference(Unpacked argument, int octant) {
    Unpacked expected = new Unpacked();
    Unpacked actual = new Unpacked();
    int expectedQuadrant = referenceReduce(argument, octant, expected);
    int actualQuadrant =
        UxRadianReduce.reduce(argument, octant, actual, new StatusFlags());
    assertEquals(expectedQuadrant, actualQuadrant, "quadrant for " + describe(argument));
    Binary128 expectedPacked =
        UxOps.pack(expected, RoundingMode.TIES_TO_EVEN, new StatusFlags());
    Binary128 actualPacked =
        UxOps.pack(actual, RoundingMode.TIES_TO_EVEN, new StatusFlags());
    int ulp = IntelF128Oracle.ulpDistance(expectedPacked, actualPacked);
    assertTrue(ulp <= 1, "reduced value for " + describe(argument) + " ulp=" + ulp);
  }

  private static int referenceReduce(Unpacked argument, int octant, Unpacked reduced) {
    BigInteger fraction = Wide.u128(argument.fracHi, argument.fracLo);
    BigInteger numerator = fraction.multiply(FOUR_OVER_PI);
    int denominatorShift =
        FOUR_OVER_PI_BINARY_POINT + 128 - argument.exponent;
    if (argument.sign != 0) {
      numerator = numerator.negate();
    }
    if (octant != 0) {
      numerator = numerator.add(BigInteger.valueOf(octant).shiftLeft(denominatorShift));
    }
    BigInteger quotient = nearestEvenPowerOfTwo(numerator, denominatorShift + 1);
    BigInteger remainder =
        numerator.subtract(quotient.shiftLeft(denominatorShift + 1));
    rationalToUnpacked(remainder, denominatorShift, reduced);
    Unpacked radians = new Unpacked();
    UxOps.mulUnpacked(
        reduced,
        UxTable.readUxFloat(TrigX.TABLE, TrigX.UX_PI_OVER_FOUR),
        radians,
        new StatusFlags());
    reduced.copyFrom(radians);
    return quotient.mod(BigInteger.valueOf(4)).intValue();
  }

  private static int referenceIntegerKind(Binary128 value) {
    if (!value.isFinite()) {
      return 0;
    }
    if (value.isZero()) {
      return 1;
    }
    int exponent = value.biasedExponent() - Binary128.BIAS;
    if (exponent < 0) {
      return 0;
    }
    if (exponent > Binary128.SIGNIFICAND_BITS) {
      return 1;
    }
    BigInteger significand = Wide.u128(value.significandHigh(), value.significandLow());
    int fractionalBits = Binary128.SIGNIFICAND_BITS - exponent;
    if (fractionalBits != 0
        && significand.and(BigInteger.ONE.shiftLeft(fractionalBits).subtract(BigInteger.ONE))
            .signum() != 0) {
      return 0;
    }
    return significand.testBit(fractionalBits) ? 2 : 1;
  }

  private static int referenceNearestInt(Unpacked value) {
    Unpacked normalized = value.copy();
    UxOps.normalize(normalized);
    if (normalized.exponent <= 0) {
      return 0;
    }
    if (normalized.exponent > 31) {
      return normalized.sign != 0 ? Integer.MIN_VALUE : Integer.MAX_VALUE;
    }
    BigInteger significand = Wide.u128(normalized.fracHi, normalized.fracLo);
    int shift = 128 - normalized.exponent;
    BigInteger integer = significand.shiftRight(shift);
    BigInteger remainder =
        significand.and(BigInteger.ONE.shiftLeft(shift).subtract(BigInteger.ONE));
    BigInteger half = BigInteger.ONE.shiftLeft(shift - 1);
    if (remainder.compareTo(half) > 0
        || remainder.equals(half) && integer.testBit(0)) {
      integer = integer.add(BigInteger.ONE);
    }
    int result = integer.intValue();
    return normalized.sign != 0 ? -result : result;
  }

  private static Unpacked unpackedIntegerAndHalf(int integer) {
    BigInteger value = BigInteger.valueOf(integer).shiftLeft(1).add(BigInteger.ONE);
    int bits = value.bitLength();
    BigInteger fraction = value.shiftLeft(128 - bits);
    Unpacked result = new Unpacked();
    result.setNorm(0, bits - 1, fraction.shiftRight(64).longValue(), fraction.longValue());
    return result;
  }

  private static BigInteger nearestEvenPowerOfTwo(BigInteger value, int shift) {
    boolean negative = value.signum() < 0;
    BigInteger magnitude = value.abs();
    BigInteger quotient = magnitude.shiftRight(shift);
    BigInteger remainder = magnitude.subtract(quotient.shiftLeft(shift));
    BigInteger half = BigInteger.ONE.shiftLeft(shift - 1);
    if (remainder.compareTo(half) > 0
        || remainder.equals(half) && quotient.testBit(0)) {
      quotient = quotient.add(BigInteger.ONE);
    }
    return negative ? quotient.negate() : quotient;
  }

  private static void rationalToUnpacked(
      BigInteger numerator, int denominatorShift, Unpacked result) {
    if (numerator.signum() == 0) {
      result.setZero(0);
      return;
    }
    int sign = numerator.signum() < 0 ? Unpacked.UX_SIGN_BIT : 0;
    BigInteger magnitude = numerator.abs();
    int bits = magnitude.bitLength();
    BigInteger fraction;
    if (bits > 128) {
      int discarded = bits - 128;
      fraction = magnitude.shiftRight(discarded);
      if (magnitude.getLowestSetBit() < discarded) {
        fraction = fraction.setBit(0);
      }
    } else {
      fraction = magnitude.shiftLeft(128 - bits);
    }
    result.setNorm(
        sign,
        bits - denominatorShift,
        fraction.shiftRight(64).longValue(),
        fraction.longValue());
  }

  private static BigInteger tableInteger() {
    BigInteger value = BigInteger.ZERO;
    for (long word : FourOverPi.TABLE.copy()) {
      value = value.shiftLeft(64).or(Wide.u128(0L, word));
    }
    return value;
  }

  private static String describe(Unpacked value) {
    return "sign=" + value.sign + " exponent=" + value.exponent
        + " fraction=" + Long.toUnsignedString(value.fracHi, 16)
        + Long.toUnsignedString(value.fracLo, 16);
  }
}
