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
package org.bidfp.binary128;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.Random;
import org.junit.jupiter.api.Test;

final class Binary128Test {
  private static final RoundingMode RN = RoundingMode.TIES_TO_EVEN;
  private static final Binary128 TWO =
      Binary128.fromRawBits(0x4000_0000_0000_0000L, 0L);

  @Test
  void classifiesZeroInfAndNan() {
    Binary128Check.main(new String[0]);
  }

  @Test
  void packedLayoutMatchesIntelHighLow() {
    Binary128 one = Binary128.ONE;
    assertEquals(0x3fff_0000_0000_0000L, one.highBits());
    assertEquals(0L, one.lowBits());
    assertEquals(0x3fff, one.biasedExponent());
    assertEquals(0L, one.fractionHigh());
    assertEquals(1L << 48, one.significandHigh());
    assertFalse(one.isSigned());
    assertTrue(one.isNormal());
  }

  @Test
  void binary64RoundTripsExactly() {
    long[] samples = {
        0x0000_0000_0000_0000L,
        0x8000_0000_0000_0000L,
        0x0000_0000_0000_0001L,
        0x000f_ffff_ffff_ffffL,
        0x0010_0000_0000_0000L,
        0x3ff8_0000_0000_0000L,
        0x7fef_ffff_ffff_ffffL,
        0xffef_ffff_ffff_ffffL
    };
    for (long bits : samples) {
      double input = Double.longBitsToDouble(bits);
      StatusFlags status = new StatusFlags();
      double output = Binary128.fromBinary64(input).toBinary64(RN, status);
      assertEquals(bits, Double.doubleToRawLongBits(output));
      assertEquals(0, status.bits());
    }
  }

  @Test
  void fromBinary64ConvertsSmallestSubnormalExactly() {
    assertEquals(
        Binary128.fromRawBits(0x3bcd_0000_0000_0000L, 0L),
        Binary128.fromBinary64(Double.MIN_VALUE));
  }

  @Test
  void binary64TieHonorsAllFiveModes() {
    Binary128 halfway = Binary128.fromRawBits(
        0x3fff_0000_0000_0000L, 1L << 59);
    assertDoubleBits(0x3ff0_0000_0000_0000L, halfway, RoundingMode.TIES_TO_EVEN);
    assertDoubleBits(0x3ff0_0000_0000_0000L, halfway, RoundingMode.TOWARD_NEGATIVE);
    assertDoubleBits(0x3ff0_0000_0000_0001L, halfway, RoundingMode.TOWARD_POSITIVE);
    assertDoubleBits(0x3ff0_0000_0000_0000L, halfway, RoundingMode.TOWARD_ZERO);
    assertDoubleBits(0x3ff0_0000_0000_0001L, halfway, RoundingMode.TIES_AWAY);

    Binary128 negativeHalfway = halfway.negate();
    assertDoubleBits(
        0xbff0_0000_0000_0000L, negativeHalfway, RoundingMode.TIES_TO_EVEN);
    assertDoubleBits(
        0xbff0_0000_0000_0001L, negativeHalfway, RoundingMode.TOWARD_NEGATIVE);
    assertDoubleBits(
        0xbff0_0000_0000_0000L, negativeHalfway, RoundingMode.TOWARD_POSITIVE);
    assertDoubleBits(
        0xbff0_0000_0000_0000L, negativeHalfway, RoundingMode.TOWARD_ZERO);
    assertDoubleBits(
        0xbff0_0000_0000_0001L, negativeHalfway, RoundingMode.TIES_AWAY);
  }

  @Test
  void binary64GradualUnderflowAndPromotionAreRounded() {
    Binary128 minimumNormal = Binary128.fromBinary64(Double.MIN_NORMAL);
    Binary128 halfMinimumSubnormal =
        Binary128.fromRawBits(0x3bcc_0000_0000_0000L, 0L);
    Binary128 midpoint = minimumNormal.subtract(
        halfMinimumSubnormal, RN, new StatusFlags());

    StatusFlags nearestStatus = new StatusFlags();
    double nearest = midpoint.toBinary64(RN, nearestStatus);
    assertEquals(0x0010_0000_0000_0000L, Double.doubleToRawLongBits(nearest));
    assertEquals(StatusFlags.INEXACT, nearestStatus.bits());

    StatusFlags zeroStatus = new StatusFlags();
    double towardZero = midpoint.toBinary64(RoundingMode.TOWARD_ZERO, zeroStatus);
    assertEquals(0x000f_ffff_ffff_ffffL, Double.doubleToRawLongBits(towardZero));
    assertEquals(
        StatusFlags.UNDERFLOW | StatusFlags.INEXACT,
        zeroStatus.bits());
  }

  @Test
  void binary64OverflowHonorsDirectedModes() {
    Binary128 twoTo1024 =
        Binary128.fromRawBits(0x43ff_0000_0000_0000L, 0L);
    StatusFlags nearestStatus = new StatusFlags();
    assertEquals(
        Double.POSITIVE_INFINITY,
        twoTo1024.toBinary64(RN, nearestStatus));
    assertEquals(
        StatusFlags.OVERFLOW | StatusFlags.INEXACT,
        nearestStatus.bits());

    StatusFlags zeroStatus = new StatusFlags();
    assertEquals(
        Double.MAX_VALUE,
        twoTo1024.toBinary64(RoundingMode.TOWARD_ZERO, zeroStatus));
    assertEquals(
        StatusFlags.OVERFLOW | StatusFlags.INEXACT,
        zeroStatus.bits());
  }

  @Test
  void binary128SubnormalRoundsUpToMinimumNormal() {
    Binary128 largestSubnormal =
        Binary128.fromRawBits(0x0000_ffff_ffff_ffffL, -1L);
    Binary128 minimumNormal =
        Binary128.fromRawBits(0x0001_0000_0000_0000L, 0L);
    Binary128 sum = largestSubnormal.add(minimumNormal, RN, new StatusFlags());

    assertEquals(
        minimumNormal,
        sum.divide(TWO, RN, new StatusFlags()));
    assertEquals(
        largestSubnormal,
        sum.divide(TWO, RoundingMode.TOWARD_ZERO, new StatusFlags()));
  }

  @Test
  void arithmeticRoundsExactResultsAndTies() {
    Binary128 halfUlp =
        Binary128.fromFields(false, 0x3fff - 113, 0L, 0L);
    Binary128 next = Binary128.fromRawBits(
        Binary128.ONE.highBits(), Binary128.ONE.lowBits() + 1);
    assertEquals(
        Binary128.ONE,
        Binary128.ONE.add(halfUlp, RN, new StatusFlags()));
    assertEquals(
        next,
        Binary128.ONE.add(
            halfUlp, RoundingMode.TOWARD_POSITIVE, new StatusFlags()));

    Binary128 oneThird = Binary128.fromRawBits(
        0x3ffd_5555_5555_5555L, 0x5555_5555_5555_5555L);
    assertEquals(
        oneThird,
        Binary128.ONE.divide(
            Binary128.fromBinary64(3.0), RN, new StatusFlags()));
    assertEquals(
        TWO,
        TWO.multiply(Binary128.ONE, RN, new StatusFlags()));
  }

  @Test
  void squareRootUsesRemainderForDirectedRounding() {
    Binary128 lower = Binary128.fromRawBits(
        0x3fff_6a09_e667_f3bcL, 0xc908_b2fb_1366_ea95L);
    Binary128 upper = Binary128.fromRawBits(
        0x3fff_6a09_e667_f3bcL, 0xc908_b2fb_1366_ea96L);
    StatusFlags downStatus = new StatusFlags();
    StatusFlags upStatus = new StatusFlags();
    assertEquals(lower, TWO.sqrt(RoundingMode.TOWARD_ZERO, downStatus));
    assertEquals(upper, TWO.sqrt(RoundingMode.TOWARD_POSITIVE, upStatus));
    assertEquals(StatusFlags.INEXACT, downStatus.bits());
    assertEquals(StatusFlags.INEXACT, upStatus.bits());
  }

  @Test
  void cancellationZeroSignFollowsRoundingMode() {
    assertEquals(
        Binary128.NEGATIVE_ZERO,
        Binary128.ONE.add(
            Binary128.ONE.negate(),
            RoundingMode.TOWARD_NEGATIVE,
            new StatusFlags()));
    assertEquals(
        Binary128.ZERO,
        Binary128.ONE.add(
            Binary128.ONE.negate(),
            RoundingMode.TOWARD_POSITIVE,
            new StatusFlags()));
    assertEquals(
        Binary128.NEGATIVE_ZERO,
        Binary128.NEGATIVE_ZERO.add(
            Binary128.NEGATIVE_ZERO, RN, new StatusFlags()));
  }

  @Test
  void nanPayloadIsQuietedAndPreserved() {
    Binary128 signaling = Binary128.fromRawBits(
        0xffff_1234_5678_9abcL, 0xdef0_1234_5678_9abcL);
    StatusFlags status = new StatusFlags();
    Binary128 result = signaling.add(Binary128.ONE, RN, status);
    assertEquals(
        Binary128.fromRawBits(
            0xffff_9234_5678_9abcL, 0xdef0_1234_5678_9abcL),
        result);
    assertEquals(StatusFlags.INVALID, status.bits());

    StatusFlags packStatus = new StatusFlags();
    assertEquals(result, UxOps.pack(UxOps.unpack(signaling), RN, packStatus));
    assertEquals(StatusFlags.INVALID, packStatus.bits());

    Binary128 quiet = Binary128.fromRawBits(
        0x7fff_abcd_0123_4567L, 0x89ab_cdef_0123_4567L);
    assertEquals(quiet, Binary128.ONE.subtract(quiet, RN, new StatusFlags()));
    assertEquals(quiet, quiet.multiply(Binary128.ONE, RN, new StatusFlags()));
  }

  @Test
  void specialArithmeticRaisesExactFlags() {
    StatusFlags divideStatus = new StatusFlags();
    assertEquals(
        Binary128.POSITIVE_INFINITY,
        Binary128.ONE.divide(Binary128.ZERO, RN, divideStatus));
    assertEquals(StatusFlags.DIVIDE_BY_ZERO, divideStatus.bits());

    StatusFlags invalidStatus = new StatusFlags();
    assertTrue(
        Binary128.ZERO.multiply(
            Binary128.POSITIVE_INFINITY, RN, invalidStatus).isNaN());
    assertEquals(StatusFlags.INVALID, invalidStatus.bits());
    assertEquals(
        Binary128.NAN,
        Binary128.ZERO.multiply(
            Binary128.POSITIVE_INFINITY, RN, new StatusFlags()));

    StatusFlags denormalStatus = new StatusFlags();
    Binary128 minimumSubnormal = Binary128.fromRawBits(0L, 1L);
    minimumSubnormal.add(Binary128.ZERO, RN, denormalStatus);
    assertTrue(denormalStatus.contains(StatusFlags.DENORMAL));
  }

  @Test
  void binary128OverflowHonorsAllModes() {
    assertOverflow(Binary128.POSITIVE_INFINITY, RoundingMode.TIES_TO_EVEN);
    assertOverflow(Binary128.POSITIVE_INFINITY, RoundingMode.TIES_AWAY);
    assertOverflow(Binary128.POSITIVE_INFINITY, RoundingMode.TOWARD_POSITIVE);
    assertOverflow(Binary128.POSITIVE_MAX, RoundingMode.TOWARD_NEGATIVE);
    assertOverflow(Binary128.POSITIVE_MAX, RoundingMode.TOWARD_ZERO);
  }

  @Test
  void directNormalPackMatchesExactRounding() {
    Random random = new Random(0x128_5eedL);
    for (int trial = 0; trial < 10_000; trial++) {
      int sign = random.nextBoolean() ? Unpacked.UX_SIGN_BIT : 0;
      int exponent = random.nextInt(20_000) - 10_000;
      long high = random.nextLong() | Unpacked.UX_MSB;
      long low = random.nextLong();
      Unpacked unpacked = new Unpacked();
      unpacked.setNorm(sign, exponent, high, low);
      for (RoundingMode mode : RoundingMode.values()) {
        StatusFlags expectedStatus = new StatusFlags();
        Binary128 expected = IeeeRound.binary128(
            sign != 0,
            Wide.u128(high, low),
            BigInteger.ONE,
            exponent - 128,
            mode,
            expectedStatus);
        StatusFlags actualStatus = new StatusFlags();
        Binary128 actual = UxOps.pack(unpacked, mode, actualStatus);
        assertEquals(expected, actual);
        assertEquals(expectedStatus.bits(), actualStatus.bits());
      }
    }
  }

  @Test
  void packedAddSubAndMultiplyMatchExactReference() {
    Random random = new Random(0xadd_0128L);
    for (int trial = 0; trial < 2_000; trial++) {
      Binary128 x = randomNormal(random);
      Binary128 y = randomNormal(random);
      for (RoundingMode mode : RoundingMode.values()) {
        assertAddMatchesExact(x, y, false, mode);
        assertAddMatchesExact(x, y, true, mode);
        assertMultiplyMatchesExact(x, y, mode);
      }
    }
  }

  @Test
  void fixedLimbDivisionMatchesBigIntegerQuotientAndRemainder() {
    Random random = new Random(0xd1f_0128L);
    for (int trial = 0; trial < 20_000; trial++) {
      long aHigh = random.nextLong() | Unpacked.UX_MSB;
      long aLow = random.nextLong();
      long bHigh = random.nextLong() | Unpacked.UX_MSB;
      long bLow = random.nextLong();
      BigInteger a = Wide.u128(aHigh, aLow);
      BigInteger b = Wide.u128(bHigh, bLow);
      BigInteger[] expected = a.shiftLeft(128).divideAndRemainder(b);
      long[] division = new long[5];
      Wide.divFrac128(aHigh, aLow, bHigh, bLow, division);
      BigInteger actualQuotient = Wide.u128(division[1], division[2]);
      if (division[0] != 0L) {
        actualQuotient = actualQuotient.setBit(128);
      }
      String context = "trial " + trial;
      assertEquals(expected[0], actualQuotient, context);
      assertEquals(expected[1], Wide.u128(division[3], division[4]), context);
    }
  }

  @Test
  void fixedLimbSquareRootMatchesBigIntegerFloorAndSticky() {
    Random random = new Random(0x5a7_0128L);
    for (int trial = 0; trial < 20_000; trial++) {
      long high = random.nextLong() | Unpacked.UX_MSB;
      long low = random.nextLong();
      for (boolean odd : new boolean[] {false, true}) {
        BigInteger radicand = Wide.u128(high, low).shiftLeft(128 + (odd ? 1 : 0));
        BigInteger expectedRoot = radicand.sqrt();
        boolean expectedSticky = !expectedRoot.multiply(expectedRoot).equals(radicand);
        long[] root = new long[3];
        boolean actualSticky = Wide.sqrtScaled128(high, low, odd, root);
        BigInteger actualRoot = Wide.u128(root[1], root[2]);
        if (root[0] != 0L) {
          actualRoot = actualRoot.setBit(128);
        }
        String context = "trial " + trial + ", odd " + odd;
        assertEquals(expectedRoot, actualRoot, context);
        assertEquals(expectedSticky, actualSticky, context);
      }
    }
  }

  @Test
  void packedDivisionAndSquareRootMatchBigIntegerReference() {
    Random random = new Random(0xd175_5a7L);
    for (int trial = 0; trial < 4_000; trial++) {
      Binary128 x = randomFiniteNonzero(random);
      Binary128 y = randomFiniteNonzero(random);
      Binary128 positive = Binary128.fromRawBits(
          x.highBits() & ~Binary128.MASK_SIGN, x.lowBits());
      for (RoundingMode mode : RoundingMode.values()) {
        assertDivideMatchesExact(x, y, mode);
        assertSqrtMatchesExact(positive, mode);
      }
    }
  }

  @Test
  void packedDivisionAndSquareRootPreserveSpecialSemantics() {
    Binary128 signaling = Binary128.fromRawBits(
        0xffff_1234_5678_9abcL, 0xdef0_1234_5678_9abcL);
    Binary128 quieted = Binary128.fromRawBits(
        0xffff_9234_5678_9abcL, 0xdef0_1234_5678_9abcL);
    StatusFlags divStatus = new StatusFlags();
    assertEquals(quieted, Binary128.ONE.divide(signaling, RN, divStatus));
    assertEquals(StatusFlags.INVALID, divStatus.bits());
    StatusFlags sqrtStatus = new StatusFlags();
    assertEquals(quieted, signaling.sqrt(RN, sqrtStatus));
    assertEquals(StatusFlags.INVALID, sqrtStatus.bits());

    StatusFlags zeroStatus = new StatusFlags();
    assertEquals(
        Binary128.NEGATIVE_INFINITY,
        Binary128.ONE.negate().divide(Binary128.ZERO, RN, zeroStatus));
    assertEquals(StatusFlags.DIVIDE_BY_ZERO, zeroStatus.bits());
    assertEquals(
        Binary128.NEGATIVE_ZERO,
        Binary128.NEGATIVE_ZERO.sqrt(RN, new StatusFlags()));
  }

  private static Binary128 randomNormal(Random random) {
    boolean sign = random.nextBoolean();
    int exponent = 1 + random.nextInt(0x7ffe);
    long fractionHigh = random.nextLong() & Binary128.MASK_FRACTION_HIGH;
    return Binary128.fromFields(sign, exponent, fractionHigh, random.nextLong());
  }

  private static Binary128 randomFiniteNonzero(Random random) {
    boolean sign = random.nextBoolean();
    int exponent = random.nextInt(0x7fff);
    long fractionHigh = random.nextLong() & Binary128.MASK_FRACTION_HIGH;
    long fractionLow = random.nextLong();
    if (exponent == 0 && fractionHigh == 0L && fractionLow == 0L) {
      fractionLow = 1L;
    }
    return Binary128.fromFields(sign, exponent, fractionHigh, fractionLow);
  }

  private static void assertAddMatchesExact(
      Binary128 x, Binary128 y, boolean subtract, RoundingMode mode) {
    IeeeRound.Finite a = IeeeRound.decode(x);
    IeeeRound.Finite b = IeeeRound.decode(y);
    int commonExponent = Math.min(a.exponent, b.exponent);
    BigInteger left = a.significand.shiftLeft(a.exponent - commonExponent);
    BigInteger right = b.significand.shiftLeft(b.exponent - commonExponent);
    if (a.negative) {
      left = left.negate();
    }
    if (b.negative ^ subtract) {
      right = right.negate();
    }
    BigInteger sum = left.add(right);
    boolean negative = sum.signum() < 0
        || (sum.signum() == 0 && mode == RoundingMode.TOWARD_NEGATIVE);
    StatusFlags expectedStatus = new StatusFlags();
    Binary128 expected = sum.signum() == 0
        ? (negative ? Binary128.NEGATIVE_ZERO : Binary128.ZERO)
        : IeeeRound.binary128(
            negative,
            sum.abs(),
            BigInteger.ONE,
            commonExponent,
            mode,
            expectedStatus);
    StatusFlags actualStatus = new StatusFlags();
    Binary128 actual = subtract
        ? x.subtract(y, mode, actualStatus)
        : x.add(y, mode, actualStatus);
    String context = (subtract ? "sub" : "add") + " " + mode + " " + x + " " + y;
    assertEquals(expected, actual, context);
    assertEquals(expectedStatus.bits(), actualStatus.bits(), context);
  }

  private static void assertMultiplyMatchesExact(
      Binary128 x, Binary128 y, RoundingMode mode) {
    IeeeRound.Finite a = IeeeRound.decode(x);
    IeeeRound.Finite b = IeeeRound.decode(y);
    StatusFlags expectedStatus = new StatusFlags();
    Binary128 expected = IeeeRound.binary128(
        a.negative ^ b.negative,
        a.significand.multiply(b.significand),
        BigInteger.ONE,
        a.exponent + b.exponent,
        mode,
        expectedStatus);
    StatusFlags actualStatus = new StatusFlags();
    Binary128 actual = x.multiply(y, mode, actualStatus);
    String context = "mul " + mode + " " + x + " " + y;
    assertEquals(expected, actual, context);
    assertEquals(expectedStatus.bits(), actualStatus.bits(), context);
  }

  private static void assertDivideMatchesExact(
      Binary128 x, Binary128 y, RoundingMode mode) {
    IeeeRound.Finite a = IeeeRound.decode(x);
    IeeeRound.Finite b = IeeeRound.decode(y);
    StatusFlags expectedStatus = new StatusFlags();
    Binary128 expected = IeeeRound.binary128(
        a.negative ^ b.negative,
        a.significand,
        b.significand,
        a.exponent - b.exponent,
        mode,
        expectedStatus);
    StatusFlags actualStatus = new StatusFlags();
    Binary128 actual = x.divide(y, mode, actualStatus);
    int expectedFlags = expectedStatus.bits();
    if (x.isSubnormal() || y.isSubnormal()) {
      expectedFlags |= StatusFlags.DENORMAL;
    }
    String context = "div " + mode + " " + x + " " + y;
    assertEquals(expected, actual, context);
    assertEquals(expectedFlags, actualStatus.bits(), context);
  }

  private static void assertSqrtMatchesExact(Binary128 x, RoundingMode mode) {
    StatusFlags expectedStatus = new StatusFlags();
    Binary128 expected = IeeeRound.sqrt(x, mode, expectedStatus);
    StatusFlags actualStatus = new StatusFlags();
    Binary128 actual = x.sqrt(mode, actualStatus);
    int expectedFlags = expectedStatus.bits();
    if (x.isSubnormal()) {
      expectedFlags |= StatusFlags.DENORMAL;
    }
    String context = "sqrt " + mode + " " + x;
    assertEquals(expected, actual, context);
    assertEquals(expectedFlags, actualStatus.bits(), context);
  }

  private static void assertDoubleBits(
      long expected, Binary128 value, RoundingMode mode) {
    StatusFlags status = new StatusFlags();
    assertEquals(
        expected,
        Double.doubleToRawLongBits(value.toBinary64(mode, status)));
    assertEquals(StatusFlags.INEXACT, status.bits());
  }

  private static void assertOverflow(Binary128 expected, RoundingMode mode) {
    StatusFlags status = new StatusFlags();
    assertEquals(expected, Binary128.POSITIVE_MAX.multiply(TWO, mode, status));
    assertEquals(
        StatusFlags.OVERFLOW | StatusFlags.INEXACT,
        status.bits());
  }
}
