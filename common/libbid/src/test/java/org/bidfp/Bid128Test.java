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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;

/** Intel raw-vector tests for the BID128 representation slice. */
public final class Bid128Test {
  private static final BigInteger TEN_TO_34 = BigInteger.TEN.pow(34);

  private static final long[][] CLASS_VECTORS = {
    {0x0001ed09bead87c0L, 0x378d8e62ffffffffL, 8},
    {0x0001ed09bead87c0L, 0x378d8e64ffffffffL, 6},
    {0x0029314dc6448d93L, 0x38c15b09ffffffffL, 8},
    {0x0029314dc6448d93L, 0x38c15b0a00000000L, 8},
    {0x002a000000000000L, 0x0000000000000000L, 6},
    {0x002a000000000000L, 0x000009184e729fffL, 8},
    {0x002a000000000000L, 0x000009184e72a000L, 8},
    {0x6000000000000000L, 0x0000000000000000L, 6},
    {0x6003b75d7734cd9eL, 0x1234567890123456L, 6},
    {0x69dbb75d7734cd9eL, 0x1234567890123456L, 6},
    {0x7800000000000000L, 0x0000000000000000L, 9},
    {0x7800000000000000L, 0x0000000000000001L, 9},
    {0x7c00000000000000L, 0x0000000000000000L, 1},
    {0x7c00000000000000L, 0x0000000000000001L, 1},
    {0x7c003fffffffffffL, 0x38c15b08ffffffffL, 1},
    {0x7c003fffffffffffL, 0x38c15b0affffffffL, 1},
    {0x7e00000000000000L, 0x0000000000000000L, 0},
    {0x7e00000000000000L, 0x0000000000000001L, 0},
    {0xe000000000000000L, 0x0000000000000001L, 5},
    {0xe003b75d7734cd9eL, 0x1234567890123456L, 5},
    {0xe9dbb75d7734cd9eL, 0x1234567890123456L, 5},
    {0xf800000000000000L, 0x0000000000000000L, 2},
    {0xfc00000000000000L, 0x0000000000000000L, 1},
    {0xfc00000000000000L, 0x0000000000000001L, 1}
  };

  private Bid128Test() {
  }

  public static void main(String[] args) {
    testIntelClassVectors();
    testPacking();
    testSignOperations();
    testTextRoundTrip();
    testRandomComparisons();
    testRandomCohorts();
    testRandomSpecialComparisons();
    testTotalOrder();
    System.out.println("Bid128Test: all tests passed");
  }

  private static void testIntelClassVectors() {
    for (long[] vector : CLASS_VECTORS) {
      Bid128 value = Bid128.fromRawBits(vector[0], vector[1]);
      int actual = value.classify().ordinal();
      int expected = (int) vector[2];
      if (actual != expected) {
        throw new IllegalStateException(
            String.format(
                "class(0x%016x%016x): expected %d, actual %d",
                vector[0], vector[1], expected, actual));
      }
    }
  }

  private static void testPacking() {
    Bid128 maximum =
        Bid128.finite(false, 6176, 0x0001ed09bead87c0L, 0x378d8e63ffffffffL);
    check(maximum.isCanonical(), "maximum coefficient");
    check(maximum.isNormal(), "maximum coefficient is normal");
    check(maximum.biasedExponent() == 6176, "exponent round trip");

    Bid128 tiny = Bid128.finite(false, 0, 0, 1);
    check(tiny.isSubnormal(), "smallest coefficient");
    check(tiny.coefficient().equals(UInt128.fromLong(1)), "coefficient round trip");
  }

  private static void testSignOperations() {
    Bid128 value = Bid128.finite(false, 6176, 0, 42);
    check(value.negate().isSigned(), "negate");
    check(value.negate().abs().equals(value), "absolute value");
    check(value.copySign(Bid128.NEGATIVE_ZERO).isSigned(), "copy sign");
    check(
        Bid128.POSITIVE_INFINITY.sameQuantum(Bid128.NEGATIVE_INFINITY),
        "infinity quantum");
  }

  private static void testTextRoundTrip() {
    Bid128[] values = {
      Bid128.POSITIVE_ZERO,
      Bid128.NEGATIVE_ZERO,
      Bid128.POSITIVE_INFINITY,
      Bid128.NEGATIVE_INFINITY,
      Bid128.QUIET_NAN,
      Bid128.SIGNALING_NAN,
      Bid128.finite(false, 6176, 0, 1),
      Bid128.finite(true, 0, 0x0001ed09bead87c0L, 0x378d8e63ffffffffL),
      Bid128.finite(false, 12_287, 0, 42)
    };
    for (Bid128 value : values) {
      check(
          value.equals(Bid128.parseExact(value.toCanonicalString())),
          "text round trip " + value);
    }
    check(
        Bid128.parseExact("1.0").equals(Bid128.finite(false, 6175, 0, 10)),
        "decimal point parsing");
  }

  private static void testRandomComparisons() {
    Random random = new Random(0xdec128L);
    for (int i = 0; i < 20_000; i++) {
      Bid128 x = randomFinite(random);
      Bid128 y = randomFinite(random);
      int expected = toBigDecimal(x).compareTo(toBigDecimal(y));
      StatusFlags flags = new StatusFlags();
      check(x.quietEqual(y, flags) == (expected == 0), "random equal");
      check(x.quietLess(y, flags) == (expected < 0), "random less");
      check(x.quietGreater(y, flags) == (expected > 0), "random greater");
      check(x.quietOrdered(y, flags), "random ordered");
    }
  }

  private static Bid128 randomFinite(Random random) {
    BigInteger coefficient;
    do {
      coefficient = new BigInteger(113, random);
    } while (coefficient.compareTo(TEN_TO_34) >= 0);
    return fromCoefficient(random.nextBoolean(), random.nextInt(12_288), coefficient);
  }

  private static BigDecimal toBigDecimal(Bid128 value) {
    if (!value.isCanonical()) {
      return BigDecimal.ZERO;
    }
    BigDecimal result = new BigDecimal(value.coefficient().toDecimalString())
        .scaleByPowerOfTen(value.biasedExponent() - 6176);
    return value.isSigned() ? result.negate() : result;
  }

  private static void testRandomCohorts() {
    Random random = new Random(0xc0_128L);
    StatusFlags flags = new StatusFlags();
    for (int shift = 1; shift < 34; shift++) {
      BigInteger limit = BigInteger.TEN.pow(34 - shift);
      BigInteger scale = BigInteger.TEN.pow(shift);
      for (int i = 0; i < 200; i++) {
        BigInteger coefficient;
        do {
          coefficient = new BigInteger(limit.bitLength(), random);
        } while (coefficient.signum() == 0 || coefficient.compareTo(limit) >= 0);
        boolean negative = random.nextBoolean();
        int exponent = shift + random.nextInt(12_288 - shift);
        Bid128 x = fromCoefficient(negative, exponent, coefficient);
        Bid128 y = fromCoefficient(
            negative, exponent - shift, coefficient.multiply(scale));
        check(x.quietEqual(y, flags), "random cohort equality");
        check(!x.quietLess(y, flags), "random cohort less");
        check(!x.quietGreater(y, flags), "random cohort greater");
        check(flags.bits() == 0, "cohort comparisons do not raise flags");
      }
    }
  }

  private static void testRandomSpecialComparisons() {
    Bid128[] values = {
      Bid128.POSITIVE_ZERO,
      Bid128.NEGATIVE_ZERO,
      Bid128.finite(false, 9000, 0, 0),
      Bid128.finite(true, 3000, 0, 0),
      Bid128.fromRawBits(0x6000_0000_0000_0000L, 1),
      Bid128.fromRawBits(0x0001_ed09_bead_87c0L, 0x378d_8e64_0000_0000L),
      Bid128.parseExact("12345E+20"),
      Bid128.parseExact("-98765E-20"),
      Bid128.POSITIVE_INFINITY,
      Bid128.NEGATIVE_INFINITY,
      Bid128.QUIET_NAN,
      Bid128.SIGNALING_NAN
    };
    Random random = new Random(0x5ec1_128L);
    for (int i = 0; i < 5_000; i++) {
      Bid128 x = values[random.nextInt(values.length)];
      Bid128 y = values[random.nextInt(values.length)];
      StatusFlags flags = new StatusFlags();
      if (x.isNaN() || y.isNaN()) {
        check(x.quietUnordered(y, flags), "random special unordered");
        boolean invalid = x.isSignalingNaN() || y.isSignalingNaN();
        check(flags.contains(StatusFlags.INVALID) == invalid, "random special flags");
      } else {
        int expected = compareSpecialOracle(x, y);
        check(x.quietEqual(y, flags) == (expected == 0), "random special equal");
        check(x.quietLess(y, flags) == (expected < 0), "random special less");
        check(x.quietGreater(y, flags) == (expected > 0), "random special greater");
        check(flags.bits() == 0, "ordered special comparisons do not raise flags");
      }
    }
  }

  private static int compareSpecialOracle(Bid128 x, Bid128 y) {
    if (x.isInfinite()) {
      if (y.isInfinite()) {
        return Boolean.compare(y.isSigned(), x.isSigned());
      }
      return x.isSigned() ? -1 : 1;
    }
    if (y.isInfinite()) {
      return y.isSigned() ? 1 : -1;
    }
    return toBigDecimal(x).compareTo(toBigDecimal(y));
  }

  private static Bid128 fromCoefficient(
      boolean negative, int biasedExponent, BigInteger coefficient) {
    BigInteger mask = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    return Bid128.finite(
        negative,
        biasedExponent,
        coefficient.shiftRight(64).longValue(),
        coefficient.and(mask).longValue());
  }

  private static void testTotalOrder() {
    long[][] vectors = {
      {
        0x0000000000000000L, 0xffffffffffffffffL,
        0x0000000000000000L, 0xffffffffffffffffL, 1
      },
      {
        0x0001ed09bead87c0L, 0x378d8e62ffffffffL,
        0x0001ed09bead87c0L, 0x378d8e64ffffffffL, 0
      },
      {
        0x0001ed09bead87c0L, 0x378d8e64ffffffffL,
        0x0001ed09bead87c0L, 0x378d8e62ffffffffL, 1
      },
      {
        0x0001ed09bead87c0L, 0x378d8e62ffffffffL,
        0x7c003fffffffffffL, 0x38c15b08ffffffffL, 1
      },
      {
        0x7c003fffffffffffL, 0x38c15b08ffffffffL,
        0x0001ed09bead87c0L, 0x378d8e62ffffffffL, 0
      }
    };
    for (long[] vector : vectors) {
      Bid128 x = Bid128.fromRawBits(vector[0], vector[1]);
      Bid128 y = Bid128.fromRawBits(vector[2], vector[3]);
      check(x.totalOrder(y) == (vector[4] != 0), "Intel totalOrder vector");
    }
    Bid128 one = Bid128.finite(false, 6176, 0, 1);
    Bid128 onePointZero = Bid128.finite(false, 6175, 0, 10);
    check(onePointZero.totalOrder(one), "cohort order");
    check(!one.totalOrder(onePointZero), "reverse cohort order");
    check(Bid128.NEGATIVE_ZERO.totalOrder(Bid128.POSITIVE_ZERO), "signed zero order");
    check(Bid128.SIGNALING_NAN.totalOrderMag(Bid128.QUIET_NAN), "NaN magnitude order");
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new IllegalStateException(message);
    }
  }
}
