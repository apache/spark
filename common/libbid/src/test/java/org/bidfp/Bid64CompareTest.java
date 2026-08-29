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
import java.util.Random;

/** Intel edge vectors and randomized differential tests for BID64 comparisons. */
public final class Bid64CompareTest {
  private static final long[][] EQUAL_VECTORS = {
    {0x0000000000000000L, 0x0000000000000000L, 1},
    {0x0000000000000000L, 0x0000000000000001L, 0},
    {0x2fe38d7ea4c67fffL, 0x31a000000000000aL, 0},
    {0x2fe38d7ea4c68000L, 0x31a000000000000aL, 1},
    {0x2fe38d7ea4c68001L, 0x31a000000000000aL, 0},
    {0x31a000000000000aL, 0x2fe38d7ea4c68000L, 1},
    {0x6b961e4dbb51b27aL, 0x7396b958dc00b5bdL, 1},
    {0x7800000000000001L, 0x7800000000000002L, 1},
    {0x7800000000000001L, 0xf800000000000002L, 0},
    {0xafe38d7ea4c68000L, 0xb1a000000000000aL, 1}
  };

  private Bid64CompareTest() {
  }

  public static void main(String[] args) {
    testIntelEqualVectors();
    testNaNFlags();
    testRandomFiniteComparisons();
    System.out.println("Bid64CompareTest: all tests passed");
  }

  private static void testIntelEqualVectors() {
    for (long[] vector : EQUAL_VECTORS) {
      Bid64 x = Bid64.fromRawBits(vector[0]);
      Bid64 y = Bid64.fromRawBits(vector[1]);
      StatusFlags flags = new StatusFlags();
      boolean expected = vector[2] != 0;
      boolean actual = x.quietEqual(y, flags);
      check(expected == actual, "quiet equal vector");
      check(!flags.contains(StatusFlags.INVALID), "finite comparison flag");
    }
  }

  private static void testNaNFlags() {
    StatusFlags flags = new StatusFlags();
    check(Bid64.QUIET_NAN.quietUnordered(Bid64.POSITIVE_ZERO, flags), "qNaN unordered");
    check(!flags.contains(StatusFlags.INVALID), "quiet comparison of qNaN");

    flags.clear();
    check(Bid64.SIGNALING_NAN.quietUnordered(Bid64.POSITIVE_ZERO, flags), "sNaN unordered");
    check(flags.contains(StatusFlags.INVALID), "quiet comparison of sNaN raises invalid");

    flags.clear();
    check(!Bid64.QUIET_NAN.signalingLess(Bid64.POSITIVE_ZERO, flags), "signaling less");
    check(flags.contains(StatusFlags.INVALID), "signaling comparison of qNaN raises invalid");
  }

  private static void testRandomFiniteComparisons() {
    Random random = new Random(0xdec64L);
    for (int i = 0; i < 20_000; i++) {
      Bid64 x = randomFinite(random);
      Bid64 y = randomFinite(random);
      int expected = toBigDecimal(x).compareTo(toBigDecimal(y));
      StatusFlags flags = new StatusFlags();

      check(x.quietEqual(y, flags) == (expected == 0), "random equal");
      check(x.quietNotEqual(y, flags) == (expected != 0), "random not equal");
      check(x.quietLess(y, flags) == (expected < 0), "random less");
      check(x.quietLessEqual(y, flags) == (expected <= 0), "random less equal");
      check(x.quietGreater(y, flags) == (expected > 0), "random greater");
      check(x.quietGreaterEqual(y, flags) == (expected >= 0), "random greater equal");
      check(x.quietOrdered(y, flags), "random ordered");
      check(!flags.contains(StatusFlags.INVALID), "random comparison flags");
    }
  }

  private static Bid64 randomFinite(Random random) {
    long coefficient = Long.remainderUnsigned(random.nextLong(), 10_000_000_000_000_000L);
    return Bid64.finite(random.nextBoolean(), random.nextInt(768), coefficient);
  }

  private static BigDecimal toBigDecimal(Bid64 value) {
    BigDecimal result =
        BigDecimal.valueOf(value.significand()).scaleByPowerOfTen(value.biasedExponent() - 398);
    return value.isSigned() ? result.negate() : result;
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
