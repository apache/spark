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

/** Differential tests for the unsigned limb layer. */
public final class UInt128Test {
  private static final BigInteger MODULUS = BigInteger.ONE.shiftLeft(128);
  private static final BigInteger MASK = MODULUS.subtract(BigInteger.ONE);

  private UInt128Test() {
  }

  public static void main(String[] args) {
    testBoundaries();
    testRandomizedDifferential();
    System.out.println("UInt128Test: all tests passed");
  }

  private static void testBoundaries() {
    long[] values = {
      0L,
      1L,
      -1L,
      Long.MAX_VALUE,
      Long.MIN_VALUE,
      0x0123_4567_89ab_cdefL,
      0xfedc_ba98_7654_3210L
    };

    for (long x : values) {
      for (long y : values) {
        checkMultiply(x, y);
        UInt128 left = new UInt128(x, y);
        UInt128 right = new UInt128(y, x);
        checkBinary(left, right);
      }
    }

    UInt128 value = new UInt128(0x0123_4567_89ab_cdefL, 0xfedc_ba98_7654_3210L);
    int[] distances = {0, 1, 31, 63, 64, 65, 95, 127, 128, 129};
    for (int distance : distances) {
      checkShift(value, distance);
    }
    long[] divisors = {
      0xffff_ffffL,
      0x1_0000_0001L,
      0x4000_0000_0000_0001L,
      Long.MAX_VALUE
    };
    for (long divisor : divisors) {
      checkDivision(value, divisor);
      checkDivision(new UInt128(-1L, -1L), divisor);
    }
  }

  private static void testRandomizedDifferential() {
    Random random = new Random(0x754_2019L);
    for (int i = 0; i < 20_000; i++) {
      long x = random.nextLong();
      long y = random.nextLong();
      checkMultiply(x, y);

      UInt128 left = new UInt128(random.nextLong(), random.nextLong());
      UInt128 right = new UInt128(random.nextLong(), random.nextLong());
      checkBinary(left, right);
      checkShift(left, random.nextInt(140));
      checkDivision(left, random.nextInt(1_000_000) + 1);
    }
  }

  private static void checkMultiply(long x, long y) {
    BigInteger expected = unsigned(x).multiply(unsigned(y));
    equal(expected, UInt128.multiply(x, y), "multiply");
  }

  private static void checkBinary(UInt128 left, UInt128 right) {
    BigInteger leftBig = toBigInteger(left);
    BigInteger rightBig = toBigInteger(right);

    equal(leftBig.add(rightBig), left.add(right), "add");
    equal(leftBig.subtract(rightBig), left.subtract(right), "subtract");
    equal(leftBig.multiply(rightBig), left.multiply(right), "multiply 128");

    int expectedComparison = Integer.signum(leftBig.compareTo(rightBig));
    int actualComparison = Integer.signum(left.compareTo(right));
    if (expectedComparison != actualComparison) {
      throw new IllegalStateException(
          "compare: expected " + expectedComparison + ", actual " + actualComparison);
    }

    equal(leftBig.add(unsigned(right.low())), left.add(right.low()), "add low limb");
    equal(
        leftBig.subtract(unsigned(right.low())),
        left.subtract(right.low()),
        "subtract low limb");
  }

  private static void checkShift(UInt128 value, int distance) {
    BigInteger source = toBigInteger(value);
    equal(source.shiftLeft(distance), value.shiftLeft(distance), "shift left " + distance);
    equal(source.shiftRight(distance), value.shiftRight(distance), "shift right " + distance);
  }

  private static void checkDivision(UInt128 value, long divisor) {
    BigInteger source = toBigInteger(value);
    BigInteger[] expected = source.divideAndRemainder(BigInteger.valueOf(divisor));
    UInt128.Division actual = value.divide(divisor);
    equal(expected[0], actual.quotient(), "divide quotient");
    if (expected[1].longValueExact() != actual.remainder()) {
      throw new IllegalStateException("divide remainder");
    }
    if (!source.toString().equals(value.toDecimalString())) {
      throw new IllegalStateException("decimal string");
    }
  }

  private static BigInteger unsigned(long value) {
    return new BigInteger(Long.toUnsignedString(value));
  }

  private static BigInteger toBigInteger(UInt128 value) {
    return unsigned(value.high()).shiftLeft(64).add(unsigned(value.low()));
  }

  private static void equal(BigInteger expected, UInt128 actual, String operation) {
    BigInteger wrapped = expected.and(MASK);
    BigInteger actualBig = toBigInteger(actual);
    if (!wrapped.equals(actualBig)) {
      throw new IllegalStateException(
          operation + ": expected 0x" + wrapped.toString(16) + ", actual " + actual);
    }
  }
}
