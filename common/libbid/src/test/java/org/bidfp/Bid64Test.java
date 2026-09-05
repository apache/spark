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

/** Dependency-free smoke tests for the first BID64 porting slice. */
public final class Bid64Test {
  private Bid64Test() {
  }

  public static void main(String[] args) {
    testSpecialValues();
    testFiniteClassification();
    testCanonicality();
    testSignOperations();
    testPackingRoundTrip();
    testTotalOrder();
    testTextRoundTrip();
    System.out.println("Bid64Test: all tests passed");
  }

  private static void testSpecialValues() {
    check(Bid64.POSITIVE_INFINITY.isInfinite(), "positive infinity");
    check(Bid64.NEGATIVE_INFINITY.isInfinite(), "negative infinity");
    check(!Bid64.POSITIVE_INFINITY.isFinite(), "infinity is not finite");
    check(Bid64.QUIET_NAN.isNaN(), "quiet NaN");
    check(!Bid64.QUIET_NAN.isSignalingNaN(), "quiet NaN is not signaling");
    check(Bid64.SIGNALING_NAN.isSignalingNaN(), "signaling NaN");
    equal(DecimalClass.QUIET_NAN, Bid64.QUIET_NAN.classify(), "quiet NaN class");
    equal(
        DecimalClass.SIGNALING_NAN,
        Bid64.SIGNALING_NAN.classify(),
        "signaling NaN class");
  }

  private static void testFiniteClassification() {
    equal(DecimalClass.POSITIVE_ZERO, Bid64.POSITIVE_ZERO.classify(), "positive zero");
    equal(DecimalClass.NEGATIVE_ZERO, Bid64.NEGATIVE_ZERO.classify(), "negative zero");

    Bid64 smallestCoefficient = Bid64.finite(false, 0, 1);
    check(smallestCoefficient.isSubnormal(), "tiny coefficient is subnormal");
    equal(
        DecimalClass.POSITIVE_SUBNORMAL,
        smallestCoefficient.classify(),
        "positive subnormal class");

    Bid64 normalAtBoundary = Bid64.finite(false, 0, 1_000_000_000_000_000L);
    check(normalAtBoundary.isNormal(), "normal boundary");

    Bid64 exponentMakesNormal = Bid64.finite(true, 15, 1);
    equal(
        DecimalClass.NEGATIVE_NORMAL,
        exponentMakesNormal.classify(),
        "exponent-adjusted normal");
  }

  private static void testCanonicality() {
    check(Bid64.QUIET_NAN.isCanonical(), "default NaN is canonical");
    check(Bid64.POSITIVE_INFINITY.isCanonical(), "default infinity is canonical");

    Bid64 nonCanonicalInfinity =
        Bid64.fromRawBits(Bid64.MASK_INFINITY | 1L);
    check(nonCanonicalInfinity.isInfinite(), "non-canonical infinity class");
    check(!nonCanonicalInfinity.isCanonical(), "infinity trailing bits");

    Bid64 nonCanonicalFinite =
        Bid64.fromRawBits(
            Bid64.MASK_STEERING_BITS | Bid64.MASK_BINARY_SIGNIFICAND2);
    check(!nonCanonicalFinite.isCanonical(), "coefficient above 10^16 - 1");
    check(nonCanonicalFinite.isZero(), "non-canonical finite encoding is zero");
  }

  private static void testSignOperations() {
    Bid64 value = Bid64.finite(false, 398, 42);
    check(!value.isSigned(), "positive value");
    check(value.negate().isSigned(), "negation");
    equal(value, value.negate().abs(), "absolute value");
    check(value.copySign(Bid64.NEGATIVE_ZERO).isSigned(), "copy negative sign");
  }

  private static void testPackingRoundTrip() {
    long[] coefficients = {
      0,
      1,
      Bid64.MASK_BINARY_SIGNIFICAND1,
      Bid64.MASK_BINARY_SIGNIFICAND1 + 1,
      9_999_999_999_999_999L
    };
    int[] exponents = {0, 1, 398, 767};

    for (long coefficient : coefficients) {
      for (int exponent : exponents) {
        Bid64 value = Bid64.finite(false, exponent, coefficient);
        equal(coefficient, value.significand(), "significand round trip");
        equal(exponent, value.biasedExponent(), "exponent round trip");
        check(value.isCanonical(), "packed value is canonical");
      }
    }
  }

  private static void testTotalOrder() {
    check(Bid64.NEGATIVE_ZERO.totalOrder(Bid64.POSITIVE_ZERO), "-0 before +0");
    check(!Bid64.POSITIVE_ZERO.totalOrder(Bid64.NEGATIVE_ZERO), "+0 after -0");
    check(Bid64.POSITIVE_ZERO.totalOrderMag(Bid64.NEGATIVE_ZERO), "mag zeros");

    Bid64 one = Bid64.finite(false, 398, 1);
    Bid64 onePointZero = Bid64.finite(false, 397, 10);
    check(onePointZero.totalOrder(one), "same value, smaller exponent first");
    check(!one.totalOrder(onePointZero), "same value, larger exponent later");

    check(Bid64.NEGATIVE_INFINITY.totalOrder(Bid64.POSITIVE_INFINITY), "-Inf < +Inf");
    check(Bid64.POSITIVE_INFINITY.totalOrder(Bid64.SIGNALING_NAN), "+Inf before +sNaN");
    check(Bid64.SIGNALING_NAN.totalOrder(Bid64.QUIET_NAN), "+sNaN before +qNaN");
    check(!Bid64.QUIET_NAN.totalOrder(Bid64.POSITIVE_INFINITY), "+qNaN after numbers");

    Bid64 negativeQuiet = Bid64.fromRawBits(Bid64.MASK_SIGN | Bid64.MASK_NAN);
    Bid64 negativeSignaling =
        Bid64.fromRawBits(Bid64.MASK_SIGN | Bid64.MASK_SIGNALING_NAN);
    check(negativeQuiet.totalOrder(negativeSignaling), "-qNaN before -sNaN");
    check(negativeSignaling.totalOrderMag(negativeQuiet), "mag sNaN before qNaN");
  }

  private static void testTextRoundTrip() {
    Bid64[] values = {
      Bid64.POSITIVE_ZERO,
      Bid64.NEGATIVE_ZERO,
      Bid64.POSITIVE_INFINITY,
      Bid64.NEGATIVE_INFINITY,
      Bid64.QUIET_NAN,
      Bid64.SIGNALING_NAN,
      Bid64.finite(false, 398, 1),
      Bid64.finite(true, 397, 10),
      Bid64.finite(false, 0, 9_999_999_999_999_999L),
      Bid64.finite(true, 767, 42)
    };
    for (Bid64 value : values) {
      equal(value, Bid64.parseExact(value.toCanonicalString()), "text round trip");
    }
    equal(
        Bid64.finite(false, 397, 10),
        Bid64.parseExact("1.0"),
        "decimal point parsing");
    equal(
        Bid64.finite(false, 399, 1_000_000_000_000_000L),
        Bid64.parseExact("10000000000000000"),
        "exact trailing-zero reduction");
    boolean inexactRejected = false;
    try {
      Bid64.parseExact("12345678901234567");
    } catch (ArithmeticException expected) {
      inexactRejected = true;
    }
    check(inexactRejected, "inexact parse rejected");
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new IllegalStateException(message);
    }
  }

  private static void equal(Object expected, Object actual, String message) {
    if (!expected.equals(actual)) {
      throw new IllegalStateException(
          message + ": expected " + expected + ", actual " + actual);
    }
  }

  private static void equal(long expected, long actual, String message) {
    if (expected != actual) {
      throw new IllegalStateException(
          message + ": expected " + expected + ", actual " + actual);
    }
  }
}
