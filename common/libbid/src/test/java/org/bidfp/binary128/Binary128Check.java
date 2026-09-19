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

/** Command-line check used by {@code build.sh} (no JUnit on that classpath). */
public final class Binary128Check {
  private Binary128Check() {
  }

  public static void main(String[] args) {
    RoundingMode nearest = RoundingMode.TIES_TO_EVEN;
    Binary128 two = Binary128.fromRawBits(0x4000_0000_0000_0000L, 0L);
    checkEquals(two, Binary128.ONE.add(Binary128.ONE, nearest, new StatusFlags()),
        "1+1");
    checkEquals(
        Binary128.fromRawBits(
            0x3ffd_5555_5555_5555L, 0x5555_5555_5555_5555L),
        Binary128.ONE.divide(Binary128.fromBinary64(3.0), nearest, new StatusFlags()),
        "1/3");
    checkEquals(
        Binary128.fromRawBits(
            0x3fff_6a09_e667_f3bcL, 0xc908_b2fb_1366_ea95L),
        two.sqrt(nearest, new StatusFlags()),
        "sqrt(2)");

    long[] doubleSamples = {
        0x0000_0000_0000_0000L,
        0x8000_0000_0000_0000L,
        0x0000_0000_0000_0001L,
        0x000f_ffff_ffff_ffffL,
        0x0010_0000_0000_0000L,
        0x3ff8_0000_0000_0000L,
        0x7fef_ffff_ffff_ffffL
    };
    for (long bits : doubleSamples) {
      Binary128 converted = Binary128.fromBinary64(Double.longBitsToDouble(bits));
      checkDouble(bits, converted.toBinary64(nearest, new StatusFlags()),
          "binary64 round trip");
    }
    checkEquals(
        Binary128.fromRawBits(0x3bcd_0000_0000_0000L, 0L),
        Binary128.fromBinary64(Double.MIN_VALUE),
        "binary64 minimum subnormal");

    Binary128 tie = Binary128.fromRawBits(
        0x3fff_0000_0000_0000L, 1L << 59);
    checkDouble(0x3ff0_0000_0000_0000L,
        tie.toBinary64(RoundingMode.TIES_TO_EVEN, new StatusFlags()), "tie even");
    checkDouble(0x3ff0_0000_0000_0001L,
        tie.toBinary64(RoundingMode.TIES_AWAY, new StatusFlags()), "tie away");
    checkDouble(0x3ff0_0000_0000_0001L,
        tie.toBinary64(RoundingMode.TOWARD_POSITIVE, new StatusFlags()), "tie up");
    checkDouble(0x3ff0_0000_0000_0000L,
        tie.toBinary64(RoundingMode.TOWARD_NEGATIVE, new StatusFlags()), "tie down");
    checkDouble(0x3ff0_0000_0000_0000L,
        tie.toBinary64(RoundingMode.TOWARD_ZERO, new StatusFlags()), "tie zero");
    Binary128 negativeTie = tie.negate();
    checkDouble(0xbff0_0000_0000_0001L,
        negativeTie.toBinary64(RoundingMode.TIES_AWAY, new StatusFlags()),
        "negative tie away");
    checkDouble(0xbff0_0000_0000_0001L,
        negativeTie.toBinary64(RoundingMode.TOWARD_NEGATIVE, new StatusFlags()),
        "negative tie down");
    checkDouble(0xbff0_0000_0000_0000L,
        negativeTie.toBinary64(RoundingMode.TOWARD_POSITIVE, new StatusFlags()),
        "negative tie up");

    Binary128 largestSubnormal =
        Binary128.fromRawBits(0x0000_ffff_ffff_ffffL, -1L);
    Binary128 minimumNormal =
        Binary128.fromRawBits(0x0001_0000_0000_0000L, 0L);
    Binary128 normalBoundary = largestSubnormal.add(
        minimumNormal, nearest, new StatusFlags());
    checkEquals(
        minimumNormal,
        normalBoundary.divide(two, nearest, new StatusFlags()),
        "minimum normal promotion");
    checkEquals(
        largestSubnormal,
        normalBoundary.divide(two, RoundingMode.TOWARD_ZERO, new StatusFlags()),
        "largest subnormal rounding");

    checkEquals(
        Binary128.NEGATIVE_ZERO,
        Binary128.ONE.add(
            Binary128.ONE.negate(),
            RoundingMode.TOWARD_NEGATIVE,
            new StatusFlags()),
        "directed cancellation");

    Binary128 signaling = Binary128.fromRawBits(
        0xffff_1234_5678_9abcL, 0xdef0_1234_5678_9abcL);
    StatusFlags nanStatus = new StatusFlags();
    checkEquals(
        Binary128.fromRawBits(
            0xffff_9234_5678_9abcL, 0xdef0_1234_5678_9abcL),
        signaling.add(Binary128.ONE, nearest, nanStatus),
        "NaN payload");
    checkFlags(StatusFlags.INVALID, nanStatus, "signaling NaN");
    StatusFlags nanConvertStatus = new StatusFlags();
    checkDouble(
        0xfff9_2345_6789_abcdL,
        signaling.toBinary64(nearest, nanConvertStatus),
        "binary64 NaN payload");
    checkFlags(StatusFlags.INVALID, nanConvertStatus, "binary64 signaling NaN");

    StatusFlags divideStatus = new StatusFlags();
    checkEquals(
        Binary128.POSITIVE_INFINITY,
        Binary128.ONE.divide(Binary128.ZERO, nearest, divideStatus),
        "divide by zero");
    checkFlags(StatusFlags.DIVIDE_BY_ZERO, divideStatus, "divide flag");

    StatusFlags overflowStatus = new StatusFlags();
    checkEquals(
        Binary128.POSITIVE_MAX,
        Binary128.POSITIVE_MAX.multiply(
            two, RoundingMode.TOWARD_ZERO, overflowStatus),
        "directed overflow");
    checkFlags(
        StatusFlags.OVERFLOW | StatusFlags.INEXACT,
        overflowStatus,
        "overflow flags");

    Binary128 minimumSubnormal = Binary128.fromRawBits(0L, 1L);
    StatusFlags underflowStatus = new StatusFlags();
    checkEquals(
        Binary128.ZERO,
        minimumSubnormal.divide(two, nearest, underflowStatus),
        "underflow to zero");
    checkFlags(
        StatusFlags.DENORMAL | StatusFlags.UNDERFLOW | StatusFlags.INEXACT,
        underflowStatus,
        "underflow flags");

    System.out.println("Binary128Check: all tests passed");
  }

  private static void checkEquals(
      Binary128 expected, Binary128 actual, String label) {
    if (!expected.equals(actual)) {
      throw new IllegalStateException(
          label + ": expected " + expected + ", got " + actual);
    }
  }

  private static void checkDouble(long expectedBits, double actual, String label) {
    long actualBits = Double.doubleToRawLongBits(actual);
    if (expectedBits != actualBits) {
      throw new IllegalStateException(String.format(
          "%s: expected %016x, got %016x", label, expectedBits, actualBits));
    }
  }

  private static void checkFlags(
      int expected, StatusFlags actual, String label) {
    if (actual.bits() != expected) {
      throw new IllegalStateException(
          label + ": expected flags " + expected + ", got " + actual.bits());
    }
  }
}
