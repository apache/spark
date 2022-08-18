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

package org.apache.spark.sql.util;

public class MoreMath {
  private MoreMath() {

  }

  /**
   * Compute carry of addition with carry
   */
  public static long unsignedCarry(long a, long b, long c) {
    // HD 2-13
    return (a & b) | ((a | b) & ~(a + b + c)) >>> 63; // TODO: verify
  }

  public static long unsignedCarry(long a, long b) {
    // HD 2-13
    return ((a >>> 1) + (b >>> 1) + ((a & b) & 1)) >>> 63;
  }

  public static long unsignedBorrow(long a, long b) {
    // HD 2-13
    return ((~a & b) | (~(a ^ b) & (a - b))) >>> 63;
  }

  public static long ifNegative(long test, long value) {
    return value & (test >> 63);
  }

  // TODO: replace with JDK 18's Math.unsignedMultiplyHigh
  public static long unsignedMultiplyHigh(long x, long y) {
    // From Hacker's Delight 2nd Ed. 8-3: High-Order Product Signed from/to Unsigned
    long result = multiplyHigh(x, y);
    result += (y & (x >> 63)); // equivalent to: if (x < 0) result += y;
    result += (x & (y >> 63)); // equivalent to: if (y < 0) result += x;
    return result;
  }

  /**
   * Returns as a {@code long} the most significant 64 bits of the 128-bit
   * product of two 64-bit factors.
   *
   * @param x the first value
   * @param y the second value
   * @return the result
   * @since 9
   */
  public static long multiplyHigh(long x, long y) {
    if (x < 0 || y < 0) {
      // Use technique from section 8-2 of Henry S. Warren, Jr.,
      // Hacker's Delight (2nd ed.) (Addison Wesley, 2013), 173-174.
      long x1 = x >> 32;
      long x2 = x & 0xFFFFFFFFL;
      long y1 = y >> 32;
      long y2 = y & 0xFFFFFFFFL;
      long z2 = x2 * y2;
      long t = x1 * y2 + (z2 >>> 32);
      long z1 = t & 0xFFFFFFFFL;
      long z0 = t >> 32;
      z1 += x2 * y1;
      return x1 * y1 + z0 + (z1 >> 32);
    } else {
      // Use Karatsuba technique with two base 2^32 digits.
      long x1 = x >>> 32;
      long y1 = y >>> 32;
      long x2 = x & 0xFFFFFFFFL;
      long y2 = y & 0xFFFFFFFFL;
      long A = x1 * y1;
      long B = x2 * y2;
      long C = (x1 + x2) * (y1 + y2);
      long K = C - A - B;
      return (((B >>> 32) + K) >>> 32) + A;
    }
  }

  public static boolean productOverflows(
    long aHigh,
    long aLow,
    long bHigh,
    long bLow,
    long z1High,
    long z2High,
    long z2Low,
    long z3High,
    long z3Low,
    long resultHigh) {
    boolean aInLongRange = Int128Math.inLongRange(aHigh, aLow);
    boolean bInLongRange = Int128Math.inLongRange(bHigh, bLow);

    if (aInLongRange && bInLongRange) {
      return false;
    }

    if (!aInLongRange && !bInLongRange) {
      return aHigh == bHigh && resultHigh <= 0 ||
        aHigh != bHigh && resultHigh >= 0 ||
        aHigh != 0 && aHigh != -1 ||
        bHigh != 0 && bHigh != -1;
    }

    // If a fits in a long, z3High is effectively "0", so we only care about z2 for
    // checking whether z2 + z3 + z1High overflows.
    // Similarly, if b fits in a long, we only care about z3.
    long wHigh;
    long wLow;
    if (!aInLongRange) {
      // correct z3 due to effects of computing the product of values
      // in 2's complement representation
      wHigh =
        z3High - ifNegative(aHigh, bLow) - ifNegative(bLow, aHigh + unsignedBorrow(z3Low, aLow));
      wLow = z3Low - ifNegative(bLow, aLow);
    } else { // !bInLongRange
      // correct z2 due to effects of computing the product of values
      // in 2's complement representation
      wHigh =
        z2High - ifNegative(bHigh, aLow) - ifNegative(aLow, bHigh + unsignedBorrow(z2Low, bLow));
      wLow = z2Low - ifNegative(aLow, bLow);
    }

    // t = w + z1High
    long tLow = wLow + z1High;
    long tHigh = wHigh + unsignedCarry(wLow, z1High);

    return !Int128Math.inLongRange(tHigh, tLow);
  }
}
