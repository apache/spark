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

import org.apache.spark.sql.types.Int128;

import java.math.BigInteger;

import static java.lang.Math.pow;
import static java.lang.Math.round;

public final class Int128Math {

  // 1..10^38 (largest value < Int128.MAX_VALUE)
  private static final Int128[] POWERS_OF_TEN = new Int128[39];
  private static final int LONG_POWERS_OF_TEN_TABLE_LENGTH = 19;
  private static final long[] LONG_POWERS_OF_TEN = new long[LONG_POWERS_OF_TEN_TABLE_LENGTH];

  // Lowest 32 bits of a long
  private static final long LOW_32_BITS = 0xFFFFFFFFL;

  /**
   * 5^13 fits in 2^31.
   */
  private static final int MAX_POWER_OF_FIVE_INT = 13;

  /**
   * 5^x. All unsigned values.
   */
  private static final int[] POWERS_OF_FIVES_INT = new int[MAX_POWER_OF_FIVE_INT + 1];

  /**
   * 10^9 fits in 2^31.
   */
  private static final int MAX_POWER_OF_TEN_INT = 9;

  /**
   * 10^18 fits in 2^63.
   */
  private static final int MAX_POWER_OF_TEN_LONG = 18;

  /**
   * 10^x. All unsigned values.
   */
  private static final int[] POWERS_OF_TEN_INT = new int[MAX_POWER_OF_TEN_INT + 1];

  static {
    for (int i = 0; i < POWERS_OF_TEN.length; ++i) {
      POWERS_OF_TEN[i] = Int128.apply(BigInteger.TEN.pow(i));
    }

    POWERS_OF_FIVES_INT[0] = 1;
    for (int i = 1; i < POWERS_OF_FIVES_INT.length; ++i) {
      POWERS_OF_FIVES_INT[i] = POWERS_OF_FIVES_INT[i - 1] * 5;
    }

    for (int i = 0; i < LONG_POWERS_OF_TEN.length; ++i) {
      // Although this computes using doubles, incidentally,
      // this is exact for all powers of 10 that fit in a long.
      LONG_POWERS_OF_TEN[i] = round(pow(10, i));
    }

    POWERS_OF_TEN_INT[0] = 1;
    for (int i = 1; i < POWERS_OF_TEN_INT.length; ++i) {
      POWERS_OF_TEN_INT[i] = POWERS_OF_TEN_INT[i - 1] * 10;
    }
  }

  public static boolean inLongRange(long high, long low)
  {
    return high == (low >> 63);
  }

  public static int compareUnsigned(long aHigh, long aLow, long bHigh, long bLow) {
    int result = Long.compareUnsigned(aHigh, bHigh);

    if (result == 0) {
      result = Long.compareUnsigned(aLow, bLow);
    }

    return result;
  }

  public static int compare(long leftHigh, long leftLow, long rightHigh, long rightLow) {
    int result = Long.compare(leftHigh, rightHigh);

    if (result == 0) {
      result = Long.compareUnsigned(leftLow, rightLow);
    }

    return result;
  }

  public static boolean isZero(long high, long low) {
    return (high | low) == 0;
  }

  public static long incrementHigh(long high, long low) {
    return high + ((low == -1) ? 1 : 0);
  }

  public static long incrementLow(long high, long low) {
    return low + 1;
  }

  public static long decrementHigh(long high, long low) {
    return high - ((low == 0) ? 1 : 0);
  }

  public static long decrementLow(long high, long low) {
    return low - 1;
  }

  public static void rescale(long high, long low, int factor, long[] result, int offset) {
    if (factor == 0) {
      result[offset] = high;
      result[offset + 1] = low;
    } else if (factor > 0) {
      shiftLeftBy10(high, low, factor, result, offset);
    } else {
      scaleDownRoundUp(high, low, -factor, result, offset);
    }
  }

  public static Int128 rescale(Int128 int128, int rescaleFactor) {
    long[] result = new long[2];
    rescale(int128.high(), int128.low(), rescaleFactor, result, 0);
    return Int128.apply(result[0], result[1]);
  }

  // Multiplies by 10^rescaleFactor. Only positive rescaleFactor values are allowed
  public static void shiftLeftBy10(
    long high, long low, int rescaleFactor, long[] result, int offset) {
    if (rescaleFactor >= POWERS_OF_TEN.length) {
      throw overflowException();
    }

    boolean negative = high < 0;

    if (negative) {
      long tmpHigh = negateHighExact(high, low);
      long tmpLow = negateLow(low);

      high = tmpHigh;
      low = tmpLow;
    }

    multiplyPositives(high, low, POWERS_OF_TEN[rescaleFactor].high(),
      POWERS_OF_TEN[rescaleFactor].low(), result, offset);

    if (negative) {
      long tmpHigh = negateHighExact(result[offset], result[offset + 1]);
      long tmpLow = negateLow(result[offset + 1]);

      result[offset] = tmpHigh;
      result[offset + 1] = tmpLow;
    }
  }

  private static long unsignedCarry(long a, long b) {
    // HD 2-13
    return ((a >>> 1) + (b >>> 1) + ((a & b) & 1)) >>> 63;
  }

  public static void add(
    long leftHigh, long leftLow, long rightHigh, long rightLow, long[] result) {
    long carry = unsignedCarry(leftLow, rightLow);

    long resultLow = leftLow + rightLow;
    long resultHigh = leftHigh + rightHigh + carry;

    result[0] = resultHigh;
    result[1] = resultLow;

    if (((resultHigh ^ leftHigh) & (resultHigh ^ rightHigh)) < 0) {
      throw overflowException();
    }
  }

  public static void add(
    long leftHigh, long leftLow, long rightHigh, long rightLow, long[] result, int offset) {
    long carry = unsignedCarry(leftLow, rightLow);

    long resultLow = leftLow + rightLow;
    long resultHigh = leftHigh + rightHigh + carry;

    result[offset] = resultHigh;
    result[offset + 1] = resultLow;

    if (((resultHigh ^ leftHigh) & (resultHigh ^ rightHigh)) < 0) {
      throw overflowException();
    }
  }

  public static long addHigh(long aHigh, long aLow, long bHigh, long bLow) {
    return aHigh + bHigh + MoreMath.unsignedCarry(aLow, bLow);
  }

  public static long addHighExact(long aHigh, long aLow, long bHigh, long bLow) {
    long result = addHigh(aHigh, aLow, bHigh, bLow);

    // HD 2-13 Overflow iff both arguments have the opposite sign of the result
    if (((result ^ aHigh) & (result ^ bHigh)) < 0) {
      throw new ArithmeticException("Integer overflow");
    }

    return result;
  }

  public static long addLow(long aLow, long bLow) {
    return aLow + bLow;
  }

  public static long subtractHigh(long aHigh, long aLow, long bHigh, long bLow) {
    return aHigh - bHigh - MoreMath.unsignedBorrow(aLow, bLow);
  }

  public static long subtractHighExact(long aHigh, long aLow, long bHigh, long bLow) {
    long result = subtractHigh(aHigh, aLow, bHigh, bLow);

    // HD 2-13 Overflow iff the arguments have different signs and
    // the sign of the result is different from the sign of x
    if (((aHigh ^ bHigh) & (aHigh ^ result)) < 0) {
      throw new ArithmeticException("Integer overflow");
    }

    return result;
  }

  public static long subtractLow(long aLow, long bLow) {
    return aLow - bLow;
  }

  public static long multiplyHigh(long aHigh, long aLow, long bHigh, long bLow) {
    return MoreMath.unsignedMultiplyHigh(aLow, bLow) + aLow * bHigh + aHigh * bLow;
  }

  public static long multiplyLow(long aLow, long bLow)
    {
        return aLow * bLow;
    }

  public static long shiftLeftHigh(long high, long low, int shift) {
    if (shift < 64) {
      return (high << shift) | (low >>> 1 >>> (63 - shift));
    } else {
      return low << (shift - 64);
    }
  }

  public static long shiftLeftLow(long high, long low, int shift) {
    if (shift < 64) {
      return low << shift;
    } else {
      return 0;
    }
  }

  public static long shiftRightUnsignedHigh(long high, long low, int shift) {
    if (shift < 64) {
      return high >>> shift;
    } else {
      return 0;
    }
  }

  public static long shiftRightUnsignedLow(long high, long low, int shift) {
    if (shift < 64) {
      return (high << 1 << (63 - shift)) | (low >>> shift);
    } else {
      return high >>> (shift - 64);
    }
  }

  public static long andHigh(long aHigh, long bHigh) {
    return aHigh & bHigh;
  }

  public static long andLow(long aLow, long bLow) {
    return aLow & bLow;
  }

  public static int numberOfLeadingZeros(long high, long low) {
    int count = Long.numberOfLeadingZeros(high);
    if (count == 64) {
      count += Long.numberOfLeadingZeros(low);
    }

    return count;
  }

  public static int numberOfTrailingZeros(long high, long low) {
    int count = Long.numberOfTrailingZeros(low);
    if (count == 64) {
      count += Long.numberOfTrailingZeros(high);
    }

    return count;
  }

  public static int bitCount(long high, long low) {
    return Long.bitCount(high) + Long.bitCount(low);
  }

  private static void negate(long[] value, int offset) {
    long high = value[offset];
    long low = value[offset + 1];

    value[offset] = negateHigh(high, low);
    value[offset + 1] = negateLow(low);
  }

  private static long negateHighExact(long high, long low) {
    if (high == Int128.MIN_VALUE().high() && low == Int128.MIN_VALUE().low()) {
      throw overflowException();
    }

    return negateHigh(high, low);
  }

  public static long negateHigh(long high, long low) {
    return -high - (low != 0 ? 1 : 0);
  }

  public static long negateLow(long low) {
    return -low;
  }

  private static void incrementUnsafe(long[] value, int offset) {
    long high = value[offset];
    long low = value[offset + 1];

    value[offset] = incrementHigh(high, low);
    value[offset + 1] = incrementLow(high, low);
  }

  private static void scaleDownRoundUp(
      long high, long low, int scaleFactor, long[] result, int offset) {
    // optimized path for smaller values
    if (scaleFactor <= MAX_POWER_OF_TEN_LONG && high == 0 && low >= 0) {
      long divisor = LONG_POWERS_OF_TEN[scaleFactor];
      long newLow = low / divisor;
      if (low % divisor >= (divisor >> 1)) {
        newLow++;
      }
      result[offset] = 0;
      result[offset + 1] = newLow;
      return;
    }

    scaleDown(high, low, scaleFactor, result, offset, true);
  }

  private static void scaleDown(
      long high, long low, int scaleFactor, long[] result, int offset, boolean roundUp) {
    boolean negative = high < 0;
    if (negative) {
      long tmpLow = negateLow(low);
      long tmpHigh = negateHighExact(high, low);

      low = tmpLow;
      high = tmpHigh;
    }

    // Scales down for 10**rescaleFactor.
    // Because divide by int has limited divisor,
    // we choose code path with the least amount of divisions
    if ((scaleFactor - 1) / MAX_POWER_OF_FIVE_INT < (scaleFactor - 1) / MAX_POWER_OF_TEN_INT) {
      // scale down for 10**rescale is equivalent to scaling down with 5**rescaleFactor first,
      // then with 2**rescaleFactor
      scaleDownFive(high, low, scaleFactor, result, offset);
      shiftRight(result[offset], result[offset + 1], scaleFactor, roundUp, result, offset);
    }
    else {
      scaleDownTen(high, low, scaleFactor, result, offset, roundUp);
    }

    if (negative) {
      // negateExact not needed since all positive values can be negated without overflow
      negate(result, offset);
    }
  }

  /**
   * Scale down the value for 5**fiveScale (result := decimal / 5**fiveScale).
   */
  private static void scaleDownFive(
      long high, long low, int fiveScale, long[] result, int offset) {
    if (high < 0) {
      throw new IllegalArgumentException("Value must be positive");
    }

    while (true) {
      int powerFive = Math.min(fiveScale, MAX_POWER_OF_FIVE_INT);
      fiveScale -= powerFive;

      int divisor = POWERS_OF_FIVES_INT[powerFive];
      dividePositives(high, low, divisor, result, offset);

      if (fiveScale == 0) {
        return;
      }

      high = result[offset];
      low = result[offset + 1];
    }
  }

  /**
   * Scale down the value for 10**tenScale (this := this / 5**tenScale). This
   * method rounds-up, eg 44/10=4, 44/10=5.
   */
  private static void scaleDownTen(
      long high, long low, int tenScale, long[] result, int offset, boolean roundUp) {
    if (high < 0) {
      throw new IllegalArgumentException("Value must be positive");
    }

    boolean needsRounding;
    do {
      int powerTen = Math.min(tenScale, MAX_POWER_OF_TEN_INT);
      tenScale -= powerTen;

      int divisor = POWERS_OF_TEN_INT[powerTen];
      needsRounding = divideCheckRound(high, low, divisor, result, offset);

      high = result[offset];
      low = result[offset + 1];
    }
    while (tenScale > 0);

    if (roundUp && needsRounding) {
      incrementUnsafe(result, offset);
    }
  }

  static void shiftRight(
      long high, long low, int shift, boolean roundUp, long[] result, int offset) {
    if (high < 0) {
      throw new IllegalArgumentException("Value must be positive");
    }

    if (shift == 0) {
      return;
    }

    boolean needsRounding;
    if (shift < 64) {
      needsRounding = roundUp && (low & (1L << (shift - 1))) != 0;

      low = (high << 1 << (63 - shift)) | (low >>> shift);
      high = high >> shift;
    }
    else {
      needsRounding = roundUp && (high & (1L << (shift - 64 - 1))) != 0;

      low = high >> (shift - 64);
      high = 0;
    }

    if (needsRounding) {
      long tmpHigh = incrementHigh(high, low);
      long tmpLow = incrementLow(high, low);

      high = tmpHigh;
      low = tmpLow;
    }

    result[offset] = high;
    result[offset + 1] = low;
  }

  /**
   * Given a and b, two 128 bit values composed of 64 bit values (a_H, a_L) and (b_H, b_L),
   * respectively, computes the product in the following way:
   *
   *                                                                      a_H a_L
   *                                                                    * b_H b_L
   * -----------------------------------------------------------------------------------
   *      64 bits    |       64 bits       |       64 bits       |        64 bits
   *                 |                     |                     |
   *                 |                     | z1_H= (a_L * b_L)_H | z1_L = (a_L * b_L)_L
   *                 | z2_H= (a_L * b_H)_H | z2_L= (a_L * b_H)_L |
   *                 | z3_H= (a_H * b_L)_H | z3_L= (a_H * b_L)_L |
   *  (a_H * b_H)_H  |       (a_H * b_H)_L |                     |
   * -----------------------------------------------------------------------------------
   *                 |                     |      result_H       |      result_L
   *
   * The product is performed on positive values. The product overflows
   * * if any of the terms above 128 bits is non-zero:
   *    * a_H and b_H are both non-zero
   *    * z2_H is non-zero
   *    * z3_H is non-zero
   * * result_H is negative (high bit of a java long is set) --
   * since the original numbers are positive, the result cannot be negative
   * * any of z1_H, z2_L and z3_L are negative --
   * since the original numbers are positive, these intermediate results cannot be negative
   */
  private static void multiplyPositives(
    long leftHigh, long leftLow, long rightHigh, long rightLow, long[] result, int offset) {

    long z1High = MoreMath.unsignedMultiplyHigh(leftLow, rightLow);
    long z1Low = leftLow * rightLow;
    long z2Low = leftLow * rightHigh;
    long z3Low = leftHigh * rightLow;

    long resultLow = z1Low;
    long resultHigh = z1High + z2Low + z3Low;

    if ((leftHigh != 0 && rightHigh != 0) ||
            resultHigh < 0 || z1High < 0 || z2Low < 0 || z3Low < 0 ||
            MoreMath.unsignedMultiplyHigh(leftLow, rightHigh) != 0 ||
            MoreMath.unsignedMultiplyHigh(leftHigh, rightLow) != 0) {
      throw overflowException();
    }

    result[offset] = resultHigh;
    result[offset + 1] = resultLow;
  }

  public static void divide(
    long dividendHigh, long dividendLow, long divisorHigh, long divisorLow,
    Int128Holder quotient, Int128Holder remainder) {
    boolean dividendNegative = dividendHigh < 0;
    boolean divisorNegative = divisorHigh < 0;

    // for self assignments
    long tmpHigh;
    long tmpLow;

    if (dividendNegative) {
      tmpHigh = negateHigh(dividendHigh, dividendLow);
      tmpLow = negateLow(dividendLow);
      dividendHigh = tmpHigh;
      dividendLow = tmpLow;
    }

    if (divisorNegative) {
      tmpHigh = negateHigh(divisorHigh, divisorLow);
      tmpLow = negateLow(divisorLow);
      divisorHigh = tmpHigh;
      divisorLow = tmpLow;
    }

    dividePositive(dividendHigh, dividendLow, divisorHigh, divisorLow, quotient, remainder);

    boolean resultNegative = dividendNegative ^ divisorNegative;
    if (resultNegative) {
      tmpHigh = negateHigh(quotient.high(), quotient.low());
      tmpLow = negateLow(quotient.low());
      quotient.set(tmpHigh, tmpLow);
    }

    if (dividendNegative) {
      // negate remainder
      tmpHigh = negateHigh(remainder.high(), remainder.low());
      tmpLow = negateLow(remainder.low());
      remainder.set(tmpHigh, tmpLow);
    }
  }

  private static long high(long value)
  {
    return value >>> 32;
  }

  private static long low(long value)
  {
    return value & LOW_32_BITS;
  }

  private static boolean divideCheckRound(
      long dividendHigh, long dividendLow, int divisor, long[] result, int offset) {
    int remainder = dividePositives(dividendHigh, dividendLow, divisor, result, offset);
    return (remainder >= (divisor >> 1));
  }

  private static int dividePositives(
      long dividendHigh, long dividendLow, int divisor, long[] result, int offset) {
    long remainder = dividendHigh;
    long high = remainder / divisor;
    remainder %= divisor;

    remainder = high(dividendLow) + (remainder << 32);
    int z1 = (int) (remainder / divisor);
    remainder %= divisor;

    remainder = low(dividendLow) + (remainder << 32);
    int z0 = (int) (remainder / divisor);

    long low = (((long) z1) << 32) | (((long) z0) & 0xFFFFFFFFL);

    result[offset] = high;
    result[offset + 1] = low;

    return (int) (remainder % divisor);
  }

  private static void dividePositive(
    long dividendHigh, long dividendLow, long divisorHigh, long divisorLow,
    Int128Holder quotient, Int128Holder remainder) {
    int dividendLeadingZeros = numberOfLeadingZeros(dividendHigh, dividendLow);
    int divisorLeadingZeros = numberOfLeadingZeros(divisorHigh, divisorLow);
    int divisorTrailingZeros = numberOfTrailingZeros(divisorHigh, divisorLow);

    int comparison = compareUnsigned(dividendHigh, dividendLow, divisorHigh, divisorLow);
    if (comparison < 0) {
      quotient.set(Int128.ZERO());
      remainder.set(dividendHigh, dividendLow);
      return;
    } else if (comparison == 0) {
      quotient.set(Int128.ONE());
      remainder.set(Int128.ZERO());
      return;
    }

    if (divisorLeadingZeros == 128) {
      throw new ArithmeticException("Divide by zero");
    } else if ((dividendHigh | divisorHigh) == 0) {
      // dividend and divisor fit in an unsigned
      quotient.set(0, Long.divideUnsigned(dividendLow, divisorLow));
      remainder.set(0, Long.remainderUnsigned(dividendLow, divisorLow));
      return;
    } else if (divisorLeadingZeros == 127) {
      // divisor is 1
      quotient.set(dividendHigh, dividendLow);
      remainder.set(Int128.ZERO());
      return;
    } else if ((divisorTrailingZeros + divisorLeadingZeros) == 127) {
      // only one bit set (i.e., power of 2), so just shift

      //  quotient = dividend >>> divisorTrailingZeros
      quotient.set(
        shiftRightUnsignedHigh(dividendHigh, dividendLow, divisorTrailingZeros),
        shiftRightUnsignedLow(dividendHigh, dividendLow, divisorTrailingZeros));

      //  remainder = dividend & (divisor - 1)
      long dLow = decrementLow(divisorHigh, divisorLow);
      long dHigh = decrementHigh(divisorHigh, divisorLow);

      // and
      remainder.set(
        andHigh(dividendHigh, dHigh),
        andLow(dividendLow, dLow));
      return;
    }

    // fastDivide when the values differ by this many orders of magnitude
    if (divisorLeadingZeros - dividendLeadingZeros > 15) {
      fastDivide(dividendHigh, dividendLow, divisorHigh, divisorLow, quotient, remainder);
    } else {
      binaryDivide(dividendHigh, dividendLow, divisorHigh, divisorLow, quotient, remainder);
    }
  }

  private static void binaryDivide(
    long dividendHigh, long dividendLow, long divisorHigh, long divisorLow,
    Int128Holder quotient, Int128Holder remainder) {
    int shift = numberOfLeadingZeros(divisorHigh, divisorLow) -
      numberOfLeadingZeros(dividendHigh, dividendLow);

    // for self assignments
    long tmpHigh;
    long tmpLow;

    // divisor = divisor << shift
    tmpHigh = shiftLeftHigh(divisorHigh, divisorLow, shift);
    tmpLow = shiftLeftLow(divisorHigh, divisorLow, shift);
    divisorHigh = tmpHigh;
    divisorLow = tmpLow;

    long quotientHigh = 0;
    long quotientLow = 0;

    do {
      // quotient = quotient << 1
      tmpHigh = shiftLeftHigh(quotientHigh, quotientLow, 1);
      tmpLow = shiftLeftLow(quotientHigh, quotientLow, 1);
      quotientHigh = tmpHigh;
      quotientLow = tmpLow;

      // if (dividend >= divisor)
      int comparison = compareUnsigned(dividendHigh, dividendLow, divisorHigh, divisorLow);
      if (comparison >= 0) {
        // dividend = dividend - divisor
        tmpHigh = subtractHigh(dividendHigh, dividendLow, divisorHigh, divisorLow);
        tmpLow = subtractLow(dividendLow, divisorLow);
        dividendHigh = tmpHigh;
        dividendLow = tmpLow;

        // quotient = quotient | 1
        quotientLow = quotientLow | 1;
      }

      // divisor = divisor >>> 1
      tmpHigh = shiftRightUnsignedHigh(divisorHigh, divisorLow, 1);
      tmpLow = shiftRightUnsignedLow(divisorHigh, divisorLow, 1);
      divisorHigh = tmpHigh;
      divisorLow = tmpLow;
    } while (shift-- != 0);

    quotient.set(quotientHigh, quotientLow);
    remainder.set(dividendHigh, dividendLow);
  }

  private static void fastDivide(
    long dividendHigh, long dividendLow, long divisorHigh, long divisorLow,
    Int128Holder quotient, Int128Holder remainder) {
    if (divisorHigh == 0) {
      if (Long.compareUnsigned(dividendHigh, divisorLow) < 0) {
        quotient.high(0);
        remainder.high(0);
        divide128by64(dividendHigh, dividendLow, divisorLow, quotient, remainder);
      } else {
        quotient.high(Long.divideUnsigned(dividendHigh, divisorLow));
        remainder.high(0);
        divide128by64(Long.remainderUnsigned(dividendHigh, divisorLow),
          dividendLow, divisorLow, quotient, remainder);
      }
    } else {
      // used for self assignments
      long tempHigh;
      long tempLow;

      int n = Long.numberOfLeadingZeros(divisorHigh);

      // v1 = divisor << n
      long v1High = shiftLeftHigh(divisorHigh, divisorLow, n);

      // u1 = dividend >>> 1
      long u1High = shiftRightUnsignedHigh(dividendHigh, dividendLow, 1);
      long u1Low = shiftRightUnsignedLow(dividendHigh, dividendLow, 1);

      divide128by64(u1High, u1Low, v1High, quotient, remainder);

      long q1High = 0;
      long q1Low = quotient.low();

      // q1 = q1 >>> (63 - n)
      tempLow = shiftRightUnsignedLow(q1High, q1Low, 63 - n);
      tempHigh = shiftRightUnsignedHigh(q1High, q1Low, 63 - n);
      q1Low = tempLow;
      q1High = tempHigh;

      // if (q1 != 0)
      if (!isZero(q1High, q1Low)) {
        // q1--
        tempLow = decrementLow(q1High, q1Low);
        tempHigh = decrementHigh(q1High, q1Low);
        q1Low = tempLow;
        q1High = tempHigh;
      }

      long quotientHigh = q1High;
      long quotientLow = q1Low;

      // r = dividend - q1 * divisor
      long productHigh = multiplyHigh(q1High, q1Low, divisorHigh, divisorLow);
      long productLow = multiplyLow(q1Low, divisorLow);

      long remainderHigh = subtractHigh(dividendHigh, dividendLow, productHigh, productLow);
      long remainderLow = subtractLow(dividendLow, productLow);

      if (compare(remainderHigh, remainderLow, divisorHigh, divisorLow) >= 0) {
        // quotient++
        tempLow = incrementLow(quotientHigh, quotientLow);
        tempHigh = incrementHigh(quotientHigh, quotientLow);
        quotientLow = tempLow;
        quotientHigh = tempHigh;

        tempLow = subtractLow(remainderLow, divisorLow);
        tempHigh = subtractHigh(remainderHigh, remainderLow, divisorHigh, divisorLow);
        remainderHigh = tempHigh;
        remainderLow = tempLow;
      }

      quotient.set(quotientHigh, quotientLow);
      remainder.set(remainderHigh, remainderLow);
    }
  }

  private static void divide128by64(
    long high, long low, long divisor, Int128Holder quotient, Int128Holder remainder) {
    int shift = Long.numberOfLeadingZeros(divisor);
    if (shift != 0) {
      divisor <<= shift;
      high <<= shift;
      high |= low >>> (64 - shift);
      low <<= shift;
    }

    long divisorHigh = divisor >>> 32;
    long divisorLow = divisor & 0xFFFFFFFFL;
    long lowHigh = low >>> 32;
    long lowLow = low & 0xFFFFFFFFL;

    // Compute high quotient digit.
    long quotientHigh = Long.divideUnsigned(high, divisorHigh);
    long rhat = Long.remainderUnsigned(high, divisorHigh);

    // qhat >>> 32 == qhat > base
    while ((quotientHigh >>> 32) != 0 ||
      Long.compareUnsigned(quotientHigh * divisorLow, (rhat << 32) | lowHigh) > 0) {
      quotientHigh -= 1;
      rhat += divisorHigh;
      if ((rhat >>> 32) != 0) {
        break;
      }
    }

    long uhat = ((high << 32) | lowHigh) - quotientHigh * divisor;

    // Compute low quotient digit.
    long quotientLow = Long.divideUnsigned(uhat, divisorHigh);
    rhat = Long.remainderUnsigned(uhat, divisorHigh);

    while ((quotientLow >>> 32) != 0 ||
      Long.compareUnsigned(quotientLow * divisorLow, ((rhat << 32) | lowLow)) > 0) {
      quotientLow -= 1;
      rhat += divisorHigh;
      if ((rhat >>> 32) != 0) {
        break;
      }
    }

    quotient.low(quotientHigh << 32 | quotientLow);
    remainder.low((uhat << 32 | lowLow) - quotientLow * divisor >>> shift);
  }

  private static ArithmeticException overflowException() {
    return new ArithmeticException("Overflow");
  }
}
