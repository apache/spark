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

public final class Int128Math {
  private Int128Math() {}

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

  public static int compare(long aHigh, long aLow, long bHigh, long bLow) {
    int result = Long.compare(aHigh, bHigh);

    if (result == 0) {
      result = Long.compareUnsigned(aLow, bLow);
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

  public static int bitCount(long high, long low)
    {
        return Long.bitCount(high) + Long.bitCount(low);
    }

  public static long negateHigh(long high, long low) {
    return -high - (low != 0 ? 1 : 0);
  }

  public static long negateLow(long low)
    {
        return -low;
    }

  public static void divide(long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, Int128Holder quotient, Int128Holder remainder) {
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

  private static void dividePositive(long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, Int128Holder quotient, Int128Holder remainder) {
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

    if (divisorLeadingZeros - dividendLeadingZeros > 15) { // fastDivide when the values differ by this many orders of magnitude
      fastDivide(dividendHigh, dividendLow, divisorHigh, divisorLow, quotient, remainder);
    } else {
      binaryDivide(dividendHigh, dividendLow, divisorHigh, divisorLow, quotient, remainder);
    }
  }

  private static void binaryDivide(long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, Int128Holder quotient, Int128Holder remainder) {
    int shift = numberOfLeadingZeros(divisorHigh, divisorLow) - numberOfLeadingZeros(dividendHigh, dividendLow);

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

  private static void fastDivide(long dividendHigh, long dividendLow, long divisorHigh, long divisorLow, Int128Holder quotient, Int128Holder remainder) {
    if (divisorHigh == 0) {
      if (Long.compareUnsigned(dividendHigh, divisorLow) < 0) {
        quotient.high(0);
        remainder.high(0);
        divide128by64(dividendHigh, dividendLow, divisorLow, quotient, remainder);
      } else {
        quotient.high(Long.divideUnsigned(dividendHigh, divisorLow));
        remainder.high(0);
        divide128by64(Long.remainderUnsigned(dividendHigh, divisorLow), dividendLow, divisorLow, quotient, remainder);
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

  private static void divide128by64(long high, long low, long divisor, Int128Holder quotient, Int128Holder remainder) {
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
    while ((quotientHigh >>> 32) != 0 || Long.compareUnsigned(quotientHigh * divisorLow, (rhat << 32) | lowHigh) > 0) {
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

    while ((quotientLow >>> 32) != 0 || Long.compareUnsigned(quotientLow * divisorLow, ((rhat << 32) | lowLow)) > 0) {
      quotientLow -= 1;
      rhat += divisorHigh;
      if ((rhat >>> 32) != 0) {
        break;
      }
    }

    quotient.low(quotientHigh << 32 | quotientLow);
    remainder.low((uhat << 32 | lowLow) - quotientLow * divisor >>> shift);
  }
}
