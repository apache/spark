package org.apache.spark.sql.util;

import org.apache.spark.sql.types.Int128;

import java.math.BigInteger;
import java.nio.ByteOrder;

import static java.lang.Integer.toUnsignedLong;
import static java.lang.Math.*;
import static java.lang.System.arraycopy;
import static java.util.Arrays.fill;

public final class Int128Math3 {
    public static final int MAX_PRECISION = 38;
    private static final int NUMBER_OF_LONGS = 2;
    private static final int NUMBER_OF_INTS = 2 * NUMBER_OF_LONGS;
    private static final int LONG_POWERS_OF_TEN_TABLE_LENGTH = 19;
    private static final long[] LONG_POWERS_OF_TEN = new long[LONG_POWERS_OF_TEN_TABLE_LENGTH];

    private static final Int128[] POWERS_OF_TEN = new Int128[39]; // 1..10^38 (largest value < Int128.MAX_VALUE)
    private static final Int128[] POWERS_OF_FIVE = new Int128[54]; // 5^54 is the largest value < Int128.MAX_VALUE

    private static final long ALL_BITS_SET_64 = 0xFFFFFFFFFFFFFFFFL;
    private static final long INT_BASE = 1L << 32;

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
     * 5^27 fits in 2^31.
     */
    private static final int MAX_POWER_OF_FIVE_LONG = 27;
    /**
     * 5^x. All unsigned values.
     */
    private static final long[] POWERS_OF_FIVE_LONG = new long[MAX_POWER_OF_FIVE_LONG + 1];
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
        for (int i = 0; i < LONG_POWERS_OF_TEN.length; ++i) {
            // Although this computes using doubles, incidentally,
            // this is exact for all powers of 10 that fit in a long.
            LONG_POWERS_OF_TEN[i] = round(pow(10, i));
        }
        for (int i = 0; i < POWERS_OF_FIVE.length; ++i) {
            POWERS_OF_FIVE[i] = Int128.apply(BigInteger.valueOf(5).pow(i));
        }
        for (int i = 0; i < POWERS_OF_TEN.length; ++i) {
            POWERS_OF_TEN[i] = Int128.apply(BigInteger.TEN.pow(i));
        }

        POWERS_OF_FIVES_INT[0] = 1;
        for (int i = 1; i < POWERS_OF_FIVES_INT.length; ++i) {
            POWERS_OF_FIVES_INT[i] = POWERS_OF_FIVES_INT[i - 1] * 5;
        }

        POWERS_OF_FIVE_LONG[0] = 1;
        for (int i = 1; i < POWERS_OF_FIVE_LONG.length; ++i) {
            POWERS_OF_FIVE_LONG[i] = POWERS_OF_FIVE_LONG[i - 1] * 5;
        }

        POWERS_OF_TEN_INT[0] = 1;
        for (int i = 1; i < POWERS_OF_TEN_INT.length; ++i) {
            POWERS_OF_TEN_INT[i] = POWERS_OF_TEN_INT[i - 1] * 10;
        }

        if (!ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN)) {
            throw new IllegalStateException("UnsignedDecimal128Arithmetic is supported on little-endian machines only");
        }
    }

    public static Int128 powerOfTen(int exponent)
    {
        return POWERS_OF_TEN[exponent];
    }

    public static void rescale(long high, long low, int factor, long[] result, int offset)
    {
        if (factor == 0) {
            result[offset] = high;
            result[offset + 1] = low;
        }
        else if (factor > 0) {
            shiftLeftBy10(high, low, factor, result, offset);
        }
        else {
            scaleDownRoundUp(high, low, -factor, result, offset);
        }
    }

    public static Int128 rescale(Int128 decimal, int rescaleFactor)
    {
        long[] result = new long[2];
        rescale(decimal.high(), decimal.low(), rescaleFactor, result, 0);
        return Int128.apply(result[0], result[1]);
    }

    public static void rescaleTruncate(long high, long low, int rescaleFactor, long[] result, int offset)
    {
        if (rescaleFactor == 0) {
            result[offset] = high;
            result[offset + 1] = low;
        }
        else if (rescaleFactor > 0) {
            shiftLeftBy10(high, low, rescaleFactor, result, offset);
        }
        else {
            scaleDownTruncate(high, low, -rescaleFactor, result, offset);
        }
    }

    public static Int128 rescaleTruncate(Int128 decimal, int rescaleFactor)
    {
        long[] result = new long[2];
        rescaleTruncate(decimal.high(), decimal.low(), rescaleFactor, result, 0);
        return Int128.apply(result[0], result[1]);
    }

    // Multiplies by 10^rescaleFactor. Only positive rescaleFactor values are allowed
    private static void shiftLeftBy10(long high, long low, int rescaleFactor, long[] result, int offset)
    {
        if (rescaleFactor >= POWERS_OF_TEN.length) {
            throw overflowException();
        }

        boolean negative = high < 0;

        if (negative) {
            long tmpHigh = negateHighExact(high, low);
            long tmpLow = negateLowExact(high, low);

            high = tmpHigh;
            low = tmpLow;
        }

        multiplyPositives(high, low, POWERS_OF_TEN[rescaleFactor].high(), POWERS_OF_TEN[rescaleFactor].low(), result, offset);

        if (negative) {
            long tmpHigh = negateHighExact(result[offset], result[offset + 1]);
            long tmpLow = negateLowExact(result[offset], result[offset + 1]);

            result[offset] = tmpHigh;
            result[offset + 1] = tmpLow;
        }
    }

    public static void add(long leftHigh, long leftLow, long rightHigh, long rightLow, long[] result, int offset)
    {
        long carry = unsignedCarry(leftLow, rightLow);

        long resultLow = leftLow + rightLow;
        long resultHigh = leftHigh + rightHigh + carry;

        result[offset] = resultHigh;
        result[offset + 1] = resultLow;

        if (((resultHigh ^ leftHigh) & (resultHigh ^ rightHigh)) < 0) {
            throw overflowException();
        }
    }

    public static long addWithOverflow(long leftHigh, long leftLow, long rightHigh, long rightLow, long[] decimal, int offset)
    {
        long low = leftLow + rightLow;
        long lowCarry = unsignedCarry(leftLow, rightLow);
        long high = leftHigh + rightHigh + lowCarry;

        long overflow = 0;
        if (leftHigh >= 0 && rightHigh >= 0 && high < 0) {
            overflow = 1;
        }
        else if (leftHigh < 0 && rightHigh < 0 && high >= 0) {
            overflow = -1;
        }

        decimal[offset] = high;
        decimal[offset + 1] = low;

        return overflow;
    }

    public static void subtract(long leftHigh, long leftLow, long rightHigh, long rightLow, long[] result, int offset)
    {
        long borrow = unsignedBorrow(leftLow, rightLow);

        long resultLow = leftLow - rightLow;
        long resultHigh = leftHigh - rightHigh - borrow;

        if (((leftHigh ^ rightHigh) & (leftHigh ^ resultHigh)) < 0) {
            throw overflowException();
        }

        result[offset] = resultHigh;
        result[offset + 1] = resultLow;
    }

    public static Int128 subtract(Int128 left, Int128 right)
    {
        long[] result = new long[2];
        subtract(left.high(), left.low(), right.high(), right.low(), result, 0);
        return Int128.apply(result[0], result[1]);
    }

    private static long unsignedCarry(long a, long b)
    {
        // HD 2-13
        return ((a >>> 1) + (b >>> 1) + ((a & b) & 1)) >>> 63;
    }

    private static long unsignedBorrow(long a, long b)
    {
        // HD 2-13
        return ((~a & b) | (~(a ^ b) & (a - b))) >>> 63;
    }

    public static Int128 multiply(Int128 left, Int128 right)
    {
        return multiply(left.high(), left.low(), right.high(), right.low());
    }

    private static void multiply(long leftHigh, long leftLow, long rightHigh, long rightLow, long[] result, int offset)
    {
        boolean leftNegative = leftHigh < 0;
        boolean rightNegative = rightHigh < 0;

        if (leftNegative) {
            long tmpLow = negateLowExact(leftHigh, leftLow);
            long tmpHigh = negateHighExact(leftHigh, leftLow);

            leftLow = tmpLow;
            leftHigh = tmpHigh;
        }

        if (rightNegative) {
            long tmpLow = negateLowExact(rightHigh, rightLow);
            long tmpHigh = negateHighExact(rightHigh, rightLow);

            rightLow = tmpLow;
            rightHigh = tmpHigh;
        }

        multiplyPositives(leftHigh, leftLow, rightHigh, rightLow, result, offset);

        if (leftNegative != rightNegative) {
            long tmpHigh = negateHighExact(result[offset], result[offset + 1]);
            long tmpLow = negateLowExact(result[offset], result[offset + 1]);

            result[offset] = tmpHigh;
            result[offset + 1] = tmpLow;
        }
    }

    private static void multiplyPositives(long leftHigh, long leftLow, long rightHigh, long rightLow, long[] result, int offset)
    {
        /*
            Given a and b, two 128 bit values composed of 64 bit values (a_H, a_L) and (b_H, b_L), respectively,
            computes the product in the following way:

                                                                                  a_H a_L
                                                                                * b_H b_L
             -----------------------------------------------------------------------------------
                  64 bits    |       64 bits       |       64 bits       |        64 bits
                             |                     |                     |
                             |                     | z1_H= (a_L * b_L)_H | z1_L = (a_L * b_L)_L
                             | z2_H= (a_L * b_H)_H | z2_L= (a_L * b_H)_L |
                             | z3_H= (a_H * b_L)_H | z3_L= (a_H * b_L)_L |
              (a_H * b_H)_H  |       (a_H * b_H)_L |                     |
             -----------------------------------------------------------------------------------
                             |                     |      result_H       |      result_L

            The product is performed on positive values. The product overflows
            * if any of the terms above 128 bits is non-zero:
               * a_H and b_H are both non-zero
               * z2_H is non-zero
               * z3_H is non-zero
            * result_H is negative (high bit of a java long is set) -- since the original numbers are positive, the result cannot be negative
            * any of z1_H, z2_L and z3_L are negative -- since the original numbers are positive, these intermediate
              results cannot be negative
         */

        long z1High = unsignedMultiplyHigh(leftLow, rightLow);
        long z1Low = leftLow * rightLow;
        long z2Low = leftLow * rightHigh;
        long z3Low = leftHigh * rightLow;

        long resultLow = z1Low;
        long resultHigh = z1High + z2Low + z3Low;

        if ((leftHigh != 0 && rightHigh != 0) ||
                resultHigh < 0 || z1High < 0 || z2Low < 0 || z3Low < 0 ||
                unsignedMultiplyHigh(leftLow, rightHigh) != 0 ||
                unsignedMultiplyHigh(leftHigh, rightLow) != 0) {
            throw overflowException();
        }

        result[offset] = resultHigh;
        result[offset + 1] = resultLow;
    }

    public static Int128 multiply(long leftHigh, long leftLow, long rightHigh, long rightLow)
    {
        long[] result = new long[2];
        multiply(leftHigh, leftLow, rightHigh, rightLow, result, 0);
        return Int128.apply(result[0], result[1]);
    }

    public static Int128 multiply(Int128 left, long right)
    {
        return multiply(left.high(), left.low(), signExtension(right), right);
    }

    public static Int128 multiply(long left, long right)
    {
        boolean rightNegative = right < 0;
        boolean leftNegative = left < 0;
        left = abs(left);
        right = abs(right);

        long resultLow = left * right;
        long resultHigh = MoreMath.multiplyHigh(left, right);

        if (leftNegative != rightNegative) {
            return Int128.apply(
                    negateHighExact(resultHigh, resultLow),
                    negateLowExact(resultHigh, resultLow));
        }

        return Int128.apply(resultHigh, resultLow);
    }

    /**
     * This an unsigned operation. Supplying negative arguments will yield wrong results.
     * Assumes left array length to be >= 8. However only first 4 int values are multiplied
     */
    static void multiply256Destructive(int[] left, Int128 right)
    {
        long l0 = toUnsignedLong(left[0]);
        long l1 = toUnsignedLong(left[1]);
        long l2 = toUnsignedLong(left[2]);
        long l3 = toUnsignedLong(left[3]);

        long r0 = low(right.low());
        long r1 = high(right.low());
        long r2 = low(right.high());
        long r3 = high(right.high());

        long z0 = 0;
        long z1 = 0;
        long z2 = 0;
        long z3 = 0;
        long z4 = 0;
        long z5 = 0;
        long z6 = 0;
        long z7 = 0;

        if (l0 != 0) {
            long accumulator = r0 * l0;
            z0 = low(accumulator);
            accumulator = high(accumulator) + r1 * l0;

            z1 = low(accumulator);
            accumulator = high(accumulator) + r2 * l0;

            z2 = low(accumulator);
            accumulator = high(accumulator) + r3 * l0;

            z3 = low(accumulator);
            z4 = high(accumulator);
        }

        if (l1 != 0) {
            long accumulator = r0 * l1 + z1;
            z1 = low(accumulator);
            accumulator = high(accumulator) + r1 * l1 + z2;

            z2 = low(accumulator);
            accumulator = high(accumulator) + r2 * l1 + z3;

            z3 = low(accumulator);
            accumulator = high(accumulator) + r3 * l1 + z4;

            z4 = low(accumulator);
            z5 = high(accumulator);
        }

        if (l2 != 0) {
            long accumulator = r0 * l2 + z2;
            z2 = low(accumulator);
            accumulator = high(accumulator) + r1 * l2 + z3;

            z3 = low(accumulator);
            accumulator = high(accumulator) + r2 * l2 + z4;

            z4 = low(accumulator);
            accumulator = high(accumulator) + r3 * l2 + z5;

            z5 = low(accumulator);
            z6 = high(accumulator);
        }

        if (l3 != 0) {
            long accumulator = r0 * l3 + z3;
            z3 = low(accumulator);
            accumulator = high(accumulator) + r1 * l3 + z4;

            z4 = low(accumulator);
            accumulator = high(accumulator) + r2 * l3 + z5;

            z5 = low(accumulator);
            accumulator = high(accumulator) + r3 * l3 + z6;

            z6 = low(accumulator);
            z7 = high(accumulator);
        }

        left[0] = (int) z0;
        left[1] = (int) z1;
        left[2] = (int) z2;
        left[3] = (int) z3;
        left[4] = (int) z4;
        left[5] = (int) z5;
        left[6] = (int) z6;
        left[7] = (int) z7;
    }

    /**
     * This an unsigned operation. Supplying negative arguments will yield wrong results.
     * Assumes left array length to be >= 6. However only first 4 int values are multiplied
     */
    static void multiply256Destructive(int[] left, long right)
    {
        long l0 = toUnsignedLong(left[0]);
        long l1 = toUnsignedLong(left[1]);
        long l2 = toUnsignedLong(left[2]);
        long l3 = toUnsignedLong(left[3]);

        long r0 = low(right);
        long r1 = high(right);

        long z0 = 0;
        long z1 = 0;
        long z2 = 0;
        long z3 = 0;
        long z4 = 0;
        long z5 = 0;

        if (l0 != 0) {
            long accumulator = r0 * l0;
            z0 = low(accumulator);
            accumulator = high(accumulator) + r1 * l0;

            z1 = low(accumulator);
            z2 = high(accumulator);
        }

        if (l1 != 0) {
            long accumulator = r0 * l1 + z1;
            z1 = low(accumulator);
            accumulator = high(accumulator) + r1 * l1 + z2;

            z2 = low(accumulator);
            z3 = high(accumulator);
        }

        if (l2 != 0) {
            long accumulator = r0 * l2 + z2;
            z2 = low(accumulator);
            accumulator = high(accumulator) + r1 * l2 + z3;

            z3 = low(accumulator);
            z4 = high(accumulator);
        }

        if (l3 != 0) {
            long accumulator = r0 * l3 + z3;
            z3 = low(accumulator);
            accumulator = high(accumulator) + r1 * l3 + z4;

            z4 = low(accumulator);
            z5 = high(accumulator);
        }

        left[0] = (int) z0;
        left[1] = (int) z1;
        left[2] = (int) z2;
        left[3] = (int) z3;
        left[4] = (int) z4;
        left[5] = (int) z5;
    }

    /**
     * This an unsigned operation. Supplying negative arguments will yield wrong results.
     * Assumes left array length to be >= 5. However only first 4 int values are multiplied
     */
    static void multiply256Destructive(int[] left, int r0)
    {
        long l0 = toUnsignedLong(left[0]);
        long l1 = toUnsignedLong(left[1]);
        long l2 = toUnsignedLong(left[2]);
        long l3 = toUnsignedLong(left[3]);

        long z0;
        long z1;
        long z2;
        long z3;
        long z4;

        long accumulator = r0 * l0;
        z0 = low(accumulator);
        z1 = high(accumulator);

        accumulator = r0 * l1 + z1;
        z1 = low(accumulator);
        z2 = high(accumulator);

        accumulator = r0 * l2 + z2;
        z2 = low(accumulator);
        z3 = high(accumulator);

        accumulator = r0 * l3 + z3;
        z3 = low(accumulator);
        z4 = high(accumulator);

        left[0] = (int) z0;
        left[1] = (int) z1;
        left[2] = (int) z2;
        left[3] = (int) z3;
        left[4] = (int) z4;
    }

    public static int compareAbsolute(Int128 left, Int128 right)
    {
        long leftHigh = left.high();
        long rightHigh = right.high();
        if (leftHigh != rightHigh) {
            return Long.compareUnsigned(leftHigh, rightHigh);
        }

        long leftLow = left.low();
        long rightLow = right.low();
        if (leftLow != rightLow) {
            return Long.compareUnsigned(leftLow, rightLow);
        }

        return 0;
    }

    public static int compareAbsolute(
            long leftLow,
            long leftHigh,
            long rightLow,
            long rightHigh)
    {
        if (leftHigh != rightHigh) {
            return Long.compareUnsigned(leftHigh, rightHigh);
        }

        if (leftLow != rightLow) {
            return Long.compareUnsigned(leftLow, rightLow);
        }

        return 0;
    }

    private static long incrementLow(long unusedHigh, long low)
    {
        return low + 1;
    }

    private static long incrementHigh(long high, long low)
    {
        return high + (low == ALL_BITS_SET_64 ? 1 : 0);
    }

    private static long decrementLow(long unusedHigh, long low)
    {
        return low - 1;
    }

    private static long decrementHigh(long high, long low)
    {
        return high - (low == 0 ? 1 : 0);
    }

    private static void incrementUnsafe(long[] value, int offset)
    {
        long high = value[offset];
        long low = value[offset + 1];

        value[offset] = incrementHigh(high, low);
        value[offset + 1] = incrementLow(high, low);
    }

    private static void decrementUnsafe(long[] value, int offset)
    {
        long high = value[offset];
        long low = value[offset + 1];

        value[offset] = decrementHigh(high, low);
        value[offset + 1] = decrementLow(high, low);
    }

    public static Int128 absExact(Int128 value)
    {
        if (value.isNegative()) {
            return negateExact(value);
        }

        return value;
    }

    public static Int128 negate(Int128 value)
    {
        return Int128.apply(
                negateHigh(value.high(), value.low()),
                negateLow(value.high(), value.low()));
    }

    public static Int128 negateExact(Int128 value)
    {
        return Int128.apply(
                negateHighExact(value.high(), value.low()),
                negateLowExact(value.high(), value.low()));
    }

    private static void negate(long[] value, int offset)
    {
        long high = value[offset];
        long low = value[offset + 1];

        value[offset] = negateHigh(high, low);
        value[offset + 1] = negateLow(high, low);
    }

    private static long negateHighExact(long high, long low)
    {
        if (high == Int128.MIN_VALUE().high() && low == Int128.MIN_VALUE().low()) {
            throw new ArithmeticException("Overflow");
        }

        return negateHigh(high, low);
    }

    private static long negateHigh(long high, long low)
    {
        return -high - (low != 0 ? 1 : 0);
    }

    private static long negateLow(long unusedHigh, long low)
    {
        return -low;
    }

    private static long negateLowExact(long high, long low)
    {
        return negateLow(high, low);
    }

    private static void scaleDownRoundUp(long high, long low, int scaleFactor, long[] result, int offset)
    {
        // optimized path for smaller values
        if (scaleFactor <= MAX_POWER_OF_TEN_LONG && high == 0 && low >= 0) {
            long divisor = longTenToNth(scaleFactor);
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

    public static long longTenToNth(int n)
    {
        return LONG_POWERS_OF_TEN[n];
    }

    private static void scaleDownTruncate(long high, long low, int scaleFactor, long[] result, int offset)
    {
        // optimized path for smaller values
        if (scaleFactor <= MAX_POWER_OF_TEN_LONG && high == 0 && low >= 0) {
            long divisor = longTenToNth(scaleFactor);
            long newLow = low / divisor;

            result[offset] = 0;
            result[offset + 1] = newLow;
            return;
        }

        scaleDown(high, low, scaleFactor, result, offset, false);
    }

    private static void scaleDown(long high, long low, int scaleFactor, long[] result, int offset, boolean roundUp)
    {
        boolean negative = high < 0;
        if (negative) {
            long tmpLow = negateLowExact(high, low);
            long tmpHigh = negateHighExact(high, low);

            low = tmpLow;
            high = tmpHigh;
        }

        // Scales down for 10**rescaleFactor.
        // Because divide by int has limited divisor, we choose code path with the least amount of divisions
        if ((scaleFactor - 1) / MAX_POWER_OF_FIVE_INT < (scaleFactor - 1) / MAX_POWER_OF_TEN_INT) {
            // scale down for 10**rescale is equivalent to scaling down with 5**rescaleFactor first, then with 2**rescaleFactor
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
    private static void scaleDownFive(long high, long low, int fiveScale, long[] result, int offset)
    {
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
    private static void scaleDownTen(long high, long low, int tenScale, long[] result, int offset, boolean roundUp)
    {
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

    static void shiftRight(long high, long low, int shift, boolean roundUp, long[] result, int offset)
    {
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

    public static Int128 floorDiv(Int128 dividend, Int128 divisor)
    {
        return floorDiv(dividend.high(), dividend.low(), divisor.high(), divisor.low());
    }

    private static Int128 floorDiv(long dividendHigh, long dividendLow, long divisorHigh, long divisorLow)
    {
        long[] quotient = new long[2];
        long[] remainder = new long[2];

        boolean dividendIsNegative = dividendHigh < 0;
        boolean divisorIsNegative = divisorHigh < 0;
        boolean quotientIsNegative = (dividendIsNegative != divisorIsNegative);

        if (dividendIsNegative) {
            long tmpLow = negateLowExact(dividendHigh, dividendLow);
            long tmpHigh = negateHighExact(dividendHigh, dividendLow);

            dividendLow = tmpLow;
            dividendHigh = tmpHigh;
        }

        if (divisorIsNegative) {
            long tmpLow = negateLowExact(divisorHigh, divisorLow);
            long tmpHigh = negateHighExact(divisorHigh, divisorLow);

            divisorLow = tmpLow;
            divisorHigh = tmpHigh;
        }

        dividePositives(dividendHigh, dividendLow, 0, divisorHigh, divisorLow, 0, quotient, remainder);

        if (quotientIsNegative) {
            // negateExact not needed since all positive values can be negated without overflow
            negate(quotient, 0);

            if ((remainder[0] != 0 || remainder[1] != 0)) {
                decrementUnsafe(quotient, 0);
            }
        }

        return Int128.apply(quotient[0], quotient[1]);
    }

    public static Int128 divideRoundUp(long dividendHigh, long dividendLow, int dividendScaleFactor, long divisorHigh, long divisorLow, int divisorScaleFactor)
    {
        if (dividendScaleFactor > MAX_PRECISION) {
            throw overflowException();
        }

        if (divisorScaleFactor >= MAX_PRECISION) {
            throw overflowException();
        }

        long[] quotient = new long[2];
        long[] remainder = new long[2];

        boolean dividendIsNegative = dividendHigh < 0;
        boolean divisorIsNegative = divisorHigh < 0;
        boolean quotientIsNegative = (dividendIsNegative != divisorIsNegative);

        if (dividendIsNegative) {
            long tmpLow = negateLowExact(dividendHigh, dividendLow);
            long tmpHigh = negateHighExact(dividendHigh, dividendLow);

            dividendLow = tmpLow;
            dividendHigh = tmpHigh;
        }

        if (divisorIsNegative) {
            long tmpLow = negateLowExact(divisorHigh, divisorLow);
            long tmpHigh = negateHighExact(divisorHigh, divisorLow);

            divisorLow = tmpLow;
            divisorHigh = tmpHigh;
        }

        dividePositives(dividendHigh, dividendLow, dividendScaleFactor, divisorHigh, divisorLow, divisorScaleFactor, quotient, remainder);

        // if (2 * remainder >= divisor) - increment quotient by one
        shiftLeft(remainder, 1);
        long remainderLow = remainder[1];
        long remainderHigh = remainder[0];

        if (compareUnsigned(remainderHigh, remainderLow, divisorHigh, divisorLow) >= 0) {
            incrementUnsafe(quotient, 0);
        }

        if (quotientIsNegative) {
            // negateExact not needed since all positive values can be negated without overflow
            negate(quotient, 0);
        }

        return Int128.apply(quotient[0], quotient[1]);
    }

    // visible for testing
    public static void shiftLeft(long[] decimal, int shift)
    {
        long high = decimal[0];
        long low = decimal[1];

        if (shift < 64) {
            high = (high << shift) | (low >>> 1 >>> (63 - shift));
            low = low << shift;
        }
        else {
            high = low << (shift - 64);
            low = 0;
        }

        decimal[0] = high;
        decimal[1] = low;
    }

    public static Int128 remainder(long dividendHigh, long dividendLow, int dividendScaleFactor, long divisorHigh, long divisorLow, int divisorScaleFactor)
    {
        long[] quotient = new long[2];
        long[] remainder = new long[2];

        boolean dividendIsNegative = dividendHigh < 0;
        boolean divisorIsNegative = divisorHigh < 0;

        if (dividendIsNegative) {
            long tmpLow = negateLowExact(dividendHigh, dividendLow);
            long tmpHigh = negateHighExact(dividendHigh, dividendLow);

            dividendLow = tmpLow;
            dividendHigh = tmpHigh;
        }

        if (divisorIsNegative) {
            long tmpLow = negateLowExact(divisorHigh, divisorLow);
            long tmpHigh = negateHighExact(divisorHigh, divisorLow);

            divisorLow = tmpLow;
            divisorHigh = tmpHigh;
        }

        dividePositives(dividendHigh, dividendLow, dividendScaleFactor, divisorHigh, divisorLow, divisorScaleFactor, quotient, remainder);

        if (dividendIsNegative) {
            // negateExact not needed since all positive values can be negated without overflow
            negate(remainder, 0);
        }

        return Int128.apply(remainder[0], remainder[1]);
    }

    private static void dividePositives(long dividendHigh, long dividendLow, int dividendScaleFactor, long divisorHigh, long divisorLow, int divisorScaleFactor, long[] quotient, long[] remainder)
    {
        if (divisorHigh == 0 && divisorLow == 0) {
            throw divisionByZeroException();
        }

        // to fit 128b * 128b * 32b unsigned multiplication
        int[] dividend = new int[NUMBER_OF_INTS * 2 + 1];
        dividend[0] = lowInt(dividendLow);
        dividend[1] = highInt(dividendLow);
        dividend[2] = lowInt(dividendHigh);
        dividend[3] = highInt(dividendHigh);

        if (dividendScaleFactor > 0) {
            shiftLeftBy5Destructive(dividend, dividendScaleFactor);
            shiftLeftMultiPrecision(dividend, NUMBER_OF_INTS * 2, dividendScaleFactor);
        }

        int[] divisor = new int[NUMBER_OF_INTS * 2];
        divisor[0] = lowInt(divisorLow);
        divisor[1] = highInt(divisorLow);
        divisor[2] = lowInt(divisorHigh);
        divisor[3] = highInt(divisorHigh);

        if (divisorScaleFactor > 0) {
            shiftLeftBy5Destructive(divisor, divisorScaleFactor);
            shiftLeftMultiPrecision(divisor, NUMBER_OF_INTS * 2, divisorScaleFactor);
        }

        int[] multiPrecisionQuotient = new int[NUMBER_OF_INTS * 2];
        divideUnsignedMultiPrecision(dividend, divisor, multiPrecisionQuotient);

        pack(multiPrecisionQuotient, quotient);
        pack(dividend, remainder);
    }

    /**
     * Value must have a length of 8
     */
    private static void shiftLeftBy5Destructive(int[] value, int shift)
    {
        if (shift <= MAX_POWER_OF_FIVE_INT) {
            multiply256Destructive(value, POWERS_OF_FIVES_INT[shift]);
        }
        else if (shift < MAX_POWER_OF_TEN_LONG) {
            multiply256Destructive(value, POWERS_OF_FIVE_LONG[shift]);
        }
        else {
            multiply256Destructive(value, POWERS_OF_FIVE[shift]);
        }
    }

    /**
     * Divides mutableDividend / mutable divisor
     * Places remainder in first argument and quotient in the last argument
     */
    private static void divideUnsignedMultiPrecision(int[] dividend, int[] divisor, int[] quotient)
    {
        checkArgument(dividend.length == NUMBER_OF_INTS * 2 + 1);
        checkArgument(divisor.length == NUMBER_OF_INTS * 2);
        checkArgument(quotient.length == NUMBER_OF_INTS * 2);

        int divisorLength = digitsInIntegerBase(divisor);
        int dividendLength = digitsInIntegerBase(dividend);

        if (dividendLength < divisorLength) {
            return;
        }

        if (divisorLength == 1) {
            int remainder = divideUnsignedMultiPrecision(dividend, dividendLength, divisor[0]);
            checkState(dividend[dividend.length - 1] == 0);
            arraycopy(dividend, 0, quotient, 0, quotient.length);
            fill(dividend, 0);
            dividend[0] = remainder;
            return;
        }

        // normalize divisor. Most significant divisor word must be > BASE/2
        // effectively it can be achieved by shifting divisor left until the leftmost bit is 1
        int nlz = Integer.numberOfLeadingZeros(divisor[divisorLength - 1]);
        shiftLeftMultiPrecision(divisor, divisorLength, nlz);
        int normalizedDividendLength = Math.min(dividend.length, dividendLength + 1);
        shiftLeftMultiPrecision(dividend, normalizedDividendLength, nlz);

        divideKnuthNormalized(dividend, normalizedDividendLength, divisor, divisorLength, quotient);

        // un-normalize remainder which is stored in dividend
        shiftRightMultiPrecision(dividend, normalizedDividendLength, nlz);
    }

    private static void divideKnuthNormalized(int[] remainder, int dividendLength, int[] divisor, int divisorLength, int[] quotient)
    {
        int v1 = divisor[divisorLength - 1];
        int v0 = divisor[divisorLength - 2];
        for (int reminderIndex = dividendLength - 1; reminderIndex >= divisorLength; reminderIndex--) {
            int qHat = estimateQuotient(remainder[reminderIndex], remainder[reminderIndex - 1], remainder[reminderIndex - 2], v1, v0);
            if (qHat != 0) {
                boolean overflow = multiplyAndSubtractUnsignedMultiPrecision(remainder, reminderIndex, divisor, divisorLength, qHat);
                // Add back - probability is 2**(-31). R += D. Q[digit] -= 1
                if (overflow) {
                    qHat--;
                    addUnsignedMultiPrecision(remainder, reminderIndex, divisor, divisorLength);
                }
            }
            quotient[reminderIndex - divisorLength] = qHat;
        }
    }

    /**
     * Use the Knuth notation
     * <p>
     * u{x} - dividend
     * v{v} - divisor
     */
    private static int estimateQuotient(int u2, int u1, int u0, int v1, int v0)
    {
        // estimate qhat based on the first 2 digits of divisor divided by the first digit of a dividend
        long u21 = combineInts(u2, u1);
        long qhat;
        if (u2 == v1) {
            qhat = INT_BASE - 1;
        }
        else if (u21 >= 0) {
            qhat = u21 / toUnsignedLong(v1);
        }
        else {
            qhat = divideUnsignedLong(u21, v1);
        }

        if (qhat == 0) {
            return 0;
        }

        // Check if qhat is greater than expected considering only first 3 digits of a dividend
        // This step help to eliminate all the cases when the estimation is greater than q by 2
        // and eliminates most of the cases when qhat is greater than q by 1
        //
        // u2 * b * b + u1 * b + u0 >= (v1 * b + v0) * qhat
        // u2 * b * b + u1 * b + u0 >= v1 * b * qhat + v0 * qhat
        // u2 * b * b + u1 * b - v1 * b * qhat >=  v0 * qhat - u0
        // (u21 - v1 * qhat) * b >=  v0 * qhat - u0
        // (u21 - v1 * qhat) * b + u0 >=  v0 * qhat
        // When ((u21 - v1 * qhat) * b + u0) is less than (v0 * qhat) decrease qhat by one

        int iterations = 0;
        long rhat = u21 - toUnsignedLong(v1) * qhat;
        while (Long.compareUnsigned(rhat, INT_BASE) < 0 && Long.compareUnsigned(toUnsignedLong(v0) * qhat, combineInts(lowInt(rhat), u0)) > 0) {
            iterations++;
            qhat--;
            rhat += toUnsignedLong(v1);
        }

        if (iterations > 2) {
            throw new IllegalStateException("qhat is greater than q by more than 2: " + iterations);
        }

        return (int) qhat;
    }

    private static long divideUnsignedLong(long dividend, int divisor)
    {
        long unsignedDivisor = toUnsignedLong(divisor);

        if (dividend > 0) {
            return dividend / unsignedDivisor;
        }

        // HD 9-3, 4) q = divideUnsigned(n, 2) / d * 2
        long quotient = ((dividend >>> 1) / unsignedDivisor) * 2;
        long remainder = dividend - quotient * unsignedDivisor;

        if (Long.compareUnsigned(remainder, unsignedDivisor) >= 0) {
            quotient++;
        }

        return quotient;
    }

    /**
     * Calculate multi-precision [left - right * multiplier] with given left offset and length.
     * Return true when overflow occurred
     */
    private static boolean multiplyAndSubtractUnsignedMultiPrecision(int[] left, int leftOffset, int[] right, int length, int multiplier)
    {
        long unsignedMultiplier = toUnsignedLong(multiplier);
        int leftIndex = leftOffset - length;
        long multiplyAccumulator = 0;
        long subtractAccumulator = INT_BASE;
        for (int rightIndex = 0; rightIndex < length; rightIndex++, leftIndex++) {
            multiplyAccumulator = toUnsignedLong(right[rightIndex]) * unsignedMultiplier + multiplyAccumulator;
            subtractAccumulator = (subtractAccumulator + toUnsignedLong(left[leftIndex])) - toUnsignedLong(lowInt(multiplyAccumulator));
            multiplyAccumulator = high(multiplyAccumulator);
            left[leftIndex] = lowInt(subtractAccumulator);
            subtractAccumulator = high(subtractAccumulator) + INT_BASE - 1;
        }
        subtractAccumulator += toUnsignedLong(left[leftIndex]) - multiplyAccumulator;
        left[leftIndex] = lowInt(subtractAccumulator);
        return highInt(subtractAccumulator) == 0;
    }

    private static void addUnsignedMultiPrecision(int[] left, int leftOffset, int[] right, int length)
    {
        int leftIndex = leftOffset - length;
        int carry = 0;
        for (int rightIndex = 0; rightIndex < length; rightIndex++, leftIndex++) {
            long accumulator = toUnsignedLong(left[leftIndex]) + toUnsignedLong(right[rightIndex]) + toUnsignedLong(carry);
            left[leftIndex] = lowInt(accumulator);
            carry = highInt(accumulator);
        }
        left[leftIndex] += carry;
    }

    // visible for testing
    static int[] shiftLeftMultiPrecision(int[] number, int length, int shifts)
    {
        if (shifts == 0) {
            return number;
        }
        // wordShifts = shifts / 32
        int wordShifts = shifts >>> 5;
        // we don't wan't to loose any leading bits
        for (int i = 0; i < wordShifts; i++) {
            checkState(number[length - i - 1] == 0);
        }
        if (wordShifts > 0) {
            arraycopy(number, 0, number, wordShifts, length - wordShifts);
            fill(number, 0, wordShifts, 0);
        }
        // bitShifts = shifts % 32
        int bitShifts = shifts & 0b11111;
        if (bitShifts > 0) {
            // we don't wan't to loose any leading bits
            checkState(number[length - 1] >>> (Integer.SIZE - bitShifts) == 0);
            for (int position = length - 1; position > 0; position--) {
                number[position] = (number[position] << bitShifts) | (number[position - 1] >>> (Integer.SIZE - bitShifts));
            }
            number[0] = number[0] << bitShifts;
        }
        return number;
    }

    // visible for testing
    static int[] shiftRightMultiPrecision(int[] number, int length, int shifts)
    {
        if (shifts == 0) {
            return number;
        }
        // wordShifts = shifts / 32
        int wordShifts = shifts >>> 5;
        // we don't wan't to loose any trailing bits
        for (int i = 0; i < wordShifts; i++) {
            checkState(number[i] == 0);
        }
        if (wordShifts > 0) {
            arraycopy(number, wordShifts, number, 0, length - wordShifts);
            fill(number, length - wordShifts, length, 0);
        }
        // bitShifts = shifts % 32
        int bitShifts = shifts & 0b11111;
        if (bitShifts > 0) {
            // we don't wan't to loose any trailing bits
            checkState(number[0] << (Integer.SIZE - bitShifts) == 0);
            for (int position = 0; position < length - 1; position++) {
                number[position] = (number[position] >>> bitShifts) | (number[position + 1] << (Integer.SIZE - bitShifts));
            }
            number[length - 1] = number[length - 1] >>> bitShifts;
        }
        return number;
    }

    private static int divideUnsignedMultiPrecision(int[] dividend, int dividendLength, int divisor)
    {
        if (divisor == 0) {
            throw divisionByZeroException();
        }

        if (dividendLength == 1) {
            long dividendUnsigned = toUnsignedLong(dividend[0]);
            long divisorUnsigned = toUnsignedLong(divisor);
            long quotient = dividendUnsigned / divisorUnsigned;
            long remainder = dividendUnsigned - (divisorUnsigned * quotient);
            dividend[0] = (int) quotient;
            return (int) remainder;
        }

        long divisorUnsigned = toUnsignedLong(divisor);
        long remainder = 0;
        for (int dividendIndex = dividendLength - 1; dividendIndex >= 0; dividendIndex--) {
            remainder = (remainder << 32) + toUnsignedLong(dividend[dividendIndex]);
            long quotient = divideUnsignedLong(remainder, divisor);
            dividend[dividendIndex] = (int) quotient;
            remainder = remainder - (quotient * divisorUnsigned);
        }
        return (int) remainder;
    }

    private static int digitsInIntegerBase(int[] digits)
    {
        int length = digits.length;
        while (length > 0 && digits[length - 1] == 0) {
            length--;
        }
        return length;
    }

    private static long combineInts(int high, int low)
    {
        return (((long) high) << 32) | toUnsignedLong(low);
    }

    private static long high(long value)
    {
        return value >>> 32;
    }

    private static long low(long value)
    {
        return value & LOW_32_BITS;
    }

    private static int highInt(long val)
    {
        return (int) (high(val));
    }

    private static int lowInt(long val)
    {
        return (int) val;
    }

    private static boolean divideCheckRound(long dividendHigh, long dividendLow, int divisor, long[] result, int offset)
    {
        int remainder = dividePositives(dividendHigh, dividendLow, divisor, result, offset);
        return (remainder >= (divisor >> 1));
    }

    private static int dividePositives(long dividendHigh, long dividendLow, int divisor, long[] result, int offset)
    {
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

    private static void pack(int[] parts, long[] result)
    {
        long high = ((long) parts[3]) << 32 | (parts[2] & 0xFFFFFFFFL);
        long low = (((long) parts[1]) << 32) | (parts[0] & 0xFFFFFFFFL);

        if (parts[4] != 0 || parts[5] != 0 || parts[6] != 0 || parts[7] != 0) {
            throw new ArithmeticException("Overflow");
        }

        result[0] = high;
        result[1] = low;
    }

    private static long signExtension(long value)
    {
        return value >> 63;
    }

    // TODO: replace with JDK 18's Math.unsignedMultiplyHigh
    private static long unsignedMultiplyHigh(long x, long y)
    {
        // From Hacker's Delight 2nd Ed. 8-3: High-Order Product Signed from/to Unsigned
        long result = MoreMath.multiplyHigh(x, y);
        result += (y & (x >> 63)); // equivalent to: if (x < 0) result += y;
        result += (x & (y >> 63)); // equivalent to: if (y < 0) result += x;
        return result;
    }

    private static int compareUnsigned(long leftHigh, long leftLow, long rightHigh, long rightLow)
    {
        int comparison = Long.compareUnsigned(leftHigh, rightHigh);
        if (comparison == 0) {
            comparison = Long.compareUnsigned(leftLow, rightLow);
        }

        return comparison;
    }

    private static ArithmeticException overflowException()
    {
        return new ArithmeticException("Overflow");
    }

    private static ArithmeticException divisionByZeroException()
    {
        return new ArithmeticException("Division by zero");
    }

    private static void checkArgument(boolean condition)
    {
        if (!condition) {
            throw new IllegalArgumentException();
        }
    }

    private static void checkState(boolean condition)
    {
        if (!condition) {
            throw new IllegalStateException();
        }
    }

    public static long multiplyHigh(long aHigh, long aLow, long bHigh, long bLow) {
        return MoreMath.unsignedMultiplyHigh(aLow, bLow) + aLow * bHigh + aHigh * bLow;
    }

}
