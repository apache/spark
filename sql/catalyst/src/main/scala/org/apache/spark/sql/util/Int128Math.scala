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

package org.apache.spark.sql.util

import java.lang.{Long => JLong}
import java.lang.Math.{pow, round}
import java.math.BigInteger
import java.util.Arrays

import org.apache.spark.sql.types.Int128

object Int128Math {

  val MAX_PRECISION = 38

  // Lowest 32 bits of a long
  private val LOW_32_BITS = 0xFFFFFFFFL

  // 1..10^38 (largest value < Int128.MAX_VALUE)
  private val POWERS_OF_TEN = Array.tabulate[Int128](39)(i => Int128(BigInteger.TEN.pow(i)))

  // 5^54 is the largest value < Int128.MAX_VALUE
  private val POWERS_OF_FIVE =
    Array.tabulate[Int128](54)(i => Int128(BigInteger.valueOf(5).pow(i)))

  private val LONG_POWERS_OF_TEN_TABLE_LENGTH = 19

  // Although this computes using doubles, incidentally,
  // this is exact for all powers of 10 that fit in a long.
  private val LONG_POWERS_OF_TEN =
    Array.tabulate[Long](LONG_POWERS_OF_TEN_TABLE_LENGTH)(i => round(pow(10, i)))

  /**
   * 5^13 fits in 2^31.
   */
  private val MAX_POWER_OF_FIVE_INT = 13

  /**
   * 5^x. All unsigned values.
   */
  private val POWERS_OF_FIVES_INT = new Array[Int](MAX_POWER_OF_FIVE_INT + 1)
  POWERS_OF_FIVES_INT(0) = 1
  for (i <- 1 until POWERS_OF_FIVES_INT.length) {
    POWERS_OF_FIVES_INT(i) = POWERS_OF_FIVES_INT(i - 1) * 5
  }

  /**
   * 5^27 fits in 2^31.
   */
  private val MAX_POWER_OF_FIVE_LONG = 27

  /**
   * 5^x. All unsigned values.
   */
  private val POWERS_OF_FIVE_LONG = new Array[Long](MAX_POWER_OF_FIVE_LONG + 1)
  POWERS_OF_FIVE_LONG(0) = 1
  for (i <- 1 until POWERS_OF_FIVE_LONG.length) {
    POWERS_OF_FIVE_LONG(i) = POWERS_OF_FIVE_LONG(i - 1) * 5
  }

  /**
   * 10^9 fits in 2^31.
   */
  private val MAX_POWER_OF_TEN_INT = 9

  /**
   * 10^18 fits in 2^63.
   */
  private val MAX_POWER_OF_TEN_LONG = 18

  /**
   * 10^x. All unsigned values.
   */
  private val POWERS_OF_TEN_INT = new Array[Int](MAX_POWER_OF_TEN_INT + 1)
  POWERS_OF_TEN_INT(0) = 1
  for (i <- 1 until POWERS_OF_TEN_INT.length) {
    POWERS_OF_TEN_INT(i) = POWERS_OF_TEN_INT(i - 1) * 10
  }

  private val NUMBER_OF_LONGS = 2
  private val NUMBER_OF_INTS = 2 * NUMBER_OF_LONGS

  private val INT_BASE = 1L << 32

  def longTenToNth(n: Int): Long = LONG_POWERS_OF_TEN(n)

  def add(left: Int128, right: Int128): (Long, Long) =
    add(left.high, left.low, right.high, right.low)

  def add(leftHigh: Long, leftLow: Long, rightHigh: Long, rightLow: Long): (Long, Long) = {
    val carry = unsignedCarry(leftLow, rightLow)

    val resultHigh = leftHigh + rightHigh + carry
    val resultLow = leftLow + rightLow

    if (((resultHigh ^ leftHigh) & (resultHigh ^ rightHigh)) < 0) {
      throw int128OverflowException(
        s"Int128($leftHigh, $leftLow) add Int128($rightHigh, $rightLow).")
    }

    (resultHigh, resultLow)
  }

  def subtract(left: Int128, right: Int128): (Long, Long) =
    subtract(left.high, left.low, right.high, right.low)

  def subtract(leftHigh: Long, leftLow: Long, rightHigh: Long, rightLow: Long): (Long, Long) = {
    val borrow = unsignedBorrow(leftLow, rightLow)

    val resultHigh = leftHigh - rightHigh - borrow
    val resultLow = leftLow - rightLow

    if (((leftHigh ^ rightHigh) & (leftHigh ^ resultHigh)) < 0) {
      throw int128OverflowException(
        s"Int128($leftHigh, $leftLow) subtract Int128($rightHigh, $rightLow).")
    }

    (resultHigh, resultLow)
  }

  def multiply(left: Int128, right: Int128): (Long, Long) =
    multiply(left.high, left.low, right.high, right.low)

  def multiply(leftHigh: Long, leftLow: Long, rightHigh: Long, rightLow: Long): (Long, Long) = {
    val leftNegative = leftHigh < 0
    val rightNegative = rightHigh < 0

    var tmpLeftLow = leftLow
    var tmpLeftHigh = leftHigh
    if (leftNegative) {
      tmpLeftLow = negateLow(tmpLeftLow)
      tmpLeftHigh = negateHighExact(tmpLeftHigh, tmpLeftLow)
    }

    var tmpRightLow = rightLow
    var tmpRightHigh = rightHigh
    if (rightNegative) {
      tmpRightLow = negateLow(tmpRightLow)
      tmpRightHigh = negateHighExact(tmpRightHigh, tmpRightLow)
    }

    val result = multiplyPositives(tmpLeftHigh, tmpLeftLow, tmpRightHigh, tmpRightLow)

    if (leftNegative == rightNegative) {
      result
    } else {
      (negateHighExact(result._1, result._2), negateLow(result._2))
    }
  }

  def divideRoundUp(
      leftHigh: Long,
      leftLow: Long,
      rightHigh: Long,
      rightLow: Long,
      leftScale: Int,
      rightScale: Int): (Long, Long) = {
    if (leftScale > MAX_PRECISION) {
      throw int128OverflowException(
        s"Int128($leftHigh, $leftLow) with scale $leftScale exceeding the max precision.")
    }

    if (rightScale > MAX_PRECISION) {
      throw int128OverflowException(
        s"Int128($rightHigh, $rightLow) with scale $rightScale exceeding the max precision.")
    }

    val dividendIsNegative = leftHigh < 0
    val divisorIsNegative = rightHigh < 0
    val quotientIsNegative = dividendIsNegative != divisorIsNegative

    var tmpLeftLow = leftLow
    var tmpLeftHigh = leftHigh
    if (dividendIsNegative) {
      tmpLeftLow = negateLow(tmpLeftLow)
      tmpLeftHigh = negateHighExact(tmpLeftHigh, tmpLeftLow)
    }

    var tmpRightLow = rightLow
    var tmpRightHigh = rightHigh
    if (divisorIsNegative) {
      tmpRightLow = negateLow(tmpRightLow)
      tmpRightHigh = negateHighExact(tmpRightHigh, tmpRightLow)
    }

    val (quotient, remainder) =
      dividePositives(tmpLeftHigh, tmpLeftLow, tmpRightHigh, tmpRightLow, leftScale, rightScale)

    // if (2 * remainder >= divisor) - increment quotient by one
    val (remainderHigh, remainderLow) = shiftLeft(remainder._1, remainder._2, 1)

    val (quotientHigh, quotientLow) =
      if (compareUnsigned(remainderHigh, remainderLow, tmpRightHigh, tmpRightLow) >= 0) {
        incrementUnsafe(quotient._1, quotient._2)
      } else {
        (quotient._1, quotient._2)
      }

    if (quotientIsNegative) {
      // negateExact not needed since all positive values can be negated without overflow
      negate(quotientHigh, quotientLow)
    } else {
      (quotientHigh, quotientLow)
    }
  }

  def remainder(
      leftHigh: Long,
      leftLow: Long,
      rightHigh: Long,
      rightLow: Long,
      leftScale: Int,
      rightScale: Int,
      returnQuotient: Boolean): (Long, Long) = {
    val dividendIsNegative = leftHigh < 0
    val divisorIsNegative = rightHigh < 0

    var tmpLeftLow = leftLow
    var tmpLeftHigh = leftHigh
    if (dividendIsNegative) {
      tmpLeftLow = negateLow(tmpLeftLow)
      tmpLeftHigh = negateHighExact(tmpLeftHigh, tmpLeftLow)
    }

    var tmpRightLow = rightLow
    var tmpRightHigh = rightHigh
    if (divisorIsNegative) {
      tmpRightLow = negateLow(tmpRightLow)
      tmpRightHigh = negateHighExact(tmpRightHigh, tmpRightLow)
    }

    val (quotient, remainder) = dividePositives(tmpLeftHigh, tmpLeftLow, tmpRightHigh, tmpRightLow,
      leftScale, rightScale, false, returnQuotient)

    if (returnQuotient) {
      if (dividendIsNegative != divisorIsNegative) {
        return negate(quotient._1, quotient._2)
      }
      return quotient
    }

    if (dividendIsNegative) {
      // negateExact not needed since all positive values can be negated without overflow
      negate(remainder._1, remainder._2)
    } else {
      (remainder._1, remainder._2)
    }
  }

  def rescaleFactor(fromScale: Long, toScale: Long): Int =
    Integer.max(0, toScale.toInt - fromScale.toInt)

  def shiftLeft(high: Long, low: Long, shift: Int): (Long, Long) = {
    var tmpHigh = high
    var tmpLow = low

    if (shift < 64) {
      tmpHigh = (tmpHigh << shift) | (low >>> 1 >>> (63 - shift))
      tmpLow = tmpLow << shift
    }
    else {
      tmpHigh = tmpLow << (shift - 64)
      tmpLow = 0
    }

    (tmpHigh, tmpLow)
  }

  def rescale(high: Long, low: Long, rescale: Int): (Long, Long) = {
    if (rescale == 0) {
      (high, low)
    } else if (rescale > 0) {
      shiftLeftBy10(high, low, rescale)
    } else {
      scaleDownRoundUp(high, low, -rescale)
    }
  }

  def rescaleTruncate(high: Long, low: Long, rescale: Int): (Long, Long) = {
    if (rescale == 0) {
      (high, low)
    } else if (rescale > 0) {
      shiftLeftBy10(high, low, rescale)
    } else {
      scaleDownTruncate(high, low, -rescale)
    }
  }

  /**
   * Multiplies by 10^rescaleFactor. Only positive rescaleFactor values are allowed.
   */
  def shiftLeftBy10(high: Long, low: Long, rescale: Int): (Long, Long) = {
    if (rescale >= POWERS_OF_TEN.length) {
      throw int128OverflowException(s"Rescale Int128($high, $low) with $rescale.")
    }

    val negative = high < 0

    var tmpHigh = high
    var tmpLow = low
    if (negative) {
      tmpHigh = negateHighExact(tmpHigh, tmpLow)
      tmpLow = negateLow(tmpLow)
    }

    val result =
      multiplyPositives(tmpHigh, tmpLow, POWERS_OF_TEN(rescale).high, POWERS_OF_TEN(rescale).low)

    if (negative) {
      (negateHighExact(result._1, result._2), negateLow(result._2))
    } else {
      result
    }
  }

  private def negate(high: Long, low: Long): (Long, Long) = (negateHigh(high, low), negateLow(low))

  def negateHighExact(high: Long, low: Long): Long = {
    if (high == Int128.MIN_VALUE.high && low == Int128.MIN_VALUE.low) {
      throw int128OverflowException(s"Cannot negate high exact for Int128($high, $low).")
    }

    negateHigh(high, low)
  }

  def negateHigh(high: Long, low: Long): Long = -high - (if (low == 0) 0 else 1)

  def negateLow(low: Long): Long = -low

  private def multiplyPositives(
      leftHigh: Long, leftLow: Long, rightHigh: Long, rightLow: Long): (Long, Long) = {
    val z1High = MoreMath.unsignedMultiplyHigh(leftLow, rightLow)
    val z1Low = leftLow * rightLow
    val z2Low = leftLow * rightHigh
    val z3Low = leftHigh * rightLow

    val resultHigh = z1High + z2Low + z3Low
    val resultLow = z1Low

    if ((leftHigh != 0 && rightHigh != 0) ||
      resultHigh < 0 || z1High < 0 || z2Low < 0 || z3Low < 0 ||
      MoreMath.unsignedMultiplyHigh(leftLow, rightHigh) != 0 ||
      MoreMath.unsignedMultiplyHigh(leftHigh, rightLow) != 0) {
      throw int128OverflowException(
        s"Int128($leftHigh, $leftLow) and Int128($rightHigh, $rightLow) in multiple positives.")
    }

    (resultHigh, resultLow)
  }

  private def dividePositives(
      dividendHigh: Long, dividendLow: Long, divisor: Int): (Long, Long, Int) = {
    var remainder = dividendHigh
    val high = remainder / divisor
    remainder %= divisor

    remainder = highValue(dividendLow) + (remainder << 32)
    val z1 = remainder / divisor
    remainder %= divisor

    remainder = lowValue(dividendLow) + (remainder << 32)
    val z0 = remainder / divisor

    val low = (z1 << 32) | (z0 & 0xFFFFFFFFL)

    (high, low, (remainder % divisor).toInt)
  }

  private def dividePositives(
      leftHigh: Long,
      leftLow: Long,
      rightHigh: Long,
      rightLow: Long,
      leftScale: Int,
      rightScale: Int,
      returnBoth: Boolean = true,
      returnQuotient: Boolean = false): ((Long, Long), (Long, Long)) = {
    if (rightHigh == 0 && rightLow == 0) {
      throw divisionByZeroException()
    }

    // to fit 128b * 128b * 32b unsigned multiplication
    val dividend = new Array[Int](NUMBER_OF_INTS * 2 + 1)
    dividend(0) = lowInt(leftLow)
    dividend(1) = highInt(leftLow)
    dividend(2) = lowInt(leftHigh)
    dividend(3) = highInt(leftHigh)

    if (leftScale > 0) {
      shiftLeftBy5Destructive(dividend, leftScale)
      shiftLeftMultiPrecision(dividend, NUMBER_OF_INTS * 2, leftScale)
    }

    val divisor = new Array[Int](NUMBER_OF_INTS * 2)
    divisor(0) = lowInt(rightLow)
    divisor(1) = highInt(rightLow)
    divisor(2) = lowInt(rightHigh)
    divisor(3) = highInt(rightHigh)

    if (rightScale > 0) {
      shiftLeftBy5Destructive(divisor, rightScale)
      shiftLeftMultiPrecision(divisor, NUMBER_OF_INTS * 2, rightScale)
    }

    val multiPrecisionQuotient = new Array[Int](NUMBER_OF_INTS * 2)
    divideUnsignedMultiPrecision(dividend, divisor, multiPrecisionQuotient)

    lazy val quotient = pack(multiPrecisionQuotient)
    lazy val remainder = pack(dividend)

    if (returnBoth) {
      (quotient, remainder)
    } else if (returnQuotient) {
      (quotient, null)
    } else {
      (null, remainder)
    }
  }

  private def scaleDownRoundUp(high: Long, low: Long, rescale: Int): (Long, Long) = {
    // optimized path for smaller values
    if (rescale <= MAX_POWER_OF_TEN_LONG && high == 0 && low >= 0) {
      val divisor = LONG_POWERS_OF_TEN(rescale)
      var newLow = low / divisor
      if (low % divisor >= (divisor >> 1)) {
        newLow += 1
      }
      return (0, newLow)
    }

    scaleDown(high, low, rescale, true)
  }

  private def scaleDownTruncate(high: Long, low: Long, rescale: Int): (Long, Long) = {
    // optimized path for smaller values
    if (rescale <= MAX_POWER_OF_TEN_LONG && high == 0 && low >= 0) {
      val divisor = LONG_POWERS_OF_TEN(rescale)
      val newLow = low / divisor

      return (0, newLow)
    }

    scaleDown(high, low, rescale, false)
  }

  private def scaleDown(high: Long, low: Long, rescale: Int, roundUp: Boolean): (Long, Long) = {
    val negative = high < 0
    var tmpHigh = high
    var tmpLow = low
    if (negative) {
      tmpHigh = negateHighExact(tmpHigh, tmpLow)
      tmpLow = negateLow(tmpLow)
    }

    // Scales down for 10**rescaleFactor.
    // Because divide by int has limited divisor,
    // we choose code path with the least amount of divisions
    val result = if ((rescale - 1) / MAX_POWER_OF_FIVE_INT < (rescale - 1) / MAX_POWER_OF_TEN_INT) {
      // scale down for 10**rescale is equivalent to scaling down with 5**rescaleFactor first,
      // then with 2**rescaleFactor
      val result = scaleDownFive(high, low, rescale)
      shiftRight(result._1, result._2, rescale, roundUp)
    } else {
      scaleDownTen(tmpHigh, tmpLow, rescale, roundUp)
    }

    if (negative) {
      // negateExact not needed since all positive values can be negated without overflow
      negate(result._1, result._2)
    } else {
      result
    }
  }

  /**
   * Scale down the value for 5**fiveScale (result := decimal / 5**fiveScale).
   */
  private def scaleDownFive(high: Long, low: Long, rescale: Int): (Long, Long) = {
    if (high < 0) {
      throw negativeInt128Error()
    }

    var tmpHigh = high
    var tmpLow = low
    var fiveScale = rescale
    while (true) {
      val powerFive = Math.min(fiveScale, MAX_POWER_OF_FIVE_INT)
      fiveScale -= powerFive

      val divisor = POWERS_OF_FIVES_INT(powerFive)
      val result = dividePositives(tmpHigh, tmpLow, divisor)

      if (fiveScale == 0) {
        return (result._1, result._2)
      }

      tmpHigh = result._1
      tmpLow = result._2
    }

    (tmpHigh, tmpLow)
  }

  /**
   * Scale down the value for 10**tenScale (this := this / 5**tenScale). This
   * method rounds-up, eg 44/10=4, 44/10=5.
   */
  private def scaleDownTen(high: Long, low: Long, rescale: Int, roundUp: Boolean): (Long, Long) = {
    if (high < 0) {
      throw negativeInt128Error()
    }

    var needsRounding: Boolean = false
    var tenScale = rescale
    var tmpHigh = high
    var tmpLow = low
    do {
      val powerTen = Math.min(tenScale, MAX_POWER_OF_TEN_INT)
      tenScale -= powerTen

      val divisor = POWERS_OF_TEN_INT(powerTen)
      val result = divideCheckRound(tmpHigh, tmpLow, divisor)

      tmpHigh = result._1
      tmpLow = result._2
      needsRounding = result._3
    } while (tenScale > 0)

    if (roundUp && needsRounding) {
      incrementUnsafe(tmpHigh, tmpLow)
    } else {
      (tmpHigh, tmpLow)
    }
  }

  def shiftRight(high: Long, low: Long, shift: Int, roundUp: Boolean): (Long, Long) = {
    if (high < 0) {
      throw negativeInt128Error()
    }

    if (shift == 0) {
      return (high, low)
    }

    var needsRounding: Boolean = false
    val (newHigh, newLow) = if (shift < 64) {
      needsRounding = roundUp && (low & (1L << (shift - 1))) != 0
      (high >> shift, (high << 1 << (63 - shift)) | (low >>> shift))
    } else {
      needsRounding = roundUp && (high & (1L << (shift - 64 - 1))) != 0
      (0L, high >> (shift - 64))
    }

    if (needsRounding) {
      (incrementHigh(newHigh, newLow), incrementLow(newLow))
    } else {
      (newHigh, newLow)
    }
  }

  private def divideCheckRound(
    dividendHigh: Long, dividendLow: Long, divisor: Int): (Long, Long, Boolean) = {
    val (high, low, remainder) = dividePositives(dividendHigh, dividendLow, divisor)
    (high, low, remainder >= (divisor >> 1))
  }

  /**
   * Value must have a length of 8
   */
  private def shiftLeftBy5Destructive(value: Array[Int], shift: Int): Unit = {
    if (shift <= MAX_POWER_OF_FIVE_INT) {
      multiply256Destructive(value, POWERS_OF_FIVES_INT(shift))
    } else if (shift < MAX_POWER_OF_TEN_LONG) {
      multiply256Destructive(value, POWERS_OF_FIVE_LONG(shift))
    } else {
      multiply256Destructive(value, POWERS_OF_FIVE(shift))
    }
  }

  /**
   * This an unsigned operation. Supplying negative arguments will yield wrong results.
   * Assumes left array length to be >= 5. However only first 4 int values are multiplied
   */
  private def multiply256Destructive(left: Array[Int], r0: Int): Unit = {
    val l0 = Integer.toUnsignedLong(left(0))
    val l1 = Integer.toUnsignedLong(left(1))
    val l2 = Integer.toUnsignedLong(left(2))
    val l3 = Integer.toUnsignedLong(left(3))

    var accumulator = r0 * l0
    val z0 = lowValue(accumulator)
    var z1 = highValue(accumulator)

    accumulator = r0 * l1 + z1
    z1 = lowValue(accumulator)
    var z2 = highValue(accumulator)

    accumulator = r0 * l2 + z2
    z2 = lowValue(accumulator)
    var z3 = highValue(accumulator)

    accumulator = r0 * l3 + z3
    z3 = lowValue(accumulator)
    val z4 = highValue(accumulator)

    left(0) = z0.toInt
    left(1) = z1.toInt
    left(2) = z2.toInt
    left(3) = z3.toInt
    left(4) = z4.toInt
  }

  /**
   * This an unsigned operation. Supplying negative arguments will yield wrong results.
   * Assumes left array length to be >= 6. However only first 4 int values are multiplied
   */
  private def multiply256Destructive(left: Array[Int], right: Long): Unit = {
    val l0 = Integer.toUnsignedLong(left(0))
    val l1 = Integer.toUnsignedLong(left(1))
    val l2 = Integer.toUnsignedLong(left(2))
    val l3 = Integer.toUnsignedLong(left(3))

    val r0 = lowValue(right)
    val r1 = highValue(right)

    var z0 = 0L
    var z1 = 0L
    var z2 = 0L
    var z3 = 0L
    var z4 = 0L
    var z5 = 0L

    if (l0 != 0) {
      var accumulator = r0 * l0
      z0 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r1 * l0

      z1 = lowValue(accumulator)
      z2 = highValue(accumulator)
    }

    if (l1 != 0) {
      var accumulator = r0 * l1 + z1
      z1 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r1 * l1 + z2

      z2 = lowValue(accumulator)
      z3 = highValue(accumulator)
    }

    if (l2 != 0) {
      var accumulator = r0 * l2 + z2
      z2 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r1 * l2 + z3

      z3 = lowValue(accumulator)
      z4 = highValue(accumulator)
    }

    if (l3 != 0) {
      var accumulator = r0 * l3 + z3
      z3 = lowValue(accumulator);
      accumulator = highValue(accumulator) + r1 * l3 + z4

      z4 = lowValue(accumulator)
      z5 = highValue(accumulator)
    }

    left(0) = z0.toInt
    left(1) = z1.toInt
    left(2) = z2.toInt
    left(3) = z3.toInt
    left(4) = z4.toInt
    left(5) = z5.toInt
  }

  /**
   * This an unsigned operation. Supplying negative arguments will yield wrong results.
   * Assumes left array length to be >= 8. However only first 4 int values are multiplied
   */
  private def multiply256Destructive(left: Array[Int], right: Int128): Unit = {
    val l0 = Integer.toUnsignedLong(left(0))
    val l1 = Integer.toUnsignedLong(left(1))
    val l2 = Integer.toUnsignedLong(left(2))
    val l3 = Integer.toUnsignedLong(left(3))

    val r0 = lowValue(right.low)
    val r1 = highValue(right.low)
    val r2 = lowValue(right.high)
    val r3 = highValue(right.high)

    var z0 = 0L
    var z1 = 0L
    var z2 = 0L
    var z3 = 0L
    var z4 = 0L
    var z5 = 0L
    var z6 = 0L
    var z7 = 0L

    if (l0 != 0) {
      var accumulator = r0 * l0
      z0 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r1 * l0

      z1 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r2 * l0

      z2 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r3 * l0

      z3 = lowValue(accumulator)
      z4 = highValue(accumulator)
    }

    if (l1 != 0) {
      var accumulator = r0 * l1 + z1
      z1 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r1 * l1 + z2

      z2 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r2 * l1 + z3

      z3 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r3 * l1 + z4

      z4 = lowValue(accumulator)
      z5 = highValue(accumulator)
    }

    if (l2 != 0) {
      var accumulator = r0 * l2 + z2
      z2 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r1 * l2 + z3

      z3 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r2 * l2 + z4

      z4 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r3 * l2 + z5

      z5 = lowValue(accumulator)
      z6 = highValue(accumulator)
    }

    if (l3 != 0) {
      var accumulator = r0 * l3 + z3
      z3 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r1 * l3 + z4

      z4 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r2 * l3 + z5

      z5 = lowValue(accumulator)
      accumulator = highValue(accumulator) + r3 * l3 + z6

      z6 = lowValue(accumulator)
      z7 = highValue(accumulator)
    }

    left(0) = z0.toInt
    left(1) = z1.toInt
    left(2) = z2.toInt
    left(3) = z3.toInt
    left(4) = z4.toInt
    left(5) = z5.toInt
    left(6) = z6.toInt
    left(7) = z7.toInt
  }

  def shiftLeftMultiPrecision(number: Array[Int], length: Int, shifts: Int): Unit = {
    if (shifts == 0) {
      return
    }
    // wordShifts = shifts / 32
    val wordShifts = shifts >>> 5
    // we don't wan't to loose any leading bits
    for (i <- 0 until wordShifts) {
      checkState(number(length - i - 1) == 0)
    }

    if (wordShifts > 0) {
      System.arraycopy(number, 0, number, wordShifts, length - wordShifts)
      Arrays.fill(number, 0, wordShifts, 0)
    }
    val bitShifts = shifts & 31
    if (bitShifts > 0) {
      // we don't wan't to loose any leading bits
      checkState(number(length - 1) >>> (Integer.SIZE - bitShifts) == 0)
      for (position <- length - 1 until 0 by -1) {
        number(position) =
          (number(position) << bitShifts) | (number(position - 1) >>> (Integer.SIZE - bitShifts))
      }
      number(0) = number(0) << bitShifts
    }
  }

  /**
   * Divides mutableDividend / mutable divisor
   * Places remainder in first argument and quotient in the last argument
   */
  private def divideUnsignedMultiPrecision(
      dividend: Array[Int], divisor: Array[Int], quotient: Array[Int]): Unit = {
    checkArgument(dividend.length == NUMBER_OF_INTS * 2 + 1)
    checkArgument(divisor.length == NUMBER_OF_INTS * 2)
    checkArgument(quotient.length == NUMBER_OF_INTS * 2)

    val divisorLength = digitsInIntegerBase(divisor)
    val dividendLength = digitsInIntegerBase(dividend)

    if (dividendLength < divisorLength) {
      return
    }

    if (divisorLength == 1) {
      val remainder = divideUnsignedMultiPrecision(dividend, dividendLength, divisor(0))
      checkState(dividend(dividend.length - 1) == 0)
      System.arraycopy(dividend, 0, quotient, 0, quotient.length)
      Arrays.fill(dividend, 0)
      dividend(0) = remainder
      return
    }

    // normalize divisor. Most significant divisor word must be > BASE/2
    // effectively it can be achieved by shifting divisor left until the leftmost bit is 1
    val nlz = Integer.numberOfLeadingZeros(divisor(divisorLength - 1))
    shiftLeftMultiPrecision(divisor, divisorLength, nlz)
    val normalizedDividendLength = Math.min(dividend.length, dividendLength + 1)
    shiftLeftMultiPrecision(dividend, normalizedDividendLength, nlz)

    divideKnuthNormalized(dividend, normalizedDividendLength, divisor, divisorLength, quotient)

    // un-normalize remainder which is stored in dividend
    shiftRightMultiPrecision(dividend, normalizedDividendLength, nlz)
  }

  private def digitsInIntegerBase(digits: Array[Int]): Int = {
    var length = digits.length
    while (length > 0 && digits(length - 1) == 0) {
      length -= 1
    }
    length
  }

  private def divideUnsignedMultiPrecision(
      dividend: Array[Int], dividendLength: Int, divisor: Int): Int = {
    if (divisor == 0) {
      throw divisionByZeroException()
    }

    if (dividendLength == 1) {
      val dividendUnsigned = Integer.toUnsignedLong(dividend(0))
      val divisorUnsigned = Integer.toUnsignedLong(divisor)
      val quotient = dividendUnsigned / divisorUnsigned
      val remainder = dividendUnsigned - (divisorUnsigned * quotient)
      dividend(0) = quotient.toInt
      return remainder.toInt
    }

    val divisorUnsigned = Integer.toUnsignedLong(divisor)
    var remainder = 0L
    for (dividendIndex <- dividendLength - 1 to 0 by -1) {
      remainder = (remainder << 32) + Integer.toUnsignedLong(dividend(dividendIndex))
      val quotient = divideUnsignedLong(remainder, divisor)
      dividend(dividendIndex) = quotient.toInt
      remainder = remainder - (quotient * divisorUnsigned)
    }
    remainder.toInt
  }

  private def divideUnsignedLong(dividend: Long, divisor: Int): Long = {
    val unsignedDivisor = Integer.toUnsignedLong(divisor)

    if (dividend > 0) {
      return dividend / unsignedDivisor
    }

    // HD 9-3, 4) q = divideUnsigned(n, 2) / d * 2
    var quotient = ((dividend >>> 1) / unsignedDivisor) * 2
    val remainder = dividend - quotient * unsignedDivisor

    if (JLong.compareUnsigned(remainder, unsignedDivisor) >= 0) {
      quotient += 1
    }

    quotient
  }

  private def divideKnuthNormalized(
      remainder: Array[Int],
      dividendLength: Int,
      divisor: Array[Int],
      divisorLength: Int,
      quotient: Array[Int]): Unit = {
    val v1 = divisor(divisorLength - 1)
    val v0 = divisor(divisorLength - 2)
    for (reminderIndex <- dividendLength - 1 to divisorLength by -1) {
      var qHat = estimateQuotient(remainder(reminderIndex), remainder(reminderIndex - 1),
        remainder(reminderIndex - 2), v1, v0)
      if (qHat != 0) {
        val overflow = multiplyAndSubtractUnsignedMultiPrecision(
          remainder, reminderIndex, divisor, divisorLength, qHat)
        // Add back - probability is 2**(-31). R += D. Q[digit] -= 1
        if (overflow) {
          qHat -= 1
          addUnsignedMultiPrecision(remainder, reminderIndex, divisor, divisorLength)
        }
      }
      quotient(reminderIndex - divisorLength) = qHat
    }
  }

  /**
   * Use the Knuth notation
   * <p>
   * u{x} - dividend
   * v{v} - divisor
   */
  private def estimateQuotient(u2: Int, u1: Int, u0: Int, v1: Int, v0: Int): Int = {
    // estimate qhat based on the first 2 digits of divisor divided by the first digit of a dividend
    val u21 = combineInts(u2, u1)
    var qhat = 0L
    if (u2 == v1) {
      qhat = INT_BASE - 1
    } else if (u21 >= 0) {
      qhat = u21 / Integer.toUnsignedLong(v1)
    } else {
      qhat = divideUnsignedLong(u21, v1)
    }

    if (qhat == 0) {
      return 0
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

    var iterations = 0
    var rhat = u21 - Integer.toUnsignedLong(v1) * qhat
    while (JLong.compareUnsigned(rhat, INT_BASE) < 0 &&
      JLong.compareUnsigned(Integer.toUnsignedLong(v0) * qhat, combineInts(lowInt(rhat), u0)) > 0) {
      iterations += 1
      qhat -= 1
      rhat += Integer.toUnsignedLong(v1)
    }

    if (iterations > 2) {
      throw new IllegalStateException("qhat is greater than q by more than 2: " + iterations)
    }

    qhat.toInt
  }

  /**
   * Calculate multi-precision [left - right * multiplier] with given left offset and length.
   * Return true when overflow occurred
   */
  private def multiplyAndSubtractUnsignedMultiPrecision(
      left: Array[Int],
      leftOffset: Int,
      right: Array[Int],
      length: Int,
      multiplier: Int): Boolean = {
    val unsignedMultiplier = Integer.toUnsignedLong(multiplier)
    var leftIndex = leftOffset - length
    var multiplyAccumulator = 0L
    var subtractAccumulator = INT_BASE

    for (rightIndex <- 0 until length) {
      multiplyAccumulator =
        Integer.toUnsignedLong(right(rightIndex)) * unsignedMultiplier + multiplyAccumulator
      subtractAccumulator = (subtractAccumulator + Integer.toUnsignedLong(left(leftIndex))) -
        Integer.toUnsignedLong(lowInt(multiplyAccumulator))
      multiplyAccumulator = highValue(multiplyAccumulator)
      left(leftIndex) = lowInt(subtractAccumulator)
      subtractAccumulator = highValue(subtractAccumulator) + INT_BASE - 1
      leftIndex += 1
    }
    subtractAccumulator += Integer.toUnsignedLong(left(leftIndex)) - multiplyAccumulator
    left(leftIndex) = lowInt(subtractAccumulator)
    highInt(subtractAccumulator) == 0
  }

  private def addUnsignedMultiPrecision(
      left: Array[Int], leftOffset: Int, right: Array[Int], length: Int): Unit = {
    var leftIndex = leftOffset - length
    var carry = 0
    for (rightIndex <- 0 until length) {
    val accumulator = Integer.toUnsignedLong(left(leftIndex)) +
      Integer.toUnsignedLong(right(rightIndex)) + Integer.toUnsignedLong(carry)
    left(leftIndex) = lowInt(accumulator)
    carry = highInt(accumulator)
    leftIndex += 1
  }
    left(leftIndex) += carry
  }

  private def shiftRightMultiPrecision(
      number: Array[Int], length: Int, shifts: Int): Array[Int] = {
    if (shifts == 0) {
      return number
    }
    // wordShifts = shifts / 32
    val wordShifts = shifts >>> 5
    // we don't wan't to loose any trailing bits
    for (i <- 0 until wordShifts) {
      checkState(number(i) == 0)
    }
    if (wordShifts > 0) {
      System.arraycopy(number, wordShifts, number, 0, length - wordShifts)
      Arrays.fill(number, length - wordShifts, length, 0)
    }
    val bitShifts = shifts & 31
    if (bitShifts > 0) {
      // we don't wan't to loose any trailing bits
      checkState(number(0) << (Integer.SIZE - bitShifts) == 0)
      for (position <- 0 until length - 1) {
        number(position) =
          (number(position) >>> bitShifts) | (number(position + 1) << (Integer.SIZE - bitShifts))
      }
      number(length - 1) = number(length - 1) >>> bitShifts
    }
    number;
  }

  private def pack(parts: Array[Int]): (Long, Long) = {
    val high = parts(3).toLong << 32 | (parts(2) & 0xFFFFFFFFL)
    val low = (parts(1).toLong << 32) | (parts(0) & 0xFFFFFFFFL)

    if (parts(4) != 0 || parts(5) != 0 || parts(6) != 0 || parts(7) != 0) {
      throw new ArithmeticException("Overflow");
    }

    (high, low)
  }

  private def compareUnsigned(
      leftHigh: Long, leftLow: Long, rightHigh: Long, rightLow: Long): Int = {
    var comparison = JLong.compareUnsigned(leftHigh, rightHigh)
    if (comparison == 0) {
      comparison = JLong.compareUnsigned(leftLow, rightLow)
    }

    comparison
  }

  private def incrementUnsafe(high: Long, low: Long): (Long, Long) =
    (incrementHigh(high, low), incrementLow(low))

  def incrementHigh(high: Long, low: Long): Long = high + (if (low == -1) 1 else 0)

  def incrementLow(low: Long): Long = low + 1

  private def combineInts(high: Int, low: Int): Long =
    (high.toLong << 32) | Integer.toUnsignedLong(low)

  private def highValue(value: Long): Long = value >>> 32

  private def lowValue(value: Long): Long = value & LOW_32_BITS

  private def highInt(value: Long): Int = highValue(value).toInt

  private def lowInt(value: Long): Int = value.toInt

  private def unsignedCarry(left: Long, right: Long): Long =
    ((left >>> 1) + (right >>> 1) + ((left & right) & 1)) >>> 63 // HD 2-13

  private def unsignedBorrow(left: Long, right: Long): Long =
    ((~left & right) | (~(left ^ right) & (left - right))) >>> 63 // HD 2-13

  private def checkState(condition: Boolean): Unit = {
    if (!condition) {
      throw new IllegalStateException();
    }
  }

  private def checkArgument(condition: Boolean): Unit = {
    if (!condition) {
      throw new IllegalArgumentException()
    }
  }

  def divisionByZeroException(): ArithmeticException =
    new ArithmeticException("Division by zero")

  private def int128OverflowException(message: String): ArithmeticException =
    new ArithmeticException(s"Int128 Overflow. $message")

  private def negativeInt128Error(): IllegalArgumentException =
    new IllegalArgumentException("Value must be positive")

}
