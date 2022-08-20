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

import java.lang.Math.{pow, round}
import java.math.BigInteger

import org.apache.spark.sql.types.Int128

object Int128Math2 {

  // Lowest 32 bits of a long
  private val LOW_32_BITS = 0xFFFFFFFFL

  private val POWERS_OF_TEN = Array.tabulate[Int128](39)(i => Int128(BigInteger.TEN.pow(i)))

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

  def rescale(high: Long, low: Long, rescale: Int): (Long, Long) = {
    if (rescale == 0) {
      (high, low)
    } else if (rescale > 0) {
      shiftLeftBy10(high, low, rescale)
    } else {
      scaleDownRoundUp(high, low, -rescale)
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
      tmpHigh = negateHighExact(result._1, result._2)
      tmpLow = negateLow(result._2)

      return (tmpHigh, tmpLow)
    }

    result
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
    }
    else {
      scaleDownTen(high, low, rescale, roundUp)
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

  private def incrementUnsafe(high: Long, low: Long): (Long, Long) =
    (incrementHigh(high, low), incrementLow(low))

  def incrementHigh(high: Long, low: Long): Long = high + (if (low == -1) 1 else 0)

  def incrementLow(low: Long): Long = low + 1

  private def highValue(value: Long): Long = value >>> 32

  private def lowValue(value: Long): Long = value & LOW_32_BITS

  private def unsignedCarry(left: Long, right: Long): Long =
    ((left >>> 1) + (right >>> 1) + ((left & right) & 1)) >>> 63 // HD 2-13

  private def int128OverflowException(message: String): ArithmeticException =
    new ArithmeticException(s"Int128 Overflow. $message")

  private def negativeInt128Error(): IllegalArgumentException =
    new IllegalArgumentException("Value must be positive")

}
