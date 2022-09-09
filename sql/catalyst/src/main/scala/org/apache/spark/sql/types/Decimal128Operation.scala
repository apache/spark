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

package org.apache.spark.sql.types

import scala.math.max

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.util.Int128Math

@Unstable
class Decimal128Operation extends DecimalOperation[Decimal128Operation] {
  import org.apache.spark.sql.types.Decimal128Operation._
  import org.apache.spark.sql.types.Decimal.{ROUND_CEILING, ROUND_FLOOR}

  private var int128: Int128 = null

  def high: Long = if (int128.eq(null)) this.longVal >> 63 else int128.high
  def low: Long = if (int128.eq(null)) this.longVal else int128.low

  def newInstance(): Decimal128Operation = new Decimal128Operation()

  def set(high: Long, low: Long, precision: Int, scale: Int): Decimal128Operation =
    set(Int128(high, low), precision, scale)

  def set(int128: Int128, precision: Int, scale: Int): Decimal128Operation = {
    this.int128 = int128
    this._precision = precision
    this._scale = scale
    this
  }

  def setLong(longVal: Long): Unit = {
    this.int128 = Int128(longVal)
  }

  def setLong(unscaled: Long, scale: Int): Unit = setLong(unscaled)

  def setBigDecimal(decimalVal: BigDecimal): Unit = {
    this.int128 = Int128(decimalVal.underlying().unscaledValue())
  }

  def setNull: Unit = {
    this.int128 = null
  }

  def isNull(): Boolean = int128.eq(null)

  def isNotNull(): Boolean = int128.ne(null)

  def getAsBigDecimal(): BigDecimal = BigDecimal(this.int128.toBigInteger, this._scale)

  def getAsJavaBigDecimal(): java.math.BigDecimal =
    new java.math.BigDecimal(int128.toBigInteger, _scale)

  def getAsJavaBigInteger(): java.math.BigInteger = this.int128.toBigInteger

  def rescale(precision: Int, scale: Int, roundMode: BigDecimal.RoundingMode.Value): Boolean = {
    val newDecimalVal = getAsBigDecimal().setScale(scale, roundMode)
    if (newDecimalVal.precision > precision) {
      return false
    }

    val rescale = scale - _scale
    if (roundMode == ROUND_CEILING || roundMode == ROUND_FLOOR) {
      set(newDecimalVal)
    } else {
      try {
        val (newLeftHigh, newLeftLow) =
          Int128Math.rescale(this.int128.high, this.int128.low, rescale)
        this.int128 = Int128(newLeftHigh, newLeftLow)
      } catch {
        case _: IllegalArgumentException =>
          set(newDecimalVal)
      }
    }

    true
  }

  def doCompare(that: Decimal128Operation): Int =
    operatorWithRescale(
      scale,
      that.scale,
      high,
      low,
      that.high,
      that.low) (Int128.compare)

  def isEqualsZero(): Boolean = this.int128.isZero()

  def doAdd(that: Decimal128Operation): Decimal128Operation = {
    val (newHigh, newLow) = operatorWithRescale(
      scale,
      that.scale,
      high,
      low,
      that.high,
      that.low) (Int128Math.add)

    checkOverflow(newHigh, newLow, "Decimal128 addition.")

    val resultScale = max(this._scale, that.scale)
    val resultPrecision = resultScale +
      max(this._precision - this._scale, that.precision - that.scale) + 1

    val decimal128Operation = newInstance()
    decimal128Operation.set(Int128(newHigh, newLow), resultPrecision, resultScale)
  }

  def doSubtract(that: Decimal128Operation): Decimal128Operation = {
    val (newHigh, newLow) = operatorWithRescale(
      scale,
      that.scale,
      high,
      low,
      that.high,
      that.low) (Int128Math.subtract)

    checkOverflow(newHigh, newLow, "Decimal128 subtract.")

    val resultScale = max(this._scale, that.scale)
    val resultPrecision = resultScale +
      max(this._precision - this._scale, that.precision - that.scale) + 1

    val decimal128Operation = newInstance()
    decimal128Operation.set(Int128(newHigh, newLow), resultPrecision, resultScale)
  }

  def multiply(that: Decimal128Operation): Decimal128Operation = {
    val (newHigh, newLow) = Int128Math.multiply(this.high, this.low, that.high, that.low)

    checkOverflow(newHigh, newLow, "Decimal128 multiply.")

    val resultScale = this._scale + that.scale
    val resultPrecision = this._precision + that.precision + 1

    val decimal128Operation = newInstance()
    decimal128Operation.set(Int128(newHigh, newLow), resultPrecision, resultScale)
  }

  def divide(that: Decimal128Operation): Decimal128Operation = {
    val resultScale =
      Math.min(Math.max(6, this._scale + that.precision + 1), DecimalType.MAX_PRECISION)
    val rescaleFactor = resultScale - this._scale + that.scale
    val (newHigh, newLow) = try {
      Int128Math.divideRoundUp(this.high, this.low, that.high, that.low, rescaleFactor, 0)
    } catch {
      case _: ArithmeticException =>
        throw overflowError("Decimal128 division.")
    }

    checkOverflow(newHigh, newLow, "Decimal128 division.")

    val resultPrecision = this._precision - this._scale + that.scale + resultScale

    val decimal128Operation = newInstance()
    decimal128Operation.set(Int128(newHigh, newLow), resultPrecision, resultScale)
  }

  def remainder(that: Decimal128Operation): Decimal128Operation = {
    val leftRescaleFactor = Int128Math.rescaleFactor(this._scale, that.scale)
    val rightRescaleFactor = Int128Math.rescaleFactor(that.scale, this._scale)
    val (newHigh, newLow) = Int128Math.remainder(
      this.high, this.low, that.high, that.low, leftRescaleFactor, rightRescaleFactor)

    checkOverflow(newHigh, newLow, "Decimal128 remainder.")

    val resultScale = Math.max(this._scale, that.scale)
    val resultPrecision =
      Math.min(this._precision - this._scale, that.precision - that.scale) + resultScale

    val decimal128Operation = newInstance()
    decimal128Operation.set(Int128(newHigh, newLow), resultPrecision, resultScale)
  }

  def quot(that: Decimal128Operation): Decimal128Operation = {
    val divided = this.divide(that)
    val (newHigh, newLow) =
      Int128Math.rescaleTruncate(divided.int128.high, divided.int128.low, -divided.scale)

    checkOverflow(newHigh, newLow, "Decimal128 quot.")

    val resultScale = 0
    val resultPrecision = divided.precision

    val decimal128Operation = newInstance()
    decimal128Operation.set(Int128(newHigh, newLow), resultPrecision, resultScale)
  }

  def doNegative: Decimal128Operation = {
    val decimal128Operation = newInstance()
    decimal128Operation.set(-this.int128, precision, scale)
  }

  def copy(from: Decimal128Operation): Unit = {
    this.int128 = from.int128
  }
}

@Unstable
object Decimal128Operation {

  def operatorWithRescale[T](
      leftScale: Int,
      rightScale: Int,
      leftHigh: Long,
      leftLow: Long,
      rightHigh: Long,
      rightLow: Long) (f: (Long, Long, Long, Long) => T): T = {
    val (rescale, rescaleLeft) = if (leftScale > rightScale) {
      (leftScale - rightScale, false)
    } else if (leftScale < rightScale) {
      (rightScale - leftScale, true)
    } else {
      (0, false)
    }
    if (rescale == 0) {
      f(leftHigh, leftLow, rightHigh, rightLow)
    } else {
      if (rescaleLeft) {
        val (newLeftHigh, newLeftLow) = Int128Math.rescale(leftHigh, leftLow, rescale)
        f(newLeftHigh, newLeftLow, rightHigh, rightLow)
      } else {
        val (newRightHigh, newRightLow) = Int128Math.rescale(rightHigh, rightLow, rescale)
        f(leftHigh, leftLow, newRightHigh, newRightLow)
      }
    }
  }

  def checkOverflow(high: Long, low: Long, msg: String): Unit = {
    if (Int128.overflows(high, low)) {
      throw overflowError(msg)
    }
  }

  def overflowError(msg: String): ArithmeticException = {
    new ArithmeticException(s"Decimal overflow: $msg")
  }
}
