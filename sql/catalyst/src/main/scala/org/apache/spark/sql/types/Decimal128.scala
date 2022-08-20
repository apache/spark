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

import java.math.BigInteger

import scala.util.Try

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.types.Decimal.ROUND_HALF_UP
import org.apache.spark.sql.util.Int128Math2

@Unstable
final class Decimal128 extends Ordered[Decimal128] with Serializable {
  import org.apache.spark.sql.types.Decimal128._

  private var int128: Int128 = null
  private var longVal: Long = 0L
  private var _scale: Int = 0

  def scale: Int = _scale
  def high: Long = int128.high
  def low: Long = int128.low

  /**
   * Set this Decimal128 to the given Long. Will have precision 20 and scale 0.
   */
  def set(longVal: Long): Decimal128 = {
    if (longVal <= -POW_10(MAX_LONG_DIGITS) || longVal >= POW_10(MAX_LONG_DIGITS)) {
      // We can't represent this compactly as a long without risking overflow
      this.int128 = Int128(longVal)
      this.longVal = 0L
    } else {
      this.int128 = null
      this.longVal = longVal
    }
    this._scale = 0
    this
  }

  /**
   * Set this Decimal128 to the given Int. Will have precision 10 and scale 0.
   */
  def set(intVal: Int): Decimal128 = {
    this.int128 = null
    this.longVal = intVal
    this._scale = 0
    this
  }

  def set(high: Long, low: Long, scale: Int): Decimal128 = {
    assert(scale >= 0)
    this.int128 = Int128(high, low)
    this.longVal = 0
    this._scale = scale
    this
  }

  def set(int128: Int128): Decimal128 = {
    this.int128 = int128
    this.longVal = 0
    this._scale = 0
    this
  }

  def set(int128: Int128, scale: Int): Decimal128 = {
    assert(scale >= 0)
    this.int128 = int128
    this.longVal = 0
    this._scale = scale
    this
  }

  /**
   * Set this Decimal128 to the given unscaled Long, with a given scale.
   */
  def set(unscaled: Long, scale: Int): Decimal128 = {
    DecimalType.checkNegativeScale(scale)
    if (unscaled <= -POW_10(MAX_LONG_DIGITS) || unscaled >= POW_10(MAX_LONG_DIGITS)) {
      this.int128 = Int128(unscaled)
      this.longVal = 0L
    } else {
      this.int128 = null
      this.longVal = unscaled
    }
    this._scale = scale
    this
  }

  /**
   * Set this Decimal128 to the given BigDecimal value, inheriting its precision and scale.
   */
  def set(decimal: BigDecimal): Decimal128 = {
    set(decimal.underlying().unscaledValue())
//    val result = Int128(decimal.underlying().unscaledValue())
//    throwIfOverflows(result.high, result.low)
    this._scale = decimal.scale
    this
  }

  /**
   * Set this Decimal128 to the given BigDecimal value, with a given scale.
   */
  def set(decimal: BigDecimal, scale: Int): Decimal128 = {
    DecimalType.checkNegativeScale(scale)
    set(decimal.setScale(scale, ROUND_HALF_UP))
  }

  /**
   * If the value is not in the range of long, convert it to BigDecimal and
   * the precision and scale are based on the converted value.
   *
   * This code avoids BigDecimal object allocation as possible to improve runtime efficiency
   */
  def set(bigInteger: BigInteger): Decimal128 = {
    try {
      this.int128 = null
      this.longVal = bigInteger.longValueExact()
      this._scale = 0
      this
    } catch {
      case _: ArithmeticException =>
        set(Int128(bigInteger))
    }
  }

  def toInt128: Int128 = {
    if (int128.ne(null)) {
      int128
    } else {
      Int128(longVal)
    }
  }

  def toBigDecimal: BigDecimal = {
    if (int128.ne(null)) {
      BigDecimal(int128.toBigInteger, _scale)
    } else {
      BigDecimal(longVal, _scale)
    }
  }

  def isPositive: Boolean = int128.isPositive()

  def isNegative: Boolean = int128.isNegative()

  override def toString: String = toBigDecimal.toString()

  def toDouble: Double = int128.toDouble

  def toFloat: Float = int128.toFloat

  def toLong: Long = int128.toLong()

  def toInt: Int = int128.toInt

  def + (that: Decimal128): Decimal128 = {
    if (this.int128.eq(null) && that.int128.eq(null) && this._scale == that.scale) {
      Decimal128(this.longVal + that.longVal, scale)
    } else {
      val (resultScale, rescale, rescaleLeft) = if (this._scale > that.scale) {
        (this._scale, this._scale - that.scale, false)
      } else if (this._scale < that.scale) {
        (that.scale, that.scale - this._scale, true)
      } else {
        (this._scale, 0, false)
      }
      val result = if (rescale == 0) {
        Int128Math2.add(toInt128, that.toInt128)
      } else {
        operatorWithRescale(toInt128, that.toInt128, rescale, rescaleLeft) (Int128Math2.add)
      }

      if (Int128.overflows(result._1, result._2)) {
        throw new ArithmeticException("Overflow in decimal addition")
      }

      Decimal128(Int128(result._1, result._2), resultScale)
    }
  }

  def - (that: Decimal128): Decimal128 = {
    if (this._scale == that._scale) {
      Decimal128(this.int128 - that.int128, this._scale)
    } else if (this._scale > that._scale) {
      val upScaleN = checkScale(that.int128, this._scale - that._scale)
      Decimal128(this.int128 - that.int128.scaleUpTen(upScaleN), this._scale)
    } else {
      val upScaleN = checkScale(this.int128, that._scale - this._scale)
      Decimal128(this.int128.scaleUpTen(upScaleN) - that.int128, that._scale)
    }
  }

  def * (that: Decimal128): Decimal128 = {
    val productScale = this._scale + that._scale
    val upScaleN1 = checkScale(this.int128, that._scale)
    val upScaleN2 = checkScale(that.int128, this._scale)
    Decimal128(this.int128.scaleUpTen(upScaleN1) * that.int128.scaleUpTen(upScaleN2), productScale)
  }

  def / (that: Decimal128): Decimal128 = {
    Decimal128(this.int128.scaleUpTen(that._scale) / that.int128.scaleUpTen(this._scale),
      this._scale + that._scale)
  }

  def quot (that: Decimal128): Decimal128 = this / that

  def unary_- : Decimal128 = {
    Decimal128(this.int128.unary_-, this._scale)
  }

  override def compare(other: Decimal128): Int = {
    if (this.int128.eq(null) && other.int128.eq(null) && this._scale == other._scale) {
      if (this.longVal < other.longVal) -1 else if (this.longVal == other.longVal) 0 else 1
    } else {
      val (rescale, rescaleLeft) = if (this._scale > other.scale) {
        (this._scale - other.scale, false)
      } else if (this._scale < other.scale) {
        (other.scale - this._scale, true)
      } else {
        (0, false)
      }
      val left = toInt128
      val right = other.toInt128
      if (rescale == 0) {
        left.compare(right)
      } else {
        operatorWithRescale(toInt128, other.toInt128, rescale, rescaleLeft) (Int128.compare)
      }
    }
  }

  override def equals(other: Any): Boolean = other match {
    case d: Decimal128 =>
      compare(d) == 0
    case _ =>
      false
  }

  override def hashCode(): Int = toInt128.hashCode() ^ scale.hashCode()

  def isZero: Boolean = if (int128.ne(null)) int128.isZero() else longVal == 0
}

@Unstable
object Decimal128 {

  /** Maximum number of decimal digits a Long can represent */
  val MAX_LONG_DIGITS = 18

  val POW_10 = Array.tabulate[Long](MAX_LONG_DIGITS + 1)(i => math.pow(10, i).toLong)

  def apply(value: Long): Decimal128 = new Decimal128().set(value)

  def apply(value: Int): Decimal128 = new Decimal128().set(value)

  def apply(value: BigDecimal): Decimal128 = new Decimal128().set(value)

  def apply(value: java.math.BigDecimal): Decimal128 = new Decimal128().set(value)

  def apply(value: BigDecimal, scale: Int): Decimal128 = new Decimal128().set(value, scale)

  def apply(high: Long, low: Long, scale: Int): Decimal128 = new Decimal128().set(high, low, scale)

  def apply(int128: Int128, scale: Int): Decimal128 = new Decimal128().set(int128, scale)

  def apply(unscaled: Long, scale: Int): Decimal128 = new Decimal128().set(unscaled, scale)

  def apply(value: String): Decimal128 = new Decimal128().set(BigDecimal(value))

  def checkScale(int128: Int128, scale: Long): Int = {
    var asInt = scale.toInt
    if (asInt != scale) {
      asInt = if (scale > Integer.MAX_VALUE) {
        Integer.MAX_VALUE
      } else {
        Integer.MIN_VALUE
      }
      if (!int128.isZero()) {
        val msg = if (asInt > 0) {
          "Underflow"
        } else {
          "Overflow"
        }
        throw new ArithmeticException(msg)
      }
    }
    asInt
  }

  def throwIfOverflows(high: Long, low: Long): Unit = {
    if (Int128.overflows(high, low)) {
      throw new ArithmeticException("Decimal overflow");
    }
  }

  def operatorWithRescale[T](
      left: Int128,
      right: Int128,
      rescale: Int,
      rescaleLeft: Boolean) (f: (Long, Long, Long, Long) => T): T = {
    operatorWithRescale(left.high, left.low, right.high, right.low, rescale, rescaleLeft) (f)
  }

  def operatorWithRescale[T](
      leftHigh: Long,
      leftLow: Long,
      rightHigh: Long,
      rightLow: Long,
      rescale: Int,
      rescaleLeft: Boolean) (f: (Long, Long, Long, Long) => T): T = {
    if (rescaleLeft) {
      val (newLeftHigh, newLeftLow) = Int128Math2.rescale(leftHigh, leftLow, rescale)
      f(newLeftHigh, newLeftLow, rightHigh, rightLow)
    } else {
      val (newRightHigh, newRightLow) = Int128Math2.rescale(rightHigh, rightLow, rescale)
      f(leftHigh, leftLow, newRightHigh, newRightLow)
    }
  }

  /** Common methods for Decimal128 evidence parameters */
  private[sql] trait Decimal128IsConflicted extends Numeric[Decimal128] {
    override def plus(x: Decimal128, y: Decimal128): Decimal128 = x + y
    override def times(x: Decimal128, y: Decimal128): Decimal128 = x * y
    override def minus(x: Decimal128, y: Decimal128): Decimal128 = x - y
    override def negate(x: Decimal128): Decimal128 = -x
    override def toDouble(x: Decimal128): Double = x.toDouble
    override def toFloat(x: Decimal128): Float = x.toFloat
    override def toInt(x: Decimal128): Int = x.toInt
    override def toLong(x: Decimal128): Long = x.toLong
    override def fromInt(x: Int): Decimal128 = new Decimal128().set(x)
    override def compare(x: Decimal128, y: Decimal128): Int = x.compare(y)
    // Added from Scala 2.13; don't override to work in 2.12
    // TODO revisit once Scala 2.12 support is dropped
    def parseString(str: String): Option[Decimal128] = Try(Decimal128(str)).toOption
  }

  /** A [[scala.math.Fractional]] evidence parameter for Decimal128s. */
  private[sql] object Decimal128IsFractional
    extends Decimal128IsConflicted with Fractional[Decimal128] {
    override def div(x: Decimal128, y: Decimal128): Decimal128 = x / y
  }

  /** A [[scala.math.Integral]] evidence parameter for Decimals. */
  private[sql] object Decimal128AsIfIntegral
    extends Decimal128IsConflicted with Integral[Decimal128] {
    override def quot(x: Decimal128, y: Decimal128): Decimal128 = x quot y
    override def rem(x: Decimal128, y: Decimal128): Decimal128 = x % y
  }

}
