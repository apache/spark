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

import scala.util.Try

import org.apache.spark.annotation.Unstable

@Unstable
final class Decimal128 extends Ordered[Decimal128] with Serializable {
  import org.apache.spark.sql.types.Decimal128._

  private var _scale: Int = 0
  private var int128: Int128 = null

  def scale: Int = _scale
  def high: Long = int128.high
  def low: Long = int128.low

  def set(intVal: Int): Decimal128 = {
    set(Int128(0, intVal.toLong), 0)
  }

  def set(str: String): Decimal128 = {
    set(Int128(str), 0)
  }

  def set(high: Long, low: Long, scale: Int): Decimal128 = {
    assert(scale >= 0)
    this.int128 = Int128(high, low)
    this._scale = scale
    this
  }

  def set(int128: Int128, scale: Int): Decimal128 = {
    assert(scale >= 0)
    this.int128 = int128
    this._scale = scale
    this
  }

  def isPositive(): Boolean = int128.isPositive()

  def isNegative(): Boolean = int128.isNegative()

  def isZero(): Boolean = int128.isZero()

  def toDouble: Double = int128.toDouble

  def toFloat: Float = int128.toFloat

  def toLong(): Long = int128.toLong()

  def toInt: Int = int128.toInt

  def + (that: Decimal128): Decimal128 = {
    if (this._scale == that._scale) {
      Decimal128(this.int128 + that.int128, this._scale)
    } else if (this._scale > that._scale) {
      val upScaleN = checkScale(that.int128, this._scale - that._scale)
      Decimal128(this.int128 + that.int128.scaleUpTen(upScaleN), this._scale)
    } else {
      val upScaleN = checkScale(this.int128, that._scale - this._scale)
      Decimal128(this.int128.scaleUpTen(upScaleN) + that.int128, that._scale)
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
    if (this == other) {
      return 0
    }

    if (this._scale == other._scale) {
      this.int128.compare(other.int128)
    } else if (this._scale > other._scale) {
      val upScaleN = this._scale - other._scale
      this.int128.compare(other.int128.scaleUpTen(upScaleN))
    } else {
      val upScaleN = other._scale - this._scale
      this.int128.scaleUpTen(upScaleN).compare(other.int128)
    }
  }
}

@Unstable
object Decimal128 {

  def apply(high: Long, low: Long, scale: Int): Decimal128 = new Decimal128().set(high, low, scale)

  def apply(int128: Int128, scale: Int): Decimal128 = new Decimal128().set(int128, scale)

  def apply(value: String): Decimal128 = new Decimal128().set(value)

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
