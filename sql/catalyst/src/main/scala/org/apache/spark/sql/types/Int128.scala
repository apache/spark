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

import java.lang.{Long => JLong}
import java.math.BigInteger
import java.nio.{ByteBuffer, ByteOrder}

import scala.util.Try

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.util.{Int128Holder, Int128Math, MoreMath}

/**
 * A mutable implementation of Int128 that hold two Long values to represent the high and low bits
 * of 128 bits respectively.
 *
 * The semantics of the fields are as follows:
 * - _high and _low represent the high and low bits of 128 bits respectively
 */
@Unstable
final class Int128 extends Ordered[Int128] with Serializable {
  import org.apache.spark.sql.types.Int128._

  private var _high: Long = 0L
  private var _low: Long = 0L

  def high: Long = _high
  def low: Long = _low

  def set(high: Long, low: Long): Int128 = {
    _high = high
    _low = low
    this
  }

  def set(value: Long): Int128 = set(value >> 63, value)

  def set(bigInteger: BigInteger): Int128 = {
    _low = bigInteger.longValue()
    try {
      _high = bigInteger.shiftRight(64).longValueExact()
    } catch {
      case _: ArithmeticException =>
        throw new ArithmeticException("BigInteger out of Int128 range")
    }

    this
  }

  def isPositive(): Boolean = _high > 0 || (_high == 0 && _low != 0)

  def isNegative(): Boolean = _high < 0

  def isZero(): Boolean = (_high | _low) == 0

  def toBigInteger: BigInteger = new BigInteger(toBigEndianBytes())

  def toBigEndianBytes(): Array[Byte] = {
    val bytes = new Array[Byte](16)
    toBigEndianBytes(bytes, 0)
    bytes
  }

  def toBigEndianBytes(bytes: Array[Byte], offset: Int): Unit = {
    val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
    byteBuffer.putLong(offset, high)
    byteBuffer.putLong(offset + JLong.BYTES, low)
  }

  def toDouble: Double = toLong.doubleValue

  def toFloat: Float = toLong.floatValue

  def toLong(): Long = low

  def toInt: Int = toLong.toInt

  def scaleUpTen(n: Int): Int128 = {
    this * Int128(n >> 63, n)
  }

  def + (that: Int128): Int128 = {
    val newHigh = Int128Math.addHighExact(_high, _low, that._high, that._low)
    val newLow = Int128Math.addLow(_low, that._low)
    new Int128().set(newHigh, newLow)
  }

  def - (that: Int128): Int128 = {
    val newHigh = Int128Math.subtractHighExact(_high, _low, that.high, that._low)
    val newLow = Int128Math.subtractLow(_low, that._low)
    new Int128().set(newHigh, newLow)
  }

  def * (that: Int128): Int128 = {
    val z1High = MoreMath.unsignedMultiplyHigh(_low, that._low)
    val z1Low = _low * that._low

    val z2High = MoreMath.unsignedMultiplyHigh(that._high, this._low)
    val z2Low = this._low * that._high

    val z3High = MoreMath.unsignedMultiplyHigh(this._high, that._low)
    val z3Low = this._high * that._low

    val resultLow = z1Low
    val resultHigh = z1High + z2Low + z3Low

    if (MoreMath.productOverflows(this._high, this._low, that._high, that._low,
      z1High, z2High, z2Low, z3High, z3Low, resultHigh)) {
      throw new ArithmeticException("overflow");
    }

    Int128(resultHigh, resultLow)
  }

  def / (that: Int128): Int128 = if (that.isZero) {
    null
  } else {
    val quotient = new Int128Holder()
    val remainder = new Int128Holder()
    divide(this, that, quotient, remainder)

    quotient.get()
  }

  def % (that: Int128): Int128 = if (that.isZero) {
    null
  } else {
    val quotient = new Int128Holder()
    val remainder = new Int128Holder()
    divide(this, that, quotient, remainder)

    remainder.get()
  }

  def quot (that: Int128): Int128 = this / that

  def unary_- : Int128 = {
    val newHigh = Int128Math.negateHigh(this.high, this.low)
    val newLow = Int128Math.negateLow(this.low)
    new Int128().set(newHigh, newLow)
  }

  override def compare(other: Int128): Int = {
    Int128.compare(this.high, this.low, other.high, other.low)
  }

  override def equals(other: Any): Boolean = other match {
    case d: Int128 =>
      compare(d) == 0
    case _ =>
      false
  }

  override def hashCode(): Int = {
    // FNV-1a style hash
    var hash = 0x9E3779B185EBCA87L
    hash = (hash ^ high) * 0xC2B2AE3D27D4EB4FL
    hash = (hash ^ low) * 0xC2B2AE3D27D4EB4FL
    JLong.hashCode(hash)
  }

  override def toString: String = toBigInteger.toString
}

@Unstable
object Int128 {

  def apply(high: Long, low: Long): Int128 = new Int128().set(high, low)

  def apply(value: Long): Int128 = new Int128().set(value)

  def apply(bigInteger: BigInteger): Int128 = new Int128().set(bigInteger)

  def apply(value: String): Int128 = new Int128().set(new BigInteger(value))

  val ZERO = Int128(0, 0)
  val ONE = Int128(0, 1)
  val MAX_VALUE = Int128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)
  val MIN_VALUE = Int128(0x8000000000000000L, 0x0000000000000000L)
  val MAX_UNSCALED_DECIMAL = Int128("99999999999999999999999999999999999999")
  val MIN_UNSCALED_DECIMAL = Int128("-99999999999999999999999999999999999999")

  def compare(leftHigh: Long, leftLow: Long, rightHigh: Long, rightLow: Long): Int = {
    var comparison = JLong.compare(leftHigh, rightHigh)
    if (comparison == 0) {
      comparison = JLong.compareUnsigned(leftLow, rightLow)
    }

    comparison
  }

  def divide(
      dividend: Int128, divisor: Int128, quotient: Int128Holder, remainder: Int128Holder): Unit = {
    Int128Math.divide(dividend.high, dividend.low, divisor.high, divisor.low, quotient, remainder)
  }

  def overflows(high: Long, low: Long): Boolean = {
    compare(high, low, MAX_UNSCALED_DECIMAL.high, MAX_UNSCALED_DECIMAL.low) > 0 ||
      compare(high, low, MIN_UNSCALED_DECIMAL.high, MIN_UNSCALED_DECIMAL.low) < 0
  }

  /** Common methods for Int128 evidence parameters */
  private[sql] trait Int128IsConflicted extends Numeric[Int128] {
    override def plus(x: Int128, y: Int128): Int128 = x + y
    override def times(x: Int128, y: Int128): Int128 = x * y
    override def minus(x: Int128, y: Int128): Int128 = x - y
    override def negate(x: Int128): Int128 = -x
    override def toDouble(x: Int128): Double = x.toDouble
    override def toFloat(x: Int128): Float = x.toFloat
    override def toInt(x: Int128): Int = x.toInt
    override def toLong(x: Int128): Long = x.toLong
    override def fromInt(x: Int): Int128 = new Int128().set(0, x)
    override def compare(x: Int128, y: Int128): Int = x.compare(y)

    def parseString(str: String): Option[Int128] = Try(Int128(str)).toOption
  }

  /** A [[scala.math.Integral]] evidence parameter for Int128s. */
  private[sql] object Int128IsIntegral extends Int128IsConflicted with Integral[Int128] {
    override def quot(x: Int128, y: Int128): Int128 = x quot y
    override def rem(x: Int128, y: Int128): Int128 = x % y
  }
}
