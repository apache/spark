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

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.util.{Int128Holder, Int128Math}

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

  def isZero(): Boolean = (_high | _low) == 0

  def toDouble: Double = toLong.doubleValue

  def toFloat: Float = toLong.floatValue

  def toLong(): Long = low

  def toInt: Int = toLong.toInt

  def + (that: Int128): Int128 = {
    val newHigh = Int128Math.addHigh(_high, _low, that.high, that.low)
    val newLow = Int128Math.addLow(_low, that.low)
    new Int128().set(newHigh, newLow)
  }

  def - (that: Int128): Int128 = {
    val newHigh = Int128Math.subtractHigh(_high, _low, that.high, that.low)
    val newLow = Int128Math.subtractLow(_low, that.low)
    new Int128().set(newHigh, newLow)
  }

  def * (that: Int128): Int128 = {
    val newHigh = Int128Math.multiplyHigh(_high, _low, that.high, that.low)
    val newLow = Int128Math.multiplyLow(_low, that.low)
    new Int128().set(newHigh, newLow)
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

  def unary_- : Int128 = {
    val newHigh = Int128Math.negateHigh(this.high, this.low)
    val newLow = Int128Math.negateLow(this.low)
    new Int128().set(newHigh, newLow)
  }

  override def compare(other: Int128): Int = {
    val result = _high.compare(other._high)
    if (result == 0) {
      return JLong.compareUnsigned(low, other._low)
    }
    result
  }

  override def equals(other: Any): Boolean = other match {
    case d: Int128 =>
      compare(d) == 0
    case _ =>
      false
  }

  override def hashCode(): Int = _high.hashCode ^ _low.hashCode
}

@Unstable
object Int128 {
  def apply(high: Long, low: Long): Int128 = new Int128().set(high, low)

  val ZERO = Int128(0, 0)
  val ONE = Int128(0, 1)

  def divide(
      dividend: Int128, divisor: Int128, quotient: Int128Holder, remainder: Int128Holder): Unit = {
    Int128Math.divide(dividend.high, dividend.low, divisor.high, divisor.low, quotient, remainder)
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
  }

  /** A [[scala.math.Integral]] evidence parameter for Int128s. */
  private[sql] object Int128IsIntegral extends Int128IsConflicted with Integral[Int128] {
    override def quot(x: Int128, y: Int128): Int128 = x / y
    override def rem(x: Int128, y: Int128): Int128 = x % y
  }
}
