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

import java.math.{MathContext, RoundingMode, BigDecimal => JavaBigDecimal}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.unsafe.PlatformDependent

/**
 * A mutable implementation of BigDecimal that can hold a Long if values are small enough.
 *
 * The semantics of the fields are as follows:
 * - _precision and _scale represent the SQL precision and scale we are looking for
 * - If decimalVal is set, it represents the whole decimal value
 * - Otherwise, the decimal value is longVal / (10 ** _scale)
 */
final class Decimal extends Ordered[Decimal] with Serializable {
  import org.apache.spark.sql.types.Decimal._

  private var decimalVal: JavaBigDecimal = BIG_DEC_ZERO
  private var _precision: Int = 1

  def precision: Int = _precision
  def scale: Int = decimalVal.scale()

  /**
   * Set this Decimal to the given Long. Will have precision 20 and scale 0.
   */
  def set(longVal: Long): Decimal = {
    decimalVal = JavaBigDecimal.valueOf(longVal)
    _precision = 20
    this
  }

  /**
   * Set this Decimal to the given Int. Will have precision 10 and scale 0.
   */
  def set(intVal: Int): Decimal = {
    decimalVal = JavaBigDecimal.valueOf(intVal)
    _precision = 10
    this
  }

  /**
   * Set this Decimal to the given unscaled Long, with a given precision and scale.
   *
   * Note: this is used in serialization, caller will make sure that it will not overflow
   */
  def set(unscaled: Long, precision: Int, scale: Int): Decimal = {
    decimalVal = JavaBigDecimal.valueOf(unscaled, scale)
    _precision = precision
    this
  }

  /**
   * Set this Decimal to the given BigDecimal value, with a given precision and scale.
   */
  def set(decimal: BigDecimal, precision: Int, scale: Int): Decimal = {
    set(decimal.underlying(), precision, scale)
  }

  /**
   * Set this Decimal to the given BigDecimal value, inheriting its precision and scale.
   */
  def set(decimal: BigDecimal): Decimal = {
    set(decimal.underlying())
  }

  /**
   * Set this Decimal to the given java.math.BigDecimal value, with a given precision and scale.
   */
  private[sql] def set(decimal: JavaBigDecimal, precision: Int, scale: Int): Decimal = {
    decimalVal = decimal.setScale(scale, ROUNDING_MODE)
    require(decimalVal.precision <= precision, "Overflowed precision")
    _precision = precision
    this
  }

  /**
   * Set this Decimal to the given java.math.BigDecimal value, inheriting its precision and scale.
   */
  private[sql] def set(decimal: JavaBigDecimal): Decimal = {
    this.decimalVal = decimal
    this._precision = decimal.precision
    this
  }

  /**
   * Set this Decimal to the given Decimal value.
   */
  def set(decimal: Decimal): Decimal = {
    this.decimalVal = decimal.decimalVal
    this._precision = decimal._precision
    this
  }

  def toBigDecimal: BigDecimal = BigDecimal(toJavaBigDecimal)

  private[sql] def toJavaBigDecimal: JavaBigDecimal = decimalVal

  def toUnscaledLong: Long = {
    val unscaled = PlatformDependent.UNSAFE.getLong(decimalVal,
      PlatformDependent.BIG_DECIMAL_INTCOMPACT_OFFSET)
    if (unscaled != Long.MinValue) {
      unscaled
    } else {
      decimalVal.unscaledValue().longValue()
    }
  }

  override def toString: String = decimalVal.toString()

  @DeveloperApi
  def toDebugString: String = {
    s"Decimal($decimalVal,${_precision})"
  }

  def toDouble: Double = toJavaBigDecimal.doubleValue()

  def toFloat: Float = toJavaBigDecimal.floatValue()

  def toLong: Long = decimalVal.longValue()

  def toInt: Int = toLong.toInt

  def toShort: Short = toLong.toShort

  def toByte: Byte = toLong.toByte

  /**
   * Update precision and scale while keeping our value the same, and return true if successful.
   *
   * @return true if successful, false if overflow would occur
   */
  def changePrecision(precision: Int, scale: Int): Boolean = {
    // fast path for UnsafeProjection
    if (precision == _precision && scale == decimalVal.scale()) {
      return true
    }

    val newVal = decimalVal.setScale(scale, ROUNDING_MODE)
    if (newVal.precision > precision) {
      return false
    }
    decimalVal = newVal
    _precision = precision
    true
  }

  override def clone(): Decimal = new Decimal().set(this)

  override def compare(other: Decimal): Int = {
    toJavaBigDecimal.compareTo(other.toJavaBigDecimal)
  }

  override def equals(other: Any): Boolean = other match {
    case d: Decimal =>
      compare(d) == 0
    case _ =>
      false
  }

  override def hashCode(): Int = toBigDecimal.hashCode()

  def isZero: Boolean = {
    decimalVal.compareTo(BIG_DEC_ZERO) == 0
  }

  def + (that: Decimal): Decimal = {
    Decimal(toJavaBigDecimal.add(that.toJavaBigDecimal, MATH_CONTEXT), precision, scale)
  }

  def - (that: Decimal): Decimal = {
    Decimal(toJavaBigDecimal.subtract(that.toJavaBigDecimal, MATH_CONTEXT), precision, scale)
  }

  // HiveTypeCoercion will take care of the precision, scale of result
  def * (that: Decimal): Decimal = {
    Decimal(toJavaBigDecimal.multiply(that.toJavaBigDecimal, MATH_CONTEXT))
  }

  def / (that: Decimal): Decimal = {
    if (that.isZero) null
    else Decimal(toJavaBigDecimal.divide(that.toJavaBigDecimal, MATH_CONTEXT))
  }

  def % (that: Decimal): Decimal = {
    if (that.isZero) null else Decimal(toJavaBigDecimal.remainder(that.toJavaBigDecimal))
  }

  def remainder(that: Decimal): Decimal = this % that

  def unary_- : Decimal = {
    Decimal(decimalVal.negate(), precision, scale)
  }

  def abs: Decimal = if (this.compare(Decimal.ZERO) < 0) this.unary_- else this
}

object Decimal {
  /** Maximum number of decimal digits a Long can represent */
  val MAX_LONG_DIGITS = 18

  val ZERO = Decimal(0)
  val ONE = Decimal(1)

  private val ROUNDING_MODE = RoundingMode.HALF_UP
  private val MATH_CONTEXT = new MathContext(DecimalType.MAX_PRECISION, ROUNDING_MODE)
  private val POW_10 = Array.tabulate[Long](MAX_LONG_DIGITS + 1)(i => math.pow(10, i).toLong)
  private val BIG_DEC_ZERO: JavaBigDecimal = JavaBigDecimal.valueOf(0)

  def apply(value: Double): Decimal = new Decimal().set(value)

  def apply(value: Long): Decimal = new Decimal().set(value)

  def apply(value: Int): Decimal = new Decimal().set(value)

  def apply(value: BigDecimal): Decimal = new Decimal().set(value)

  def apply(value: JavaBigDecimal): Decimal = new Decimal().set(value)

  def apply(value: BigDecimal, precision: Int, scale: Int): Decimal =
    new Decimal().set(value, precision, scale)

  def apply(value: JavaBigDecimal, precision: Int, scale: Int): Decimal =
    new Decimal().set(value, precision, scale)

  def apply(unscaled: Long, precision: Int, scale: Int): Decimal =
    new Decimal().set(unscaled, precision, scale)

  def apply(value: String): Decimal = new Decimal().set(BigDecimal(value))

  // Evidence parameters for Decimal considered either as Fractional or Integral. We provide two
  // parameters inheriting from a common trait since both traits define mkNumericOps.
  // See scala.math's Numeric.scala for examples for Scala's built-in types.

  /** Common methods for Decimal evidence parameters */
  private[sql] trait DecimalIsConflicted extends Numeric[Decimal] {
    override def plus(x: Decimal, y: Decimal): Decimal = x + y
    override def times(x: Decimal, y: Decimal): Decimal = x * y
    override def minus(x: Decimal, y: Decimal): Decimal = x - y
    override def negate(x: Decimal): Decimal = -x
    override def toDouble(x: Decimal): Double = x.toDouble
    override def toFloat(x: Decimal): Float = x.toFloat
    override def toInt(x: Decimal): Int = x.toInt
    override def toLong(x: Decimal): Long = x.toLong
    override def fromInt(x: Int): Decimal = new Decimal().set(x)
    override def compare(x: Decimal, y: Decimal): Int = x.compare(y)
  }

  /** A [[scala.math.Fractional]] evidence parameter for Decimals. */
  private[sql] object DecimalIsFractional extends DecimalIsConflicted with Fractional[Decimal] {
    override def div(x: Decimal, y: Decimal): Decimal = x / y
  }

  /** A [[scala.math.Integral]] evidence parameter for Decimals. */
  private[sql] object DecimalAsIfIntegral extends DecimalIsConflicted with Integral[Decimal] {
    override def quot(x: Decimal, y: Decimal): Decimal = x / y
    override def rem(x: Decimal, y: Decimal): Decimal = x % y
  }
}
