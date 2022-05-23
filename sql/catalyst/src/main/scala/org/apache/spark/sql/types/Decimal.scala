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

import java.math.{BigDecimal => JavaBigDecimal, BigInteger, MathContext, RoundingMode}

import scala.util.Try

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String

/**
 * A mutable implementation of BigDecimal that can hold a Long if values are small enough.
 *
 * The semantics of the fields are as follows:
 * - _precision and _scale represent the SQL precision and scale we are looking for
 * - If decimalVal is set, it represents the whole decimal value
 * - Otherwise, the decimal value is longVal / (10 ** _scale)
 *
 * Note, for values between -1.0 and 1.0, precision digits are only counted after dot.
 */
@Unstable
final class Decimal extends Ordered[Decimal] with Serializable {
  import org.apache.spark.sql.types.Decimal._

  private var decimalVal: BigDecimal = null
  private var longVal: Long = 0L
  private var _precision: Int = 1
  private var _scale: Int = 0

  def precision: Int = _precision
  def scale: Int = _scale

  /**
   * Set this Decimal to the given Long. Will have precision 20 and scale 0.
   */
  def set(longVal: Long): Decimal = {
    if (longVal <= -POW_10(MAX_LONG_DIGITS) || longVal >= POW_10(MAX_LONG_DIGITS)) {
      // We can't represent this compactly as a long without risking overflow
      this.decimalVal = BigDecimal(longVal)
      this.longVal = 0L
    } else {
      this.decimalVal = null
      this.longVal = longVal
    }
    this._precision = 20
    this._scale = 0
    this
  }

  /**
   * Set this Decimal to the given Int. Will have precision 10 and scale 0.
   */
  def set(intVal: Int): Decimal = {
    this.decimalVal = null
    this.longVal = intVal
    this._precision = 10
    this._scale = 0
    this
  }

  /**
   * Set this Decimal to the given unscaled Long, with a given precision and scale.
   */
  def set(unscaled: Long, precision: Int, scale: Int): Decimal = {
    if (setOrNull(unscaled, precision, scale) == null) {
      throw QueryExecutionErrors.unscaledValueTooLargeForPrecisionError()
    }
    this
  }

  /**
   * Set this Decimal to the given unscaled Long, with a given precision and scale,
   * and return it, or return null if it cannot be set due to overflow.
   */
  def setOrNull(unscaled: Long, precision: Int, scale: Int): Decimal = {
    DecimalType.checkNegativeScale(scale)
    if (unscaled <= -POW_10(MAX_LONG_DIGITS) || unscaled >= POW_10(MAX_LONG_DIGITS)) {
      // We can't represent this compactly as a long without risking overflow
      if (precision < 19) {
        return null  // Requested precision is too low to represent this value
      }
      this.decimalVal = BigDecimal(unscaled, scale)
      this.longVal = 0L
    } else {
      val p = POW_10(math.min(precision, MAX_LONG_DIGITS))
      if (unscaled <= -p || unscaled >= p) {
        return null  // Requested precision is too low to represent this value
      }
      this.decimalVal = null
      this.longVal = unscaled
    }
    this._precision = precision
    this._scale = scale
    this
  }

  /**
   * Set this Decimal to the given BigDecimal value, with a given precision and scale.
   */
  def set(decimal: BigDecimal, precision: Int, scale: Int): Decimal = {
    DecimalType.checkNegativeScale(scale)
    this.decimalVal = decimal.setScale(scale, ROUND_HALF_UP)
    if (decimalVal.precision > precision) {
      throw QueryExecutionErrors.decimalPrecisionExceedsMaxPrecisionError(
        decimalVal.precision, precision)
    }
    this.longVal = 0L
    this._precision = precision
    this._scale = scale
    this
  }

  /**
   * Set this Decimal to the given BigDecimal value, inheriting its precision and scale.
   */
  def set(decimal: BigDecimal): Decimal = {
    this.decimalVal = decimal
    this.longVal = 0L
    if (decimal.precision < decimal.scale) {
      // For Decimal, we expect the precision is equal to or large than the scale, however,
      // in BigDecimal, the digit count starts from the leftmost nonzero digit of the exact
      // result. For example, the precision of 0.01 equals to 1 based on the definition, but
      // the scale is 2. The expected precision should be 2.
      this._precision = decimal.scale
      this._scale = decimal.scale
    } else if (decimal.scale < 0 && !SQLConf.get.allowNegativeScaleOfDecimalEnabled) {
      this._precision = decimal.precision - decimal.scale
      this._scale = 0
      // set scale to 0 to correct unscaled value
      this.decimalVal = decimal.setScale(0)
    } else {
      this._precision = decimal.precision
      this._scale = decimal.scale
    }
    this
  }

  /**
   * If the value is not in the range of long, convert it to BigDecimal and
   * the precision and scale are based on the converted value.
   *
   * This code avoids BigDecimal object allocation as possible to improve runtime efficiency
   */
  def set(bigintval: BigInteger): Decimal = {
    try {
      this.decimalVal = null
      this.longVal = bigintval.longValueExact()
      this._precision = DecimalType.MAX_PRECISION
      this._scale = 0
      this
    } catch {
      case _: ArithmeticException =>
        set(BigDecimal(bigintval))
    }
  }

  /**
   * Set this Decimal to the given Decimal value.
   */
  def set(decimal: Decimal): Decimal = {
    this.decimalVal = decimal.decimalVal
    this.longVal = decimal.longVal
    this._precision = decimal._precision
    this._scale = decimal._scale
    this
  }

  def toBigDecimal: BigDecimal = {
    if (decimalVal.ne(null)) {
      decimalVal
    } else {
      BigDecimal(longVal, _scale)
    }
  }

  def toJavaBigDecimal: java.math.BigDecimal = {
    if (decimalVal.ne(null)) {
      decimalVal.underlying()
    } else {
      java.math.BigDecimal.valueOf(longVal, _scale)
    }
  }

  def toScalaBigInt: BigInt = {
    if (decimalVal.ne(null)) {
      decimalVal.toBigInt
    } else {
      BigInt(toLong)
    }
  }

  def toJavaBigInteger: java.math.BigInteger = {
    if (decimalVal.ne(null)) {
      decimalVal.underlying().toBigInteger()
    } else {
      java.math.BigInteger.valueOf(toLong)
    }
  }

  def toUnscaledLong: Long = {
    if (decimalVal.ne(null)) {
      decimalVal.underlying().unscaledValue().longValueExact()
    } else {
      longVal
    }
  }

  override def toString: String = toBigDecimal.toString()

  def toDebugString: String = {
    if (decimalVal.ne(null)) {
      s"Decimal(expanded, $decimalVal, $precision, $scale)"
    } else {
      s"Decimal(compact, $longVal, $precision, $scale)"
    }
  }

  def toDouble: Double = toBigDecimal.doubleValue

  def toFloat: Float = toBigDecimal.floatValue

  def toLong: Long = {
    if (decimalVal.eq(null)) {
      longVal / POW_10(_scale)
    } else {
      decimalVal.longValue
    }
  }

  def toInt: Int = toLong.toInt

  def toShort: Short = toLong.toShort

  def toByte: Byte = toLong.toByte

  /**
   * @return the Byte value that is equal to the rounded decimal.
   * @throws ArithmeticException if the decimal is too big to fit in Byte type.
   */
  private[sql] def roundToByte(): Byte = {
    if (decimalVal.eq(null)) {
      val actualLongVal = longVal / POW_10(_scale)
      if (actualLongVal == actualLongVal.toByte) {
        actualLongVal.toByte
      } else {
        throw QueryExecutionErrors.castingCauseOverflowError(
          this, DecimalType(this.precision, this.scale), ByteType)
      }
    } else {
      val doubleVal = decimalVal.toDouble
      if (Math.floor(doubleVal) <= Byte.MaxValue && Math.ceil(doubleVal) >= Byte.MinValue) {
        doubleVal.toByte
      } else {
        throw QueryExecutionErrors.castingCauseOverflowError(
          this, DecimalType(this.precision, this.scale), ByteType)
      }
    }
  }

  /**
   * @return the Short value that is equal to the rounded decimal.
   * @throws ArithmeticException if the decimal is too big to fit in Short type.
   */
  private[sql] def roundToShort(): Short = {
    if (decimalVal.eq(null)) {
      val actualLongVal = longVal / POW_10(_scale)
      if (actualLongVal == actualLongVal.toShort) {
        actualLongVal.toShort
      } else {
        throw QueryExecutionErrors.castingCauseOverflowError(
          this, DecimalType(this.precision, this.scale), ShortType)
      }
    } else {
      val doubleVal = decimalVal.toDouble
      if (Math.floor(doubleVal) <= Short.MaxValue && Math.ceil(doubleVal) >= Short.MinValue) {
        doubleVal.toShort
      } else {
        throw QueryExecutionErrors.castingCauseOverflowError(
          this, DecimalType(this.precision, this.scale), ShortType)
      }
    }
  }

  /**
   * @return the Int value that is equal to the rounded decimal.
   * @throws ArithmeticException if the decimal too big to fit in Int type.
   */
  private[sql] def roundToInt(): Int = {
    if (decimalVal.eq(null)) {
      val actualLongVal = longVal / POW_10(_scale)
      if (actualLongVal == actualLongVal.toInt) {
        actualLongVal.toInt
      } else {
        throw QueryExecutionErrors.castingCauseOverflowError(
          this, DecimalType(this.precision, this.scale), IntegerType)
      }
    } else {
      val doubleVal = decimalVal.toDouble
      if (Math.floor(doubleVal) <= Int.MaxValue && Math.ceil(doubleVal) >= Int.MinValue) {
        doubleVal.toInt
      } else {
        throw QueryExecutionErrors.castingCauseOverflowError(
          this, DecimalType(this.precision, this.scale), IntegerType)
      }
    }
  }

  /**
   * @return the Long value that is equal to the rounded decimal.
   * @throws ArithmeticException if the decimal too big to fit in Long type.
   */
  private[sql] def roundToLong(): Long = {
    if (decimalVal.eq(null)) {
      longVal / POW_10(_scale)
    } else {
      try {
        // We cannot store Long.MAX_VALUE as a Double without losing precision.
        // Here we simply convert the decimal to `BigInteger` and use the method
        // `longValueExact` to make sure the range check is accurate.
        decimalVal.bigDecimal.toBigInteger.longValueExact()
      } catch {
        case _: ArithmeticException =>
          throw QueryExecutionErrors.castingCauseOverflowError(
            this, DecimalType(this.precision, this.scale), LongType)
      }
    }
  }

  /**
   * Update precision and scale while keeping our value the same, and return true if successful.
   *
   * @return true if successful, false if overflow would occur
   */
  def changePrecision(precision: Int, scale: Int): Boolean = {
    changePrecision(precision, scale, ROUND_HALF_UP)
  }

  /**
   * Create new `Decimal` with given precision and scale.
   *
   * @return a non-null `Decimal` value if successful. Otherwise, if `nullOnOverflow` is true, null
   *         is returned; if `nullOnOverflow` is false, an `ArithmeticException` is thrown.
   */
  private[sql] def toPrecision(
      precision: Int,
      scale: Int,
      roundMode: BigDecimal.RoundingMode.Value = ROUND_HALF_UP,
      nullOnOverflow: Boolean = true,
      context: String = ""): Decimal = {
    val copy = clone()
    if (copy.changePrecision(precision, scale, roundMode)) {
      copy
    } else {
      if (nullOnOverflow) {
        null
      } else {
        throw QueryExecutionErrors.cannotChangeDecimalPrecisionError(
          this, precision, scale, context)
      }
    }
  }

  /**
   * Update precision and scale while keeping our value the same, and return true if successful.
   *
   * @return true if successful, false if overflow would occur
   */
  private[sql] def changePrecision(
      precision: Int,
      scale: Int,
      roundMode: BigDecimal.RoundingMode.Value): Boolean = {
    // fast path for UnsafeProjection
    if (precision == this.precision && scale == this.scale) {
      return true
    }
    DecimalType.checkNegativeScale(scale)
    // First, update our longVal if we can, or transfer over to using a BigDecimal
    if (decimalVal.eq(null)) {
      if (scale < _scale) {
        // Easier case: we just need to divide our scale down
        val diff = _scale - scale
        val pow10diff = POW_10(diff)
        // % and / always round to 0
        val droppedDigits = longVal % pow10diff
        longVal /= pow10diff
        roundMode match {
          case ROUND_FLOOR =>
            if (droppedDigits < 0) {
              longVal += -1L
            }
          case ROUND_CEILING =>
            if (droppedDigits > 0) {
              longVal += 1L
            }
          case ROUND_HALF_UP =>
            if (math.abs(droppedDigits) * 2 >= pow10diff) {
              longVal += (if (droppedDigits < 0) -1L else 1L)
            }
          case ROUND_HALF_EVEN =>
            val doubled = math.abs(droppedDigits) * 2
            if (doubled > pow10diff || doubled == pow10diff && longVal % 2 != 0) {
              longVal += (if (droppedDigits < 0) -1L else 1L)
            }
          case _ =>
            throw QueryExecutionErrors.unsupportedRoundingMode(roundMode)
        }
      } else if (scale > _scale) {
        // We might be able to multiply longVal by a power of 10 and not overflow, but if not,
        // switch to using a BigDecimal
        val diff = scale - _scale
        val p = POW_10(math.max(MAX_LONG_DIGITS - diff, 0))
        if (diff <= MAX_LONG_DIGITS && longVal > -p && longVal < p) {
          // Multiplying longVal by POW_10(diff) will still keep it below MAX_LONG_DIGITS
          longVal *= POW_10(diff)
        } else {
          // Give up on using Longs; switch to BigDecimal, which we'll modify below
          decimalVal = BigDecimal(longVal, _scale)
        }
      }
      // In both cases, we will check whether our precision is okay below
    }

    if (decimalVal.ne(null)) {
      // We get here if either we started with a BigDecimal, or we switched to one because we would
      // have overflowed our Long; in either case we must rescale decimalVal to the new scale.
      val newVal = decimalVal.setScale(scale, roundMode)
      if (newVal.precision > precision) {
        return false
      }
      decimalVal = newVal
    } else {
      // We're still using Longs, but we should check whether we match the new precision
      val p = POW_10(math.min(precision, MAX_LONG_DIGITS))
      if (longVal <= -p || longVal >= p) {
        // Note that we shouldn't have been able to fix this by switching to BigDecimal
        return false
      }
    }

    _precision = precision
    _scale = scale
    true
  }

  override def clone(): Decimal = new Decimal().set(this)

  override def compare(other: Decimal): Int = {
    if (decimalVal.eq(null) && other.decimalVal.eq(null) && _scale == other._scale) {
      if (longVal < other.longVal) -1 else if (longVal == other.longVal) 0 else 1
    } else {
      toBigDecimal.compare(other.toBigDecimal)
    }
  }

  override def equals(other: Any): Boolean = other match {
    case d: Decimal =>
      compare(d) == 0
    case _ =>
      false
  }

  override def hashCode(): Int = toBigDecimal.hashCode()

  def isZero: Boolean = if (decimalVal.ne(null)) decimalVal == BIG_DEC_ZERO else longVal == 0

  def + (that: Decimal): Decimal = {
    if (decimalVal.eq(null) && that.decimalVal.eq(null) && scale == that.scale) {
      Decimal(longVal + that.longVal, Math.max(precision, that.precision), scale)
    } else {
      Decimal(toBigDecimal.bigDecimal.add(that.toBigDecimal.bigDecimal))
    }
  }

  def - (that: Decimal): Decimal = {
    if (decimalVal.eq(null) && that.decimalVal.eq(null) && scale == that.scale) {
      Decimal(longVal - that.longVal, Math.max(precision, that.precision), scale)
    } else {
      Decimal(toBigDecimal.bigDecimal.subtract(that.toBigDecimal.bigDecimal))
    }
  }

  // TypeCoercion will take care of the precision, scale of result
  def * (that: Decimal): Decimal =
    Decimal(toJavaBigDecimal.multiply(that.toJavaBigDecimal, MATH_CONTEXT))

  def / (that: Decimal): Decimal =
    if (that.isZero) null else Decimal(toJavaBigDecimal.divide(that.toJavaBigDecimal, MATH_CONTEXT))

  def % (that: Decimal): Decimal =
    if (that.isZero) null
    else Decimal(toJavaBigDecimal.remainder(that.toJavaBigDecimal, MATH_CONTEXT))

  def quot(that: Decimal): Decimal =
    if (that.isZero) null
    else Decimal(toJavaBigDecimal.divideToIntegralValue(that.toJavaBigDecimal, MATH_CONTEXT))

  def remainder(that: Decimal): Decimal = this % that

  def unary_- : Decimal = {
    if (decimalVal.ne(null)) {
      Decimal(-decimalVal, precision, scale)
    } else {
      Decimal(-longVal, precision, scale)
    }
  }

  def abs: Decimal = if (this.compare(Decimal.ZERO) < 0) this.unary_- else this

  def floor: Decimal = if (scale == 0) this else {
    val newPrecision = DecimalType.bounded(precision - scale + 1, 0).precision
    toPrecision(newPrecision, 0, ROUND_FLOOR, nullOnOverflow = false)
  }

  def ceil: Decimal = if (scale == 0) this else {
    val newPrecision = DecimalType.bounded(precision - scale + 1, 0).precision
    toPrecision(newPrecision, 0, ROUND_CEILING, nullOnOverflow = false)
  }
}

@Unstable
object Decimal {
  val ROUND_HALF_UP = BigDecimal.RoundingMode.HALF_UP
  val ROUND_HALF_EVEN = BigDecimal.RoundingMode.HALF_EVEN
  val ROUND_CEILING = BigDecimal.RoundingMode.CEILING
  val ROUND_FLOOR = BigDecimal.RoundingMode.FLOOR

  /** Maximum number of decimal digits an Int can represent */
  val MAX_INT_DIGITS = 9

  /** Maximum number of decimal digits a Long can represent */
  val MAX_LONG_DIGITS = 18

  val POW_10 = Array.tabulate[Long](MAX_LONG_DIGITS + 1)(i => math.pow(10, i).toLong)

  private val BIG_DEC_ZERO = BigDecimal(0)

  private val MATH_CONTEXT = new MathContext(DecimalType.MAX_PRECISION, RoundingMode.HALF_UP)

  private[sql] val ZERO = Decimal(0)
  private[sql] val ONE = Decimal(1)

  def apply(value: Double): Decimal = new Decimal().set(value)

  def apply(value: Long): Decimal = new Decimal().set(value)

  def apply(value: Int): Decimal = new Decimal().set(value)

  def apply(value: BigDecimal): Decimal = new Decimal().set(value)

  def apply(value: java.math.BigDecimal): Decimal = new Decimal().set(value)

  def apply(value: java.math.BigInteger): Decimal = new Decimal().set(value)

  def apply(value: scala.math.BigInt): Decimal = new Decimal().set(value.bigInteger)

  def apply(value: BigDecimal, precision: Int, scale: Int): Decimal =
    new Decimal().set(value, precision, scale)

  def apply(value: java.math.BigDecimal, precision: Int, scale: Int): Decimal =
    new Decimal().set(value, precision, scale)

  def apply(unscaled: Long, precision: Int, scale: Int): Decimal =
    new Decimal().set(unscaled, precision, scale)

  def apply(value: String): Decimal = new Decimal().set(BigDecimal(value))

  // This is used for RowEncoder to handle Decimal inside external row.
  def fromDecimal(value: Any): Decimal = {
    value match {
      case j: java.math.BigDecimal => apply(j)
      case d: BigDecimal => apply(d)
      case k: scala.math.BigInt => apply(k)
      case l: java.math.BigInteger => apply(l)
      case d: Decimal => d
    }
  }

  private def numDigitsInIntegralPart(bigDecimal: JavaBigDecimal): Int = {
      bigDecimal.precision - bigDecimal.scale
  }

  private def stringToJavaBigDecimal(str: UTF8String): JavaBigDecimal = {
    // According the benchmark test,  `s.toString.trim` is much faster than `s.trim.toString`.
    // Please refer to https://github.com/apache/spark/pull/26640
    new JavaBigDecimal(str.toString.trim)
  }

  def fromString(str: UTF8String): Decimal = {
    try {
      val bigDecimal = stringToJavaBigDecimal(str)
      // We fast fail because constructing a very large JavaBigDecimal to Decimal is very slow.
      // For example: Decimal("6.0790316E+25569151")
      if (numDigitsInIntegralPart(bigDecimal) > DecimalType.MAX_PRECISION &&
          !SQLConf.get.allowNegativeScaleOfDecimalEnabled) {
        null
      } else {
        Decimal(bigDecimal)
      }
    } catch {
      case _: NumberFormatException =>
        null
    }
  }

  def fromStringANSI(
      str: UTF8String,
      to: DecimalType = DecimalType.USER_DEFAULT,
      errorContext: String = ""): Decimal = {
    try {
      val bigDecimal = stringToJavaBigDecimal(str)
      // We fast fail because constructing a very large JavaBigDecimal to Decimal is very slow.
      // For example: Decimal("6.0790316E+25569151")
      if (numDigitsInIntegralPart(bigDecimal) > DecimalType.MAX_PRECISION &&
          !SQLConf.get.allowNegativeScaleOfDecimalEnabled) {
        throw QueryExecutionErrors.outOfDecimalTypeRangeError(str)
      } else {
        Decimal(bigDecimal)
      }
    } catch {
      case _: NumberFormatException =>
        throw QueryExecutionErrors.invalidInputInCastToNumberError(to, str, errorContext)
    }
  }

  /**
   * Creates a decimal from unscaled, precision and scale without checking the bounds.
   */
  def createUnsafe(unscaled: Long, precision: Int, scale: Int): Decimal = {
    DecimalType.checkNegativeScale(scale)
    val dec = new Decimal()
    dec.longVal = unscaled
    dec._precision = precision
    dec._scale = scale
    dec
  }

  // Max precision of a decimal value stored in `numBytes` bytes
  def maxPrecisionForBytes(numBytes: Int): Int = {
    Math.round(                               // convert double to long
      Math.floor(Math.log10(                  // number of base-10 digits
        Math.pow(2, 8 * numBytes - 1) - 1)))  // max value stored in numBytes
      .asInstanceOf[Int]
  }

  // Returns the minimum number of bytes needed to store a decimal with a given `precision`.
  lazy val minBytesForPrecision = Array.tabulate[Int](39)(computeMinBytesForPrecision)

  private def computeMinBytesForPrecision(precision : Int) : Int = {
    var numBytes = 1
    while (math.pow(2.0, 8 * numBytes - 1) < math.pow(10.0, precision)) {
      numBytes += 1
    }
    numBytes
  }

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
    // Added from Scala 2.13; don't override to work in 2.12
    // TODO revisit once Scala 2.12 support is dropped
    def parseString(str: String): Option[Decimal] = Try(Decimal(str)).toOption
  }

  /** A [[scala.math.Fractional]] evidence parameter for Decimals. */
  private[sql] object DecimalIsFractional extends DecimalIsConflicted with Fractional[Decimal] {
    override def div(x: Decimal, y: Decimal): Decimal = x / y
  }

  /** A [[scala.math.Integral]] evidence parameter for Decimals. */
  private[sql] object DecimalAsIfIntegral extends DecimalIsConflicted with Integral[Decimal] {
    override def quot(x: Decimal, y: Decimal): Decimal = x quot y
    override def rem(x: Decimal, y: Decimal): Decimal = x % y
  }
}
