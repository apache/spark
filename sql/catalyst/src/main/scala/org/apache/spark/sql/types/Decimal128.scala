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

import java.lang.{Long => JavaLong}
import java.math.{BigDecimal => JavaBigDecimal, BigInteger}

import scala.math.BigDecimal.RoundingMode
import scala.util.Try

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.Int128Math
import org.apache.spark.unsafe.types.UTF8String

/**
 * A mutable implementation of Decimal that can hold a Long if values are small enough.
 * Otherwise, hold an Int128.
 *
 * The semantics of the fields are as follows:
 * - _precision and _scale represent the SQL precision and scale we are looking for
 * - If int128 is set, the decimal value is int128.toBigInteger / (10 ** _scale)
 * - Otherwise, the decimal value is longVal / (10 ** _scale)
 *
 * Note, for values between -1.0 and 1.0, precision digits are only counted after dot.
 */
@Unstable
final class Decimal128 extends Ordered[Decimal128] with Serializable {
  import org.apache.spark.sql.types.Decimal128._

  private var int128Val: Int128 = null
  private var longVal: Long = 0L
  private var _precision: Int = 1
  private var _scale: Int = 0

  def high: Long = if (int128Val.eq(null)) longVal >> 63 else int128Val.high
  def low: Long = if (int128Val.eq(null)) longVal else int128Val.low
  def precision: Int = _precision
  def scale: Int = _scale

  /**
   * Set this Decimal128 to the given Long. Will have precision 20 and scale 0.
   */
  def set(longVal: Long): Decimal128 = {
    if (longVal <= -POW_10(MAX_LONG_DIGITS) || longVal >= POW_10(MAX_LONG_DIGITS)) {
      // We can't represent this compactly as a long without risking overflow
      this.int128Val = Int128(longVal)
      this.longVal = 0L
    } else {
      this.int128Val = null
      this.longVal = longVal
    }
    this._precision = 20
    this._scale = 0
    this
  }

  /**
   * Set this Decimal128 to the given Int. Will have precision 10 and scale 0.
   */
  def set(intVal: Int): Decimal128 = {
    this.int128Val = null
    this.longVal = intVal
    this._precision = 10
    this._scale = 0
    this
  }

  /**
   * Set this Decimal128 to the given Int128, with a given precision and scale.
   */
  def set(high: Long, low: Long, precision: Int, scale: Int): Decimal128 = {
    checkOverflow(high, low, s"Construct Int128($high, $low) instance.")
    set(Int128(high, low), precision, scale)
  }

  /**
   * Set this Decimal128 to the given Int128, with a given precision and scale.
   */
  def set(int128: Int128, precision: Int, scale: Int): Decimal128 = {
    DecimalType.checkNegativeScale(scale)
    this.int128Val = int128
    this.longVal = 0
    this._precision = precision
    this._scale = scale
    this
  }

  /**
   * Set this Decimal128 to the given unscaled Long, with a given precision and scale.
   */
  def set(unscaled: Long, precision: Int, scale: Int): Decimal128 = {
    if (setOrNull(unscaled, precision, scale) == null) {
      throw QueryExecutionErrors.unscaledValueTooLargeForPrecisionError(
        this.toJavaBigDecimal, precision, scale)
    }
    this
  }

  /**
   * Set this Decimal128 to the given unscaled Long, with a given precision and scale,
   * and return it, or return null if it cannot be set due to overflow.
   */
  def setOrNull(unscaled: Long, precision: Int, scale: Int): Decimal128 = {
    DecimalType.checkNegativeScale(scale)
    if (unscaled <= -POW_10(MAX_LONG_DIGITS) || unscaled >= POW_10(MAX_LONG_DIGITS)) {
      // We can't represent this compactly as a long without risking overflow
      if (precision < 19) {
        return null  // Requested precision is too low to represent this value
      }
      this.int128Val = Int128(unscaled)
      this.longVal = 0L
    } else {
      val p = POW_10(math.min(precision, MAX_LONG_DIGITS))
      if (unscaled <= -p || unscaled >= p) {
        return null  // Requested precision is too low to represent this value
      }
      this.int128Val = null
      this.longVal = unscaled
    }
    this._precision = precision
    this._scale = scale
    this
  }

  /**
   * Set this Decimal128 to the given BigDecimal value, with a given scale.
   */
  def set(decimal: BigDecimal, precision: Int, scale: Int): Decimal128 = {
    DecimalType.checkNegativeScale(scale)
    val scaledDecimal = decimal.setScale(scale, RoundingMode.HALF_UP)
    if (scaledDecimal.precision > precision) {
      throw QueryExecutionErrors.decimalPrecisionExceedsMaxPrecisionError(
        scaledDecimal.precision, precision)
    }
    this.int128Val = Int128(scaledDecimal.underlying().unscaledValue())
    this.longVal = 0L
    this._precision = precision
    this._scale = scale
    this
  }

  /**
   * Set this Decimal128 to the given BigDecimal value, inheriting its precision and scale.
   */
  def set(decimal: BigDecimal): Decimal128 = {
    this.longVal = 0L
    if (decimal.precision < decimal.scale) {
      // For Decimal128, we expect the precision is equal to or large than the scale, however,
      // in BigDecimal, the digit count starts from the leftmost nonzero digit of the exact
      // result. For example, the precision of 0.01 equals to 1 based on the definition, but
      // the scale is 2. The expected precision should be 2.
      this.int128Val = Int128(decimal.underlying().unscaledValue())
      this._precision = decimal.scale
      this._scale = decimal.scale
    } else if (decimal.scale < 0 && !SQLConf.get.allowNegativeScaleOfDecimalEnabled) {
      this._precision = decimal.precision - decimal.scale
      this._scale = 0
      // set scale to 0 to correct unscaled value
      this.int128Val = Int128(decimal.setScale(0).underlying().unscaledValue())
    } else {
      this.int128Val = Int128(decimal.underlying().unscaledValue())
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
  def set(bigInteger: BigInteger): Decimal128 = {
    try {
      this.int128Val = null
      this.longVal = bigInteger.longValueExact()
      this._precision = DecimalType.MAX_PRECISION
      this._scale = 0
      this
    } catch {
      case _: ArithmeticException =>
        set(Int128(bigInteger), DecimalType.MAX_PRECISION, 0)
    }
  }

  /**
   * Set this Decimal128 to the given Decimal128 value.
   */
  def set(decimal: Decimal128): Decimal128 = {
    this.int128Val = decimal.int128Val
    this.longVal = decimal.longVal
    this._precision = decimal._precision
    this._scale = decimal._scale
    this
  }

  def toBigDecimal: BigDecimal = {
    if (int128Val.ne(null)) {
      BigDecimal(int128Val.toBigInteger, _scale)
    } else {
      BigDecimal(longVal, _scale)
    }
  }

  def toJavaBigDecimal: java.math.BigDecimal = {
    if (int128Val.ne(null)) {
      new java.math.BigDecimal(int128Val.toBigInteger, _scale)
    } else {
      java.math.BigDecimal.valueOf(longVal, _scale)
    }
  }

  def toScalaBigInt: BigInt = if (int128Val.ne(null)) {
    toBigDecimal.toBigInt
  } else {
    BigInt(actualLongVal)
  }

  def toJavaBigInteger: java.math.BigInteger = if (int128Val.ne(null)) {
    toJavaBigDecimal.toBigInteger()
  } else {
    java.math.BigInteger.valueOf(actualLongVal)
  }

  def toUnscaledLong: Long = {
    if (int128Val.ne(null)) {
      toJavaBigDecimal.unscaledValue().longValueExact()
    } else {
      longVal
    }
  }

  def isPositive: Boolean = int128Val.isPositive()

  def isNegative: Boolean = int128Val.isNegative()

  override def toString: String = toBigDecimal.toString()

  def toPlainString: String = toJavaBigDecimal.toPlainString

  def toDebugString: String = {
    if (int128Val.ne(null)) {
      s"Decimal128(expanded, $int128Val, $precision, $scale)"
    } else {
      s"Decimal128(compact, $longVal, $precision, $scale)"
    }
  }

  def toDouble: Double = toBigDecimal.doubleValue

  def toFloat: Float = toBigDecimal.floatValue

  private def actualLongVal: Long = longVal / POW_10(_scale)

  def toLong: Long = if (int128Val.eq(null)) {
    actualLongVal
  } else {
    toBigDecimal.longValue
  }

  def toInt: Int = toLong.toInt

  /**
   * @return the Byte value that is equal to the rounded decimal128.
   * @throws ArithmeticException if the decimal128 is too big to fit in Byte type.
   */
  private[sql] def roundToByte(): Byte =
    roundToNumeric[Byte](ByteType, Byte.MaxValue, Byte.MinValue) (_.toByte) (_.toByte)

  /**
   * @return the Short value that is equal to the rounded decimal128.
   * @throws ArithmeticException if the decimal128 is too big to fit in Short type.
   */
  private[sql] def roundToShort(): Short =
    roundToNumeric[Short](ShortType, Short.MaxValue, Short.MinValue) (_.toShort) (_.toShort)

  /**
   * @return the Int value that is equal to the rounded decimal128.
   * @throws ArithmeticException if the decimal128 too big to fit in Int type.
   */
  private[sql] def roundToInt(): Int =
    roundToNumeric[Int](IntegerType, Int.MaxValue, Int.MinValue) (_.toInt) (_.toInt)

  private def roundToNumeric[T <: AnyVal](integralType: IntegralType, maxValue: Int, minValue: Int)
    (f1: Long => T) (f2: Double => T): T = {
    if (int128Val.eq(null)) {
      val actualLongVal = longVal / POW_10(_scale)
      val numericVal = f1(actualLongVal)
      if (actualLongVal == numericVal) {
        numericVal
      } else {
        throw QueryExecutionErrors.castingCauseOverflowError(
          this, DecimalType(this.precision, this.scale), integralType)
      }
    } else {
      val doubleVal = this.toDouble
      if (Math.floor(doubleVal) <= maxValue && Math.ceil(doubleVal) >= minValue) {
        f2(doubleVal)
      } else {
        throw QueryExecutionErrors.castingCauseOverflowError(
          this, DecimalType(this.precision, this.scale), integralType)
      }
    }
  }

  /**
   * @return the Long value that is equal to the rounded decimal128.
   * @throws ArithmeticException if the decimal128 too big to fit in Long type.
   */
  private[sql] def roundToLong(): Long = {
    if (int128Val.eq(null)) {
      longVal / POW_10(_scale)
    } else {
      try {
        // We cannot store Long.MAX_VALUE as a Double without losing precision.
        // Here we simply convert the decimal to `BigInteger` and use the method
        // `longValueExact` to make sure the range check is accurate.
        toJavaBigDecimal.toBigInteger.longValueExact()
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
   * Create new `Decimal128` with given precision and scale.
   *
   * @return a non-null `Decimal128` value if successful. Otherwise, if `nullOnOverflow` is true,
   *         null is returned; if `nullOnOverflow` is false, an `ArithmeticException` is thrown.
   */
  private[sql] def toPrecision(
      precision: Int,
      scale: Int,
      roundMode: BigDecimal.RoundingMode.Value = ROUND_HALF_UP,
      nullOnOverflow: Boolean = true,
      context: SQLQueryContext = null): Decimal128 = {
    val copy = clone()
    if (copy.changePrecision(precision, scale, roundMode)) {
      copy
    } else {
      if (nullOnOverflow) {
        null
      } else {
        throw QueryExecutionErrors.cannotChangeDecimal128PrecisionError(
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
    var lv = longVal
    var iv = int128Val
    // First, update our lv if we can, or transfer over to using a BigDecimal
    if (iv.eq(null)) {
      if (scale < _scale) {
        // Easier case: we just need to divide our scale down
        val diff = _scale - scale
        // If diff is greater than max number of digits we store in Long, then
        // value becomes 0. Otherwise we calculate new value dividing by power of 10.
        // In both cases we apply rounding after that.
        if (diff > MAX_LONG_DIGITS) {
          lv = roundMode match {
            case ROUND_FLOOR => if (lv < 0) -1L else 0L
            case ROUND_CEILING => if (lv > 0) 1L else 0L
            case ROUND_HALF_UP | ROUND_HALF_EVEN => 0L
            case _ => throw QueryExecutionErrors.unsupportedRoundingMode(roundMode)
          }
        } else {
          val pow10diff = POW_10(diff)
          // % and / always round to 0
          val droppedDigits = lv % pow10diff
          lv /= pow10diff
          roundMode match {
            case ROUND_FLOOR =>
              if (droppedDigits < 0) {
                lv += -1L
              }
            case ROUND_CEILING =>
              if (droppedDigits > 0) {
                lv += 1L
              }
            case ROUND_HALF_UP =>
              if (math.abs(droppedDigits) * 2 >= pow10diff) {
                lv += (if (droppedDigits < 0) -1L else 1L)
              }
            case ROUND_HALF_EVEN =>
              val doubled = math.abs(droppedDigits) * 2
              if (doubled > pow10diff || doubled == pow10diff && lv % 2 != 0) {
                lv += (if (droppedDigits < 0) -1L else 1L)
              }
            case _ =>
              throw QueryExecutionErrors.unsupportedRoundingMode(roundMode)
          }
        }
      } else if (scale > _scale) {
        // We might be able to multiply lv by a power of 10 and not overflow, but if not,
        // switch to using a BigDecimal
        val diff = scale - _scale
        val p = POW_10(math.max(MAX_LONG_DIGITS - diff, 0))
        if (diff <= MAX_LONG_DIGITS && lv > -p && lv < p) {
          // Multiplying lv by POW_10(diff) will still keep it below MAX_LONG_DIGITS
          lv *= POW_10(diff)
        } else {
          // Give up on using Longs; switch to BigDecimal, which we'll modify below
          iv = Int128(lv)
        }
      }
      // In both cases, we will check whether our precision is okay below
    }

    if (iv.ne(null)) {
      // We get here if either we started with a BigDecimal, or we switched to one because we would
      // have overflowed our Long; in either case we must rescale dv to the new scale.
      if (!rescale(precision, scale, roundMode)) {
        return false
      }
      iv = int128Val
    } else {
      // We're still using Longs, but we should check whether we match the new precision
      val p = POW_10(math.min(precision, MAX_LONG_DIGITS))
      if (lv <= -p || lv >= p) {
        // Note that we shouldn't have been able to fix this by switching to BigDecimal
        return false
      }
    }
    int128Val = iv
    longVal = lv
    _precision = precision
    _scale = scale
    true
  }

  def rescale(precision: Int, scale: Int, roundMode: BigDecimal.RoundingMode.Value): Boolean = {
    val newDecimalVal = toJavaBigDecimal.setScale(scale, roundMode)
    if (newDecimalVal.precision > precision) {
      return false
    }

    if (roundMode == ROUND_CEILING || roundMode == ROUND_FLOOR) {
      set(newDecimalVal)
    } else {
      try {
        val rescale = scale - _scale
        val (newLeftHigh, newLeftLow) = Int128Math.rescale(this.high, this.low, rescale)
        this.int128Val = Int128(newLeftHigh, newLeftLow)
      } catch {
        case _: IllegalArgumentException =>
          set(newDecimalVal)
      }
    }

    true
  }

  override def compare(other: Decimal128): Int = {
    if (this.int128Val.eq(null) && other.int128Val.eq(null) && this._scale == other._scale) {
      if (this.longVal < other.longVal) -1 else if (this.longVal == other.longVal) 0 else 1
    } else {
      val (rescale, rescaleLeft) = if (this._scale > other.scale) {
        (this._scale - other.scale, false)
      } else if (this._scale < other.scale) {
        (other.scale - this._scale, true)
      } else {
        (0, false)
      }
      if (rescale == 0) {
        Int128.compare(this.high, this.low, other.high, other.low)
      } else {
        operatorWithRescale(
          this.high, this.low, other.high, other.low, rescale, rescaleLeft) (Int128.compare)
      }
    }
  }

  override def clone(): Decimal128 = new Decimal128().set(this)

  override def equals(other: Any): Boolean = other match {
    case d: Decimal128 =>
      compare(d) == 0
    case _ =>
      false
  }

  override def hashCode(): Int = toBigDecimal.hashCode()

  def isZero: Boolean = if (this.int128Val.ne(null)) this.int128Val.isZero() else this.longVal == 0

  def + (that: Decimal128): Decimal128 = {
    val resultScale = math.max(this._scale, that._scale)
    val resultPrecision = math.min(DecimalType.MAX_PRECISION, resultScale +
      math.max(this._precision - this._scale, that._precision - that._scale) + 1)

    val leftRescaleFactor = Int128Math.rescaleFactor(this._scale, that._scale)
    val rightRescaleFactor = Int128Math.rescaleFactor(that._scale, this._scale)

    if (this.int128Val.eq(null) && that.int128Val.eq(null)) {
      val newLongVal = this.longVal * Int128Math.longTenToNth(leftRescaleFactor) +
        that.longVal * Int128Math.longTenToNth(rightRescaleFactor)
      Decimal128(newLongVal, resultPrecision, resultScale)
    } else {
      val (newHigh, newLow) = if (leftRescaleFactor == 0 && rightRescaleFactor == 0) {
        Int128Math.add(this.high, this.low, that.high, that.low)
      } else if (leftRescaleFactor == 0) {
        operatorWithRescale(
          this.high, this.low, that.high, that.low, rightRescaleFactor, false) (Int128Math.add)
      } else {
        operatorWithRescale(
          this.high, this.low, that.high, that.low, leftRescaleFactor, true)(Int128Math.add)
      }

      checkOverflow(newHigh, newLow, "Decimal128 addition.")

      Decimal128(Int128(newHigh, newLow), resultPrecision, resultScale)
    }
  }

  def - (that: Decimal128): Decimal128 = {
    val resultScale = math.max(this._scale, that._scale)
    val resultPrecision = math.min(DecimalType.MAX_PRECISION, resultScale +
      math.max(this._precision - this._scale, that._precision - that._scale) + 1)

    val leftRescaleFactor = Int128Math.rescaleFactor(this._scale, that._scale)
    val rightRescaleFactor = Int128Math.rescaleFactor(that._scale, this._scale)

    if (this.int128Val.eq(null) && that.int128Val.eq(null)) {
      val newLongVal = this.longVal * Int128Math.longTenToNth(leftRescaleFactor) -
        that.longVal * Int128Math.longTenToNth(rightRescaleFactor)

      Decimal128(newLongVal, resultPrecision, resultScale)
    } else {
      val (newHigh, newLow) = if (leftRescaleFactor == 0 && rightRescaleFactor == 0) {
        Int128Math.subtract(this.high, this.low, that.high, that.low)
      } else if (leftRescaleFactor == 0) {
        operatorWithRescale(
          this.high, this.low, that.high, that.low, rightRescaleFactor, false)(Int128Math.subtract)
      } else {
        operatorWithRescale(
          this.high, this.low, that.high, that.low, leftRescaleFactor, true)(Int128Math.subtract)
      }

      checkOverflow(newHigh, newLow, "Decimal128 subtract.")

      Decimal128(Int128(newHigh, newLow), resultPrecision, resultScale)
    }
  }

  def * (that: Decimal128): Decimal128 = {
    val resultScale = this._scale + that._scale
    val resultPrecision = math.min(DecimalType.MAX_PRECISION, this._precision + that._precision)

    if (this.int128Val.eq(null) && that.int128Val.eq(null)) {
      val newLongVal = this.longVal * that.longVal
      Decimal128(newLongVal, resultPrecision, resultScale)
    } else {
      val (newHigh, newLow) = Int128Math.multiply(this.high, this.low, that.high, that.low)

      checkOverflow(newHigh, newLow, "Decimal128 multiply.")

      Decimal128(Int128(newHigh, newLow), resultPrecision, resultScale)
    }
  }

  def / (that: Decimal128): Decimal128 = if (that.isZero) {
    null
  } else if (this.isZero) {
    Decimal128.ZERO
  } else {
    val resultScale = math.max(this._scale, that._scale)
    val resultPrecision = math.min(DecimalType.MAX_PRECISION,
      this._precision + that._scale + math.max(that._scale - this._scale, 0))
    val rescaleFactor = resultScale - this._scale + that._scale

    if (this.int128Val.eq(null) && that.int128Val.eq(null)) {
      val resultSignum = this.longVal.signum * that.longVal.signum
      val unsignedLeft = this.longVal.abs
      val unsignedRight = that.longVal.abs
      val rescaledUnsignedLeft = unsignedLeft * Int128Math.longTenToNth(rescaleFactor)
      var quotient = rescaledUnsignedLeft / unsignedRight
      val remainder = rescaledUnsignedLeft - (quotient * unsignedRight)

      if (JavaLong.compareUnsigned(remainder * 2, unsignedRight) >= 0) {
        quotient += 1
      }

      Decimal128(resultSignum * quotient, resultPrecision, resultScale)
    } else {
      val (newHigh, newLow) = try {
        Int128Math.divideRoundUp(this.high, this.low, that.high, that.low, rescaleFactor, 0)
      } catch {
        case _: ArithmeticException =>
          throw overflowError("Decimal128 division.")
      }

      checkOverflow(newHigh, newLow, "Decimal128 division.")

      if (newHigh == (newLow >> 63)) {
        Decimal128(newLow, resultPrecision, resultScale)
      } else {
        Decimal128(Int128(newHigh, newLow), resultPrecision, resultScale)
      }
    }
  }

  def % (that: Decimal128): Decimal128 = if (that.isZero) {
    null
  } else if (this.isZero) {
    Decimal128.ZERO
  } else {
    val resultScale = math.max(this._scale, that._scale)
    val resultPrecision =
      math.min(this._precision - this._scale, that._precision - that._scale) + resultScale

    val leftRescaleFactor = Int128Math.rescaleFactor(this._scale, that._scale)
    val rightRescaleFactor = Int128Math.rescaleFactor(that._scale, this._scale)

    val (newHigh, newLow) = try {
      Int128Math.remainder(
        this.high, this.low, that.high, that.low, leftRescaleFactor, rightRescaleFactor, false)
    } catch {
      case _: ArithmeticException =>
        throw overflowError(s"Get remainder of ${this.int128Val} divide ${that.int128Val}.")
    }

    if (this.int128Val.eq(null) && that.int128Val.eq(null)) {
      Decimal128(newLow, resultPrecision, resultScale)
    } else if (newHigh == (newLow >> 63)) {
      Decimal128(newLow, resultPrecision, resultScale)
    } else {
      Decimal128(Int128(newHigh, newLow), resultPrecision, resultScale)
    }
  }

  def quot(that: Decimal128): Decimal128 = {
    if (that.isZero) {
      null
    } else {
      val resultScale = 0
      val resultPrecision = this._precision - this._scale + that.scale + resultScale

      val leftRescaleFactor = Int128Math.rescaleFactor(this._scale, that.scale)
      val rightRescaleFactor = Int128Math.rescaleFactor(that.scale, this._scale)
      val (newHigh, newLow) = try {
        Int128Math.remainder(
          this.high, this.low, that.high, that.low, leftRescaleFactor, rightRescaleFactor, true)
      } catch {
        case _: ArithmeticException =>
          throw overflowError(s"Get quotient of ${this.int128Val} divide ${that.int128Val}.")
      }

      if (this.int128Val.eq(null) || that.int128Val.eq(null)) {
        if (newHigh == (newLow >> 63)) {
          Decimal128(newLow, resultPrecision, resultScale)
        } else {
          Decimal128(Int128(newHigh, newLow), resultPrecision, resultScale)
        }
      } else {
        Decimal128(Int128(newHigh, newLow), resultPrecision, resultScale)
      }
    }
  }

  def unary_- : Decimal128 = if (int128Val.ne(null)) {
    Decimal128(-int128Val, this._precision, this._scale)
  } else {
    Decimal128(-longVal, this._precision, this._scale)
  }

  def abs: Decimal128 = if (this < Decimal128.ZERO) this.unary_- else this

  def floor: Decimal128 = if (scale == 0) this else {
    val newPrecision = DecimalType.bounded(precision - scale + 1, 0).precision
    toPrecision(newPrecision, 0, ROUND_FLOOR, nullOnOverflow = false)
  }

  def ceil: Decimal128 = if (scale == 0) this else {
    val newPrecision = DecimalType.bounded(precision - scale + 1, 0).precision
    toPrecision(newPrecision, 0, ROUND_CEILING, nullOnOverflow = false)
  }
}

@Unstable
object Decimal128 {
  val ROUND_HALF_UP = BigDecimal.RoundingMode.HALF_UP
  val ROUND_HALF_EVEN = BigDecimal.RoundingMode.HALF_EVEN
  val ROUND_CEILING = BigDecimal.RoundingMode.CEILING
  val ROUND_FLOOR = BigDecimal.RoundingMode.FLOOR

  /** Maximum number of decimal digits a Long can represent */
  val MAX_LONG_DIGITS = 18

  val POW_10 = Array.tabulate[Long](MAX_LONG_DIGITS + 1)(i => math.pow(10, i).toLong)

  private[sql] val ZERO = Decimal128(0)
  private[sql] val ONE = Decimal128(1)

  def apply(value: Double): Decimal128 = new Decimal128().set(value)

  def apply(value: Long): Decimal128 = new Decimal128().set(value)

  def apply(value: Int): Decimal128 = new Decimal128().set(value)

  def apply(value: BigDecimal): Decimal128 = new Decimal128().set(value)

  def apply(value: java.math.BigDecimal): Decimal128 = new Decimal128().set(value)

  def apply(value: java.math.BigInteger): Decimal128 = new Decimal128().set(value)

  def apply(value: scala.math.BigInt): Decimal128 = new Decimal128().set(value.bigInteger)

  def apply(value: BigDecimal, precision: Int, scale: Int): Decimal128 =
    new Decimal128().set(value, precision, scale)

  def apply(value: java.math.BigDecimal, precision: Int, scale: Int): Decimal128 =
    new Decimal128().set(value, precision, scale)

  def apply(high: Long, low: Long, precision: Int, scale: Int): Decimal128 =
    new Decimal128().set(high, low, precision, scale)

  def apply(int128: Int128, precision: Int, scale: Int): Decimal128 =
    new Decimal128().set(int128, precision, scale)

  def apply(unscaled: Long, precision: Int, scale: Int): Decimal128 =
    new Decimal128().set(unscaled, precision, scale)

  def apply(value: String): Decimal128 = new Decimal128().set(BigDecimal(value))

  def operatorWithRescale[T](
      leftHigh: Long,
      leftLow: Long,
      rightHigh: Long,
      rightLow: Long,
      rescale: Int,
      rescaleLeft: Boolean) (f: (Long, Long, Long, Long) => T): T = {
    if (rescaleLeft) {
      val (newLeftHigh, newLeftLow) = Int128Math.rescale(leftHigh, leftLow, rescale)
      f(newLeftHigh, newLeftLow, rightHigh, rightLow)
    } else {
      val (newRightHigh, newRightLow) = Int128Math.rescale(rightHigh, rightLow, rescale)
      f(leftHigh, leftLow, newRightHigh, newRightLow)
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

  // This is used for RowEncoder to handle Decimal inside external row.
  def fromDecimal128(value: Any): Decimal128 = {
    value match {
      case j: java.math.BigDecimal => apply(j)
      case d: BigDecimal => apply(d)
      case k: scala.math.BigInt => apply(k)
      case l: java.math.BigInteger => apply(l)
      case d: Decimal128 => d
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

  def fromString(str: UTF8String): Decimal128 = {
    try {
      val bigDecimal = stringToJavaBigDecimal(str)
      // We fast fail because constructing a very large JavaBigDecimal to Decimal is very slow.
      // For example: Decimal("6.0790316E+25569151")
      if (numDigitsInIntegralPart(bigDecimal) > DecimalType.MAX_PRECISION &&
        !SQLConf.get.allowNegativeScaleOfDecimalEnabled) {
        null
      } else {
        Decimal128(bigDecimal)
      }
    } catch {
      case _: NumberFormatException =>
        null
    }
  }

  def fromStringANSI(
      str: UTF8String,
      to: DecimalType = DecimalType.USER_DEFAULT,
      context: SQLQueryContext = null): Decimal128 = {
    try {
      val bigDecimal = stringToJavaBigDecimal(str)
      // We fast fail because constructing a very large JavaBigDecimal to Decimal is very slow.
      // For example: Decimal("6.0790316E+25569151")
      if (numDigitsInIntegralPart(bigDecimal) > DecimalType.MAX_PRECISION &&
        !SQLConf.get.allowNegativeScaleOfDecimalEnabled) {
        throw QueryExecutionErrors.outOfDecimalTypeRangeError(str)
      } else {
        Decimal128(bigDecimal)
      }
    } catch {
      case _: NumberFormatException =>
        throw QueryExecutionErrors.invalidInputInCastToNumberError(to, str, context)
    }
  }

  /**
   * Creates a decimal128 from unscaled, precision and scale without checking the bounds.
   */
  def createUnsafe(unscaled: Long, precision: Int, scale: Int): Decimal128 = {
    DecimalType.checkNegativeScale(scale)
    val dec = new Decimal128()
    dec.longVal = unscaled
    dec._precision = precision
    dec._scale = scale
    dec
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

  /** A [[scala.math.Integral]] evidence parameter for Decimal128s. */
  private[sql] object Decimal128AsIfIntegral
    extends Decimal128IsConflicted with Integral[Decimal128] {
    override def quot(x: Decimal128, y: Decimal128): Decimal128 = x quot y
    override def rem(x: Decimal128, y: Decimal128): Decimal128 = x % y
  }

}
