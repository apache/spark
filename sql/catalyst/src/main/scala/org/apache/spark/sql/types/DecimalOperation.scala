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

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * The proxy layer of Decimal's underlying implementation.
 * At present, the only underlying implementation supported is Java BigDecimal.
 *
 * `DecimalOperation` can hold a Long if values are small enough.
 * _precision and _scale represent the SQL precision and scale we are looking for.
 *
 * Note, for values between -1.0 and 1.0, precision digits are only counted after dot.
 */
trait DecimalOperation extends Serializable {
  import org.apache.spark.sql.types.Decimal._

  private var longVal: Long = 0L
  private var _precision: Int = 1
  private var _scale: Int = 0

  def precision: Int = _precision
  def scale: Int = _scale

  /**
   * Create a new instance for the subclass of DecimalOperation.
   */
  def newInstance(): DecimalOperation

  /**
   * Set Long as the underlying value.
   */
  protected def setUnderlyingValue(longVal: Long): Unit

  /**
   * Set unscaled Long and scale as the underlying value.
   */
  protected def setUnderlyingValue(unscaled: Long, scale: Int): Unit

  /**
   * Set scala.BigDecimal as the underlying value.
   */
  protected def setUnderlyingValue(decimalVal: BigDecimal): Unit

  /**
   * Set the underlying value to null.
   */
  protected def setNullUnderlying(): Unit

  /**
   * Whether or not the underlying value is null.
   */
  protected def underlyingIsNull: Boolean

  /**
   * Whether or not the underlying value is not null.
   */
  protected def underlyingIsNotNull: Boolean

  /**
   * Set this DecimalOperation to the given Long. Will have precision 20 and scale 0.
   */
  def set(longVal: Long): DecimalOperation = {
    if (longVal <= -POW_10(MAX_LONG_DIGITS) || longVal >= POW_10(MAX_LONG_DIGITS)) {
      // We can't represent this compactly as a long without risking overflow
      setUnderlyingValue(longVal)
      this.longVal = 0L
    } else {
      setNullUnderlying()
      this.longVal = longVal
    }
    this._precision = 20
    this._scale = 0
    this
  }

  /**
   * Set this DecimalOperation to the given Int. Will have precision 10 and scale 0.
   */
  def set(intVal: Int): DecimalOperation = {
    setNullUnderlying()
    this.longVal = intVal
    this._precision = 10
    this._scale = 0
    this
  }

  /**
   * Set this DecimalOperation to the given unscaled Long, with a given precision and scale.
   */
  def set(unscaled: Long, precision: Int, scale: Int): DecimalOperation = {
    if (setOrNull(unscaled, precision, scale) == null) {
      throw QueryExecutionErrors.unscaledValueTooLargeForPrecisionError()
    }
    this
  }

  /**
   * Set this DecimalOperation to the given unscaled Long, with a given precision and scale,
   * and return it, or return null if it cannot be set due to overflow.
   */
  def setOrNull(unscaled: Long, precision: Int, scale: Int): DecimalOperation = {
    if (unscaled <= -POW_10(MAX_LONG_DIGITS) || unscaled >= POW_10(MAX_LONG_DIGITS)) {
      // We can't represent this compactly as a long without risking overflow
      if (precision < 19) {
        return null  // Requested precision is too low to represent this value
      }
      setUnderlyingValue(unscaled, scale)
      this.longVal = 0L
    } else {
      val p = POW_10(math.min(precision, MAX_LONG_DIGITS))
      if (unscaled <= -p || unscaled >= p) {
        return null  // Requested precision is too low to represent this value
      }
      setNullUnderlying()
      this.longVal = unscaled
    }
    this._precision = precision
    this._scale = scale
    this
  }

  /**
   * Set this DecimalOperation to the given BigDecimal value, with a given precision and scale.
   */
  def set(decimal: BigDecimal, precision: Int, scale: Int): DecimalOperation = {
    val scaledDecimal = decimal.setScale(scale, ROUND_HALF_UP)
    if (scaledDecimal.precision > precision) {
      throw QueryExecutionErrors.decimalPrecisionExceedsMaxPrecisionError(
        scaledDecimal.precision, precision)
    }
    setUnderlyingValue(scaledDecimal)
    this.longVal = 0L
    this._precision = precision
    this._scale = scale
    this
  }

  /**
   * Set this DecimalOperation to the given BigDecimal value, inheriting its precision and scale.
   */
  def set(decimal: BigDecimal): DecimalOperation = {
    setUnderlyingValue(decimal)
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
      setUnderlyingValue(decimal.setScale(0))
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
  def set(bigintval: BigInteger): DecimalOperation = {
    try {
      setNullUnderlying()
      this.longVal = bigintval.longValueExact()
      this._precision = DecimalType.MAX_PRECISION
      this._scale = 0
      this
    } catch {
      case _: ArithmeticException =>
        set(BigDecimal(bigintval))
    }
  }

  def toBigDecimal: BigDecimal = if (underlyingIsNotNull) {
    getAsBigDecimal()
  } else {
    BigDecimal(longVal, _scale)
  }

  /**
   * Converts the underlying value to scala.BigDecimal.
   */
  def getAsBigDecimal(): BigDecimal

  def toJavaBigDecimal: java.math.BigDecimal = if (underlyingIsNotNull) {
    getAsJavaBigDecimal()
  } else {
    java.math.BigDecimal.valueOf(longVal, _scale)
  }

  /**
   * Converts the underlying value to java.math.BigDecimal.
   */
  protected def getAsJavaBigDecimal(): java.math.BigDecimal

  def toScalaBigInt: BigInt = if (underlyingIsNotNull) {
    getAsBigDecimal().toBigInt
  } else {
    BigInt(actualLongVal)
  }

  def toJavaBigInteger: java.math.BigInteger = if (underlyingIsNotNull) {
    getAsJavaBigInteger()
  } else {
    java.math.BigInteger.valueOf(actualLongVal)
  }

  /**
   * Converts the underlying value to java.math.BigInteger.
   */
  protected def getAsJavaBigInteger(): java.math.BigInteger

  def toUnscaledLong: Long = if (underlyingIsNotNull) {
    getAsJavaBigDecimal().unscaledValue().longValueExact()
  } else {
    longVal
  }

  def toDebugString: String = if (underlyingIsNotNull) {
    s"Decimal(expanded, ${getAsBigDecimal()}, $precision, $scale)"
  } else {
    s"Decimal(compact, $longVal, $precision, $scale)"
  }

  def toLong: Long = if (underlyingIsNull) {
    actualLongVal
  } else {
    getAsLongValue
  }

  private def actualLongVal: Long = longVal / POW_10(_scale)

  /**
   * Converts the underlying value to Long.
   */
  protected def getAsLongValue: Long

  def roundToNumeric[T <: AnyVal](
      decimal: Decimal,
      integralType: IntegralType,
      maxValue: Int,
      minValue: Int) (f1: Long => T) (f2: Double => T): T = {
    if (underlyingIsNull) {
      val numericVal = f1(actualLongVal)
      if (actualLongVal == numericVal) {
        numericVal
      } else {
        throw QueryExecutionErrors.castingCauseOverflowError(
          decimal, DecimalType(this.precision, this.scale), integralType)
      }
    } else {
      val doubleVal = getAsBigDecimal().toDouble
      if (Math.floor(doubleVal) <= maxValue && Math.ceil(doubleVal) >= minValue) {
        f2(doubleVal)
      } else {
        throw QueryExecutionErrors.castingCauseOverflowError(
          decimal, DecimalType(this.precision, this.scale), integralType)
      }
    }
  }

  def roundToLong(decimal: Decimal): Long = if (underlyingIsNull) {
    actualLongVal
  } else {
    try {
      // We cannot store Long.MAX_VALUE as a Double without losing precision.
      // Here we simply convert the decimal to `BigInteger` and use the method
      // `longValueExact` to make sure the range check is accurate.
      getAsJavaBigDecimal().toBigInteger.longValueExact()
    } catch {
      case _: ArithmeticException =>
        throw QueryExecutionErrors.castingCauseOverflowError(
          decimal, DecimalType(this.precision, this.scale), LongType)
    }
  }

  /**
   * Update precision and scale while keeping our value the same, and return true if successful.
   *
   * @return true if successful, false if overflow would occur
   */
  def changePrecision(
      precision: Int,
      scale: Int,
      roundMode: BigDecimal.RoundingMode.Value): Boolean = {
    // fast path for UnsafeProjection
    if (precision == this.precision && scale == this.scale) {
      return true
    }
    DecimalType.checkNegativeScale(scale)
    var lv = longVal
    // First, update our lv if we can, or transfer over to using a BigDecimal
    if (underlyingIsNull) {
      if (scale < _scale) {
        // Easier case: we just need to divide our scale down
        val diff = _scale - scale
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
          setUnderlyingValue(lv, _scale)
        }
      }
      // In both cases, we will check whether our precision is okay below
    }

    if (underlyingIsNotNull) {
      // We get here if either we started with a BigDecimal, or we switched to one because we would
      // have overflowed our Long; in either case we must rescale dv to the new scale.
      if (!rescale(precision, scale, roundMode)) {
        return false
      }
    } else {
      // We're still using Longs, but we should check whether we match the new precision
      val p = POW_10(math.min(precision, MAX_LONG_DIGITS))
      if (lv <= -p || lv >= p) {
        // Note that we shouldn't have been able to fix this by switching to BigDecimal
        return false
      }
    }
    longVal = lv
    _precision = precision
    _scale = scale
    true
  }

  protected def rescale(
      precision: Int, scale: Int, roundMode: BigDecimal.RoundingMode.Value): Boolean

  override def clone(): DecimalOperation = {
    val newDecimalOperation = newInstance()
    newDecimalOperation.copy(this)
    newDecimalOperation
  }

  def compare(that: DecimalOperation): Int =
    if (underlyingIsNull && that.underlyingIsNull && _scale == that._scale) {
      if (longVal < that.longVal) -1 else if (longVal == that.longVal) 0 else 1
    } else {
      doCompare(that)
    }

  protected def doCompare(other: DecimalOperation): Int

  def isZero: Boolean = if (underlyingIsNotNull) isEqualsZero() else longVal == 0

  protected def isEqualsZero(): Boolean

  def add(that: DecimalOperation): DecimalOperation = {
    if (underlyingIsNull && that.underlyingIsNull && _scale == that._scale) {
      val newDecimalOperation = newInstance()
      newDecimalOperation.set(
        longVal + that.longVal, Math.max(precision, that.precision) + 1, scale)
    } else {
      addUnderlying(that)
    }
  }

  protected def addUnderlying(that: DecimalOperation): DecimalOperation

  def subtract(that: DecimalOperation): DecimalOperation = {
    if (underlyingIsNull && that.underlyingIsNull && _scale == that._scale) {
      val newDecimalOperation = newInstance()
      newDecimalOperation.set(
        longVal - that.longVal, Math.max(precision, that.precision) + 1, scale)
    } else {
      subtractUnderlying(that)
    }
  }

  protected def subtractUnderlying(that: DecimalOperation): DecimalOperation

  def multiply(that: DecimalOperation): DecimalOperation

  def divide(that: DecimalOperation): DecimalOperation

  def remainder(that: DecimalOperation): DecimalOperation

  def quot(that: DecimalOperation): DecimalOperation

  def negative: DecimalOperation = if (underlyingIsNotNull) {
    doNegative
  } else {
    val newDecimalOperation = newInstance()
    newDecimalOperation.set(-longVal, precision, scale)
  }

  def doNegative: DecimalOperation

  def copy(from: DecimalOperation): Unit = {
    this.longVal = from.longVal
    this._precision = from._precision
    this._scale = from._scale
    if (from.underlyingIsNotNull) {
      copyUnderlying(from)
    }
  }

  def copyUnderlying(from: DecimalOperation): Unit
}

@Unstable
object DecimalOperation {

  def createDecimalOperation(): DecimalOperation = {
    SQLConf.get.getConf(SQLConf.DECIMAL_UNDERLYING_IMPLEMENTATION) match {
      case "JDKBigDecimal" => new JDKDecimalOperation()
      // We can using other implementation for Decimal.
      case _ => new JDKDecimalOperation()
    }
  }

  def createUnsafe(unscaled: Long, precision: Int, scale: Int): DecimalOperation = {
    val decimalOperation = createDecimalOperation()
    decimalOperation.longVal = unscaled
    decimalOperation._precision = precision
    decimalOperation._scale = scale
    decimalOperation
  }
}
