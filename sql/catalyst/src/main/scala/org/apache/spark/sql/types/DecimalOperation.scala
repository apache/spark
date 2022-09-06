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

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf

trait DecimalOperation[T <: DecimalOperation[T]] extends Serializable {
  import org.apache.spark.sql.types.Decimal._

  protected var longVal: Long = 0L
  protected var _precision: Int = 1
  protected var _scale: Int = 0

  def precision: Int = _precision
  def scale: Int = _scale

  def newInstance(): T

  /**
   * Set this DecimalOperation to the given Long. Will have precision 20 and scale 0.
   */
  def set(longVal: Long): DecimalOperation[T] = {
    if (longVal <= -POW_10(MAX_LONG_DIGITS) || longVal >= POW_10(MAX_LONG_DIGITS)) {
      // We can't represent this compactly as a long without risking overflow
      setLong(longVal)
      this.longVal = 0L
    } else {
      setNull()
      this.longVal = longVal
    }
    this._precision = 20
    this._scale = 0
    this
  }

  protected def setLong(longVal: Long): Unit

  protected def setLong(unscaled: Long, scale: Int): Unit

  protected def setBigDecimal(decimalVal: BigDecimal): Unit

  protected def setNull(): Unit

  protected def isNull(): Boolean

  protected def isNotNull(): Boolean

  /**
   * Set this DecimalOperation to the given Int. Will have precision 10 and scale 0.
   */
  def set(intVal: Int): DecimalOperation[T] = {
    setNull()
    this.longVal = intVal
    this._precision = 10
    this._scale = 0
    this
  }

  /**
   * Set this DecimalOperation to the given unscaled Long, with a given precision and scale.
   */
  def set(unscaled: Long, precision: Int, scale: Int): DecimalOperation[T] = {
    if (setOrNull(unscaled, precision, scale) == null) {
      throw QueryExecutionErrors.unscaledValueTooLargeForPrecisionError()
    }
    this
  }

  /**
   * Set this DecimalOperation to the given unscaled Long, with a given precision and scale,
   * and return it, or return null if it cannot be set due to overflow.
   */
  def setOrNull(unscaled: Long, precision: Int, scale: Int): DecimalOperation[T] = {
    if (unscaled <= -POW_10(MAX_LONG_DIGITS) || unscaled >= POW_10(MAX_LONG_DIGITS)) {
      // We can't represent this compactly as a long without risking overflow
      if (precision < 19) {
        return null  // Requested precision is too low to represent this value
      }
      setLong(unscaled, scale)
      this.longVal = 0L
    } else {
      val p = POW_10(math.min(precision, MAX_LONG_DIGITS))
      if (unscaled <= -p || unscaled >= p) {
        return null  // Requested precision is too low to represent this value
      }
      setNull()
      this.longVal = unscaled
    }
    this._precision = precision
    this._scale = scale
    this
  }

  /**
   * Set this DecimalOperation to the given BigDecimal value, with a given precision and scale.
   */
  def set(decimal: BigDecimal, precision: Int, scale: Int): DecimalOperation[T] = {
    val scaledDecimal = decimal.setScale(scale, ROUND_HALF_UP)
    if (scaledDecimal.precision > precision) {
      throw QueryExecutionErrors.decimalPrecisionExceedsMaxPrecisionError(
        scaledDecimal.precision, precision)
    }
    setBigDecimal(scaledDecimal)
    this.longVal = 0L
    this._precision = precision
    this._scale = scale
    this
  }

  /**
   * Set this DecimalOperation to the given BigDecimal value, inheriting its precision and scale.
   */
  def set(decimal: BigDecimal): DecimalOperation[T] = {
    setBigDecimal(decimal)
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
      setBigDecimal(decimal.setScale(0))
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
  def set(bigintval: BigInteger): DecimalOperation[T] = {
    try {
      setNull()
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
   * Set this DecimalOperation to the given DecimalOperation value.
   */
  def set(decimalOperation: T): DecimalOperation[T] = {
    copy(decimalOperation)
    this.longVal = decimalOperation.longVal
    this._precision = decimalOperation._precision
    this._scale = decimalOperation._scale
    this
  }

  def toBigDecimal: BigDecimal = if (isNotNull()) {
    getAsBigDecimal()
  } else {
    BigDecimal(longVal, _scale)
  }

  def getAsBigDecimal(): BigDecimal

  def toJavaBigDecimal: java.math.BigDecimal = if (isNotNull()) {
    getAsJavaBigDecimal()
  } else {
    java.math.BigDecimal.valueOf(longVal, _scale)
  }

  protected def getAsJavaBigDecimal(): java.math.BigDecimal

  def toScalaBigInt: BigInt = if (isNotNull()) {
    getAsBigDecimal().toBigInt
  } else {
    BigInt(toLong)
  }

  def toJavaBigInteger: java.math.BigInteger = if (isNotNull()) {
    getAsJavaBigInteger()
  } else {
    java.math.BigInteger.valueOf(toLong)
  }

  protected def getAsJavaBigInteger(): java.math.BigInteger

  def toUnscaledLong: Long = if (isNotNull()) {
    getAsJavaBigDecimal().unscaledValue().longValueExact()
  } else {
    longVal
  }

  def toDebugString: String = if (isNotNull()) {
    s"Decimal(expanded, ${getAsBigDecimal()}, $precision, $scale)"
  } else {
    s"Decimal(compact, $longVal, $precision, $scale)"
  }

  def toLong: Long = if (isNull()) {
    longVal / POW_10(_scale)
  } else {
    getAsBigDecimal().longValue
  }

  def roundToNumeric[T <: AnyVal](
      decimal: Decimal,
      integralType: IntegralType,
      maxValue: Int,
      minValue: Int) (f1: Long => T) (f2: Double => T): T = {
    if (isNull()) {
      val actualLongVal = longVal / POW_10(_scale)
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

  def roundToLong(decimal: Decimal): Long = if (isNull()) {
    longVal / POW_10(_scale)
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
    if (isNull()) {
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
          setLong(lv, _scale)
        }
      }
      // In both cases, we will check whether our precision is okay below
    }

    if (isNotNull()) {
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

  def compare(that: T): Int =
    if (isNull() && that.isNull() && _scale == that._scale) {
      if (longVal < that.longVal) -1 else if (longVal == that.longVal) 0 else 1
    } else {
      doCompare(that)
    }

  protected def doCompare(other: T): Int

  def isZero: Boolean = if (isNotNull()) isEqualsZero() else longVal == 0

  protected def isEqualsZero(): Boolean

  def add(that: T): T = {
    if (isNull() && that.isNull() && _scale == that._scale) {
      val newDecimalOperation = newInstance()
      newDecimalOperation.set(
        longVal + that.longVal, Math.max(precision, that.precision) + 1, scale)
      newDecimalOperation
    } else {
      doAdd(that)
    }
  }

  protected def doAdd(that: T): T

  def subtract(that: T): T = {
    if (isNull() && that.isNull() && _scale == that._scale) {
      val newDecimalOperation = newInstance()
      newDecimalOperation.set(
        longVal - that.longVal, Math.max(precision, that.precision) + 1, scale)
      newDecimalOperation
    } else {
      doSubtract(that)
    }
  }

  protected def doSubtract(that: T): T

  def multiply(that: T): T

  def divide(that: T): T

  def remainder(that: T): T

  def quot(that: T): T

  def negative: T = if (isNotNull()) {
    doNegative
  } else {
    val newDecimalOperation = newInstance()
    newDecimalOperation.set(-longVal, precision, scale)
    newDecimalOperation
  }

  def doNegative: T

  protected def copy(from: T): Unit
}

object DecimalOperation {

  def createDecimalOperation[T <: DecimalOperation[T]](): T =
    if (SQLConf.get.getConf(SQLConf.DECIMAL_OPERATION_IMPLEMENTATION) == "JDKBigDecimal") {
      new JDKDecimalOperation().asInstanceOf[T]
    } else {
      new Decimal128Operation().asInstanceOf[T]
    }
}
