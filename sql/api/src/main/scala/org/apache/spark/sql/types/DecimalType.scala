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

import java.util.Locale

import scala.annotation.tailrec

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.errors.DataTypeErrors
import org.apache.spark.sql.internal.SqlApiConf

/**
 * The data type representing `java.math.BigDecimal` values. A Decimal that must have fixed
 * precision (the maximum number of digits) and scale (the number of digits on right side of dot).
 *
 * The precision can be up to 38, scale can also be up to 38 (less or equal to precision).
 *
 * The default precision and scale is (10, 0).
 *
 * Please use `DataTypes.createDecimalType()` to create a specific instance.
 *
 * @since 1.3.0
 */
@Stable
case class DecimalType(precision: Int, scale: Int) extends FractionalType {

  DecimalType.checkNegativeScale(scale)

  if (scale > precision) {
    throw DataTypeErrors.decimalCannotGreaterThanPrecisionError(scale, precision)
  }

  if (precision > DecimalType.MAX_PRECISION) {
    throw DataTypeErrors.decimalPrecisionExceedsMaxPrecisionError(
      precision,
      DecimalType.MAX_PRECISION)
  }

  // default constructor for Java
  def this(precision: Int) = this(precision, 0)
  def this() = this(10)

  override def typeName: String = s"decimal($precision,$scale)"

  override def toString: String = s"DecimalType($precision,$scale)"

  override def sql: String = typeName.toUpperCase(Locale.ROOT)

  /**
   * Returns whether this DecimalType is wider than `other`. If yes, it means `other` can be
   * casted into `this` safely without losing any precision or range.
   */
  private[sql] def isWiderThan(other: DataType): Boolean = isWiderThanInternal(other)

  @tailrec
  private def isWiderThanInternal(other: DataType): Boolean = other match {
    case dt: DecimalType =>
      (precision - scale) >= (dt.precision - dt.scale) && scale >= dt.scale
    case dt: IntegralType =>
      isWiderThanInternal(DecimalType.forType(dt))
    case _ => false
  }

  /**
   * Returns whether this DecimalType is tighter than `other`. If yes, it means `this` can be
   * casted into `other` safely without losing any precision or range.
   */
  private[sql] def isTighterThan(other: DataType): Boolean = other match {
    case dt: DecimalType =>
      (precision - scale) <= (dt.precision - dt.scale) && scale <= dt.scale
    case dt: IntegralType =>
      val integerAsDecimal = DecimalType.forType(dt)
      assert(integerAsDecimal.scale == 0)
      // If the precision equals `integerAsDecimal.precision`, there can be integer overflow
      // during casting.
      precision < integerAsDecimal.precision && scale == 0
    case _ => false
  }

  /**
   * The default size of a value of the DecimalType is 8 bytes when precision is at most 18, and
   * 16 bytes otherwise.
   */
  override def defaultSize: Int = if (precision <= Decimal.MAX_LONG_DIGITS) 8 else 16

  override def simpleString: String = s"decimal($precision,$scale)"

  private[spark] override def asNullable: DecimalType = this
}

/**
 * Extra factory methods and pattern matchers for Decimals.
 *
 * @since 1.3.0
 */
@Stable
object DecimalType extends AbstractDataType {
  import scala.math.min

  val MAX_PRECISION = 38
  val MAX_SCALE = 38
  val DEFAULT_SCALE = 18
  val SYSTEM_DEFAULT: DecimalType = DecimalType(MAX_PRECISION, DEFAULT_SCALE)
  val USER_DEFAULT: DecimalType = DecimalType(10, 0)
  val MINIMUM_ADJUSTED_SCALE = 6

  // The decimal types compatible with other numeric types
  private[sql] val BooleanDecimal = DecimalType(1, 0)
  private[sql] val ByteDecimal = DecimalType(3, 0)
  private[sql] val ShortDecimal = DecimalType(5, 0)
  private[sql] val IntDecimal = DecimalType(10, 0)
  private[sql] val LongDecimal = DecimalType(20, 0)
  private[sql] val FloatDecimal = DecimalType(14, 7)
  private[sql] val DoubleDecimal = DecimalType(30, 15)
  private[sql] val BigIntDecimal = DecimalType(38, 0)

  private[sql] def forType(dataType: DataType): DecimalType = dataType match {
    case ByteType => ByteDecimal
    case ShortType => ShortDecimal
    case IntegerType => IntDecimal
    case LongType => LongDecimal
    case FloatType => FloatDecimal
    case DoubleType => DoubleDecimal
  }

  private[sql] def fromDecimal(d: Decimal): DecimalType = DecimalType(d.precision, d.scale)

  private[sql] def bounded(precision: Int, scale: Int): DecimalType = {
    DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
  }

  private[sql] def boundedPreferIntegralDigits(precision: Int, scale: Int): DecimalType = {
    if (precision <= MAX_PRECISION) {
      DecimalType(precision, scale)
    } else {
      // If we have to reduce the precision, we should retain the digits in the integral part first,
      // as they are more significant to the value. Here we reduce the scale as well to drop the
      // digits in the fractional part.
      val diff = precision - MAX_PRECISION
      DecimalType(MAX_PRECISION, math.max(0, scale - diff))
    }
  }

  private[sql] def checkNegativeScale(scale: Int): Unit = {
    if (scale < 0 && !SqlApiConf.get.allowNegativeScaleOfDecimalEnabled) {
      throw DataTypeErrors.negativeScaleNotAllowedError(scale)
    }
  }

  /**
   * Scale adjustment implementation is based on Hive's one, which is itself inspired to
   * SQLServer's one. In particular, when a result precision is greater than
   * {@link #MAX_PRECISION} , the corresponding scale is reduced to prevent the integral part of a
   * result from being truncated.
   *
   * This method is used only when `spark.sql.decimalOperations.allowPrecisionLoss` is set to
   * true.
   */
  private[sql] def adjustPrecisionScale(precision: Int, scale: Int): DecimalType = {
    // Assumptions:
    checkNegativeScale(scale)
    assert(precision >= scale)

    if (precision <= MAX_PRECISION) {
      // Adjustment only needed when we exceed max precision
      DecimalType(precision, scale)
    } else if (scale < 0) {
      // Decimal can have negative scale (SPARK-24468). In this case, we cannot allow a precision
      // loss since we would cause a loss of digits in the integer part.
      // In this case, we are likely to meet an overflow.
      DecimalType(MAX_PRECISION, scale)
    } else {
      // Precision/scale exceed maximum precision. Result must be adjusted to MAX_PRECISION.
      val intDigits = precision - scale
      // If original scale is less than MINIMUM_ADJUSTED_SCALE, use original scale value; otherwise
      // preserve at least MINIMUM_ADJUSTED_SCALE fractional digits
      val minScaleValue = Math.min(scale, MINIMUM_ADJUSTED_SCALE)
      // The resulting scale is the maximum between what is available without causing a loss of
      // digits for the integer part of the decimal and the minimum guaranteed scale, which is
      // computed above
      val adjustedScale = Math.max(MAX_PRECISION - intDigits, minScaleValue)

      DecimalType(MAX_PRECISION, adjustedScale)
    }
  }

  override private[sql] def defaultConcreteType: DataType = SYSTEM_DEFAULT

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[DecimalType]
  }

  override private[sql] def simpleString: String = "decimal"

  private[sql] object Fixed {
    def unapply(t: DecimalType): Option[(Int, Int)] = Some((t.precision, t.scale))
  }

  /**
   * Returns if dt is a DecimalType that fits inside an int
   */
  def is32BitDecimalType(dt: DataType): Boolean = {
    dt match {
      case t: DecimalType =>
        t.precision <= Decimal.MAX_INT_DIGITS
      case _ => false
    }
  }

  /**
   * Returns if dt is a DecimalType that fits inside a long
   */
  def is64BitDecimalType(dt: DataType): Boolean = {
    dt match {
      case t: DecimalType =>
        t.precision <= Decimal.MAX_LONG_DIGITS
      case _ => false
    }
  }

  /**
   * Returns if dt is a DecimalType that doesn't fit inside a long
   */
  def isByteArrayDecimalType(dt: DataType): Boolean = {
    dt match {
      case t: DecimalType =>
        t.precision > Decimal.MAX_LONG_DIGITS
      case _ => false
    }
  }

  def unapply(t: DataType): Boolean = t.isInstanceOf[DecimalType]
}
