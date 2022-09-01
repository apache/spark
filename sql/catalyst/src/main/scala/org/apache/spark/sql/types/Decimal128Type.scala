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

import scala.annotation.tailrec
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * The data type is a new implement following decimal behavior.
 * A Decimal128 that must have fixed precision (the maximum number of digits) and scale (the number
 * of digits on right side of dot).
 *
 * The precision can be up to 38, scale can also be up to 38 (less or equal to precision).
 *
 * The default precision and scale is (10, 0).
 *
 * Please use `DataTypes.createDecimal128Type()` to create a specific instance.
 *
 * @since 3.4.0
 */
@Unstable
case class Decimal128Type(precision: Int, scale: Int) extends FractionalType {

  Decimal128Type.checkNegativeScale(scale)

  if (scale > precision) {
    throw QueryCompilationErrors.decimalCannotGreaterThanPrecisionError(scale, precision)
  }

  if (precision > Decimal128Type.MAX_PRECISION) {
    throw QueryCompilationErrors.decimalOnlySupportPrecisionUptoError(
      Decimal128Type.simpleString, Decimal128Type.MAX_PRECISION)
  }

  private[sql] type InternalType = Decimal128
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val numeric = Decimal128.Decimal128IsFractional
  private[sql] val fractional = Decimal128.Decimal128IsFractional
  private[sql] val ordering = Decimal128.Decimal128IsFractional
  private[sql] val asIntegral = Decimal128.Decimal128AsIfIntegral

  /**
   * Returns whether this Decimal128Type is wider than `other`. If yes, it means `other`
   * can be casted into `this` safely without losing any precision or range.
   */
  private[sql] def isWiderThan(other: DataType): Boolean = isWiderThanInternal(other)

  @tailrec
  private def isWiderThanInternal(other: DataType): Boolean = other match {
    case dt: Decimal128Type =>
      (precision - scale) >= (dt.precision - dt.scale) && scale >= dt.scale
    case dt: IntegralType =>
      isWiderThanInternal(Decimal128Type.forType(dt))
    case _ => false
  }

  /**
   * Returns whether this Decimal128Type is tighter than `other`. If yes, it means `this`
   * can be casted into `other` safely without losing any precision or range.
   */
  private[sql] def isTighterThan(other: DataType): Boolean = isTighterThanInternal(other)

  @tailrec
  private def isTighterThanInternal(other: DataType): Boolean = other match {
    case dt: Decimal128Type =>
      (precision - scale) <= (dt.precision - dt.scale) && scale <= dt.scale
    case dt: IntegralType =>
      isTighterThanInternal(Decimal128Type.forType(dt))
    case _ => false
  }

  /**
   * The default size of a value of the Decimal128Type is 8 bytes when precision is at most 18,
   * and 16 bytes otherwise.
   */
  override def defaultSize: Int = if (precision <= Decimal128.MAX_LONG_DIGITS) 8 else 16

  override def simpleString: String = s"decimal128($precision,$scale)"

  override private[spark] def asNullable: Decimal128Type = this
}

@Unstable
object Decimal128Type extends AbstractDataType {
  import scala.math.min

  val MAX_PRECISION = 38
  val MAX_SCALE = 38
  val DEFAULT_SCALE = 18
  val SYSTEM_DEFAULT: Decimal128Type = Decimal128Type(MAX_PRECISION, DEFAULT_SCALE)
  val USER_DEFAULT: Decimal128Type = Decimal128Type(10, 0)
  val MINIMUM_ADJUSTED_SCALE = 6

  // The decimal types compatible with other numeric types
  private[sql] val BooleanDecimal = Decimal128Type(1, 0)
  private[sql] val ByteDecimal = Decimal128Type(3, 0)
  private[sql] val ShortDecimal = Decimal128Type(5, 0)
  private[sql] val IntDecimal = Decimal128Type(10, 0)
  private[sql] val LongDecimal = Decimal128Type(20, 0)
  private[sql] val FloatDecimal = Decimal128Type(14, 7)
  private[sql] val DoubleDecimal = Decimal128Type(30, 15)
  private[sql] val BigIntDecimal = Decimal128Type(38, 0)

  private[sql] def forType(dataType: DataType): Decimal128Type = dataType match {
    case ByteType => ByteDecimal
    case ShortType => ShortDecimal
    case IntegerType => IntDecimal
    case LongType => LongDecimal
    case FloatType => FloatDecimal
    case DoubleType => DoubleDecimal
  }

  private[sql] def fromDecimal128(d: Decimal128): Decimal128Type =
    Decimal128Type(d.precision, d.scale)

  private[sql] def bounded(precision: Int, scale: Int): Decimal128Type = {
    Decimal128Type(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
  }

  private[sql] def checkNegativeScale(scale: Int): Unit = {
    if (scale < 0 && !SQLConf.get.allowNegativeScaleOfDecimalEnabled) {
      throw QueryCompilationErrors.negativeScaleNotAllowedError(scale)
    }
  }

  private[sql] def adjustPrecisionScale(precision: Int, scale: Int): Decimal128Type = {
    // Assumptions:
    checkNegativeScale(scale)
    assert(precision >= scale)

    if (precision <= MAX_PRECISION) {
      // Adjustment only needed when we exceed max precision
      Decimal128Type(precision, scale)
    } else if (scale < 0) {
      // Decimal can have negative scale (SPARK-24468). In this case, we cannot allow a precision
      // loss since we would cause a loss of digits in the integer part.
      // In this case, we are likely to meet an overflow.
      Decimal128Type(MAX_PRECISION, scale)
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

      Decimal128Type(MAX_PRECISION, adjustedScale)
    }
  }

  override private[sql] def defaultConcreteType = SYSTEM_DEFAULT

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[Decimal128Type]
  }

  override private[sql] def simpleString: String = "decimal128"

  private[sql] object Fixed {
    def unapply(t: Decimal128Type): Option[(Int, Int)] = Some((t.precision, t.scale))
  }

  def unapply(t: DataType): Boolean = t.isInstanceOf[Decimal128Type]
}
