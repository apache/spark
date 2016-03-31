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

import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.catalyst.expressions.Expression


/** Precision parameters for a Decimal */
@deprecated("Use DecimalType(precision, scale) directly", "1.5")
case class PrecisionInfo(precision: Int, scale: Int) {
  if (scale > precision) {
    throw new AnalysisException(
      s"Decimal scale ($scale) cannot be greater than precision ($precision).")
  }
  if (precision > DecimalType.MAX_PRECISION) {
    throw new AnalysisException(
      s"DecimalType can only support precision up to 38"
    )
  }
}

/**
 * :: DeveloperApi ::
 * The data type representing `java.math.BigDecimal` values.
 * A Decimal that must have fixed precision (the maximum number of digits) and scale (the number
 * of digits on right side of dot).
 *
 * The precision can be up to 38, scale can also be up to 38 (less or equal to precision).
 *
 * The default precision and scale is (10, 0).
 *
 * Please use [[DataTypes.createDecimalType()]] to create a specific instance.
 */
@DeveloperApi
case class DecimalType(precision: Int, scale: Int) extends FractionalType {

  // default constructor for Java
  def this(precision: Int) = this(precision, 0)
  def this() = this(10)

  @deprecated("Use DecimalType(precision, scale) instead", "1.5")
  def this(precisionInfo: Option[PrecisionInfo]) {
    this(precisionInfo.getOrElse(PrecisionInfo(10, 0)).precision,
      precisionInfo.getOrElse(PrecisionInfo(10, 0)).scale)
  }

  @deprecated("Use DecimalType.precision and DecimalType.scale instead", "1.5")
  val precisionInfo = Some(PrecisionInfo(precision, scale))

  private[sql] type InternalType = Decimal
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }
  private[sql] val numeric = Decimal.DecimalIsFractional
  private[sql] val fractional = Decimal.DecimalIsFractional
  private[sql] val ordering = Decimal.DecimalIsFractional
  private[sql] val asIntegral = Decimal.DecimalAsIfIntegral

  override def typeName: String = s"decimal($precision,$scale)"

  override def toString: String = s"DecimalType($precision,$scale)"

  /**
   * Returns whether this DecimalType is wider than `other`. If yes, it means `other`
   * can be casted into `this` safely without losing any precision or range.
   */
  private[sql] def isWiderThan(other: DataType): Boolean = other match {
    case dt: DecimalType =>
      (precision - scale) >= (dt.precision - dt.scale) && scale >= dt.scale
    case dt: IntegralType =>
      isWiderThan(DecimalType.forType(dt))
    case _ => false
  }

  /**
   * Returns whether this DecimalType is tighter than `other`. If yes, it means `this`
   * can be casted into `other` safely without losing any precision or range.
   */
  private[sql] def isTighterThan(other: DataType): Boolean = other match {
    case dt: DecimalType =>
      (precision - scale) <= (dt.precision - dt.scale) && scale <= dt.scale
    case dt: IntegralType =>
      isTighterThan(DecimalType.forType(dt))
    case _ => false
  }

  /**
   * The default size of a value of the DecimalType is 4096 bytes.
   */
  override def defaultSize: Int = 4096

  override def simpleString: String = s"decimal($precision,$scale)"

  private[spark] override def asNullable: DecimalType = this
}


/** Extra factory methods and pattern matchers for Decimals */
object DecimalType extends AbstractDataType {
  import scala.math.min

  val MAX_PRECISION = 38
  val MAX_SCALE = 38
  val SYSTEM_DEFAULT: DecimalType = DecimalType(MAX_PRECISION, 18)
  val USER_DEFAULT: DecimalType = DecimalType(10, 0)

  @deprecated("Does not support unlimited precision, please specify the precision and scale", "1.5")
  val Unlimited: DecimalType = SYSTEM_DEFAULT

  // The decimal types compatible with other numeric types
  private[sql] val ByteDecimal = DecimalType(3, 0)
  private[sql] val ShortDecimal = DecimalType(5, 0)
  private[sql] val IntDecimal = DecimalType(10, 0)
  private[sql] val LongDecimal = DecimalType(20, 0)
  private[sql] val FloatDecimal = DecimalType(14, 7)
  private[sql] val DoubleDecimal = DecimalType(30, 15)

  private[sql] def forType(dataType: DataType): DecimalType = dataType match {
    case ByteType => ByteDecimal
    case ShortType => ShortDecimal
    case IntegerType => IntDecimal
    case LongType => LongDecimal
    case FloatType => FloatDecimal
    case DoubleType => DoubleDecimal
  }

  @deprecated("please specify precision and scale", "1.5")
  def apply(): DecimalType = USER_DEFAULT

  @deprecated("Use DecimalType(precision, scale) instead", "1.5")
  def apply(precisionInfo: Option[PrecisionInfo]) {
    this(precisionInfo.getOrElse(PrecisionInfo(10, 0)).precision,
      precisionInfo.getOrElse(PrecisionInfo(10, 0)).scale)
  }

  private[sql] def bounded(precision: Int, scale: Int): DecimalType = {
    DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
  }

  override private[sql] def defaultConcreteType: DataType = SYSTEM_DEFAULT

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[DecimalType]
  }

  override private[sql] def simpleString: String = "decimal"

  private[sql] object Fixed {
    def unapply(t: DecimalType): Option[(Int, Int)] = Some((t.precision, t.scale))
  }

  private[sql] object Expression {
    def unapply(e: Expression): Option[(Int, Int)] = e.dataType match {
      case t: DecimalType => Some((t.precision, t.scale))
      case _ => None
    }
  }

  def unapply(t: DataType): Boolean = t.isInstanceOf[DecimalType]

  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[DecimalType]
}
