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
import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.catalyst.expressions.Expression


/** Precision parameters for a Decimal */
case class PrecisionInfo(precision: Int, scale: Int)


/**
 * :: DeveloperApi ::
 * The data type representing `java.math.BigDecimal` values.
 * A Decimal that might have fixed precision and scale, or unlimited values for these.
 *
 * Please use [[DataTypes.createDecimalType()]] to create a specific instance.
 *
 * @group dataType
 */
@DeveloperApi
case class DecimalType(precisionInfo: Option[PrecisionInfo]) extends FractionalType {

  /** No-arg constructor for kryo. */
  protected def this() = this(null)

  private[sql] type InternalType = Decimal
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }
  private[sql] val numeric = Decimal.DecimalIsFractional
  private[sql] val fractional = Decimal.DecimalIsFractional
  private[sql] val ordering = Decimal.DecimalIsFractional
  private[sql] val asIntegral = Decimal.DecimalAsIfIntegral

  def precision: Int = precisionInfo.map(_.precision).getOrElse(-1)

  def scale: Int = precisionInfo.map(_.scale).getOrElse(-1)

  override def typeName: String = precisionInfo match {
    case Some(PrecisionInfo(precision, scale)) => s"decimal($precision,$scale)"
    case None => "decimal"
  }

  override def toString: String = precisionInfo match {
    case Some(PrecisionInfo(precision, scale)) => s"DecimalType($precision,$scale)"
    case None => "DecimalType()"
  }

  /**
   * The default size of a value of the DecimalType is 4096 bytes.
   */
  override def defaultSize: Int = 4096

  override def simpleString: String = precisionInfo match {
    case Some(PrecisionInfo(precision, scale)) => s"decimal($precision,$scale)"
    case None => "decimal(10,0)"
  }

  private[spark] override def asNullable: DecimalType = this
}


/** Extra factory methods and pattern matchers for Decimals */
object DecimalType {
  val Unlimited: DecimalType = DecimalType(None)

  object Fixed {
    def unapply(t: DecimalType): Option[(Int, Int)] =
      t.precisionInfo.map(p => (p.precision, p.scale))
  }

  object Expression {
    def unapply(e: Expression): Option[(Int, Int)] = e.dataType match {
      case t: DecimalType => t.precisionInfo.map(p => (p.precision, p.scale))
      case _ => None
    }
  }

  def apply(): DecimalType = Unlimited

  def apply(precision: Int, scale: Int): DecimalType =
    DecimalType(Some(PrecisionInfo(precision, scale)))

  def unapply(t: DataType): Boolean = t.isInstanceOf[DecimalType]

  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[DecimalType]

  def isFixed(dataType: DataType): Boolean = dataType match {
    case DecimalType.Fixed(_, _) => true
    case _ => false
  }
}
