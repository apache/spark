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

import scala.math.{Fractional, Numeric}
import scala.reflect.runtime.universe.typeTag
import scala.util.Try

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalDoubleType}
import org.apache.spark.sql.catalyst.util.SQLOrderingUtil

/**
 * The data type representing `Double` values. Please use the singleton `DataTypes.DoubleType`.
 *
 * @since 1.3.0
 */
@Stable
class DoubleType private() extends FractionalType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "DoubleType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Double
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val numeric = implicitly[Numeric[Double]]
  private[sql] val fractional = implicitly[Fractional[Double]]
  private[sql] val ordering =
    (x: Double, y: Double) => SQLOrderingUtil.compareDoubles(x, y)
  private[sql] val asIntegral = DoubleType.DoubleAsIfIntegral

  override private[sql] def exactNumeric = DoubleExactNumeric

  /**
   * The default size of a value of the DoubleType is 8 bytes.
   */
  override def defaultSize: Int = 8

  override def physicalDataType: PhysicalDataType = PhysicalDoubleType

  private[spark] override def asNullable: DoubleType = this
}

/**
 * @since 1.3.0
 */
@Stable
case object DoubleType extends DoubleType {

  // Traits below copied from Scala 2.12; not present in 2.13
  // TODO: SPARK-30011 revisit once Scala 2.12 support is dropped
  trait DoubleIsConflicted extends Numeric[Double] {
    def plus(x: Double, y: Double): Double = x + y
    def minus(x: Double, y: Double): Double = x - y
    def times(x: Double, y: Double): Double = x * y
    def negate(x: Double): Double = -x
    def fromInt(x: Int): Double = x.toDouble
    def toInt(x: Double): Int = x.toInt
    def toLong(x: Double): Long = x.toLong
    def toFloat(x: Double): Float = x.toFloat
    def toDouble(x: Double): Double = x
    // logic in Numeric base trait mishandles abs(-0.0)
    override def abs(x: Double): Double = math.abs(x)
    // Added from Scala 2.13; don't override to work in 2.12
    def parseString(str: String): Option[Double] =
      Try(java.lang.Double.parseDouble(str)).toOption

  }

  trait DoubleAsIfIntegral extends DoubleIsConflicted with Integral[Double] {
    def quot(x: Double, y: Double): Double = (BigDecimal(x) quot BigDecimal(y)).doubleValue
    def rem(x: Double, y: Double): Double = (BigDecimal(x) remainder BigDecimal(y)).doubleValue
  }

  object DoubleAsIfIntegral extends DoubleAsIfIntegral {
    override def compare(x: Double, y: Double): Int = java.lang.Double.compare(x, y)
  }
}
