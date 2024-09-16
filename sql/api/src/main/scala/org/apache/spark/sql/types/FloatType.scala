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

import scala.util.Try

import org.apache.spark.annotation.Stable

/**
 * The data type representing `Float` values. Please use the singleton `DataTypes.FloatType`.
 *
 * @since 1.3.0
 */
@Stable
class FloatType private () extends FractionalType {

  /**
   * The default size of a value of the FloatType is 4 bytes.
   */
  override def defaultSize: Int = 4

  private[spark] override def asNullable: FloatType = this
}

/**
 * @since 1.3.0
 */
@Stable
case object FloatType extends FloatType {

  trait FloatIsConflicted extends Numeric[Float] {
    def plus(x: Float, y: Float): Float = x + y
    def minus(x: Float, y: Float): Float = x - y
    def times(x: Float, y: Float): Float = x * y
    def negate(x: Float): Float = -x
    def fromInt(x: Int): Float = x.toFloat
    def toInt(x: Float): Int = x.toInt
    def toLong(x: Float): Long = x.toLong
    def toFloat(x: Float): Float = x
    def toDouble(x: Float): Double = x.toDouble
    // logic in Numeric base trait mishandles abs(-0.0f)
    override def abs(x: Float): Float = math.abs(x)
    override def parseString(str: String): Option[Float] =
      Try(java.lang.Float.parseFloat(str)).toOption
  }

  trait FloatAsIfIntegral extends FloatIsConflicted with Integral[Float] {
    def quot(x: Float, y: Float): Float = {
      (BigDecimal(x.toDouble) quot BigDecimal(y.toDouble)).floatValue
    }
    def rem(x: Float, y: Float): Float = {
      (BigDecimal(x.toDouble) remainder BigDecimal(y.toDouble)).floatValue
    }
  }

  object FloatAsIfIntegral extends FloatAsIfIntegral {
    override def compare(x: Float, y: Float): Int = java.lang.Float.compare(x, y)
  }
}
