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

import java.math.{MathContext, RoundingMode}

import org.apache.spark.annotation.Unstable

/**
 * A mutable implementation of BigDecimal that hold a `BigDecimal`.
 *
 * The semantics of the fields are as follows:
 * - If decimalVal is set, it represents the whole decimal value
 * - Otherwise, the decimal value is longVal / (10 ** _scale)
 */
@Unstable
class JDKDecimalOperation extends DecimalOperation[JDKDecimalOperation] {
  import org.apache.spark.sql.types.JDKDecimalOperation._

  private var decimalVal: BigDecimal = null

  def newInstance(): JDKDecimalOperation = new JDKDecimalOperation()

  def setUnderlyingValue(longVal: Long): Unit = {
    this.decimalVal = BigDecimal(longVal)
  }

  def setUnderlyingValue(unscaled: Long, scale: Int): Unit = {
    this.decimalVal = BigDecimal(unscaled, scale)
  }

  def setUnderlyingValue(decimalVal: BigDecimal): Unit = {
    this.decimalVal = decimalVal
  }

  def setNullUnderlying(): Unit = {
    this.decimalVal = null
  }

  def underlyingIsNull: Boolean = decimalVal.eq(null)

  def underlyingIsNotNull: Boolean = decimalVal.ne(null)

  def getAsBigDecimal(): BigDecimal = this.decimalVal

  def getAsJavaBigDecimal(): java.math.BigDecimal = this.decimalVal.underlying()

  def getAsJavaBigInteger(): java.math.BigInteger = this.decimalVal.underlying().toBigInteger

  def getAsLongValue: Long = decimalVal.longValue

  def rescale(precision: Int, scale: Int, roundMode: BigDecimal.RoundingMode.Value): Boolean = {
    val newDecimalVal = this.decimalVal.setScale(scale, roundMode)
    if (newDecimalVal.precision > precision) {
      return false
    }
    this.decimalVal = newDecimalVal
    true
  }

  def doCompare(other: DecimalOperation[_]): Int = toBigDecimal.compare(other.toBigDecimal)

  def isEqualsZero(): Boolean = {
    assert(underlyingIsNotNull)
    this.decimalVal.signum == 0
  }

  def doAdd(that: DecimalOperation[_]): JDKDecimalOperation = withNewInstance(this, that) {
    (left, right) => left.add(right)
  }

  def doSubtract(that: DecimalOperation[_]): JDKDecimalOperation = withNewInstance(this, that) {
    (left, right) => left.subtract(right)
  }

  def multiply(that: DecimalOperation[_]): JDKDecimalOperation = withNewInstance(this, that) {
    (left, right) => left.multiply(right, MATH_CONTEXT)
  }

  def divide(that: DecimalOperation[_]): JDKDecimalOperation = withNewInstance(this, that) {
    (left, right) => left.divide(right, DecimalType.MAX_SCALE, MATH_CONTEXT.getRoundingMode)
  }

  def remainder(that: DecimalOperation[_]): JDKDecimalOperation = withNewInstance(this, that) {
    (left, right) => left.remainder(right, MATH_CONTEXT)
  }

  def quot(that: DecimalOperation[_]): JDKDecimalOperation = withNewInstance(this, that) {
    (left, right) => left.divideToIntegralValue(right, MATH_CONTEXT)
  }

  def doNegative: JDKDecimalOperation = {
    val newDecimalOperation = new JDKDecimalOperation()
    newDecimalOperation.set(-this.decimalVal, precision, scale)
    newDecimalOperation
  }

  def copy(from: JDKDecimalOperation): Unit = {
    this.decimalVal = from.decimalVal
  }
}

@Unstable
object JDKDecimalOperation {

  val MATH_CONTEXT = new MathContext(DecimalType.MAX_PRECISION, RoundingMode.HALF_UP)

  def withNewInstance(
      left: JDKDecimalOperation,
      right: DecimalOperation[_])
      (f: (java.math.BigDecimal, java.math.BigDecimal) => BigDecimal): JDKDecimalOperation = {
    val newBigDecimal = f(left.toJavaBigDecimal, right.toJavaBigDecimal)
    val jDKDecimalOperation = new JDKDecimalOperation()
    jDKDecimalOperation.set(newBigDecimal)
    jDKDecimalOperation
  }
}
