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

@Unstable
class JDKDecimalOperation extends DecimalOperation[JDKDecimalOperation] {
  import org.apache.spark.sql.types.JDKDecimalOperation._

  private var decimalVal: BigDecimal = null

  def newInstance(): JDKDecimalOperation = new JDKDecimalOperation()

  def setLong(longVal: Long): Unit = {
    this.decimalVal = BigDecimal(longVal)
  }

  def setLong(unscaled: Long, scale: Int): Unit = {
    this.decimalVal = BigDecimal(unscaled, scale)
  }

  def setBigDecimal(decimalVal: BigDecimal): Unit = {
    this.decimalVal = decimalVal
  }

  def setNull: Unit = {
    this.decimalVal = null
  }

  def isNull(): Boolean = decimalVal.eq(null)

  def isNotNull(): Boolean = decimalVal.ne(null)

  def getAsBigDecimal(): BigDecimal = this.decimalVal

  def getAsJavaBigDecimal(): java.math.BigDecimal = this.decimalVal.underlying()

  def getAsJavaBigInteger(): java.math.BigInteger = this.decimalVal.underlying().toBigInteger

  def rescale(precision: Int, scale: Int, roundMode: BigDecimal.RoundingMode.Value): Boolean = {
    val newDecimalVal = this.decimalVal.setScale(scale, roundMode)
    if (newDecimalVal.precision > precision) {
      return false
    }
    this.decimalVal = newDecimalVal
    true
  }

  def doCompare(other: JDKDecimalOperation): Int = toBigDecimal.compare(other.toBigDecimal)

  def isEqualsZero(): Boolean = this.decimalVal.signum == 0

  def doAdd(that: JDKDecimalOperation): JDKDecimalOperation = {
    val jDKDecimalOperation = new JDKDecimalOperation()
    val newBigDecimal = toJavaBigDecimal.add(that.toJavaBigDecimal)
    jDKDecimalOperation.set(newBigDecimal)
    jDKDecimalOperation
  }

  def doSubtract(that: JDKDecimalOperation): JDKDecimalOperation = {
    val jDKDecimalOperation = new JDKDecimalOperation()
    val newBigDecimal = toJavaBigDecimal.subtract(that.toJavaBigDecimal)
    jDKDecimalOperation.set(newBigDecimal)
    jDKDecimalOperation
  }

  def multiply(that: JDKDecimalOperation): JDKDecimalOperation = withNewInstance(this, that) {
    (left, right) => left.multiply(right, MATH_CONTEXT)
  }

  def divide(that: JDKDecimalOperation): JDKDecimalOperation = withNewInstance(this, that) {
    (left, right) => left.divide(right, DecimalType.MAX_SCALE, MATH_CONTEXT.getRoundingMode)
  }

  def remainder(that: JDKDecimalOperation): JDKDecimalOperation = withNewInstance(this, that) {
    (left, right) => left.remainder(right, MATH_CONTEXT)
  }

  def quot(that: JDKDecimalOperation): JDKDecimalOperation = withNewInstance(this, that) {
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
      right: JDKDecimalOperation)
      (f: (java.math.BigDecimal, java.math.BigDecimal) => BigDecimal): JDKDecimalOperation = {
    val jDKDecimalOperation = new JDKDecimalOperation()
    val newBigDecimal = f(left.toJavaBigDecimal, right.toJavaBigDecimal)
    jDKDecimalOperation.set(newBigDecimal)
    jDKDecimalOperation
  }
}
