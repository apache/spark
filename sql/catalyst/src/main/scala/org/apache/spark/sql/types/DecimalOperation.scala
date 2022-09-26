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

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.internal.SQLConf

/**
 * The proxy layer of Decimal's underlying implementation.
 * At present, Java BigDecimal is the only underlying implementation.
 *
 * `DecimalOperation` can hold a Long if values are small enough.
 * _precision and _scale represent the SQL precision and scale we are looking for.
 *
 * Note, for values between -1.0 and 1.0, precision digits are only counted after dot.
 */
trait DecimalOperation extends Serializable {

  /**
   * Create a new instance for the subclass of DecimalOperation.
   */
  def newInstance(): DecimalOperation

  /**
   * Set Long as the underlying value.
   */
  def setUnderlyingValue(longVal: Long): Unit

  /**
   * Set unscaled Long and scale as the underlying value.
   */
  def setUnderlyingValue(unscaled: Long, scale: Int): Unit

  /**
   * Set scala.BigDecimal as the underlying value.
   */
  def setUnderlyingValue(decimalVal: BigDecimal): Unit

  /**
   * Whether or not the underlying value is null.
   */
  def underlyingIsNull: Boolean

  /**
   * Whether or not the underlying value is not null.
   */
  def underlyingIsNotNull: Boolean

  /**
   * Converts the underlying value to scala.BigDecimal.
   */
  def getAsBigDecimal(): BigDecimal

  /**
   * Converts the underlying value to java.math.BigDecimal.
   */
  def getAsJavaBigDecimal(): java.math.BigDecimal

  /**
   * Converts the underlying value to java.math.BigInteger.
   */
  def getAsJavaBigInteger(): java.math.BigInteger

  /**
   * Converts the underlying value to Long.
   */
  def getAsLongValue(): Long

  def rescale(
      precision: Int, scale: Int, roundMode: BigDecimal.RoundingMode.Value): Boolean

  override def clone(): DecimalOperation = newInstance().copyUnderlyingValue(this)

  protected def copyUnderlyingValue(from: DecimalOperation): DecimalOperation

  def compare(other: DecimalOperation): Int

  def isEqualsZero(): Boolean

  def add(that: DecimalOperation): DecimalOperation

  def subtract(that: DecimalOperation): DecimalOperation

  def multiply(that: DecimalOperation): DecimalOperation

  def divide(that: DecimalOperation): DecimalOperation

  def remainder(that: DecimalOperation): DecimalOperation

  def quot(that: DecimalOperation): DecimalOperation

  def negative: DecimalOperation
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
}
