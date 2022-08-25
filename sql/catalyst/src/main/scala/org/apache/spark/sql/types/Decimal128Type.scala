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

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

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
   * The default size of a value of the Decimal128Type, used internally for size estimation.
   */
  override def defaultSize: Int = 16

  override def simpleString: String = s"decimal128($scale)"

  override private[spark] def asNullable: Decimal128Type = this
}

@Stable
object Decimal128Type extends AbstractDataType {

  val MAX_PRECISION = 38
  val DEFAULT_SCALE = 18
  val SYSTEM_DEFAULT: Decimal128Type = Decimal128Type(MAX_PRECISION, DEFAULT_SCALE)
  val USER_DEFAULT: Decimal128Type = Decimal128Type(10, 0)

  private[sql] def checkNegativeScale(scale: Int): Unit = {
    if (scale < 0 && !SQLConf.get.allowNegativeScaleOfDecimalEnabled) {
      throw QueryCompilationErrors.negativeScaleNotAllowedError(scale)
    }
  }

  override private[sql] def defaultConcreteType = SYSTEM_DEFAULT

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[Decimal128Type]
  }

  override private[sql] def simpleString: String = "decimal128"
}
