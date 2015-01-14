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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.decimal.Decimal
import org.apache.spark.sql.types.{DecimalType, LongType, DoubleType, DataType}

/** Return the unscaled Long value of a Decimal, assuming it fits in a Long */
case class UnscaledValue(child: Expression) extends UnaryExpression {
  override type EvaluatedType = Any

  override def dataType: DataType = LongType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"UnscaledValue($child)"

  override def eval(input: Row): Any = {
    val childResult = child.eval(input)
    if (childResult == null) {
      null
    } else {
      childResult.asInstanceOf[Decimal].toUnscaledLong
    }
  }
}

/** Create a Decimal from an unscaled Long value */
case class MakeDecimal(child: Expression, precision: Int, scale: Int) extends UnaryExpression {
  override type EvaluatedType = Decimal

  override def dataType: DataType = DecimalType(precision, scale)
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"MakeDecimal($child,$precision,$scale)"

  override def eval(input: Row): Decimal = {
    val childResult = child.eval(input)
    if (childResult == null) {
      null
    } else {
      new Decimal().setOrNull(childResult.asInstanceOf[Long], precision, scale)
    }
  }
}
