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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{
  And,
  Cast,
  EqualNullSafe,
  EqualTo,
  Expression,
  IsNotNull,
  Literal,
  Not
}
import org.apache.spark.sql.types.{
  BooleanTypeExpression,
  Decimal,
  NumericType,
  NumericTypeExpression
}

/**
 * Type coercion helper that matches against [[Equality]] expressions in order to type coerce
 * children from numeric type to boolean.
 */
object BooleanEqualityTypeCoercion {
  private val trueValues = Seq(1.toByte, 1.toShort, 1, 1L, Decimal.ONE)
  private val falseValues = Seq(0.toByte, 0.toShort, 0, 0L, Decimal.ZERO)

  def apply(expression: Expression): Expression = expression match {
    // Hive treats (true = 1) as true and (false = 0) as true,
    // all other cases are considered as false.

    // We may simplify the expression if one side is literal numeric values
    // TODO: Maybe these rules should go into the optimizer.
    case EqualTo(bool @ BooleanTypeExpression(), Literal(value, _: NumericType))
        if trueValues.contains(value) =>
      bool
    case EqualTo(bool @ BooleanTypeExpression(), Literal(value, _: NumericType))
        if falseValues.contains(value) =>
      Not(bool)
    case EqualTo(Literal(value, _: NumericType), bool @ BooleanTypeExpression())
        if trueValues.contains(value) =>
      bool
    case EqualTo(Literal(value, _: NumericType), bool @ BooleanTypeExpression())
        if falseValues.contains(value) =>
      Not(bool)
    case EqualNullSafe(bool @ BooleanTypeExpression(), Literal(value, _: NumericType))
        if trueValues.contains(value) =>
      And(IsNotNull(bool), bool)
    case EqualNullSafe(bool @ BooleanTypeExpression(), Literal(value, _: NumericType))
        if falseValues.contains(value) =>
      And(IsNotNull(bool), Not(bool))
    case EqualNullSafe(Literal(value, _: NumericType), bool @ BooleanTypeExpression())
        if trueValues.contains(value) =>
      And(IsNotNull(bool), bool)
    case EqualNullSafe(Literal(value, _: NumericType), bool @ BooleanTypeExpression())
        if falseValues.contains(value) =>
      And(IsNotNull(bool), Not(bool))

    case EqualTo(left @ BooleanTypeExpression(), right @ NumericTypeExpression()) =>
      EqualTo(Cast(left, right.dataType), right)
    case EqualTo(left @ NumericTypeExpression(), right @ BooleanTypeExpression()) =>
      EqualTo(left, Cast(right, left.dataType))
    case EqualNullSafe(left @ BooleanTypeExpression(), right @ NumericTypeExpression()) =>
      EqualNullSafe(Cast(left, right.dataType), right)
    case EqualNullSafe(left @ NumericTypeExpression(), right @ BooleanTypeExpression()) =>
      EqualNullSafe(left, Cast(right, left.dataType))

    case other => other
  }
}
