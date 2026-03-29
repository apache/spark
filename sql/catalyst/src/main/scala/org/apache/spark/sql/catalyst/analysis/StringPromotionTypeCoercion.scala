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

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.PromoteStrings.conf
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.findCommonTypeForBinaryComparison
import org.apache.spark.sql.catalyst.expressions.{
  BinaryArithmetic,
  BinaryComparison,
  Cast,
  Equality,
  Expression,
  Literal
}
import org.apache.spark.sql.types.{
  AnsiIntervalType,
  CalendarIntervalType,
  DataType,
  DoubleType,
  NullType,
  StringTypeExpression,
  TimestampType,
  TimestampTypeExpression
}

/**
 * Type coercion helper that matches against [[BinaryArithmetic]] and [[BinaryComparison]]
 * expressions in order to type coerce children to a wider type when one of the children is a
 * string.
 */
object StringPromotionTypeCoercion {

  def apply(expression: Expression): Expression = expression match {
    case a @ BinaryArithmetic(left @ StringTypeExpression(), right)
        if !isIntervalType(right.dataType) =>
      a.withNewChildren(Seq(Cast(left, DoubleType), right))
    case a @ BinaryArithmetic(left, right @ StringTypeExpression())
        if !isIntervalType(left.dataType) =>
      a.withNewChildren(Seq(left, Cast(right, DoubleType)))

    // For equality between string and timestamp we cast the string to a timestamp
    // so that things like rounding of subsecond precision does not affect the comparison.
    case p @ Equality(left @ StringTypeExpression(), right @ TimestampTypeExpression()) =>
      p.withNewChildren(Seq(Cast(left, TimestampType), right))
    case p @ Equality(left @ TimestampTypeExpression(), right @ StringTypeExpression()) =>
      p.withNewChildren(Seq(left, Cast(right, TimestampType)))

    case p @ BinaryComparison(left, right)
        if findCommonTypeForBinaryComparison(left.dataType, right.dataType, conf).isDefined =>
      val commonType = findCommonTypeForBinaryComparison(left.dataType, right.dataType, conf).get
      p.withNewChildren(Seq(castExpr(left, commonType), castExpr(right, commonType)))

    case other => other
  }

  private def castExpr(expr: Expression, targetType: DataType): Expression = {
    (expr.dataType, targetType) match {
      case (NullType, dt) => Literal.create(null, targetType)
      case (l, dt) if (l != dt) => Cast(expr, targetType)
      case _ => expr
    }
  }

  private def isIntervalType(dt: DataType): Boolean = dt match {
    case _: CalendarIntervalType | _: AnsiIntervalType => true
    case _ => false
  }
}
