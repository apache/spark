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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

/**
 * Rewrite arithmetic filters on int or long column to its equivalent form,
 * leaving attribute alone in one side, so that we can push it down to
 * parquet or other file format.
 * For example, this rule can optimize
 * {{{
 *   SELECT * FROM table WHERE i + 3 = 5
 * }}}
 * to
 * {{{
 *   SELECT * FROM table WHERE i = 5 - 3
 * }}}
 * when i is Int or Long, and then other rules will further optimize it to
 * {{{
 *   SELECT * FROM table WHERE i = 2
 * }}}
 */
object RewriteArithmeticFiltersOnIntOrLongColumn extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f: Filter =>
      f transformExpressionsUp {
        case e @ BinaryComparison(left: BinaryArithmetic, right: Literal)
            if isDataTypeSafe(left.dataType) =>
          transformLeft(e, left, right)
        case e @ BinaryComparison(left: Literal, right: BinaryArithmetic)
            if isDataTypeSafe(right.dataType) =>
          transformRight(e, left, right)
      }
  }

  private def transformLeft(
      bc: BinaryComparison,
      left: BinaryArithmetic,
      right: Literal): Expression = {
    left match {
      case Add(ar: AttributeReference, lit: Literal) if isOptSafe(Subtract(right, lit)) =>
        bc.makeCopy(Array(ar, Subtract(right, lit)))
      case Add(lit: Literal, ar: AttributeReference) if isOptSafe(Subtract(right, lit)) =>
        bc.makeCopy(Array(ar, Subtract(right, lit)))
      case Subtract(ar: AttributeReference, lit: Literal) if isOptSafe(Add(right, lit)) =>
        bc.makeCopy(Array(ar, Add(right, lit)))
      case Subtract(lit: Literal, ar: AttributeReference) if isOptSafe(Subtract(lit, right)) =>
        bc.makeCopy(Array(Subtract(lit, right), ar))
      case _ => bc
    }
  }

  private def transformRight(
      bc: BinaryComparison,
      left: Literal,
      right: BinaryArithmetic): Expression = {
    right match {
      case Add(ar: AttributeReference, lit: Literal) if isOptSafe(Subtract(left, lit)) =>
        bc.makeCopy(Array(Subtract(left, lit), ar))
      case Add(lit: Literal, ar: AttributeReference) if isOptSafe(Subtract(left, lit)) =>
        bc.makeCopy(Array(Subtract(left, lit), ar))
      case Subtract(ar: AttributeReference, lit: Literal) if isOptSafe(Add(left, lit)) =>
        bc.makeCopy(Array(Add(left, lit), ar))
      case Subtract(lit: Literal, ar: AttributeReference) if isOptSafe(Subtract(lit, left)) =>
        bc.makeCopy(Array(ar, Subtract(lit, left)))
      case _ => bc
    }
  }

  private def isDataTypeSafe(dataType: DataType): Boolean = dataType match {
    case IntegerType | LongType => true
    case _ => false
  }

  private def isOptSafe(e: BinaryArithmetic): Boolean = {
    val leftVal = e.left.eval(EmptyRow)
    val rightVal = e.right.eval(EmptyRow)

    e match {
      case Add(_: Literal, _: Literal) =>
        e.dataType match {
          case IntegerType =>
            isAddSafe(leftVal, rightVal, Int.MinValue, Int.MaxValue)
          case LongType =>
            isAddSafe(leftVal, rightVal, Long.MinValue, Long.MaxValue)
          case _ => false
        }

      case Subtract(_: Literal, _: Literal) =>
        e.dataType match {
          case IntegerType =>
            isSubtractSafe(leftVal, rightVal, Int.MinValue, Int.MaxValue)
          case LongType =>
            isSubtractSafe(leftVal, rightVal, Long.MinValue, Long.MaxValue)
          case _ => false
        }

      case _ => false
    }
  }

  private def isAddSafe[T](left: Any, right: Any, minValue: T, maxValue: T)(
      implicit num: Numeric[T]): Boolean = {
    import num._
    val leftVal = left.asInstanceOf[T]
    val rightVal = right.asInstanceOf[T]
    if (rightVal > zero) {
      leftVal <= maxValue - rightVal
    } else {
      leftVal >= minValue - rightVal
    }
  }

  private def isSubtractSafe[T](left: Any, right: Any, minValue: T, maxValue: T)(
      implicit num: Numeric[T]): Boolean = {
    import num._
    val leftVal = left.asInstanceOf[T]
    val rightVal = right.asInstanceOf[T]
    if (rightVal > zero) {
      leftVal >= minValue + rightVal
    } else {
      leftVal <= maxValue + rightVal
    }
  }
}
