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
import org.apache.spark.sql.types._

/**
 * Rewrite arithmetic filters on an integral-type (e.g., byte, short, int and long)
 * column to its equivalent form, leaving attribute alone in a left side, so that
 * we can push it down to datasources (e.g., Parquet and ORC).
 *
 * For example, this rule can optimize a query as follows:
 * {{{
 *   SELECT * FROM table WHERE i + 3 = 5
 *   ==> SELECT * FROM table WHERE i = 5 - 3
 * }}}
 *
 * Then, the [[ConstantFolding]] rule will further optimize it as follows:
 * {{{
 *   SELECT * FROM table WHERE i = 2
 * }}}
 *
 * Note:
 * 1. This rule supports `Add` and `Subtract` in arithmetic expressions.
 * 2. This rule supports `=`, `>=`, `<=`, `>`, `<`, and `!=` in comparators.
 * 3. This rule supports integral-type (`byte`, `short`, `int`, `long`) only.
 *    It doesn't support `float` or `double` because of precision issues.
  */
object RewriteArithmeticFiltersOnIntegralColumn extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f: Filter =>
      f transformExpressionsUp {
        case e @ BinaryComparison(left: BinaryArithmetic, right: Expression)
            if right.foldable && isDataTypeSafe(left.dataType) =>
          transformLeft(e, left, right)
        case e @ BinaryComparison(left: Expression, right: BinaryArithmetic)
            if left.foldable && isDataTypeSafe(right.dataType) =>
          transformRight(e, left, right)
      }
  }

  private def transformLeft(
      bc: BinaryComparison,
      left: BinaryArithmetic,
      right: Expression): Expression = {
    left match {
      case Add(ar: AttributeReference, e) if e.foldable && isOptSafe(Subtract(right, e)) =>
        bc.makeCopy(Array(ar, Subtract(right, e)))
      case Add(e, ar: AttributeReference) if e.foldable && isOptSafe(Subtract(right, e)) =>
        bc.makeCopy(Array(ar, Subtract(right, e)))
      case Subtract(ar: AttributeReference, e) if e.foldable && isOptSafe(Add(right, e)) =>
        bc.makeCopy(Array(ar, Add(right, e)))
      case Subtract(e, ar: AttributeReference) if e.foldable && isOptSafe(Subtract(e, right)) =>
        bc.makeCopy(Array(Subtract(e, right), ar))
      case _ => bc
    }
  }

  private def transformRight(
      bc: BinaryComparison,
      left: Expression,
      right: BinaryArithmetic): Expression = {
    right match {
      case Add(ar: AttributeReference, e) if e.foldable && isOptSafe(Subtract(left, e)) =>
        bc.makeCopy(Array(Subtract(left, e), ar))
      case Add(e, ar: AttributeReference) if e.foldable && isOptSafe(Subtract(left, e)) =>
        bc.makeCopy(Array(Subtract(left, e), ar))
      case Subtract(ar: AttributeReference, e) if e.foldable && isOptSafe(Add(left, e)) =>
        bc.makeCopy(Array(Add(left, e), ar))
      case Subtract(e, ar: AttributeReference) if e.foldable && isOptSafe(Subtract(e, left)) =>
        bc.makeCopy(Array(ar, Subtract(e, left)))
      case _ => bc
    }
  }

  private def isDataTypeSafe(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType => true
    case _ => false
  }

  private def isOptSafe(e: BinaryArithmetic): Boolean = {
    val leftVal = e.left.eval(EmptyRow)
    val rightVal = e.right.eval(EmptyRow)

    e match {
      case Add(_, _) =>
        e.dataType match {
          case ByteType =>
            isAddSafe(leftVal, rightVal, Byte.MinValue, Byte.MaxValue)
          case ShortType =>
            isAddSafe(leftVal, rightVal, Short.MinValue, Short.MaxValue)
          case IntegerType =>
            isAddSafe(leftVal, rightVal, Int.MinValue, Int.MaxValue)
          case LongType =>
            isAddSafe(leftVal, rightVal, Long.MinValue, Long.MaxValue)
          case _ => false
        }

      case Subtract(_, _) =>
        e.dataType match {
          case ByteType =>
            isSubtractSafe(leftVal, rightVal, Byte.MinValue, Byte.MaxValue)
          case ShortType =>
            isSubtractSafe(leftVal, rightVal, Short.MinValue, Short.MaxValue)
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
