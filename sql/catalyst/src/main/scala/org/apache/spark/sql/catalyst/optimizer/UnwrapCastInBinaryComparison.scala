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
import org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

/**
 * Unwrap casts in binary comparison operations with patterns like following:
 *
 * `BinaryComparison(Cast(fromExp, toType), Literal(value, toType))`
 *   or
 * `BinaryComparison(Literal(value, toType), Cast(fromExp, toType))`
 *
 * This rule optimizes expressions with the above pattern by either replacing the cast with simpler
 * constructs, or moving the cast from the expression side to the literal side, which enables them
 * to be optimized away later and pushed down to data sources.
 *
 * Currently this only handles cases where `fromType` (of `fromExp`) and `toType` are of integral
 * types (i.e., byte, short, int and long). The rule checks to see if the literal `value` is
 * within range `(min, max)`, where `min` and `max` are the minimum and maximum value of
 * `fromType`, respectively. If this is true then it means we can safely cast `value` to `fromType`
 * and thus able to move the cast to the literal side.
 *
 * If the `value` is not within range `(min, max)`, the rule breaks the scenario into different
 * cases and try to replace each with simpler constructs.
 *
 * if `value > max`, the cases are of following:
 *  - `cast(fromExp, toType) > value` ==> if(isnull(fromExp), null, false)
 *  - `cast(fromExp, toType) >= value` ==> if(isnull(fromExp), null, false)
 *  - `cast(fromExp, toType) === value` ==> if(isnull(fromExp), null, false)
 *  - `cast(fromExp, toType) <=> value` ==> false (only if `fromExp` is deterministic)
 *  - `cast(fromExp, toType) <= value` ==> if(isnull(fromExp), null, true)
 *  - `cast(fromExp, toType) < value` ==> if(isnull(fromExp), null, true)
 *
 * if `value == max`, the cases are of following:
 *  - `cast(fromExp, toType) > value` ==> if(isnull(fromExp), null, false)
 *  - `cast(fromExp, toType) >= value` ==> fromExp == max
 *  - `cast(fromExp, toType) === value` ==> fromExp == max
 *  - `cast(fromExp, toType) <=> value` ==> fromExp == max
 *  - `cast(fromExp, toType) <= value` ==> if(isnull(fromExp), null, true)
 *  - `cast(fromExp, toType) < value` ==> fromExp =!= max
 *
 * Similarly for the cases when `value == min` and `value < min`.
 *
 * Further, the above `if(isnull(fromExp), null, false)` is represented using conjunction
 * `and(isnull(fromExp), null)`, to enable further optimization and filter pushdown to data sources.
 * Similarly, `if(isnull(fromExp), null, true)` is represented with `or(isnotnull(fromExp), null)`.
 */
object UnwrapCastInBinaryComparison extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case l: LogicalPlan =>
      l transformExpressionsUp {
        case e @ BinaryComparison(_, _) => unwrapCast(e)
      }
  }

  private def unwrapCast(exp: Expression): Expression = exp match {
    // Not a canonical form. In this case we first canonicalize the expression by swapping the
    // literal and cast side, then process the result and swap the literal and cast again to
    // restore the original order.
    case BinaryComparison(Literal(_, toType: IntegralType), Cast(fromExp, _: IntegralType, _))
        if canImplicitlyCast(fromExp, toType) =>
      def swap(e: Expression): Expression = e match {
        case GreaterThan(left, right) => LessThan(right, left)
        case GreaterThanOrEqual(left, right) => LessThanOrEqual(right, left)
        case EqualTo(left, right) => EqualTo(right, left)
        case EqualNullSafe(left, right) => EqualNullSafe(right, left)
        case LessThanOrEqual(left, right) => GreaterThanOrEqual(right, left)
        case LessThan(left, right) => GreaterThan(right, left)
        case _ => e
      }
      swap(unwrapCast(swap(exp)))

    // In case both sides have integral type, optimize the comparison by removing casts or
    // moving cast to the literal side.
    case be @ BinaryComparison(
      Cast(fromExp, toType: IntegralType, _), Literal(value, _: IntegralType))
        if canImplicitlyCast(fromExp, toType) =>
      simplifyIntegral(be, fromExp, toType, value)

    case _ => exp
  }

  /**
   * Check if the input `value` is within range `(min, max)` of the `fromType`, where `min` and
   * `max` are the minimum and maximum value of the `fromType`. If the above is true, this
   * optimizes the expression by moving the cast to the literal side. Otherwise if result is not
   * true, this replaces the input binary comparison `exp` with simpler expressions.
   */
  private def simplifyIntegral(
      exp: BinaryComparison,
      fromExp: Expression,
      toType: IntegralType,
      value: Any): Expression = {

    val fromType = fromExp.dataType
    val (min, max) = getRange(fromType)
    val (minInToType, maxInToType) = {
      (Cast(Literal(min), toType).eval(), Cast(Literal(max), toType).eval())
    }
    val ordering = toType.ordering.asInstanceOf[Ordering[Any]]
    val minCmp = ordering.compare(value, minInToType)
    val maxCmp = ordering.compare(value, maxInToType)

    if (maxCmp > 0) {
      exp match {
        case EqualTo(_, _) | GreaterThan(_, _) | GreaterThanOrEqual(_, _) =>
          fromExp.falseIfNotNull
        case LessThan(_, _) | LessThanOrEqual(_, _) =>
          fromExp.trueIfNotNull
        // make sure the expression is evaluated if it is non-deterministic
        case EqualNullSafe(_, _) if exp.deterministic =>
          FalseLiteral
        case _ => exp // impossible but safe guard, same below
      }
    } else if (maxCmp == 0) {
      exp match {
        case GreaterThan(_, _) =>
          fromExp.falseIfNotNull
        case LessThanOrEqual(_, _) =>
          fromExp.trueIfNotNull
        case LessThan(_, _) =>
          Not(EqualTo(fromExp, Literal(max, fromType)))
        case GreaterThanOrEqual(_, _) | EqualTo(_, _) | EqualNullSafe(_, _) =>
          EqualTo(fromExp, Literal(max, fromType))
        case _ => exp
      }
    } else if (minCmp < 0) {
      exp match {
        case GreaterThan(_, _) | GreaterThanOrEqual(_, _) =>
          fromExp.trueIfNotNull
        case LessThan(_, _) | LessThanOrEqual(_, _) | EqualTo(_, _) =>
          fromExp.falseIfNotNull
        // make sure the expression is evaluated if it is non-deterministic
        case EqualNullSafe(_, _) if exp.deterministic =>
          FalseLiteral
        case _ => exp
      }
    } else if (minCmp == 0) {
      exp match {
        case LessThan(_, _) =>
          fromExp.falseIfNotNull
        case GreaterThanOrEqual(_, _) =>
          fromExp.trueIfNotNull
        case GreaterThan(_, _) =>
          Not(EqualTo(fromExp, Literal(min, fromType)))
        case LessThanOrEqual(_, _) | EqualTo(_, _) | EqualNullSafe(_, _) =>
          EqualTo(fromExp, Literal(min, fromType))
        case _ => exp
      }
    } else {
      // This means `value` is within range `(min, max)`. Optimize this by moving the cast to the
      // literal side.
      val lit = Cast(Literal(value), fromType)
      exp match {
        case GreaterThan(_, _) => GreaterThan(fromExp, lit)
        case GreaterThanOrEqual(_, _) => GreaterThanOrEqual(fromExp, lit)
        case EqualTo(_, _) => EqualTo(fromExp, lit)
        case EqualNullSafe(_, _) => EqualNullSafe(fromExp, lit)
        case LessThan(_, _) => LessThan(fromExp, lit)
        case LessThanOrEqual(_, _) => LessThanOrEqual(fromExp, lit)
        case _ => exp
      }
    }
  }

  /**
   * Check if the input `fromExp` can be safely cast to `toType` without any loss of precision,
   * i.e., the conversion is injective. Note this only handles the case when both sides are of
   * integral type.
   */
  private def canImplicitlyCast(fromExp: Expression, toType: DataType): Boolean = {
    fromExp.dataType.isInstanceOf[IntegralType] && toType.isInstanceOf[IntegralType] &&
      Cast.canUpCast(fromExp.dataType, toType)
  }

  private def getRange(dt: DataType): (Any, Any) = dt match {
    case ByteType => (Byte.MinValue, Byte.MaxValue)
    case ShortType => (Short.MinValue, Short.MaxValue)
    case IntegerType => (Int.MinValue, Int.MaxValue)
    case LongType => (Long.MinValue, Long.MaxValue)
  }

  private[optimizer] implicit class ExpressionWrapper(e: Expression) {
    /**
     * Wraps input expression `e` with `if(isnull(e), null, false)`. The if-clause is represented
     * using `and(isnull(e), null)` which is semantically equivalent by applying 3-valued logic.
     */
    def falseIfNotNull: Expression = And(IsNull(e), Literal(null, BooleanType))

    /**
     * Wraps input expression `e` with `if(isnull(e), null, true)`. The if-clause is represented
     * using `or(isnotnull(e), null)` which is semantically equivalent by applying 3-valued logic.
     */
    def trueIfNotNull: Expression = Or(IsNotNull(e), Literal(null, BooleanType))
  }
}
