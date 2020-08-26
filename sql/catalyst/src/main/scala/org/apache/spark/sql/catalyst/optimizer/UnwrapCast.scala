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
 * Unwrap casts in binary comparison operations with the following pattern:
 *   `BinaryComparison(Cast(fromExp, _), Literal(value, toType))`
 * This rule optimize expressions with this pattern by either replacing the cast with simpler
 * constructs, or moving the cast from the expression side to the literal side, so they can be
 * optimized away and pushed down to data sources.
 *
 * Currently this only handles the case where `fromType` (of `fromExp`) and `toType` are of
 * integral types (i.e., byte, short, int and long). It checks to see if the literal `value` is
 * within range (min, max) of the `fromType`. If this is true then it means we can safely cast the
 * `value` to the `fromType` and thus able to move the cast to the literal side. Otherwise, it
 * replaces the cast with different simpler constructs, such as
 * `EqualTo(fromExp, Literal(max, fromType)` when input is
 * `GreaterThanOrEqualTo(Cast(fromExp, fromType), Literal(max, toType))`
 */
object UnwrapCast extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case l: LogicalPlan => l transformExpressionsUp {
      case e @ BinaryComparison(_, _) => unwrapCast(e)
    }
  }

  private def unwrapCast(exp: Expression): Expression = exp match {
    case BinaryComparison(Literal(_, _), Cast(_, _, _)) =>
      def swap(e: Expression): Expression = e match {
        case GreaterThan(left, right) => LessThan(right, left)
        case GreaterThanOrEqual(left, right) => LessThanOrEqual(right, left)
        case EqualTo(left, right) => EqualTo(right, left)
        case EqualNullSafe(left, right) => EqualNullSafe(right, left)
        case LessThanOrEqual(left, right) => GreaterThanOrEqual(right, left)
        case LessThan(left, right) => GreaterThan(right, left)
        case other => other
      }
      swap(unwrapCast(swap(exp)))

    case BinaryComparison(Cast(fromExp, _, _), Literal(value, toType)) =>
      val fromType = fromExp.dataType
      if (!fromType.isInstanceOf[IntegralType] || !toType.isInstanceOf[IntegralType]
        || !Cast.canUpCast(fromType, toType)) {
        return exp
      }

      // Check if the literal value is within the range of the `fromType`, and handle the boundary
      // cases in the following
      val toIntegralType = toType.asInstanceOf[IntegralType]
      val (min, max) = getRange(fromType)
      val (minInToType, maxInToType) =
        (Cast(Literal(min), toType).eval(), Cast(Literal(max), toType).eval())

      // Compare upper bounds
      val maxCmp = toIntegralType.ordering.asInstanceOf[Ordering[Any]].compare(value, maxInToType)
      if (maxCmp > 0) {
        exp match {
          case EqualTo(_, _) | GreaterThan(_, _) | GreaterThanOrEqual(_, _) =>
            return falseIfNotNull(fromExp)
          case LessThan(_, _) | LessThanOrEqual(_, _) =>
            return trueIfNotNull(fromExp)
          case EqualNullSafe(_, _) =>
            return FalseLiteral
          case _ => return exp // impossible but safe guard, same below
        }
      } else if (maxCmp == 0) {
        exp match {
          case GreaterThan(_, _) =>
            return falseIfNotNull(fromExp)
          case LessThanOrEqual(_, _) =>
            return trueIfNotNull(fromExp)
          case LessThan(_, _) =>
            return Not(EqualTo(fromExp, Literal(max, fromType)))
          case GreaterThanOrEqual(_, _) | EqualTo(_, _) | EqualNullSafe(_, _) =>
            return EqualTo(fromExp, Literal(max, fromType))
          case _ => return exp
        }
      }

      // Compare lower bounds
      val minCmp = toIntegralType.ordering.asInstanceOf[Ordering[Any]].compare(value, minInToType)
      if (minCmp < 0) {
        exp match {
          case GreaterThan(_, _) | GreaterThanOrEqual(_, _) =>
            return trueIfNotNull(fromExp)
          case LessThan(_, _) | LessThanOrEqual(_, _) | EqualTo(_, _) =>
            return falseIfNotNull(fromExp)
          case EqualNullSafe(_, _) =>
            return FalseLiteral
          case _ => return exp
        }
      } else if (minCmp == 0) {
        exp match {
          case LessThan(_, _) =>
            return falseIfNotNull(fromExp)
          case GreaterThanOrEqual(_, _) =>
            return trueIfNotNull(fromExp)
          case GreaterThan(_, _) =>
            return Not(EqualTo(fromExp, Literal(min, fromType)))
          case LessThanOrEqual(_, _) | EqualTo(_, _) | EqualNullSafe(_, _) =>
            return EqualTo(fromExp, Literal(min, fromType))
          case _ => return exp
        }
      }

      // Now we can assume `value` is within the bound of the source type, e.g., min < value < max
      val lit = Cast(Literal(value), fromType)
      exp match {
        case GreaterThan(_, _) => GreaterThan(fromExp, lit)
        case GreaterThanOrEqual(_, _) => GreaterThanOrEqual(fromExp, lit)
        case EqualTo(_, _) => EqualTo(fromExp, lit)
        case EqualNullSafe(_, _) => EqualNullSafe(fromExp, lit)
        case LessThan(_, _) => LessThan(fromExp, lit)
        case LessThanOrEqual(_, _) => LessThanOrEqual(fromExp, lit)
      }

    case _ => exp
  }

  private[sql] def falseIfNotNull(e: Expression): Expression =
    And(IsNull(e), Literal(null, BooleanType))

  private[sql] def trueIfNotNull(e: Expression): Expression =
    Or(IsNotNull(e), Literal(null, BooleanType))

  private def getRange(ty: DataType): (Any, Any) = ty match {
    case ByteType => (Byte.MinValue, Byte.MaxValue)
    case ShortType => (Short.MinValue, Short.MaxValue)
    case IntegerType => (Int.MinValue, Int.MaxValue)
    case LongType => (Long.MinValue, Long.MaxValue)
  }
}


