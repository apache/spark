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

import org.apache.spark.sql.catalyst.expressions.{Add, BinaryComparison, Divide,
  DoubleLiteral, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan,
  LessThanOrEqual, Literal, Multiply, Rand, Subtract}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXPRESSION_WITH_RANDOM_SEED

object OptimizeRand extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformAllExpressionsWithPruning(_.containsAnyPattern(
      EXPRESSION_WITH_RANDOM_SEED), ruleId) {
      case op @ EqualTo(DoubleLiteral(_), _: Rand) =>
        eliminateRand(EqualTo(op.right, op.left))
      case op @ BinaryComparison(DoubleLiteral(_), _: Rand)
          if !op.isInstanceOf[EqualTo] =>
        eliminateRand(swapComparison(op))
      case op @ BinaryComparison(_: Rand, DoubleLiteral(_)) =>
        eliminateRand(op)
      case op: BinaryComparison
          if isDirectRandChild(op.left) || isDirectRandChild(op.right) =>
        optimizeArithmetic(op)
  }

  private def isDirectRandChild(expr: Expression): Boolean = expr match {
    case _: Rand => true
    case Add(l, r, _) => l.isInstanceOf[Rand] || r.isInstanceOf[Rand]
    case Subtract(l, r, _) => l.isInstanceOf[Rand] || r.isInstanceOf[Rand]
    case Multiply(l, r, _) => l.isInstanceOf[Rand] || r.isInstanceOf[Rand]
    case Divide(l, r, _) => l.isInstanceOf[Rand] || r.isInstanceOf[Rand]
    case _ => false
  }

  private def hasRand(expr: Expression): Boolean = expr match {
    case _: Rand => true
    case a: Add => hasRand(a.left) || hasRand(a.right)
    case s: Subtract => hasRand(s.left) || hasRand(s.right)
    case m: Multiply => hasRand(m.left) || hasRand(m.right)
    case d: Divide => hasRand(d.left) || hasRand(d.right)
    case _ => false
  }

  private def swapComparison(comparison: BinaryComparison): BinaryComparison =
    comparison match {
      case GreaterThan(l, r) => LessThan(r, l)
      case GreaterThanOrEqual(l, r) => LessThanOrEqual(r, l)
      case LessThan(l, r) => GreaterThan(r, l)
      case LessThanOrEqual(l, r) => GreaterThanOrEqual(r, l)
      case o => o
    }

  private def eliminateRand(op: BinaryComparison): Expression = op match {
    case GreaterThan(_: Rand, DoubleLiteral(v)) =>
      if (v < 0.0) TrueLiteral else if (v >= 1.0) FalseLiteral else op
    case GreaterThanOrEqual(_: Rand, DoubleLiteral(v)) =>
      if (v <= 0.0) TrueLiteral else if (v >= 1.0) FalseLiteral else op
    case LessThan(_: Rand, DoubleLiteral(v)) =>
      if (v >= 1.0) TrueLiteral else if (v <= 0.0) FalseLiteral else op
    case LessThanOrEqual(_: Rand, DoubleLiteral(v)) =>
      if (v >= 1.0) TrueLiteral else if (v < 0.0) FalseLiteral else op
    case EqualTo(_: Rand, DoubleLiteral(v)) =>
      if (v < 0.0 || v >= 1.0) FalseLiteral else op
    case other => other
  }

  private def extractDouble(lit: Expression): Option[Double] = lit match {
    case DoubleLiteral(v) => Some(v)
    case Literal(v: Double, _) => Some(v)
    case Literal(v: java.lang.Double, _) => Some(v.doubleValue())
    case Literal(v: java.lang.Number, _) => Some(v.doubleValue())
    case _ => None
  }

  case class RandExpr(coeff: Double, offset: Double)

  private def extractRandCoeffOffset(expr: Expression): Option[RandExpr] = {
    // This function extracts coefficient and offset from expressions containing rand().
    // It normalizes expressions into the form: coeff * rand() + offset
    // Note: Only supports patterns where rand() is a direct child of arithmetic
    // operations. Deeply nested expressions like (rand() + 1) * 2 are not supported.
    expr match {
      case _: Rand => Some(RandExpr(1.0, 0.0))
      case m: Multiply =>
        if (m.left.isInstanceOf[Rand]) {
          extractDouble(m.right).map(coeff => RandExpr(coeff, 0.0))
        } else if (m.right.isInstanceOf[Rand]) {
          extractDouble(m.left).map(coeff => RandExpr(coeff, 0.0))
        } else {
          None
        }
      case a: Add =>
        extractRandCoeffOffset(a.left).flatMap { left =>
          extractDouble(a.right).map(right => RandExpr(left.coeff, left.offset + right))
        }.orElse {
          extractRandCoeffOffset(a.right).flatMap { right =>
            extractDouble(a.left).map(left => RandExpr(right.coeff, right.offset + left))
          }
        }
      case s: Subtract =>
        for {
          left <- extractRandCoeffOffset(s.left)
          right <- extractDouble(s.right)
        } yield RandExpr(left.coeff, left.offset - right)
      case d: Divide =>
        for {
          left <- extractRandCoeffOffset(d.left)
          denom <- extractDouble(d.right) if denom != 0.0
        } yield RandExpr(left.coeff / denom, left.offset / denom)
      case _ => None
    }
  }

  private def optimizeArithmetic(op: BinaryComparison): Expression = {
    extractDouble(op.right).flatMap { litVal =>
      if (hasRand(op.left)) {
        val opName = op match {
          case _: LessThan => Some("LT")
          case _: GreaterThan => Some("GT")
          case _: LessThanOrEqual => Some("LTE")
          case _: GreaterThanOrEqual => Some("GTE")
          case _: EqualTo => Some("EQ")
          case _ => None
        }
        opName.flatMap { name =>
          extractRandCoeffOffset(op.left).map { randExpr =>
            optimizeWithCoeffOffset(randExpr.coeff, randExpr.offset, litVal, name, op)
          }
        }
      } else {
        None
      }
    }.orElse {
      extractDouble(op.left).flatMap { litVal =>
        if (hasRand(op.right)) {
          val swapped = swapComparison(op)
          val opName = swapped match {
            case _: LessThan => Some("LT")
            case _: GreaterThan => Some("GT")
            case _: LessThanOrEqual => Some("LTE")
            case _: GreaterThanOrEqual => Some("GTE")
            case _: EqualTo => Some("EQ")
            case _ => None
          }
          opName.flatMap { name =>
            extractRandCoeffOffset(op.right).map { randExpr =>
              optimizeWithCoeffOffset(randExpr.coeff, randExpr.offset, litVal, name, op)
            }
          }
        } else {
          None
        }
      }
    }.getOrElse(op)
  }

  private def optimizeWithCoeffOffset(coeff: Double, offset: Double,
      value: Double, op: String, original: Expression): Expression = {
    if (coeff == 0.0) {
      val compVal = offset
      op match {
        case "GT" => if (compVal > value) TrueLiteral else FalseLiteral
        case "GTE" => if (compVal >= value) TrueLiteral else FalseLiteral
        case "LT" => if (compVal < value) TrueLiteral else FalseLiteral
        case "LTE" => if (compVal <= value) TrueLiteral else FalseLiteral
        case "EQ" => if (compVal == value) TrueLiteral else FalseLiteral
        case _ => original
      }
    } else if (coeff > 0.0) {
      val t = (value - offset) / coeff
      op match {
        case "GT" => if (t < 0.0) TrueLiteral
            else if (t >= 1.0) FalseLiteral else original
        case "GTE" => if (t <= 0.0) TrueLiteral
            else if (t > 1.0) FalseLiteral else original
        case "LT" => if (t >= 1.0) TrueLiteral
            else if (t <= 0.0) FalseLiteral else original
        case "LTE" => if (t >= 1.0) TrueLiteral
            else if (t < 0.0) FalseLiteral else original
        case "EQ" => if (t < 0.0 || t >= 1.0) FalseLiteral else original
        case _ => original
      }
    } else {
      val t = (value - offset) / coeff
      op match {
        case "GT" => if (t <= 0.0) FalseLiteral
            else if (t > 1.0) TrueLiteral else original
        case "GTE" => if (t < 0.0) FalseLiteral
            else if (t >= 1.0) TrueLiteral else original
        case "LT" => if (t < 0.0) TrueLiteral
            else if (t >= 1.0) FalseLiteral else original
        case "LTE" => if (t <= 0.0) TrueLiteral
            else if (t > 1.0) FalseLiteral else original
        case "EQ" => if (t < 0.0 || t >= 1.0) FalseLiteral else original
        case _ => original
      }
    }
  }
}
