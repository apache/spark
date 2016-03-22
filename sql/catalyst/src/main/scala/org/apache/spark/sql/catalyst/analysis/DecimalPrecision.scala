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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._


// scalastyle:off
/**
 * Calculates and propagates precision for fixed-precision decimals. Hive has a number of
 * rules for this based on the SQL standard and MS SQL:
 * https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
 * https://msdn.microsoft.com/en-us/library/ms190476.aspx
 *
 * In particular, if we have expressions e1 and e2 with precision/scale p1/s2 and p2/s2
 * respectively, then the following operations have the following precision / scale:
 *
 *   Operation    Result Precision                        Result Scale
 *   ------------------------------------------------------------------------
 *   e1 + e2      max(s1, s2) + max(p1-s1, p2-s2) + 1     max(s1, s2)
 *   e1 - e2      max(s1, s2) + max(p1-s1, p2-s2) + 1     max(s1, s2)
 *   e1 * e2      p1 + p2 + 1                             s1 + s2
 *   e1 / e2      p1 - s1 + s2 + max(6, s1 + p2 + 1)      max(6, s1 + p2 + 1)
 *   e1 % e2      min(p1-s1, p2-s2) + max(s1, s2)         max(s1, s2)
 *   e1 union e2  max(s1, s2) + max(p1-s1, p2-s2)         max(s1, s2)
 *   sum(e1)      p1 + 10                                 s1
 *   avg(e1)      p1 + 4                                  s1 + 4
 *
 * To implement the rules for fixed-precision types, we introduce casts to turn them to unlimited
 * precision, do the math on unlimited-precision numbers, then introduce casts back to the
 * required fixed precision. This allows us to do all rounding and overflow handling in the
 * cast-to-fixed-precision operator.
 *
 * In addition, when mixing non-decimal types with decimals, we use the following rules:
 * - BYTE gets turned into DECIMAL(3, 0)
 * - SHORT gets turned into DECIMAL(5, 0)
 * - INT gets turned into DECIMAL(10, 0)
 * - LONG gets turned into DECIMAL(20, 0)
 * - FLOAT and DOUBLE cause fixed-length decimals to turn into DOUBLE
 */
// scalastyle:on
object DecimalPrecision extends Rule[LogicalPlan] {
  import scala.math.{max, min}

  private def isFloat(t: DataType): Boolean = t == FloatType || t == DoubleType

  // Returns the wider decimal type that's wider than both of them
  def widerDecimalType(d1: DecimalType, d2: DecimalType): DecimalType = {
    widerDecimalType(d1.precision, d1.scale, d2.precision, d2.scale)
  }
  // max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2)
  def widerDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val scale = max(s1, s2)
    val range = max(p1 - s1, p2 - s2)
    DecimalType.bounded(range + scale, scale)
  }

  private def promotePrecision(e: Expression, dataType: DataType): Expression = {
    PromotePrecision(Cast(e, dataType))
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // fix decimal precision for expressions
    case q => q.transformExpressions(
      decimalAndDecimal.orElse(integralAndDecimalLiteral).orElse(nondecimalAndDecimal))
  }

  /** Decimal precision promotion for +, -, *, /, %, pmod, and binary comparison. */
  private val decimalAndDecimal: PartialFunction[Expression, Expression] = {
    // Skip nodes whose children have not been resolved yet
    case e if !e.childrenResolved => e

    // Skip nodes who is already promoted
    case e: BinaryArithmetic if e.left.isInstanceOf[PromotePrecision] => e

    case Add(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
      val dt = DecimalType.bounded(max(s1, s2) + max(p1 - s1, p2 - s2) + 1, max(s1, s2))
      CheckOverflow(Add(promotePrecision(e1, dt), promotePrecision(e2, dt)), dt)

    case Subtract(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
      val dt = DecimalType.bounded(max(s1, s2) + max(p1 - s1, p2 - s2) + 1, max(s1, s2))
      CheckOverflow(Subtract(promotePrecision(e1, dt), promotePrecision(e2, dt)), dt)

    case Multiply(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
      val resultType = DecimalType.bounded(p1 + p2 + 1, s1 + s2)
      val widerType = widerDecimalType(p1, s1, p2, s2)
      CheckOverflow(Multiply(promotePrecision(e1, widerType), promotePrecision(e2, widerType)),
        resultType)

    case Divide(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
      var intDig = min(DecimalType.MAX_SCALE, p1 - s1 + s2)
      var decDig = min(DecimalType.MAX_SCALE, max(6, s1 + p2 + 1))
      val diff = (intDig + decDig) - DecimalType.MAX_SCALE
      if (diff > 0) {
        decDig -= diff / 2 + 1
        intDig = DecimalType.MAX_SCALE - decDig
      }
      val resultType = DecimalType.bounded(intDig + decDig, decDig)
      val widerType = widerDecimalType(p1, s1, p2, s2)
      CheckOverflow(Divide(promotePrecision(e1, widerType), promotePrecision(e2, widerType)),
        resultType)

    case Remainder(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
      val resultType = DecimalType.bounded(min(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2))
      // resultType may have lower precision, so we cast them into wider type first.
      val widerType = widerDecimalType(p1, s1, p2, s2)
      CheckOverflow(Remainder(promotePrecision(e1, widerType), promotePrecision(e2, widerType)),
        resultType)

    case Pmod(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
      val resultType = DecimalType.bounded(min(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2))
      // resultType may have lower precision, so we cast them into wider type first.
      val widerType = widerDecimalType(p1, s1, p2, s2)
      CheckOverflow(Pmod(promotePrecision(e1, widerType), promotePrecision(e2, widerType)),
        resultType)

    case b @ BinaryComparison(e1 @ DecimalType.Expression(p1, s1),
    e2 @ DecimalType.Expression(p2, s2)) if p1 != p2 || s1 != s2 =>
      val resultType = widerDecimalType(p1, s1, p2, s2)
      b.makeCopy(Array(Cast(e1, resultType), Cast(e2, resultType)))

    // TODO: MaxOf, MinOf, etc might want other rules
    // SUM and AVERAGE are handled by the implementations of those expressions
  }

  /**
   * Strength reduction for comparing integral expressions with decimal literals.
   * 1. int_col > decimal_literal => int_col > floor(decimal_literal)
   * 2. int_col >= decimal_literal => int_col >= ceil(decimal_literal)
   * 3. int_col < decimal_literal => int_col < ceil(decimal_literal)
   * 4. int_col <= decimal_literal => int_col <= floor(decimal_literal)
   * 5. decimal_literal > int_col => ceil(decimal_literal) > int_col
   * 6. decimal_literal >= int_col => floor(decimal_literal) >= int_col
   * 7. decimal_literal < int_col => floor(decimal_literal) < int_col
   * 8. decimal_literal <= int_col => ceil(decimal_literal) <= int_col
   *
   * Note that technically this is an "optimization" and should go into the optimizer. However,
   * by the time the optimizer runs, these comparison expressions would be pretty hard to pattern
   * match because there are multiple (at least 2) levels of casts involved.
   *
   * There are a lot more possible rules we can implement, but we don't do them
   * because we are not sure how common they are.
   */
  private val integralAndDecimalLiteral: PartialFunction[Expression, Expression] = {

    case GreaterThan(i @ IntegralType(), DecimalLiteral(value)) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        TrueLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        FalseLiteral
      } else {
        GreaterThan(i, Literal(value.floor.toLong))
      }

    case GreaterThanOrEqual(i @ IntegralType(), DecimalLiteral(value)) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        TrueLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        FalseLiteral
      } else {
        GreaterThanOrEqual(i, Literal(value.ceil.toLong))
      }

    case LessThan(i @ IntegralType(), DecimalLiteral(value)) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        FalseLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        TrueLiteral
      } else {
        LessThan(i, Literal(value.ceil.toLong))
      }

    case LessThanOrEqual(i @ IntegralType(), DecimalLiteral(value)) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        FalseLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        TrueLiteral
      } else {
        LessThanOrEqual(i, Literal(value.floor.toLong))
      }

    case GreaterThan(DecimalLiteral(value), i @ IntegralType()) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        FalseLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        TrueLiteral
      } else {
        GreaterThan(Literal(value.ceil.toLong), i)
      }

    case GreaterThanOrEqual(DecimalLiteral(value), i @ IntegralType()) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        FalseLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        TrueLiteral
      } else {
        GreaterThanOrEqual(Literal(value.floor.toLong), i)
      }

    case LessThan(DecimalLiteral(value), i @ IntegralType()) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        TrueLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        FalseLiteral
      } else {
        LessThan(Literal(value.floor.toLong), i)
      }

    case LessThanOrEqual(DecimalLiteral(value), i @ IntegralType()) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        TrueLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        FalseLiteral
      } else {
        LessThanOrEqual(Literal(value.ceil.toLong), i)
      }
  }

  /**
   * Type coercion for BinaryOperator in which one side is a non-decimal numeric, and the other
   * side is a decimal.
   */
  private val nondecimalAndDecimal: PartialFunction[Expression, Expression] = {
    // Promote integers inside a binary expression with fixed-precision decimals to decimals,
    // and fixed-precision decimals in an expression with floats / doubles to doubles
    case b @ BinaryOperator(left, right) if left.dataType != right.dataType =>
      (left.dataType, right.dataType) match {
        case (t: IntegralType, DecimalType.Fixed(p, s)) =>
          b.makeCopy(Array(Cast(left, DecimalType.forType(t)), right))
        case (DecimalType.Fixed(p, s), t: IntegralType) =>
          b.makeCopy(Array(left, Cast(right, DecimalType.forType(t))))
        case (t, DecimalType.Fixed(p, s)) if isFloat(t) =>
          b.makeCopy(Array(left, Cast(right, DoubleType)))
        case (DecimalType.Fixed(p, s), t) if isFloat(t) =>
          b.makeCopy(Array(Cast(left, DoubleType), right))
        case _ =>
          b
      }
  }
}
