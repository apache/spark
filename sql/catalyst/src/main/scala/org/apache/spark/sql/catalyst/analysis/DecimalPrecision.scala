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
import org.apache.spark.sql.catalyst.types.DataTypeUtils
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
 *   e1 union e2  max(s1, s2) + max(p1-s1, p2-s2)         max(s1, s2)
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
 * - Literals INT and LONG get turned into DECIMAL with the precision strictly needed by the value
 */
// scalastyle:on
object DecimalPrecision extends TypeCoercionRule {
  import scala.math.max

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

  override def transform: PartialFunction[Expression, Expression] = {
    decimalAndDecimal()
      .orElse(integralAndDecimalLiteral)
      .orElse(nondecimalAndDecimal(conf.literalPickMinimumPrecision))
  }

  /** Decimal precision promotion for  binary comparison. */
  private def decimalAndDecimal(): PartialFunction[Expression, Expression] = {
    // Skip nodes whose children have not been resolved yet
    case e if !e.childrenResolved => e

    case b @ BinaryComparison(e1 @ DecimalExpression(p1, s1),
    e2 @ DecimalExpression(p2, s2)) if p1 != p2 || s1 != s2 =>
      val resultType = widerDecimalType(p1, s1, p2, s2)
      val newE1 = if (e1.dataType == resultType) e1 else Cast(e1, resultType)
      val newE2 = if (e2.dataType == resultType) e2 else Cast(e2, resultType)
      b.withNewChildren(Seq(newE1, newE2))
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

    case GreaterThan(i @ IntegralTypeExpression(), DecimalLiteral(value)) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        TrueLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        FalseLiteral
      } else {
        GreaterThan(i, Literal(value.floor.toLong))
      }

    case GreaterThanOrEqual(i @ IntegralTypeExpression(), DecimalLiteral(value)) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        TrueLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        FalseLiteral
      } else {
        GreaterThanOrEqual(i, Literal(value.ceil.toLong))
      }

    case LessThan(i @ IntegralTypeExpression(), DecimalLiteral(value)) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        FalseLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        TrueLiteral
      } else {
        LessThan(i, Literal(value.ceil.toLong))
      }

    case LessThanOrEqual(i @ IntegralTypeExpression(), DecimalLiteral(value)) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        FalseLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        TrueLiteral
      } else {
        LessThanOrEqual(i, Literal(value.floor.toLong))
      }

    case GreaterThan(DecimalLiteral(value), i @ IntegralTypeExpression()) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        FalseLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        TrueLiteral
      } else {
        GreaterThan(Literal(value.ceil.toLong), i)
      }

    case GreaterThanOrEqual(DecimalLiteral(value), i @ IntegralTypeExpression()) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        FalseLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        TrueLiteral
      } else {
        GreaterThanOrEqual(Literal(value.floor.toLong), i)
      }

    case LessThan(DecimalLiteral(value), i @ IntegralTypeExpression()) =>
      if (DecimalLiteral.smallerThanSmallestLong(value)) {
        TrueLiteral
      } else if (DecimalLiteral.largerThanLargestLong(value)) {
        FalseLiteral
      } else {
        LessThan(Literal(value.floor.toLong), i)
      }

    case LessThanOrEqual(DecimalLiteral(value), i @ IntegralTypeExpression()) =>
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
  private def nondecimalAndDecimal(literalPickMinimumPrecision: Boolean)
    : PartialFunction[Expression, Expression] = {
    // Promote integers inside a binary expression with fixed-precision decimals to decimals,
    // and fixed-precision decimals in an expression with floats / doubles to doubles
    case b @ BinaryOperator(left, right) if left.dataType != right.dataType =>
      (left, right) match {
        // Promote literal integers inside a binary expression with fixed-precision decimals to
        // decimals. The precision and scale are the ones strictly needed by the integer value.
        // Requiring more precision than necessary may lead to a useless loss of precision.
        // Consider the following example: multiplying a column which is DECIMAL(38, 18) by 2.
        // If we use the default precision and scale for the integer type, 2 is considered a
        // DECIMAL(10, 0). According to the rules, the result would be DECIMAL(38 + 10 + 1, 18),
        // which is out of range and therefore it will become DECIMAL(38, 7), leading to
        // potentially loosing 11 digits of the fractional part. Using only the precision needed
        // by the Literal, instead, the result would be DECIMAL(38 + 1 + 1, 18), which would
        // become DECIMAL(38, 16), safely having a much lower precision loss.
        case (l: Literal, r) if r.dataType.isInstanceOf[DecimalType] &&
            l.dataType.isInstanceOf[IntegralType] &&
            literalPickMinimumPrecision =>
          b.withNewChildren(Seq(Cast(l, DataTypeUtils.fromLiteral(l)), r))
        case (l, r: Literal) if l.dataType.isInstanceOf[DecimalType] &&
            r.dataType.isInstanceOf[IntegralType] &&
            literalPickMinimumPrecision =>
          b.withNewChildren(Seq(l, Cast(r, DataTypeUtils.fromLiteral(r))))
        // Promote integers inside a binary expression with fixed-precision decimals to decimals,
        // and fixed-precision decimals in an expression with floats / doubles to doubles
        case (l @ IntegralTypeExpression(), r @ DecimalExpression(_, _)) =>
          b.withNewChildren(Seq(Cast(l, DecimalType.forType(l.dataType)), r))
        case (l @ DecimalExpression(_, _), r @ IntegralTypeExpression()) =>
          b.withNewChildren(Seq(l, Cast(r, DecimalType.forType(r.dataType))))
        case (l, r @ DecimalExpression(_, _)) if isFloat(l.dataType) =>
          b.withNewChildren(Seq(l, Cast(r, DoubleType)))
        case (l @ DecimalExpression(_, _), r) if isFloat(r.dataType) =>
          b.withNewChildren(Seq(Cast(l, DoubleType), r))
        case _ => b
      }
  }

}
