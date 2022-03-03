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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.expressions.{Add, And, Attribute, BinaryComparison, BinaryOperator, BitwiseAnd, BitwiseNot, BitwiseOr, BitwiseXor, CaseWhen, Divide, EqualTo, Expression, IsNotNull, IsNull, Literal, Multiply, Not, Or, Remainder, Subtract, UnaryMinus}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference, GeneralScalarExpression, LiteralValue}

/**
 * The builder to generate V2 expressions from catalyst expressions.
 */
class V2ExpressionBuilder(e: Expression) {

  def build(): Option[V2Expression] = generateExpression(e)

  private def canTranslate(b: BinaryOperator) = b match {
    case _: And | _: Or => true
    case _: BinaryComparison => true
    case _: BitwiseAnd | _: BitwiseOr | _: BitwiseXor => true
    case add: Add => add.failOnError
    case sub: Subtract => sub.failOnError
    case mul: Multiply => mul.failOnError
    case div: Divide => div.failOnError
    case r: Remainder => r.failOnError
    case _ => false
  }

  private def generateExpression(expr: Expression): Option[V2Expression] = expr match {
    case Literal(value, dataType) => Some(LiteralValue(value, dataType))
    case attr: Attribute => Some(FieldReference.column(attr.name))
    case IsNull(col) => generateExpression(col)
      .map(c => new GeneralScalarExpression("IS_NULL", Array[V2Expression](c)))
    case IsNotNull(col) => generateExpression(col)
      .map(c => new GeneralScalarExpression("IS_NOT_NULL", Array[V2Expression](c)))
    case b: BinaryOperator if canTranslate(b) =>
      val left = generateExpression(b.left)
      val right = generateExpression(b.right)
      if (left.isDefined && right.isDefined) {
        Some(new GeneralScalarExpression(b.sqlOperator, Array[V2Expression](left.get, right.get)))
      } else {
        None
      }
    case Not(eq: EqualTo) =>
      val left = generateExpression(eq.left)
      val right = generateExpression(eq.right)
      if (left.isDefined && right.isDefined) {
        Some(new GeneralScalarExpression("!=", Array[V2Expression](left.get, right.get)))
      } else {
        None
      }
    case Not(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("NOT", Array[V2Expression](v)))
    case UnaryMinus(child, true) => generateExpression(child)
      .map(v => new GeneralScalarExpression("-", Array[V2Expression](v)))
    case BitwiseNot(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("~", Array[V2Expression](v)))
    case CaseWhen(branches, elseValue) =>
      val conditions = branches.map(_._1).flatMap(generateExpression)
      val values = branches.map(_._2).flatMap(generateExpression)
      if (conditions.length == branches.length && values.length == branches.length) {
        val branchExpressions = conditions.zip(values).flatMap { case (c, v) =>
          Seq[V2Expression](c, v)
        }
        if (elseValue.isDefined) {
          elseValue.flatMap(generateExpression).map { v =>
            val children = (branchExpressions :+ v).toArray[V2Expression]
            // The children looks like [condition1, value1, ..., conditionN, valueN, elseValue]
            new GeneralScalarExpression("CASE_WHEN", children)
          }
        } else {
          // The children looks like [condition1, value1, ..., conditionN, valueN]
          Some(new GeneralScalarExpression("CASE_WHEN", branchExpressions.toArray[V2Expression]))
        }
      } else {
        None
      }
    // TODO supports other expressions
    case _ => None
  }
}
