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

import org.apache.spark.sql.catalyst.expressions.{Attribute, BinaryArithmetic, BinaryOperator, CaseWhen, EqualTo, Expression, IsNotNull, IsNull, Literal, Not}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference, GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.internal.SQLConf

/**
 * The builder to generate V2 expressions from catalyst expressions.
 */
class V2ExpressionBuilder(e: Expression) {

  def build(): Option[V2Expression] = generateExpression(e)

  private def generateExpression(expr: Expression): Option[V2Expression] = expr match {
    case Literal(value, dataType) => Some(LiteralValue(value, dataType))
    case attr: Attribute => Some(FieldReference.column(quoteIfNeeded(attr.name)))
    case IsNull(col) => generateExpression(col)
      .map(c => new GeneralScalarExpression("IS_NULL", Array[V2Expression](c)))
    case IsNotNull(col) => generateExpression(col)
      .map(c => new GeneralScalarExpression("IS_NOT_NULL", Array[V2Expression](c)))
    case b: BinaryOperator =>
      if (!b.isInstanceOf[BinaryArithmetic] || SQLConf.get.ansiEnabled) {
        val left = generateExpression(b.left)
        val right = generateExpression(b.right)
        if (left.isDefined && right.isDefined) {
          Some(new GeneralScalarExpression(b.sqlOperator, Array[V2Expression](left.get, right.get)))
        } else {
          None
        }
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
