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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.expressions.{Cast => V2Cast, Expression => V2Expression, FieldReference, GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue, And => V2And, Not => V2Not, Or => V2Or, Predicate => V2Predicate}
import org.apache.spark.sql.types.BooleanType

/**
 * The builder to generate V2 expressions from catalyst expressions.
 */
class V2ExpressionBuilder(e: Expression, isPredicate: Boolean = false) {

  def build(): Option[V2Expression] = generateExpression(e, isPredicate)

  private def canTranslate(b: BinaryOperator) = b match {
    case _: BinaryComparison => true
    case _: BitwiseAnd | _: BitwiseOr | _: BitwiseXor => true
    case add: Add => add.failOnError
    case sub: Subtract => sub.failOnError
    case mul: Multiply => mul.failOnError
    case div: Divide => div.failOnError
    case r: Remainder => r.failOnError
    case _ => false
  }

  private def generateExpression(
      expr: Expression, isPredicate: Boolean = false): Option[V2Expression] = expr match {
    case Literal(true, BooleanType) => Some(new AlwaysTrue())
    case Literal(false, BooleanType) => Some(new AlwaysFalse())
    case Literal(value, dataType) => Some(LiteralValue(value, dataType))
    case col @ ColumnOrField(nameParts) =>
      val ref = FieldReference(nameParts)
      if (isPredicate && col.dataType.isInstanceOf[BooleanType]) {
        Some(new V2Predicate("=", Array(ref, LiteralValue(true, BooleanType))))
      } else {
        Some(ref)
      }
    case in @ InSet(child, hset) =>
      generateExpression(child).map { v =>
        val children =
          (v +: hset.toSeq.map(elem => LiteralValue(elem, in.dataType))).toArray[V2Expression]
        new V2Predicate("IN", children)
      }
    // Because we only convert In to InSet in Optimizer when there are more than certain
    // items. So it is possible we still get an In expression here that needs to be pushed
    // down.
    case In(value, list) =>
      val v = generateExpression(value)
      val listExpressions = list.flatMap(generateExpression(_))
      if (v.isDefined && list.length == listExpressions.length) {
        val children = (v.get +: listExpressions).toArray[V2Expression]
        // The children looks like [expr, value1, ..., valueN]
        Some(new V2Predicate("IN", children))
      } else {
        None
      }
    case IsNull(col) => generateExpression(col)
      .map(c => new V2Predicate("IS_NULL", Array[V2Expression](c)))
    case IsNotNull(col) => generateExpression(col)
      .map(c => new V2Predicate("IS_NOT_NULL", Array[V2Expression](c)))
    case p: StringPredicate =>
      val left = generateExpression(p.left)
      val right = generateExpression(p.right)
      if (left.isDefined && right.isDefined) {
        val name = p match {
          case _: StartsWith => "STARTS_WITH"
          case _: EndsWith => "ENDS_WITH"
          case _: Contains => "CONTAINS"
        }
        Some(new V2Predicate(name, Array[V2Expression](left.get, right.get)))
      } else {
        None
      }
    case Cast(child, dataType, _, true) =>
      generateExpression(child).map(v => new V2Cast(v, dataType))
    case Abs(child, true) => generateExpression(child)
      .map(v => new GeneralScalarExpression("ABS", Array[V2Expression](v)))
    case Coalesce(children) =>
      val childrenExpressions = children.flatMap(generateExpression(_))
      if (children.length == childrenExpressions.length) {
        Some(new GeneralScalarExpression("COALESCE", childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case Log(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("LN", Array[V2Expression](v)))
    case Exp(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("EXP", Array[V2Expression](v)))
    case Pow(left, right) =>
      val l = generateExpression(left)
      val r = generateExpression(right)
      if (l.isDefined && r.isDefined) {
        Some(new GeneralScalarExpression("POWER", Array[V2Expression](l.get, r.get)))
      } else {
        None
      }
    case Sqrt(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("SQRT", Array[V2Expression](v)))
    case Floor(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("FLOOR", Array[V2Expression](v)))
    case Ceil(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("CEIL", Array[V2Expression](v)))
    case wb: WidthBucket =>
      val childrenExpressions = wb.children.flatMap(generateExpression(_))
      if (childrenExpressions.length == wb.children.length) {
        Some(new GeneralScalarExpression("WIDTH_BUCKET",
          childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case and: And =>
      // AND expects predicate
      val l = generateExpression(and.left, true)
      val r = generateExpression(and.right, true)
      if (l.isDefined && r.isDefined) {
        assert(l.get.isInstanceOf[V2Predicate] && r.get.isInstanceOf[V2Predicate])
        Some(new V2And(l.get.asInstanceOf[V2Predicate], r.get.asInstanceOf[V2Predicate]))
      } else {
        None
      }
    case or: Or =>
      // OR expects predicate
      val l = generateExpression(or.left, true)
      val r = generateExpression(or.right, true)
      if (l.isDefined && r.isDefined) {
        assert(l.get.isInstanceOf[V2Predicate] && r.get.isInstanceOf[V2Predicate])
        Some(new V2Or(l.get.asInstanceOf[V2Predicate], r.get.asInstanceOf[V2Predicate]))
      } else {
        None
      }
    case b: BinaryOperator if canTranslate(b) =>
      val l = generateExpression(b.left)
      val r = generateExpression(b.right)
      if (l.isDefined && r.isDefined) {
        b match {
          case _: Predicate =>
            Some(new V2Predicate(b.sqlOperator, Array[V2Expression](l.get, r.get)))
          case _ =>
            Some(new GeneralScalarExpression(b.sqlOperator, Array[V2Expression](l.get, r.get)))
        }
      } else {
        None
      }
    case Not(eq: EqualTo) =>
      val left = generateExpression(eq.left)
      val right = generateExpression(eq.right)
      if (left.isDefined && right.isDefined) {
        Some(new V2Predicate("<>", Array[V2Expression](left.get, right.get)))
      } else {
        None
      }
    case Not(child) => generateExpression(child, true) // NOT expects predicate
      .map { v =>
        assert(v.isInstanceOf[V2Predicate])
        new V2Not(v.asInstanceOf[V2Predicate])
      }
    case UnaryMinus(child, true) => generateExpression(child)
      .map(v => new GeneralScalarExpression("-", Array[V2Expression](v)))
    case BitwiseNot(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("~", Array[V2Expression](v)))
    case CaseWhen(branches, elseValue) =>
      val conditions = branches.map(_._1).flatMap(generateExpression(_, true))
      val values = branches.map(_._2).flatMap(generateExpression(_, true))
      if (conditions.length == branches.length && values.length == branches.length) {
        val branchExpressions = conditions.zip(values).flatMap { case (c, v) =>
          Seq[V2Expression](c, v)
        }
        if (elseValue.isDefined) {
          elseValue.flatMap(generateExpression(_)).map { v =>
            val children = (branchExpressions :+ v).toArray[V2Expression]
            // The children looks like [condition1, value1, ..., conditionN, valueN, elseValue]
            new V2Predicate("CASE_WHEN", children)
          }
        } else {
          // The children looks like [condition1, value1, ..., conditionN, valueN]
          Some(new V2Predicate("CASE_WHEN", branchExpressions.toArray[V2Expression]))
        }
      } else {
        None
      }
    case substring: Substring =>
      val children = if (substring.len == Literal(Integer.MAX_VALUE)) {
        Seq(substring.str, substring.pos)
      } else {
        substring.children
      }
      val childrenExpressions = children.flatMap(generateExpression(_))
      if (childrenExpressions.length == children.length) {
        Some(new GeneralScalarExpression("SUBSTRING",
          childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case Upper(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("UPPER", Array[V2Expression](v)))
    case Lower(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("LOWER", Array[V2Expression](v)))
    case translate: StringTranslate =>
      val childrenExpressions = translate.children.flatMap(generateExpression(_))
      if (childrenExpressions.length == translate.children.length) {
        Some(new GeneralScalarExpression("TRANSLATE",
          childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case trim: StringTrim =>
      val childrenExpressions = trim.children.flatMap(generateExpression(_))
      if (childrenExpressions.length == trim.children.length) {
        Some(new GeneralScalarExpression("TRIM", childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case trim: StringTrimLeft =>
      val childrenExpressions = trim.children.flatMap(generateExpression(_))
      if (childrenExpressions.length == trim.children.length) {
        Some(new GeneralScalarExpression("LTRIM", childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case trim: StringTrimRight =>
      val childrenExpressions = trim.children.flatMap(generateExpression(_))
      if (childrenExpressions.length == trim.children.length) {
        Some(new GeneralScalarExpression("RTRIM", childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case overlay: Overlay =>
      val children = if (overlay.len == Literal(-1)) {
        Seq(overlay.input, overlay.replace, overlay.pos)
      } else {
        overlay.children
      }
      val childrenExpressions = children.flatMap(generateExpression(_))
      if (childrenExpressions.length == children.length) {
        Some(new GeneralScalarExpression("OVERLAY",
          childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    // TODO supports other expressions
    case _ => None
  }
}

object ColumnOrField {
  def unapply(e: Expression): Option[Seq[String]] = e match {
    case a: Attribute => Some(Seq(a.name))
    case s: GetStructField =>
      unapply(s.child).map(_ :+ s.childSchema(s.ordinal).name)
    case _ => None
  }
}
