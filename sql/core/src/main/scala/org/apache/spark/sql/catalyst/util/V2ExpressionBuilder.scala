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
import org.apache.spark.sql.connector.expressions.{Cast => V2Cast, Expression => V2Expression, Extract => V2Extract, FieldReference, GeneralScalarExpression, LiteralValue, UserDefinedScalarFunc}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue, And => V2And, Not => V2Not, Or => V2Or, Predicate => V2Predicate}
import org.apache.spark.sql.types.{BooleanType, IntegerType}

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
    case Greatest(children) =>
      val childrenExpressions = children.flatMap(generateExpression(_))
      if (children.length == childrenExpressions.length) {
        Some(new GeneralScalarExpression("GREATEST", childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case Least(children) =>
      val childrenExpressions = children.flatMap(generateExpression(_))
      if (children.length == childrenExpressions.length) {
        Some(new GeneralScalarExpression("LEAST", childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case Rand(child, hideSeed) =>
      if (hideSeed) {
        Some(new GeneralScalarExpression("RAND", Array.empty[V2Expression]))
      } else {
        generateExpression(child)
          .map(v => new GeneralScalarExpression("RAND", Array[V2Expression](v)))
      }
    case log: Logarithm =>
      val l = generateExpression(log.left)
      val r = generateExpression(log.right)
      if (l.isDefined && r.isDefined) {
        Some(new GeneralScalarExpression("LOG", Array[V2Expression](l.get, r.get)))
      } else {
        None
      }
    case Log10(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("LOG10", Array[V2Expression](v)))
    case Log2(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("LOG2", Array[V2Expression](v)))
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
    case round: Round =>
      val l = generateExpression(round.left)
      val r = generateExpression(round.right)
      if (l.isDefined && r.isDefined) {
        Some(new GeneralScalarExpression("ROUND", Array[V2Expression](l.get, r.get)))
      } else {
        None
      }
    case Sin(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("SIN", Array[V2Expression](v)))
    case Sinh(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("SINH", Array[V2Expression](v)))
    case Cos(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("COS", Array[V2Expression](v)))
    case Cosh(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("COSH", Array[V2Expression](v)))
    case Tan(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("TAN", Array[V2Expression](v)))
    case Tanh(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("TANH", Array[V2Expression](v)))
    case Cot(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("COT", Array[V2Expression](v)))
    case Asin(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("ASIN", Array[V2Expression](v)))
    case Asinh(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("ASINH", Array[V2Expression](v)))
    case Acos(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("ACOS", Array[V2Expression](v)))
    case Acosh(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("ACOSH", Array[V2Expression](v)))
    case Atan(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("ATAN", Array[V2Expression](v)))
    case Atanh(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("ATANH", Array[V2Expression](v)))
    case atan2: Atan2 =>
      val l = generateExpression(atan2.left)
      val r = generateExpression(atan2.right)
      if (l.isDefined && r.isDefined) {
        Some(new GeneralScalarExpression("ATAN2", Array[V2Expression](l.get, r.get)))
      } else {
        None
      }
    case Cbrt(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("CBRT", Array[V2Expression](v)))
    case ToDegrees(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("DEGREES", Array[V2Expression](v)))
    case ToRadians(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("RADIANS", Array[V2Expression](v)))
    case Signum(child) => generateExpression(child)
      .map(v => new GeneralScalarExpression("SIGN", Array[V2Expression](v)))
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
    case iff: If =>
      val childrenExpressions = iff.children.flatMap(generateExpression(_))
      if (iff.children.length == childrenExpressions.length) {
        Some(new GeneralScalarExpression("CASE_WHEN", childrenExpressions.toArray[V2Expression]))
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
    case date: DateAdd =>
      val childrenExpressions = date.children.flatMap(generateExpression(_))
      if (childrenExpressions.length == date.children.length) {
        Some(new GeneralScalarExpression("DATE_ADD", childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case date: DateDiff =>
      val childrenExpressions = date.children.flatMap(generateExpression(_))
      if (childrenExpressions.length == date.children.length) {
        Some(new GeneralScalarExpression("DATE_DIFF", childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case date: TruncDate =>
      val childrenExpressions = date.children.flatMap(generateExpression(_))
      if (childrenExpressions.length == date.children.length) {
        Some(new GeneralScalarExpression("TRUNC", childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case Second(child, _) =>
      generateExpression(child).map(v => new V2Extract("SECOND", v))
    case Minute(child, _) =>
      generateExpression(child).map(v => new V2Extract("MINUTE", v))
    case Hour(child, _) =>
      generateExpression(child).map(v => new V2Extract("HOUR", v))
    case Month(child) =>
      generateExpression(child).map(v => new V2Extract("MONTH", v))
    case Quarter(child) =>
      generateExpression(child).map(v => new V2Extract("QUARTER", v))
    case Year(child) =>
      generateExpression(child).map(v => new V2Extract("YEAR", v))
    // DayOfWeek uses Sunday = 1, Monday = 2, ... and ISO standard is Monday = 1, ...,
    // so we use the formula ((ISO_standard % 7) + 1) to do translation.
    case DayOfWeek(child) =>
      generateExpression(child).map(v => new GeneralScalarExpression("+",
        Array[V2Expression](new GeneralScalarExpression("%",
          Array[V2Expression](new V2Extract("DAY_OF_WEEK", v), LiteralValue(7, IntegerType))),
          LiteralValue(1, IntegerType))))
    // WeekDay uses Monday = 0, Tuesday = 1, ... and ISO standard is Monday = 1, ...,
    // so we use the formula (ISO_standard - 1) to do translation.
    case WeekDay(child) =>
      generateExpression(child).map(v => new GeneralScalarExpression("-",
        Array[V2Expression](new V2Extract("DAY_OF_WEEK", v), LiteralValue(1, IntegerType))))
    case DayOfMonth(child) =>
      generateExpression(child).map(v => new V2Extract("DAY", v))
    case DayOfYear(child) =>
      generateExpression(child).map(v => new V2Extract("DAY_OF_YEAR", v))
    case WeekOfYear(child) =>
      generateExpression(child).map(v => new V2Extract("WEEK", v))
    case YearOfWeek(child) =>
      generateExpression(child).map(v => new V2Extract("YEAR_OF_WEEK", v))
    // TODO supports other expressions
    case ApplyFunctionExpression(function, children) =>
      val childrenExpressions = children.flatMap(generateExpression(_))
      if (childrenExpressions.length == children.length) {
        Some(new UserDefinedScalarFunc(
          function.name(), function.canonicalName(), childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
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
