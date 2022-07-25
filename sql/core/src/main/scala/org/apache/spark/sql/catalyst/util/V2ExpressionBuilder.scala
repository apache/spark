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
    case InSet(child, hset) =>
      generateExpression(child).map { v =>
        val children =
          (v +: hset.toSeq.map(elem => LiteralValue(elem, child.dataType))).toArray[V2Expression]
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
    case Abs(child, true) => generateExpressionWithName("ABS", Seq(child))
    case Coalesce(children) => generateExpressionWithName("COALESCE", children)
    case Greatest(children) => generateExpressionWithName("GREATEST", children)
    case Least(children) => generateExpressionWithName("LEAST", children)
    case Rand(child, hideSeed) =>
      if (hideSeed) {
        Some(new GeneralScalarExpression("RAND", Array.empty[V2Expression]))
      } else {
        generateExpressionWithName("RAND", Seq(child))
      }
    case log: Logarithm => generateExpressionWithName("LOG", log.children)
    case Log10(child) => generateExpressionWithName("LOG10", Seq(child))
    case Log2(child) => generateExpressionWithName("LOG2", Seq(child))
    case Log(child) => generateExpressionWithName("LN", Seq(child))
    case Exp(child) => generateExpressionWithName("EXP", Seq(child))
    case pow: Pow => generateExpressionWithName("POWER", pow.children)
    case Sqrt(child) => generateExpressionWithName("SQRT", Seq(child))
    case Floor(child) => generateExpressionWithName("FLOOR", Seq(child))
    case Ceil(child) => generateExpressionWithName("CEIL", Seq(child))
    case round: Round => generateExpressionWithName("ROUND", round.children)
    case Sin(child) => generateExpressionWithName("SIN", Seq(child))
    case Sinh(child) => generateExpressionWithName("SINH", Seq(child))
    case Cos(child) => generateExpressionWithName("COS", Seq(child))
    case Cosh(child) => generateExpressionWithName("COSH", Seq(child))
    case Tan(child) => generateExpressionWithName("TAN", Seq(child))
    case Tanh(child) => generateExpressionWithName("TANH", Seq(child))
    case Cot(child) => generateExpressionWithName("COT", Seq(child))
    case Asin(child) => generateExpressionWithName("ASIN", Seq(child))
    case Asinh(child) => generateExpressionWithName("ASINH", Seq(child))
    case Acos(child) => generateExpressionWithName("ACOS", Seq(child))
    case Acosh(child) => generateExpressionWithName("ACOSH", Seq(child))
    case Atan(child) => generateExpressionWithName("ATAN", Seq(child))
    case Atanh(child) => generateExpressionWithName("ATANH", Seq(child))
    case atan2: Atan2 => generateExpressionWithName("ATAN2", atan2.children)
    case Cbrt(child) => generateExpressionWithName("CBRT", Seq(child))
    case ToDegrees(child) => generateExpressionWithName("DEGREES", Seq(child))
    case ToRadians(child) => generateExpressionWithName("RADIANS", Seq(child))
    case Signum(child) => generateExpressionWithName("SIGN", Seq(child))
    case wb: WidthBucket => generateExpressionWithName("WIDTH_BUCKET", wb.children)
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
          case _: BinaryComparison if l.get.isInstanceOf[LiteralValue[_]] &&
              r.get.isInstanceOf[FieldReference] =>
            Some(new V2Predicate(flipComparisonOperatorName(b.sqlOperator),
              Array[V2Expression](r.get, l.get)))
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
    case UnaryMinus(child, true) => generateExpressionWithName("-", Seq(child))
    case BitwiseNot(child) => generateExpressionWithName("~", Seq(child))
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
    case iff: If => generateExpressionWithName("CASE_WHEN", iff.children)
    case substring: Substring =>
      val children = if (substring.len == Literal(Integer.MAX_VALUE)) {
        Seq(substring.str, substring.pos)
      } else {
        substring.children
      }
      generateExpressionWithName("SUBSTRING", children)
    case Upper(child) => generateExpressionWithName("UPPER", Seq(child))
    case Lower(child) => generateExpressionWithName("LOWER", Seq(child))
    case translate: StringTranslate => generateExpressionWithName("TRANSLATE", translate.children)
    case trim: StringTrim => generateExpressionWithName("TRIM", trim.children)
    case trim: StringTrimLeft => generateExpressionWithName("LTRIM", trim.children)
    case trim: StringTrimRight => generateExpressionWithName("RTRIM", trim.children)
    case overlay: Overlay =>
      val children = if (overlay.len == Literal(-1)) {
        Seq(overlay.input, overlay.replace, overlay.pos)
      } else {
        overlay.children
      }
      generateExpressionWithName("OVERLAY", children)
    case date: DateAdd => generateExpressionWithName("DATE_ADD", date.children)
    case date: DateDiff => generateExpressionWithName("DATE_DIFF", date.children)
    case date: TruncDate => generateExpressionWithName("TRUNC", date.children)
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

  private def flipComparisonOperatorName(operatorName: String): String = {
    operatorName match {
      case ">" => "<"
      case "<" => ">"
      case ">=" => "<="
      case "<=" => ">="
      case _ => operatorName
    }
  }

  private def generateExpressionWithName(
      v2ExpressionName: String, children: Seq[Expression]): Option[V2Expression] = {
    val childrenExpressions = children.flatMap(generateExpression(_))
    if (childrenExpressions.length == children.length) {
      Some(new GeneralScalarExpression(v2ExpressionName, childrenExpressions.toArray[V2Expression]))
    } else {
      None
    }
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
