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

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.EXPR
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Complete}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.connector.expressions.{Cast => V2Cast, Expression => V2Expression, Extract => V2Extract, FieldReference, GeneralScalarExpression, LiteralValue, NullOrdering, SortDirection, SortValue, UserDefinedScalarFunc}
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Avg, Count, CountStar, GeneralAggregateFunc, Max, Min, Sum, UserDefinedAggregateFunc}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue, And => V2And, Not => V2Not, Or => V2Or, Predicate => V2Predicate}
import org.apache.spark.sql.execution.datasources.PushableExpression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, StringType}

/**
 * The builder to generate V2 expressions from catalyst expressions.
 */
class V2ExpressionBuilder(e: Expression, isPredicate: Boolean = false) extends Logging  {

  def build(): Option[V2Expression] = generateExpression(e, isPredicate)

  def buildPredicate(): Option[V2Predicate] = {

    if (isPredicate) {
      val translated = build()

      val modifiedExprOpt = if (
        SQLConf.get.getConf(SQLConf.DATA_SOURCE_DONT_ASSERT_ON_PREDICATE)
          && translated.isDefined
          && !translated.get.isInstanceOf[V2Predicate]) {

        // If a predicate is expected but the translation yields something else,
        // log a warning and proceed as if the translation was not possible.
        logWarning(log"Predicate expected but got class: ${MDC(EXPR, translated.get.describe())}")
        None
      } else {
        translated
      }

      modifiedExprOpt.map { v =>
        assert(v.isInstanceOf[V2Predicate], s"Expected Predicate but got ${v.describe()}")
        v.asInstanceOf[V2Predicate]
      }
    } else {
      None
    }
  }

  private def canTranslate(b: BinaryOperator) = b match {
    case _: BinaryComparison => true
    case _: BitwiseAnd | _: BitwiseOr | _: BitwiseXor => true
    case add: Add => add.evalMode == EvalMode.ANSI
    case sub: Subtract => sub.evalMode == EvalMode.ANSI
    case mul: Multiply => mul.evalMode == EvalMode.ANSI
    case div: Divide => div.evalMode == EvalMode.ANSI
    case r: Remainder => r.evalMode == EvalMode.ANSI
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
    case Cast(child, dataType, _, evalMode)
        if evalMode == EvalMode.ANSI || Cast.canUpCast(child.dataType, dataType) =>
      generateExpression(child).map(v => new V2Cast(v, child.dataType, dataType))
    case AggregateExpression(aggregateFunction, Complete, isDistinct, None, _) =>
      generateAggregateFunc(aggregateFunction, isDistinct)
    case Abs(_, true) => generateExpressionWithName("ABS", expr, isPredicate)
    case _: Coalesce => generateExpressionWithName("COALESCE", expr, isPredicate)
    case _: Greatest => generateExpressionWithName("GREATEST", expr, isPredicate)
    case _: Least => generateExpressionWithName("LEAST", expr, isPredicate)
    case Rand(_, hideSeed) =>
      if (hideSeed) {
        Some(new GeneralScalarExpression("RAND", Array.empty[V2Expression]))
      } else {
        generateExpressionWithName("RAND", expr, isPredicate)
      }
    case _: Logarithm => generateExpressionWithName("LOG", expr, isPredicate)
    case _: Log10 => generateExpressionWithName("LOG10", expr, isPredicate)
    case _: Log2 => generateExpressionWithName("LOG2", expr, isPredicate)
    case _: Log => generateExpressionWithName("LN", expr, isPredicate)
    case _: Exp => generateExpressionWithName("EXP", expr, isPredicate)
    case _: Pow => generateExpressionWithName("POWER", expr, isPredicate)
    case _: Sqrt => generateExpressionWithName("SQRT", expr, isPredicate)
    case _: Floor => generateExpressionWithName("FLOOR", expr, isPredicate)
    case _: Ceil => generateExpressionWithName("CEIL", expr, isPredicate)
    case _: Round => generateExpressionWithName("ROUND", expr, isPredicate)
    case _: Sin => generateExpressionWithName("SIN", expr, isPredicate)
    case _: Sinh => generateExpressionWithName("SINH", expr, isPredicate)
    case _: Cos => generateExpressionWithName("COS", expr, isPredicate)
    case _: Cosh => generateExpressionWithName("COSH", expr, isPredicate)
    case _: Tan => generateExpressionWithName("TAN", expr, isPredicate)
    case _: Tanh => generateExpressionWithName("TANH", expr, isPredicate)
    case _: Cot => generateExpressionWithName("COT", expr, isPredicate)
    case _: Asin => generateExpressionWithName("ASIN", expr, isPredicate)
    case _: Asinh => generateExpressionWithName("ASINH", expr, isPredicate)
    case _: Acos => generateExpressionWithName("ACOS", expr, isPredicate)
    case _: Acosh => generateExpressionWithName("ACOSH", expr, isPredicate)
    case _: Atan => generateExpressionWithName("ATAN", expr, isPredicate)
    case _: Atanh => generateExpressionWithName("ATANH", expr, isPredicate)
    case _: Atan2 => generateExpressionWithName("ATAN2", expr, isPredicate)
    case _: Cbrt => generateExpressionWithName("CBRT", expr, isPredicate)
    case _: ToDegrees => generateExpressionWithName("DEGREES", expr, isPredicate)
    case _: ToRadians => generateExpressionWithName("RADIANS", expr, isPredicate)
    case _: Signum => generateExpressionWithName("SIGN", expr, isPredicate)
    case _: WidthBucket => generateExpressionWithName("WIDTH_BUCKET", expr, isPredicate)
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
    case UnaryMinus(_, true) => generateExpressionWithName("-", expr, isPredicate)
    case _: BitwiseNot => generateExpressionWithName("~", expr, isPredicate)
    case caseWhen @ CaseWhen(branches, elseValue) =>
      val conditions = branches.map(_._1).flatMap(generateExpression(_, true))
      val values = branches.map(_._2).flatMap(child =>
        generateExpression(
          child,
          isPredicate && child.dataType.isInstanceOf[BooleanType]
        )
      )
      val elseExprOpt = elseValue.flatMap(child =>
        generateExpression(
          child,
          isPredicate && child.dataType.isInstanceOf[BooleanType]
        )
      )
      if (conditions.length == branches.length && values.length == branches.length &&
          elseExprOpt.size == elseValue.size) {
        val branchExpressions = conditions.zip(values).flatMap { case (c, v) =>
          Seq[V2Expression](c, v)
        }
        val children = (branchExpressions ++ elseExprOpt).toArray[V2Expression]
        // The children looks like [condition1, value1, ..., conditionN, valueN (, elseValue)]
        if (isPredicate && caseWhen.dataType.isInstanceOf[BooleanType]) {
          Some(new V2Predicate("CASE_WHEN", children))
        } else {
          Some(new GeneralScalarExpression("CASE_WHEN", children))
        }
      } else {
        None
      }
    case _: If => generateExpressionWithName("CASE_WHEN", expr, isPredicate)
    case substring: Substring =>
      val children = if (substring.len == Literal(Integer.MAX_VALUE)) {
        Seq(substring.str, substring.pos)
      } else {
        substring.children
      }
      generateExpressionWithNameByChildren("SUBSTRING", children, substring.dataType, isPredicate)
    case _: Upper => generateExpressionWithName("UPPER", expr, isPredicate)
    case _: Lower => generateExpressionWithName("LOWER", expr, isPredicate)
    case BitLength(child) if child.dataType.isInstanceOf[StringType] =>
      generateExpressionWithName("BIT_LENGTH", expr, isPredicate)
    case Length(child) if child.dataType.isInstanceOf[StringType] =>
      generateExpressionWithName("CHAR_LENGTH", expr, isPredicate)
    case _: Concat => generateExpressionWithName("CONCAT", expr, isPredicate)
    case _: StringTranslate => generateExpressionWithName("TRANSLATE", expr, isPredicate)
    case _: StringTrim => generateExpressionWithName("TRIM", expr, isPredicate)
    case _: StringTrimLeft => generateExpressionWithName("LTRIM", expr, isPredicate)
    case _: StringTrimRight => generateExpressionWithName("RTRIM", expr, isPredicate)
    case overlay: Overlay =>
      val children = if (overlay.len == Literal(-1)) {
        Seq(overlay.input, overlay.replace, overlay.pos)
      } else {
        overlay.children
      }
      generateExpressionWithNameByChildren("OVERLAY", children, overlay.dataType, isPredicate)
    case _: DateAdd => generateExpressionWithName("DATE_ADD", expr, isPredicate)
    case _: DateDiff => generateExpressionWithName("DATE_DIFF", expr, isPredicate)
    case _: TruncDate => generateExpressionWithName("TRUNC", expr, isPredicate)
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
    case _: AesEncrypt => generateExpressionWithName("AES_ENCRYPT", expr, isPredicate)
    case _: AesDecrypt => generateExpressionWithName("AES_DECRYPT", expr, isPredicate)
    case _: Crc32 => generateExpressionWithName("CRC32", expr, isPredicate)
    case _: Md5 => generateExpressionWithName("MD5", expr, isPredicate)
    case _: Sha1 => generateExpressionWithName("SHA1", expr, isPredicate)
    case _: Sha2 => generateExpressionWithName("SHA2", expr, isPredicate)
    // TODO supports other expressions
    case ApplyFunctionExpression(function, children) =>
      val childrenExpressions = children.flatMap(generateExpression(_))
      if (childrenExpressions.length == children.length) {
        Some(new UserDefinedScalarFunc(
          function.name(), function.canonicalName(), childrenExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case Invoke(Literal(obj, _), functionName, _, arguments, _, _, _, _) =>
      obj match {
        case function: ScalarFunction[_] if ScalarFunction.MAGIC_METHOD_NAME == functionName =>
          val argumentExpressions = arguments.flatMap(generateExpression(_))
          if (argumentExpressions.length == arguments.length) {
            Some(new UserDefinedScalarFunc(
              function.name(), function.canonicalName(), argumentExpressions.toArray[V2Expression]))
          } else {
            None
          }
        case _ =>
          None
      }
    case StaticInvoke(_, _, _, arguments, _, _, _, _, Some(scalarFunc)) =>
      val argumentExpressions = arguments.flatMap(generateExpression(_))
      if (argumentExpressions.length == arguments.length) {
        Some(new UserDefinedScalarFunc(
          scalarFunc.name(), scalarFunc.canonicalName(), argumentExpressions.toArray[V2Expression]))
      } else {
        None
      }
    case _ => None
  }

  private def generateAggregateFunc(
      aggregateFunction: AggregateFunction,
      isDistinct: Boolean): Option[AggregateFunc] = aggregateFunction match {
    case aggregate.Min(PushableExpression(expr)) => Some(new Min(expr))
    case aggregate.Max(PushableExpression(expr)) => Some(new Max(expr))
    case count: aggregate.Count if count.children.length == 1 =>
      count.children.head match {
        // COUNT(any literal) is the same as COUNT(*)
        case Literal(_, _) => Some(new CountStar())
        case PushableExpression(expr) => Some(new Count(expr, isDistinct))
        case _ => None
      }
    case aggregate.Sum(PushableExpression(expr), _) => Some(new Sum(expr, isDistinct))
    case aggregate.Average(PushableExpression(expr), _) => Some(new Avg(expr, isDistinct))
    case aggregate.VariancePop(PushableExpression(expr), _) =>
      Some(new GeneralAggregateFunc("VAR_POP", isDistinct, Array(expr)))
    case aggregate.VarianceSamp(PushableExpression(expr), _) =>
      Some(new GeneralAggregateFunc("VAR_SAMP", isDistinct, Array(expr)))
    case aggregate.StddevPop(PushableExpression(expr), _) =>
      Some(new GeneralAggregateFunc("STDDEV_POP", isDistinct, Array(expr)))
    case aggregate.StddevSamp(PushableExpression(expr), _) =>
      Some(new GeneralAggregateFunc("STDDEV_SAMP", isDistinct, Array(expr)))
    case aggregate.CovPopulation(PushableExpression(left), PushableExpression(right), _) =>
      Some(new GeneralAggregateFunc("COVAR_POP", isDistinct, Array(left, right)))
    case aggregate.CovSample(PushableExpression(left), PushableExpression(right), _) =>
      Some(new GeneralAggregateFunc("COVAR_SAMP", isDistinct, Array(left, right)))
    case aggregate.Corr(PushableExpression(left), PushableExpression(right), _) =>
      Some(new GeneralAggregateFunc("CORR", isDistinct, Array(left, right)))
    case aggregate.RegrIntercept(PushableExpression(left), PushableExpression(right)) =>
      Some(new GeneralAggregateFunc("REGR_INTERCEPT", isDistinct, Array(left, right)))
    case aggregate.RegrR2(PushableExpression(left), PushableExpression(right)) =>
      Some(new GeneralAggregateFunc("REGR_R2", isDistinct, Array(left, right)))
    case aggregate.RegrSlope(PushableExpression(left), PushableExpression(right)) =>
      Some(new GeneralAggregateFunc("REGR_SLOPE", isDistinct, Array(left, right)))
    case aggregate.RegrSXY(PushableExpression(left), PushableExpression(right)) =>
      Some(new GeneralAggregateFunc("REGR_SXY", isDistinct, Array(left, right)))
    // Translate Mode if it is deterministic or reverse is defined.
    case aggregate.Mode(PushableExpression(expr), _, _, Some(reverse)) =>
      Some(new GeneralAggregateFunc(
        "MODE", isDistinct, Array.empty, Array(generateSortValue(expr, !reverse))))
    case aggregate.Percentile(
      PushableExpression(left), PushableExpression(right), LongLiteral(1L), _, _, reverse) =>
      Some(new GeneralAggregateFunc("PERCENTILE_CONT", isDistinct,
        Array(right), Array(generateSortValue(left, reverse))))
    case aggregate.PercentileDisc(
      PushableExpression(left), PushableExpression(right), reverse, _, _, _) =>
      Some(new GeneralAggregateFunc("PERCENTILE_DISC", isDistinct,
        Array(right), Array(generateSortValue(left, reverse))))
    // TODO supports other aggregate functions
    case aggregate.V2Aggregator(aggrFunc, children, _, _) =>
      val translatedExprs = children.flatMap(PushableExpression.unapply(_))
      if (translatedExprs.length == children.length) {
        Some(new UserDefinedAggregateFunc(aggrFunc.name(),
          aggrFunc.canonicalName(), isDistinct, translatedExprs.toArray[V2Expression]))
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
      v2ExpressionName: String,
      expr: Expression,
      isPredicate: Boolean): Option[V2Expression] = {
    generateExpressionWithNameByChildren(
      v2ExpressionName, expr.children, expr.dataType, isPredicate)
  }

  private def generateExpressionWithNameByChildren(
      v2ExpressionName: String,
      children: Seq[Expression],
      dataType: DataType,
      isPredicate: Boolean): Option[V2Expression] = {
    val childrenExpressions = children.flatMap(child =>
      generateExpression(
        child,
        isPredicate && child.dataType.isInstanceOf[BooleanType]
      )
    )
    if (childrenExpressions.length == children.length) {
      if (isPredicate && dataType.isInstanceOf[BooleanType]) {
        Some(new V2Predicate(v2ExpressionName, childrenExpressions.toArray[V2Expression]))
      } else {
        Some(new GeneralScalarExpression(
          v2ExpressionName, childrenExpressions.toArray[V2Expression]))
      }
    } else {
      None
    }
  }

  private def generateSortValue(expr: V2Expression, reverse: Boolean): SortValue = if (reverse) {
    SortValue(expr, SortDirection.DESCENDING, NullOrdering.NULLS_LAST)
  } else {
    SortValue(expr, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
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
