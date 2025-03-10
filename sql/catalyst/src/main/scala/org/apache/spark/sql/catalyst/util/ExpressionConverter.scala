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

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.EXPR
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{StartsWith, _}
import org.apache.spark.sql.connector.expressions.{Cast => V2Cast, Expression => V2Expression, FieldReference, GeneralScalarExpression, Literal => V2Literal, LiteralValue, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue, Predicate => V2Predicate}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.util.ArrayImplicits._

object ExpressionConverter extends SQLConfHelper with Logging {

  private val converters = Seq(
    LiteralConverter,
    ReferenceConverter,
    MathFunctionConverter,
    PredicateConverter,
    ConditionalFunctionConverter,
    CastConverter)

  def toV1(expr: V2Expression): Option[Expression] = {
    converters.find(_.toV1Func.isDefinedAt(expr)).flatMap(_.toV1Func(expr))
  }

  def toV2(expr: Expression): Option[V2Expression] = {
    converters.find(_.toV2Func.isDefinedAt(expr)).flatMap(_.toV2Func(expr))
  }

  def toV2Predicate(expr: Expression): Option[V2Predicate] = {
    toV2(expr) match {
      case Some(p: V2Predicate) =>
        Some(p)

      case Some(e) if conf.getConf(SQLConf.DATA_SOURCE_DONT_ASSERT_ON_PREDICATE) =>
        // if a predicate is expected but the translation yields something else,
        // log a warning and proceed as if the translation was not possible
        logWarning(log"Predicate expected but got class: ${MDC(EXPR, e.describe())}")
        None

      case Some(e) =>
        throw SparkException.internalError(s"Expected Predicate but got ${e.describe()}")

      case None => None
    }
  }

  private object LiteralConverter extends Converter {
    override def toV1Func: PartialFunction[V2Expression, Option[Literal]] = {
      case _: AlwaysTrue => Some(Literal.TrueLiteral)
      case _: AlwaysFalse => Some(Literal.FalseLiteral)
      case l: V2Literal[_] => Some(Literal(l.value, l.dataType))
    }

    override def toV2Func: PartialFunction[Expression, Option[V2Literal[_]]] = {
      case Literal(true, BooleanType) => Some(new AlwaysTrue())
      case Literal(false, BooleanType) => Some(new AlwaysFalse())
      case Literal(value, dataType) => Some(LiteralValue(value, dataType))
    }
  }

  private object ReferenceConverter extends Converter {
    override def toV1Func: PartialFunction[V2Expression, Option[Attribute]] = {
      case r: NamedReference => Some(UnresolvedAttribute(r.fieldNames.toIndexedSeq))
    }

    override def toV2Func: PartialFunction[Expression, Option[V2Expression]] = {
      case c @ ColumnOrField(nameParts) if c.dataType.isInstanceOf[BooleanType] =>
        val v2Ref = FieldReference(nameParts)
        if (c.dataType.isInstanceOf[BooleanType]) {
          Some(new V2Predicate("=", Array(v2Ref, new AlwaysTrue())))
        } else {
          Some(v2Ref)
        }
    }
  }

  private object MathFunctionConverter extends Converter {
    private val leafToV1Conversions: Map[String, () => Expression] = Map(
      "RAND" -> (() => new Rand()))

    private val unaryToV1Conversions: Map[String, Expression => Expression] = Map(
      "-" -> (child => UnaryMinus(child, failOnError = true)),
      "ABS" -> (child => Abs(child, failOnError = true)),
      "RAND" -> (child => new Rand(child)),
      "LOG10" -> (child => Log10(child)),
      "LOG2" -> (child => Log2(child)),
      "LN" -> (child => Log(child)),
      "EXP" -> (child => Exp(child)),
      "SQRT" -> (child => Sqrt(child)),
      "FLOOR" -> (child => Floor(child)),
      "CEIL" -> (child => Ceil(child)),
      "SIN" -> (child => Sin(child)),
      "SINH" -> (child => Sinh(child)),
      "COS" -> (child => Cos(child)),
      "COSH" -> (child => Cosh(child)),
      "TAN" -> (child => Tan(child)),
      "TANH" -> (child => Tanh(child)),
      "COT" -> (child => Cot(child)),
      "ASIN" -> (child => Asin(child)),
      "ASINH" -> (child => Asinh(child)),
      "ACOS" -> (child => Acos(child)),
      "ACOSH" -> (child => Acosh(child)),
      "ATAN" -> (child => Atan(child)),
      "ATANH" -> (child => Atanh(child)),
      "CBRT" -> (child => Cbrt(child)),
      "DEGREES" -> (child => ToDegrees(child)),
      "RADIANS" -> (child => ToRadians(child)),
      "SIGN" -> (child => Signum(child)))

    private val binaryToV1Conversions: Map[String, (Expression, Expression) => Expression] = Map(
      "+" -> ((left, right) => Add(left, right, evalMode = EvalMode.ANSI)),
      "-" -> ((left, right) => Subtract(left, right, evalMode = EvalMode.ANSI)),
      "*" -> ((left, right) => Multiply(left, right, evalMode = EvalMode.ANSI)),
      "/" -> ((left, right) => Divide(left, right, evalMode = EvalMode.ANSI)),
      "%" -> ((left, right) => Remainder(left, right, evalMode = EvalMode.ANSI)),
      "LOG" -> ((left, right) => Logarithm(left, right)),
      "POWER" -> ((left, right) => Pow(left, right)),
      "ROUND" -> ((left, right) => Round(left, right, ansiEnabled = true)),
      "ATAN2" -> ((left, right) => Atan2(left, right)))

    private val toV1Conversions: Map[String, Seq[Expression] => Expression] = Map(
      "COALESCE" -> (children => Coalesce(children)),
      "GREATEST" -> (children => Greatest(children)),
      "LEAST" -> (children => Least(children)),
      "WIDTH_BUCKET" -> (children => WidthBucket(
        children(0),
        children(1),
        children(2),
        children(3))))

    override def toV1Func: PartialFunction[V2Expression, Option[Expression]] = {
      case e: GeneralScalarExpression
          if e.children.isEmpty && leafToV1Conversions.contains(e.name) =>
        Some(leafToV1Conversions(e.name)())

      case UnaryScalarExpr(name, child) if unaryToV1Conversions.contains(name) =>
        toV1(child).map(v1Child => unaryToV1Conversions(name)(v1Child))

      case BinaryScalarExpr(name, left, right) if binaryToV1Conversions.contains(name) =>
        for {
          v1Left <- toV1(left)
          v1Right <- toV1(right)
        } yield binaryToV1Conversions(name)(v1Left, v1Right)

      case e: GeneralScalarExpression if toV1Conversions.contains(e.name) =>
        val v1Children = e.children.flatMap(toV1)
        if (e.children.length == v1Children.length) {
          Some(toV1Conversions(e.name)(v1Children.toImmutableArraySeq))
        } else {
          None
        }
    }
  }

  private object PredicateConverter extends Converter {

    private val unaryToV1Conversions: Map[String, Expression => Predicate] = Map(
      "IS_NULL" -> (child => IsNull(child)),
      "IS_NOT_NULL" -> (child => IsNotNull(child)),
      "NOT" -> (child => Not(child)))

    private val binaryToV1Conversions: Map[String, (Expression, Expression) => Predicate] = Map(
      "=" -> ((left, right) => EqualTo(left, right)),
      "<=>" -> ((left, right) => EqualNullSafe(left, right)),
      ">" -> ((left, right) => GreaterThan(left, right)),
      ">=" -> ((left, right) => GreaterThanOrEqual(left, right)),
      "<" -> ((left, right) => LessThan(left, right)),
      "<=" -> ((left, right) => LessThanOrEqual(left, right)),
      "<>" -> ((left, right) => Not(EqualTo(left, right))),
      "AND" -> ((left, right) => And(left, right)),
      "OR" -> ((left, right) => Or(left, right)),
      "STARTS_WITH" -> ((left, right) => StartsWith(left, right)),
      "ENDS_WITH" -> ((left, right) => EndsWith(left, right)),
      "CONTAINS" -> ((left, right) => Contains(left, right)))

    private val toV1Conversions: Map[String, Seq[Expression] => Predicate] = Map(
      "IN" -> (children => In(children.head, children.tail)))

    override def toV1Func: PartialFunction[V2Expression, Option[Predicate]] = {
      case UnaryPredicate(name, child) if unaryToV1Conversions.contains(name) =>
        toV1(child).map(v1Child => unaryToV1Conversions(name)(v1Child))

      case BinaryPredicate(name, left, right) if binaryToV1Conversions.contains(name) =>
        for {
          v1Left <- toV1(left)
          v1Right <- toV1(right)
        } yield binaryToV1Conversions(name)(v1Left, v1Right)

      case p: V2Predicate if toV1Conversions.contains(p.name) =>
        val v1Children = p.children.flatMap(toV1)
        if (p.children.length == v1Children.length) {
          Some(toV1Conversions(p.name)(v1Children.toImmutableArraySeq))
        } else {
          None
        }
    }
  }

  private object ConditionalFunctionConverter extends Converter {
    override def toV1Func: PartialFunction[V2Expression, Option[Expression]] = {
      case e: GeneralScalarExpression if e.name == "CASE_WHEN" =>
        val v1Children = e.children.flatMap(toV1)
        if (e.children.length == v1Children.length) {
          if (v1Children.length % 2 == 0) {
            val branches = v1Children.grouped(2).map { case Array(a, b) => (a, b) }.toSeq
            Some(CaseWhen(branches, None))
          } else {
            val (pairs, last) = v1Children.splitAt(v1Children.length - 1)
            val branches = pairs.grouped(2).map { case Array(a, b) => (a, b) }.toSeq
            Some(CaseWhen(branches, Some(last.head)))
          }
        } else {
          None
        }
    }
  }

  private object CastConverter extends Converter {
    override def toV1Func: PartialFunction[V2Expression, Option[Cast]] = {
      case c: V2Cast =>
        toV1(c.expression).map(v1Child => Cast(v1Child, c.dataType, ansiEnabled = true))
    }

    override def toV2Func: PartialFunction[Expression, Option[V2Cast]] = {
      case Cast(child, dataType, _, evalMode)
          if evalMode == EvalMode.ANSI || Cast.canUpCast(child.dataType, dataType) =>
        toV2(child).map(v2Child => new V2Cast(v2Child, child.dataType, dataType))
    }
  }

  private trait Converter {
    def toV1Func: PartialFunction[V2Expression, Option[Expression]] = PartialFunction.empty
    def toV2Func: PartialFunction[Expression, Option[V2Expression]] = PartialFunction.empty
  }

  private object UnaryScalarExpr {
    def unapply(expr: V2Expression): Option[(String, V2Expression)] = expr match {
      case e: GeneralScalarExpression if e.children.length == 1 =>
        Some(e.name, e.children.head)
      case _ =>
        None
    }
  }

  private object UnaryPredicate {
    def unapply(expr: V2Expression): Option[(String, V2Expression)] = expr match {
      case p: V2Predicate if p.children.length == 1 =>
        Some(p.name, p.children.head)
      case _ =>
        None
    }
  }

  private object BinaryScalarExpr {
    def unapply(expr: V2Expression): Option[(String, V2Expression, V2Expression)] = expr match {
      case e: GeneralScalarExpression if e.children.length == 2 =>
        Some(e.name, e.children.apply(0), e.children.apply(1))
      case _ =>
        None
    }
  }

  private object BinaryPredicate {
    def unapply(expr: V2Expression): Option[(String, V2Expression, V2Expression)] = expr match {
      case p: V2Predicate if p.children.length == 2 =>
        Some(p.name, p.children.apply(0), p.children.apply(1))
      case _ =>
        None
    }
  }
}
