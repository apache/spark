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

package org.apache.spark.sql.catalyst.expressions

import java.lang.reflect.{Method, Modifier}

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{FUNCTION_NAME, FUNCTION_PARAM}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.{NoSuchFunctionException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.encoders.EncoderUtils
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{FunctionCatalog, Identifier}
import org.apache.spark.sql.connector.catalog.functions._
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction.MAGIC_METHOD_NAME
import org.apache.spark.sql.connector.expressions.{BucketTransform, Cast => V2Cast, Expression => V2Expression, FieldReference, GeneralScalarExpression, IdentityTransform, Literal => V2Literal, NamedReference, NamedTransform, NullOrdering => V2NullOrdering, SortDirection => V2SortDirection, SortOrder => V2SortOrder, SortValue, Transform}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue}
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * A utility class that converts public connector expressions into Catalyst expressions.
 */
object V2ExpressionUtils extends SQLConfHelper with Logging {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper

  def resolveRef[T <: NamedExpression](ref: NamedReference, plan: LogicalPlan): T = {
    plan.resolve(ref.fieldNames.toImmutableArraySeq, conf.resolver) match {
      case Some(namedExpr) =>
        namedExpr.asInstanceOf[T]
      case None =>
        val name = ref.fieldNames.toImmutableArraySeq.quoted
        val outputString = plan.output.map(_.name).mkString(",")
        throw QueryCompilationErrors.cannotResolveAttributeError(name, outputString)
    }
  }

  def resolveRefs[T <: NamedExpression](refs: Seq[NamedReference], plan: LogicalPlan): Seq[T] = {
    refs.map(ref => resolveRef[T](ref, plan))
  }

  /**
   * Converts the array of input V2 [[V2SortOrder]] into their counterparts in catalyst.
   */
  def toCatalystOrdering(
      ordering: Array[V2SortOrder],
      query: LogicalPlan,
      funCatalogOpt: Option[FunctionCatalog] = None): Seq[SortOrder] = {
    ordering.map(toCatalyst(_, query, funCatalogOpt).asInstanceOf[SortOrder]).toImmutableArraySeq
  }

  def toCatalyst(
      expr: V2Expression,
      query: LogicalPlan,
      funCatalogOpt: Option[FunctionCatalog] = None): Expression =
    toCatalystOpt(expr, query, funCatalogOpt)
        .getOrElse(throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3054", messageParameters = Map("expr" -> expr.toString)))

  def toCatalystOpt(
      expr: V2Expression,
      query: LogicalPlan,
      funCatalogOpt: Option[FunctionCatalog] = None): Option[Expression] = {
    expr match {
      case l: V2Literal[_] =>
        Some(Literal.create(l.value, l.dataType))
      case t: Transform =>
        toCatalystTransformOpt(t, query, funCatalogOpt)
      case SortValue(child, direction, nullOrdering) =>
        toCatalystOpt(child, query, funCatalogOpt).map { catalystChild =>
          SortOrder(catalystChild, toCatalyst(direction), toCatalyst(nullOrdering), Seq.empty)
        }
      case ref: FieldReference =>
        Some(resolveRef[NamedExpression](ref, query))
      case _ =>
        throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3054",
          messageParameters = Map("expr" -> expr.toString))
    }
  }

  def toCatalystTransformOpt(
      trans: Transform,
      query: LogicalPlan,
      funCatalogOpt: Option[FunctionCatalog] = None): Option[Expression] = trans match {
    case IdentityTransform(ref) =>
      Some(resolveRef[NamedExpression](ref, query))
    case BucketTransform(numBuckets, refs, sorted)
        if sorted.isEmpty && refs.length == 1 && refs.forall(_.isInstanceOf[NamedReference]) =>
      val resolvedRefs = refs.map(r => resolveRef[NamedExpression](r, query))
      // Create a dummy reference for `numBuckets` here and use that, together with `refs`, to
      // look up the V2 function.
      val numBucketsRef = AttributeReference("numBuckets", IntegerType, nullable = false)()
      funCatalogOpt.flatMap { catalog =>
        loadV2FunctionOpt(catalog, "bucket", Seq(numBucketsRef) ++ resolvedRefs).map { bound =>
          TransformExpression(bound, resolvedRefs, Some(numBuckets))
        }
      }
    case NamedTransform(name, args) =>
      val catalystArgs = args.map(toCatalyst(_, query, funCatalogOpt))
      funCatalogOpt.flatMap { catalog =>
        loadV2FunctionOpt(catalog, name, catalystArgs).map { bound =>
          TransformExpression(bound, catalystArgs)
        }
      }
  }

  private def loadV2FunctionOpt(
      catalog: FunctionCatalog,
      name: String,
      args: Seq[Expression]): Option[BoundFunction] = {
    val inputType = StructType(args.zipWithIndex.map {
      case (exp, pos) => StructField(s"_$pos", exp.dataType, exp.nullable)
    })
    try {
      val unbound = catalog.loadFunction(Identifier.of(Array.empty, name))
      Some(unbound.bind(inputType))
    } catch {
      case _: NoSuchFunctionException =>
        val parameterString = args.map(_.dataType.typeName).mkString("(", ", ", ")")
        logWarning(log"V2 function ${MDC(FUNCTION_NAME, name)} " +
          log"with parameter types ${MDC(FUNCTION_PARAM, parameterString)} is used in " +
          log"partition transforms, but its definition couldn't be found in the function catalog " +
          log"provided")
        None
      case _: UnsupportedOperationException =>
        None
    }
  }

  private def toCatalyst(direction: V2SortDirection): SortDirection = direction match {
    case V2SortDirection.ASCENDING => Ascending
    case V2SortDirection.DESCENDING => Descending
  }

  private def toCatalyst(nullOrdering: V2NullOrdering): NullOrdering = nullOrdering match {
    case V2NullOrdering.NULLS_FIRST => NullsFirst
    case V2NullOrdering.NULLS_LAST => NullsLast
  }

  def resolveScalarFunction(
      scalarFunc: ScalarFunction[_],
      arguments: Seq[Expression]): Expression = {
    val declaredInputTypes = scalarFunc.inputTypes().toImmutableArraySeq
    val argClasses = declaredInputTypes.map(EncoderUtils.dataTypeJavaClass)
    findMethod(scalarFunc, MAGIC_METHOD_NAME, argClasses) match {
      case Some(m) if Modifier.isStatic(m.getModifiers) =>
        StaticInvoke(scalarFunc.getClass, scalarFunc.resultType(),
          MAGIC_METHOD_NAME, arguments, inputTypes = declaredInputTypes,
          propagateNull = false, returnNullable = scalarFunc.isResultNullable,
          isDeterministic = scalarFunc.isDeterministic, scalarFunction = Some(scalarFunc))
      case Some(_) =>
        val caller = Literal.create(scalarFunc, ObjectType(scalarFunc.getClass))
        Invoke(caller, MAGIC_METHOD_NAME, scalarFunc.resultType(),
          arguments, methodInputTypes = declaredInputTypes, propagateNull = false,
          returnNullable = scalarFunc.isResultNullable,
          isDeterministic = scalarFunc.isDeterministic)
      case _ =>
        // TODO: handle functions defined in Scala too - in Scala, even if a
        //  subclass do not override the default method in parent interface
        //  defined in Java, the method can still be found from
        //  `getDeclaredMethod`.
        findMethod(scalarFunc, "produceResult", Seq(classOf[InternalRow])) match {
          case Some(_) =>
            ApplyFunctionExpression(scalarFunc, arguments)
          case _ =>
            throw new AnalysisException(
              errorClass = "SCALAR_FUNCTION_NOT_FULLY_IMPLEMENTED",
              messageParameters = Map("scalarFunc" -> toSQLId(scalarFunc.name())))
        }
    }
  }

  /**
   * Check if the input `fn` implements the given `methodName` with parameter types specified
   * via `argClasses`.
   */
  private def findMethod(
      fn: BoundFunction,
      methodName: String,
      argClasses: Seq[Class[_]]): Option[Method] = {
    val cls = fn.getClass
    try {
      Some(cls.getDeclaredMethod(methodName, argClasses: _*))
    } catch {
      case _: NoSuchMethodException =>
        None
    }
  }

  def toCatalyst(expr: V2Expression): Option[Expression] = expr match {
    case _: AlwaysTrue => Some(Literal.TrueLiteral)
    case _: AlwaysFalse => Some(Literal.FalseLiteral)
    case l: V2Literal[_] => Some(Literal(l.value, l.dataType))
    case r: NamedReference => Some(UnresolvedAttribute(r.fieldNames.toImmutableArraySeq))
    case c: V2Cast => toCatalyst(c.expression).map(Cast(_, c.dataType, ansiEnabled = true))
    case e: GeneralScalarExpression => convertScalarExpr(e)
    case _ => None
  }

  private def convertScalarExpr(expr: GeneralScalarExpression): Option[Expression] = {
    convertPredicate(expr)
      .orElse(convertConditionalFunc(expr))
      .orElse(convertMathFunc(expr))
      .orElse(convertBitwiseFunc(expr))
      .orElse(convertTrigonometricFunc(expr))
  }

  private def convertPredicate(expr: GeneralScalarExpression): Option[Expression] = {
    expr.name match {
      case "IS_NULL" => convertUnaryExpr(expr, IsNull)
      case "IS_NOT_NULL" => convertUnaryExpr(expr, IsNotNull)
      case "NOT" => convertUnaryExpr(expr, Not)
      case "=" => convertBinaryExpr(expr, EqualTo)
      case "<=>" => convertBinaryExpr(expr, EqualNullSafe)
      case ">" => convertBinaryExpr(expr, GreaterThan)
      case ">=" => convertBinaryExpr(expr, GreaterThanOrEqual)
      case "<" => convertBinaryExpr(expr, LessThan)
      case "<=" => convertBinaryExpr(expr, LessThanOrEqual)
      case "<>" => convertBinaryExpr(expr, (left, right) => Not(EqualTo(left, right)))
      case "AND" => convertBinaryExpr(expr, And)
      case "OR" => convertBinaryExpr(expr, Or)
      case "STARTS_WITH" => convertBinaryExpr(expr, StartsWith)
      case "ENDS_WITH" => convertBinaryExpr(expr, EndsWith)
      case "CONTAINS" => convertBinaryExpr(expr, Contains)
      case "IN" => convertExpr(expr, children => In(children.head, children.tail))
      case _ => None
    }
  }

  private def convertConditionalFunc(expr: GeneralScalarExpression): Option[Expression] = {
    expr.name match {
      case "CASE_WHEN" =>
        convertExpr(expr, children =>
          if (children.length % 2 == 0) {
            val branches = children.grouped(2).map { case Seq(c, v) => (c, v) }.toSeq
            CaseWhen(branches, None)
          } else {
            val (pairs, last) = children.splitAt(children.length - 1)
            val branches = pairs.grouped(2).map { case Seq(c, v) => (c, v) }.toSeq
            CaseWhen(branches, Some(last.head))
          })
      case _ => None
    }
  }

  private def convertMathFunc(expr: GeneralScalarExpression): Option[Expression] = {
    expr.name match {
      case "+" => convertBinaryExpr(expr, Add(_, _, evalMode = EvalMode.ANSI))
      case "-" =>
        if (expr.children.length == 1) {
          convertUnaryExpr(expr, UnaryMinus(_, failOnError = true))
        } else if (expr.children.length == 2) {
          convertBinaryExpr(expr, Subtract(_, _, evalMode = EvalMode.ANSI))
        } else {
          None
        }
      case "*" => convertBinaryExpr(expr, Multiply(_, _, evalMode = EvalMode.ANSI))
      case "/" => convertBinaryExpr(expr, Divide(_, _, evalMode = EvalMode.ANSI))
      case "%" => convertBinaryExpr(expr, Remainder(_, _, evalMode = EvalMode.ANSI))
      case "ABS" => convertUnaryExpr(expr, Abs(_, failOnError = true))
      case "COALESCE" => convertExpr(expr, Coalesce)
      case "GREATEST" => convertExpr(expr, Greatest)
      case "LEAST" => convertExpr(expr, Least)
      case "RAND" =>
        if (expr.children.isEmpty) {
          Some(new Rand())
        } else if (expr.children.length == 1) {
          convertUnaryExpr(expr, new Rand(_))
        } else {
          None
        }
      case "LOG" => convertBinaryExpr(expr, Logarithm)
      case "LOG10" => convertUnaryExpr(expr, Log10)
      case "LOG2" => convertUnaryExpr(expr, Log2)
      case "LN" => convertUnaryExpr(expr, Log)
      case "EXP" => convertUnaryExpr(expr, Exp)
      case "POWER" => convertBinaryExpr(expr, Pow)
      case "SQRT" => convertUnaryExpr(expr, Sqrt)
      case "FLOOR" => convertUnaryExpr(expr, Floor)
      case "CEIL" => convertUnaryExpr(expr, Ceil)
      case "ROUND" => convertBinaryExpr(expr, Round(_, _, ansiEnabled = true))
      case "CBRT" => convertUnaryExpr(expr, Cbrt)
      case "DEGREES" => convertUnaryExpr(expr, ToDegrees)
      case "RADIANS" => convertUnaryExpr(expr, ToRadians)
      case "SIGN" => convertUnaryExpr(expr, Signum)
      case "WIDTH_BUCKET" =>
        convertExpr(
          expr,
          children => WidthBucket(children(0), children(1), children(2), children(3)))
      case _ => None
    }
  }

  private def convertTrigonometricFunc(expr: GeneralScalarExpression): Option[Expression] = {
    expr.name match {
      case "SIN" => convertUnaryExpr(expr, Sin)
      case "SINH" => convertUnaryExpr(expr, Sinh)
      case "COS" => convertUnaryExpr(expr, Cos)
      case "COSH" => convertUnaryExpr(expr, Cosh)
      case "TAN" => convertUnaryExpr(expr, Tan)
      case "TANH" => convertUnaryExpr(expr, Tanh)
      case "COT" => convertUnaryExpr(expr, Cot)
      case "ASIN" => convertUnaryExpr(expr, Asin)
      case "ASINH" => convertUnaryExpr(expr, Asinh)
      case "ACOS" => convertUnaryExpr(expr, Acos)
      case "ACOSH" => convertUnaryExpr(expr, Acosh)
      case "ATAN" => convertUnaryExpr(expr, Atan)
      case "ATANH" => convertUnaryExpr(expr, Atanh)
      case "ATAN2" => convertBinaryExpr(expr, Atan2)
      case _ => None
    }
  }

  private def convertBitwiseFunc(expr: GeneralScalarExpression): Option[Expression] = {
    expr.name match {
      case "~" => convertUnaryExpr(expr, BitwiseNot)
      case "&" => convertBinaryExpr(expr, BitwiseAnd)
      case "|" => convertBinaryExpr(expr, BitwiseOr)
      case "^" => convertBinaryExpr(expr, BitwiseXor)
      case _ => None
    }
  }

  private def convertUnaryExpr(
      expr: GeneralScalarExpression,
      catalystExprBuilder: Expression => Expression): Option[Expression] = {
    expr.children match {
      case Array(child) => toCatalyst(child).map(catalystExprBuilder)
      case _ => None
    }
  }

  private def convertBinaryExpr(
     expr: GeneralScalarExpression,
     catalystExprBuilder: (Expression, Expression) => Expression): Option[Expression] = {
    expr.children match {
      case Array(left, right) =>
        for {
          catalystLeft <- toCatalyst(left)
          catalystRight <- toCatalyst(right)
        } yield catalystExprBuilder(catalystLeft, catalystRight)
      case _ => None
    }
  }

  private def convertExpr(
      expr: GeneralScalarExpression,
      catalystExprBuilder: Seq[Expression] => Expression): Option[Expression] = {
    val catalystChildren = expr.children.flatMap(toCatalyst).toImmutableArraySeq
    if (expr.children.length == catalystChildren.length) {
      Some(catalystExprBuilder(catalystChildren))
    } else {
      None
    }
  }
}
