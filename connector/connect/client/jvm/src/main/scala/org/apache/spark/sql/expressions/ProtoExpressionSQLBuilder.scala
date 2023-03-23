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

package org.apache.spark.sql.expressions

import scala.collection.JavaConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, InvalidPlanInput, LiteralValueProtoConverter}

/**
 * The builder to generate SQL from proto expressions.
 */
class ProtoExpressionSQLBuilder(expr: proto.Expression, extended: Boolean) {

  def build(): String = generate(expr)

  private def generate(expr: proto.Expression): String = {
    expr.getExprTypeCase match {
      case proto.Expression.ExprTypeCase.LITERAL => visitLiteral(expr.getLiteral)
      case proto.Expression.ExprTypeCase.UNRESOLVED_ATTRIBUTE =>
        visitUnresolvedAttribute(expr.getUnresolvedAttribute)
      case proto.Expression.ExprTypeCase.UNRESOLVED_FUNCTION =>
        visitUnresolvedFunction(expr.getUnresolvedFunction)
      case proto.Expression.ExprTypeCase.UNRESOLVED_EXTRACT_VALUE =>
        visitUnresolvedExtractValue(expr.getUnresolvedExtractValue)
      case proto.Expression.ExprTypeCase.UPDATE_FIELDS =>
        visitUpdateFields(expr.getUpdateFields)
      case proto.Expression.ExprTypeCase.ALIAS => visitAlias(expr.getAlias)
      case proto.Expression.ExprTypeCase.CAST =>
        val cast = expr.getCast
        cast.getCastToTypeCase match {
          case proto.Expression.Cast.CastToTypeCase.TYPE =>
            visitCast(expr.getCast)
          case _ => visitUnexpectedExpr(expr)
        }
      case proto.Expression.ExprTypeCase.SORT_ORDER => visitSortOrder(expr.getSortOrder)
      case _ =>
        throw InvalidPlanInput(
          s"Expression with ID: ${expr.getExprTypeCase.getNumber} is not supported")
    }
  }

  private def visitLiteral(literal: proto.Expression.Literal): String = {
    val value = LiteralValueProtoConverter.toCatalystValue(literal)
    literal.getLiteralTypeCase match {
      case proto.Expression.Literal.LiteralTypeCase.STRING =>
        s"'$value'"
      case _ =>
        s"$value"
    }
  }

  private def visitUnresolvedAttribute(attr: proto.Expression.UnresolvedAttribute): String =
    attr.getUnparsedIdentifier

  private def visitUnresolvedFunction(fun: proto.Expression.UnresolvedFunction): String = {
    val functionName = fun.getFunctionName
    functionName match {
      case "=" | "<=>" | "<" | "<=" | ">" | ">=" =>
        visitBinaryComparison(
          functionName,
          generate(fun.getArguments(0)),
          generate(fun.getArguments(1)))
      case "+" | "-" | "*" | "/" | "%" | "&" | "|" | "^" =>
        visitBinaryArithmetic(
          functionName,
          generate(fun.getArguments(0)),
          generate(fun.getArguments(1)))
      case "and" | "or" =>
        visitOr(functionName, generate(fun.getArguments(0)), generate(fun.getArguments(1)))
      case "negative" =>
        visitUnaryArithmetic("-", generate(fun.getArguments(0)))
      case "!" =>
        visitUnaryArithmetic("NOT", generate(fun.getArguments(0)))
      case "when" =>
        visitCaseWhen(fun.getArgumentsList.asScala.toSeq.map(generate(_)))
      case "isNull" =>
        visitIsNull(generate(fun.getArguments(0)))
      case "isNotNull" =>
        visitIsNotNull(generate(fun.getArguments(0)))
      case "in" =>
        visitJoin(fun.getArgumentsList.asScala.toSeq.map(generate(_)))
      case "like" =>
        visitLike(generate(fun.getArguments(0)), generate(fun.getArguments(1)))
      case "isNaN" =>
        visitCommonFunction("isnan", fun.getArgumentsList.asScala.toSeq.map(generate(_)))
      case "rlike" =>
        visitCommonFunction("RLIKE", fun.getArgumentsList.asScala.toSeq.map(generate(_)))
      case "substr" =>
        visitCommonFunction("substring", fun.getArgumentsList.asScala.toSeq.map(generate(_)))
      case "ilike" | "contains" | "startswith" | "endswith" =>
        visitCommonFunction(functionName, fun.getArgumentsList.asScala.toSeq.map(generate(_)))
      // TODO: Support more functions
    }
  }

  private def visitBinaryComparison(name: String, l: String, r: String): String =
    s"($l $name $r)"

  private def visitBinaryArithmetic(name: String, l: String, r: String): String =
    s"($l $name $r)"

  private def visitUnaryArithmetic(name: String, v: String): String =
    s"($name $v)"

  private def visitOr(name: String, l: String, r: String): String =
    s"($l $name $r)"

  private def visitCaseWhen(children: Seq[String]): String = {
    val sb = new StringBuilder("CASE");
    var i = 0
    while (i < children.length) {
      val c = children(i)
      val j = i + 1
      if (j < children.length) {
        val v = children(j)
        sb.append(" WHEN ")
        sb.append(c)
        sb.append(" THEN ")
        sb.append(v)
      } else {
        sb.append(" ELSE ")
        sb.append(c)
      }

      i += 2
    }
    sb.append(" END")
    sb.toString()
  }

  private def visitIsNull(c: String): String = s"($c IS NULL)"

  private def visitIsNotNull(c: String): String = s"($c IS NOT NULL)"

  private def visitJoin(children: Seq[String]): String =
    s"(${children.head} IN (${children.tail.mkString(", ")}))"

  private def visitLike(l: String, r: String): String =
    s"($l LIKE $r)"

  private def visitCommonFunction(name: String, children: Seq[String]): String =
    s"$name(${children.mkString(", ")})"

  private def visitUnresolvedExtractValue(
      extract: proto.Expression.UnresolvedExtractValue): String = {
    val child = generate(extract.getChild)
    val extraction = generate(extract.getExtraction)
    s"$child[$extraction]"
  }

  private def visitUpdateFields(update: proto.Expression.UpdateFields): String = {
    if (update.hasValueExpression) {
      val col = generate(update.getStructExpression)
      val value = generate(update.getValueExpression)
      s"update_fields($col, WithField($value))"
    } else {
      // drop a field
      val col = generate(update.getStructExpression)
      s"update_fields($col, dropfield())"
    }
  }

  private def visitAlias(alias: proto.Expression.Alias): String = {
    val child = generate(alias.getExpr)
    if (alias.getNameCount == 1) {
      val name = alias.getName(0)
      s"$child AS $name"
    } else {
      s"multialias($child)"
    }
  }

  private def visitCast(cast: proto.Expression.Cast): String = {
    val child = generate(cast.getExpr)
    val catalystType = DataTypeProtoConverter.toCatalystType(cast.getType)
    s"CAST($child AS ${catalystType.sql})"
  }

  private def visitSortOrder(order: proto.Expression.SortOrder): String = {
    val child = generate(order.getChild)
    val direction = order.getDirection match {
      case proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING =>
        "ASC"
      case _ => "DESC"
    }
    val nullOrdering = order.getNullOrdering match {
      case proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST =>
        "NULLS FIRST"
      case _ => "NULLS LAST"
    }

    s"$child $direction $nullOrdering"
  }

  private def visitUnexpectedExpr(expr: proto.Expression) =
    throw new IllegalArgumentException(s"Unexpected proto expression: $expr");
}
