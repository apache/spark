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
package org.apache.spark.sql.execution.datasources.v2.pushdown.sql

import java.math.BigInteger
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.{analysis, expressions => expr}
import org.apache.spark.sql.catalyst.expressions.{BinaryArithmetic, BinaryComparison, BinaryExpression, CheckOverflow, Expression, Literal, PromotePrecision}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * SQL builder class.
 */
object SQLBuilder {

  private def longToReadableTimestamp(t: Long): String = {
    throw new UnsupportedOperationException
    // DateTimeUtils.timestampToString(t) + "." +
    //  "%07d".format(DateTimeUtils.toJavaTimestamp(t).getNanos()/100)
  }

  protected def formatAttributeWithQualifiers(qualifiers: Seq[String], name: String): String =
    (qualifiers :+ name).map({ s => s""""$s"""" }).mkString(".")

  protected def literalToSql(value: Any): String = value match {
    case s: String => s"'$s'"
    case s: UTF8String => s"'$s'"
    case b: Byte => s"$b"
    case i: Int => s"$i"
    case l: Long => s"$l"
    case f: Float => s"$f"
    case d: Double => s"$d"
    case b: Boolean => s"$b"
    case bi: BigInteger => s"$bi"
    case t: Timestamp => s"TO_TIMESTAMP('$t')"
    case d: Date => s"TO_DATE('$d')"
    case null => "NULL"
    case other => other.toString
  }

  def typeToSql(sparkType: DataType): String = sparkType match {
      case `StringType` => "VARCHAR(*)"
      case `IntegerType` => "INTEGER"
      case `ByteType` => "TINYINT"
      case `ShortType` => "SMALLINT"
      case `LongType` => "BIGINT"
      case `FloatType` => "FLOAT"
      case `DoubleType` => "DOUBLE"
      case DecimalType.Fixed(precision, scale) => s"DECIMAL($precision,$scale)"
      case `DateType` => "DATE"
      case `BooleanType` => "BOOLEAN"
      case `TimestampType` => "TIMESTAMP"
      case _ =>
        throw new IllegalArgumentException(s"Type $sparkType cannot be converted to SQL type")
    }

  private def toUnderscoreUpper(str: String): String = {
    var result: String = str(0).toUpper.toString
    for (i <- 1 until str.length) {
      if (str(i-1).isLower && str(i).isUpper) {
        result += '_'
      }
      result += str(i).toUpper
    }
    result
  }

  private def generalExpressionToSql(expression: expr.Expression): String = {
    val clazz = expression.getClass
    val name = try {
      clazz.getDeclaredMethod("prettyName").invoke(expression).asInstanceOf[String]
    } catch {
      case _: NoSuchMethodException =>
        toUnderscoreUpper(clazz.getSimpleName)
    }
    val children = expression.children
    val childStr = children.map(expressionToSql).mkString(", ")
    s"$name($childStr)"
  }

  /**
   * Convenience functions to take several expressions
   */
  def expressionsToSql(expressions: Seq[expr.Expression], delimiter: String = " "): String = {
    expressions.map(expressionToSql).reduceLeft((x, y) => x + delimiter + y)
  }

  def expressionToSql(expression: expr.Expression): String =
    expression match {
      case expr.And(left, right) => s"(${expressionToSql(left)} AND ${expressionToSql(right)})"
      case expr.Or(left, right) => s"(${expressionToSql(left)} OR ${expressionToSql(right)})"
      case expr.Remainder(child, div, _) =>
        s"MOD(${expressionToSql(child)}, ${expressionToSql(div)})"
      case expr.UnaryMinus(child, _) => s"-(${expressionToSql(child)})"
      case expr.IsNull(child) => s"${expressionToSql(child)} IS NULL"
      case expr.IsNotNull(child) => s"${expressionToSql(child)} IS NOT NULL"
      case expr.Like(left, right, _) => s"${expressionToSql(left)} LIKE ${expressionToSql(right)}"
      // TODO: case expr.SortOrder(child,direction) =>
      //  val sortDirection = if (direction == Ascending) "ASC" else "DESC"
      //  s"${expressionToSql(child)} $sortDirection"
      // in Spark 1.5 timestamps are longs and processed internally, however we have to
      // convert that to TO_TIMESTAMP()
      case t@Literal(_, dataType) if dataType.equals(TimestampType) =>
        s"TO_TIMESTAMP('${longToReadableTimestamp(t.value.asInstanceOf[Long])}')"
      case expr.Literal(value, _) => literalToSql(value)
      case expr.Cast(child, dataType, _) =>
        s"CAST(${expressionToSql(child)} AS ${typeToSql(dataType)})"
      // TODO work on that, for SPark 1.6
      // case expr.CountDistinct(children) => s"COUNT(DISTINCT ${expressionsToSql(children, ",")})"
      case expr.aggregate.AggregateExpression(aggFunc, _, _, _, _)
      => s"${aggFunc.prettyName}(${expressionsToSql(aggFunc.children, ",")})"
      case expr.Coalesce(children) => s"COALESCE(${expressionsToSql(children, ",")})"
      case expr.DayOfMonth(date) => s"EXTRACT(DAY FROM ${expressionToSql(date)})"
      case expr.Month(date) => s"EXTRACT(MONTH FROM ${expressionToSql(date)})"
      case expr.Year(date) => s"EXTRACT(YEAR FROM ${expressionToSql(date)})"
      case expr.Hour(date, _) => s"EXTRACT(HOUR FROM ${expressionToSql(date)})"
      case expr.Minute(date, _) => s"EXTRACT(MINUTE FROM ${expressionToSql(date)})"
      case expr.Second(date, _) => s"EXTRACT(SECOND FROM ${expressionToSql(date)})"
      case expr.CurrentDate(_) => s"CURRENT_DATE()"
      case expr.Pow(left, right) => s"POWER(${expressionToSql(left)}, ${expressionToSql(right)})"
      case expr.Substring(str, pos, len) =>
        s"SUBSTRING(${expressionToSql(str)}, $pos, $len)"
      // TODO work on that, for SPark 1.6
      // case expr.Average(child) => s"AVG(${expressionToSql(child)})"
      case expr.In(value, list) =>
        s"${expressionToSql(value)} IN (${list.map(expressionToSql).mkString(", ")})"
      case expr.InSet(value, hset) =>
        s"${expressionToSql(value)} IN (${hset.map(literalToSql).mkString(", ")})"
      case a@expr.Alias(child, name) =>
        s"""${expressionToSql(child)} AS "$name""""
      case a@expr.AttributeReference(name, _, _, _) => s""""$name""""
        // formatAttributeWithQualifiers(a.qualifier, name)
      case analysis.UnresolvedAttribute(name) =>
        formatAttributeWithQualifiers(name.reverse.tail.reverse, name.last)
      case _: analysis.Star => "*"
      case BinarySymbolExpression(left, symbol, right) =>
        s"(${expressionToSql(left)} $symbol ${expressionToSql(right)})"
      case CheckOverflow(child, _, _) => expressionToSql(child)
      case PromotePrecision(child) => expressionToSql(child)
      case x =>
        generalExpressionToSql(x)
    }
}

// TODO optimize this. maybe we can substitute it completely with its logic.
object BinarySymbolExpression {
  def isBinaryExpressionWithSymbol(be: BinaryExpression): Boolean =
    be.isInstanceOf[BinaryArithmetic] || be.isInstanceOf[BinaryComparison]

  def getBinaryExpressionSymbol(be: BinaryExpression): String =
    be match {
      case be: BinaryComparison => be.symbol
      case be: BinaryArithmetic => be.symbol
      case _ => sys.error(s"${be.getClass.getName} has no symbol attribute")
    }

  def unapply(any: Any): Option[(Expression, String, Expression)] = any match {
    case be: BinaryExpression if isBinaryExpressionWithSymbol(be) =>
      Some(be.left, getBinaryExpressionSymbol(be), be.right)
    case _ => None
  }
}
