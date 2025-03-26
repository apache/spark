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

import java.time.DateTimeException

import org.apache.spark.sql.catalyst.analysis.ExpressionBuilder
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.TimeFormatter
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, IntegerType, ObjectType, TimeType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Parses a column to a time based on the given format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str[, format]) - Parses the `str` expression with the `format` expression to a time.
    If `format` is malformed or its application does not result in a well formed time, the function
    raises an error. By default, it follows casting rules to a time if the `format` is omitted.
  """,
  arguments = """
    Arguments:
      * str - A string to be parsed to time.
      * format - Time format pattern to follow. See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a> for valid
                 time format patterns.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('00:12:00');
       00:12:00
      > SELECT _FUNC_('12.10.05', 'HH.mm.ss');
       12:10:05
  """,
  group = "datetime_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class ToTime(str: Expression, format: Option[Expression])
  extends RuntimeReplaceable with ExpectsInputTypes {

  def this(str: Expression, format: Expression) = this(str, Option(format))
  def this(str: Expression) = this(str, None)

  private def invokeParser(
      fmt: Option[String] = None,
      arguments: Seq[Expression] = children): Expression = {
    Invoke(
      targetObject = Literal.create(ToTimeParser(fmt), ObjectType(classOf[ToTimeParser])),
      functionName = "parse",
      dataType = TimeType(),
      arguments = arguments,
      methodInputTypes = arguments.map(_.dataType))
  }

  override lazy val replacement: Expression = format match {
    case None => invokeParser()
    case Some(expr) if expr.foldable =>
      Option(expr.eval())
        .map(f => invokeParser(Some(f.toString), Seq(str)))
        .getOrElse(Literal(null, expr.dataType))
    case _ => invokeParser()
  }

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(StringTypeWithCollation(supportsTrimCollation = true)) ++
      format.map(_ => StringTypeWithCollation(supportsTrimCollation = true))
  }

  override def prettyName: String = "to_time"

  override def children: Seq[Expression] = str +: format.toSeq

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    if (format.isDefined) {
      copy(str = newChildren.head, format = Some(newChildren.last))
    } else {
      copy(str = newChildren.head)
    }
  }
}

case class ToTimeParser(fmt: Option[String]) {
  private lazy val formatter = TimeFormatter(fmt, isParsing = true)

  def this() = this(None)

  private def withErrorCondition(input: => UTF8String, fmt: => Option[String])
      (f: => Long): Long = {
    try f
    catch {
      case e: DateTimeException =>
        throw QueryExecutionErrors.timeParseError(input.toString, fmt, e)
    }
  }

  def parse(s: UTF8String): Long = withErrorCondition(s, fmt)(formatter.parse(s.toString))

  def parse(s: UTF8String, fmt: UTF8String): Long = {
    val format = fmt.toString
    withErrorCondition(s, Some(format)) {
      TimeFormatter(format, isParsing = true).parse(s.toString)
    }
  }
}

/**
 * * Parses a column to a time based on the supplied format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str[, format]) - Parses the `str` expression with the `format` expression to a time.
    If `format` is malformed or its application does not result in a well formed time, the function
    returns NULL. By default, it follows casting rules to a time if the `format` is omitted.
  """,
  arguments = """
    Arguments:
      * str - A string to be parsed to time.
      * format - Time format pattern to follow. See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a> for valid
                 time format patterns.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('00:12:00.001');
       00:12:00.001
      > SELECT _FUNC_('12.10.05.999999', 'HH.mm.ss.SSSSSS');
       12:10:05.999999
      > SELECT _FUNC_('foo', 'HH:mm:ss');
       NULL
  """,
  group = "datetime_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
object TryToTimeExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 1 || numArgs == 2) {
      TryEval(ToTime(expressions.head, expressions.drop(1).lastOption))
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(1, 2), numArgs)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(time_expr) - Returns the minute component of the given time.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(TIME'23:59:59.999999');
       59
  """,
  since = "4.1.0",
  group = "datetime_funcs")
// scalastyle:on line.size.limit
case class MinutesOfTime(child: Expression)
  extends RuntimeReplaceable
    with ExpectsInputTypes {

  override def replacement: Expression = StaticInvoke(
    classOf[DateTimeUtils.type],
    IntegerType,
    "getMinutesOfTime",
    Seq(child),
    Seq(child.dataType)
  )

  override def inputTypes: Seq[AbstractDataType] = Seq(TimeType())

  override def children: Seq[Expression] = Seq(child)

  override def prettyName: String = "minute"

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): Expression = {
    copy(child = newChildren.head)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the minute component of the given expression.

    If `expr` is a TIMESTAMP or a string that can be cast to timestamp,
    it returns the minute of that timestamp.
    If `expr` is a TIME type (since 4.1.0), it returns the minute of the time-of-day.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30 12:58:59');
       58
      > SELECT _FUNC_(TIME'23:59:59.999999');
       59
  """,
  since = "1.5.0",
  group = "datetime_funcs")
// scalastyle:on line.size.limit
object MinuteExpressionBuilder extends ExpressionBuilder {
  override def build(name: String, expressions: Seq[Expression]): Expression = {
    if (expressions.isEmpty) {
      throw QueryCompilationErrors.wrongNumArgsError(name, Seq("> 0"), expressions.length)
    } else {
      val child = expressions.head
      child.dataType match {
        case _: TimeType =>
          MinutesOfTime(child)
        case _ =>
          Minute(child)
      }
    }
  }
}
