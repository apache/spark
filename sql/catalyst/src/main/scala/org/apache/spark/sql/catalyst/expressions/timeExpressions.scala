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

import java.time.format.DateTimeParseException

import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.util.TimeFormatter
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, ObjectType, TimeType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Parses a column to a time based on the given format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(time_str[, fmt]) - Parses the `time_str` expression with the `fmt` expression to
      a time. Returns null with invalid input. By default, it follows casting rules to a time if
      the `fmt` is omitted.
  """,
  arguments = """
    Arguments:
      * time_str - A string to be parsed to time.
      * fmt - Time format pattern to follow. See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a> for valid
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
case class ParseToTime(
    left: Expression,
    format: Option[Expression])
  extends RuntimeReplaceable with ImplicitCastInputTypes {

  def this(left: Expression, format: Expression) = {
    this(left, Option(format))
  }

  def this(left: Expression) = {
    this(left, None)
  }

  override lazy val replacement: Expression = format match {
    case None => Invoke(
      targetObject = Literal.create(new ToTimeParser(), ObjectType(classOf[ToTimeParser])),
      functionName = "parse",
      dataType = TimeType(),
      arguments = Seq(left),
      methodInputTypes = Seq(left.dataType))
    case Some(expr) if expr.foldable => Invoke(
      targetObject = Literal.create(
        ToTimeParser(Some(expr.eval().toString)), ObjectType(classOf[ToTimeParser])),
      functionName = "parse",
      dataType = TimeType(),
      arguments = Seq(left),
      methodInputTypes = Seq(left.dataType))
    case _ => Invoke(
      targetObject = Literal.create(new ToTimeParser(), ObjectType(classOf[ToTimeParser])),
      functionName = "parse",
      dataType = TimeType(),
      arguments = children,
      methodInputTypes = children.map(_.dataType))
  }

  override def prettyName: String = "to_time"

  override def children: Seq[Expression] = left +: format.toSeq

  override def inputTypes: Seq[AbstractDataType] = {
    TypeCollection(
      StringTypeWithCollation(supportsTrimCollation = true)) +:
      format.map(_ => StringTypeWithCollation(supportsTrimCollation = true)).toSeq
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    if (format.isDefined) {
      copy(left = newChildren.head, format = Some(newChildren.last))
    } else {
      copy(left = newChildren.head)
    }
  }
}

case class ToTimeParser(fmt: Option[String]) {
  private lazy val formatter = TimeFormatter(fmt, isParsing = true)

  def this() = this(None)
  private def withErrorCondition(f: => Long): Long = {
    try f
    catch {
      case e: DateTimeParseException => throw QueryExecutionErrors.timeParseError(e)
    }
  }

  def parse(s: UTF8String): Long = withErrorCondition(formatter.parse(s.toString))

  def parse(s: UTF8String, fmt: UTF8String): Long = {
    withErrorCondition(TimeFormatter(fmt.toString, isParsing = true).parse(s.toString))
  }
}
