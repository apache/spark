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
import java.util.Locale

import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.Cast.{toSQLExpr, toSQLId, toSQLType, toSQLValue}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.trees.TreePattern.{CURRENT_LIKE, TreePattern}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.TimeFormatter
import org.apache.spark.sql.catalyst.util.TypeUtils.{ordinalNumber}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, DataType, DecimalType, IntegerType, ObjectType, TimeType, TypeCollection}
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

object TimePart {

  def parseExtractField(extractField: String, source: Expression): Expression =
    extractField.toUpperCase(Locale.ROOT) match {
      case "HOUR" | "H" | "HOURS" | "HR" | "HRS" => HoursOfTime(source)
      case "MINUTE" | "M" | "MIN" | "MINS" | "MINUTES" => MinutesOfTime(source)
      case "SECOND" | "S" | "SEC" | "SECONDS" | "SECS" => SecondsOfTimeWithFraction(source)
      case _ =>
        throw QueryCompilationErrors.literalTypeUnsupportedForSourceTypeError(
          extractField,
          source)
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

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(TimeType.MIN_PRECISION to TimeType.MAX_PRECISION map TimeType.apply: _*))

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

case class HoursOfTime(child: Expression)
  extends RuntimeReplaceable
    with ExpectsInputTypes {

  override def replacement: Expression = StaticInvoke(
    classOf[DateTimeUtils.type],
    IntegerType,
    "getHoursOfTime",
    Seq(child),
    Seq(child.dataType)
  )

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(TimeType.MIN_PRECISION to TimeType.MAX_PRECISION map TimeType.apply: _*))

  override def children: Seq[Expression] = Seq(child)

  override def prettyName: String = "hour"

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): Expression = {
    copy(child = newChildren.head)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the hour component of the given expression.

    If `expr` is a TIMESTAMP or a string that can be cast to timestamp,
    it returns the hour of that timestamp.
    If `expr` is a TIME type (since 4.1.0), it returns the hour of the time-of-day.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('2018-02-14 12:58:59');
       12
      > SELECT _FUNC_(TIME'13:59:59.999999');
       13
  """,
  since = "1.5.0",
  group = "datetime_funcs")
object HourExpressionBuilder extends ExpressionBuilder {
  override def build(name: String, expressions: Seq[Expression]): Expression = {
    if (expressions.isEmpty) {
      throw QueryCompilationErrors.wrongNumArgsError(name, Seq("> 0"), expressions.length)
    } else {
      val child = expressions.head
      child.dataType match {
        case _: TimeType =>
          HoursOfTime(child)
        case _ =>
          Hour(child)
      }
    }
  }
}

case class SecondsOfTimeWithFraction(child: Expression)
  extends RuntimeReplaceable
  with ExpectsInputTypes {

  override def replacement: Expression = {

    StaticInvoke(
      classOf[DateTimeUtils.type],
      DecimalType(8, 6),
      "getSecondsOfTimeWithFraction",
      Seq(child, Literal(precision)),
      Seq(child.dataType, IntegerType))
  }
  private val precision: Int = child.dataType.asInstanceOf[TimeType].precision

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TimeType(precision))

  override def children: Seq[Expression] = Seq(child)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(child = newChildren.head)
  }
}

case class SecondsOfTime(child: Expression)
  extends RuntimeReplaceable
    with ExpectsInputTypes {

  override def replacement: Expression = StaticInvoke(
    classOf[DateTimeUtils.type],
    IntegerType,
    "getSecondsOfTime",
    Seq(child),
    Seq(child.dataType)
  )

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(TimeType.MIN_PRECISION to TimeType.MAX_PRECISION map TimeType.apply: _*))

  override def children: Seq[Expression] = Seq(child)

  override def prettyName: String = "second"

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): Expression = {
      copy(child = newChildren.head)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the second component of the given expression.

    If `expr` is a TIMESTAMP or a string that can be cast to timestamp,
    it returns the second of that timestamp.
    If `expr` is a TIME type (since 4.1.0), it returns the second of the time-of-day.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('2018-02-14 12:58:59');
       59
      > SELECT _FUNC_(TIME'13:25:59.999999');
       59
  """,
  since = "1.5.0",
  group = "datetime_funcs")
object SecondExpressionBuilder extends ExpressionBuilder {
  override def build(name: String, expressions: Seq[Expression]): Expression = {
    if (expressions.isEmpty) {
      throw QueryCompilationErrors.wrongNumArgsError(name, Seq("> 0"), expressions.length)
    } else {
      val child = expressions.head
      child.dataType match {
        case _: TimeType =>
          SecondsOfTime(child)
        case _ =>
          Second(child)
      }
    }
  }
}

/**
 * Returns the current time at the start of query evaluation.
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_([precision]) - Returns the current time at the start of query evaluation.
    All calls of current_time within the same query return the same value.

    _FUNC_ - Returns the current time at the start of query evaluation.
  """,
  arguments = """
    Arguments:
      * precision - An optional integer literal in the range [0..6], indicating how many
                    fractional digits of seconds to include. If omitted, the default is 6.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_();
       15:49:11.914120
      > SELECT _FUNC_;
       15:49:11.914120
      > SELECT _FUNC_(0);
       15:49:11
      > SELECT _FUNC_(3);
       15:49:11.914
      > SELECT _FUNC_(1+1);
       15:49:11.91
  """,
  group = "datetime_funcs",
  since = "4.1.0"
)
case class CurrentTime(child: Expression = Literal(TimeType.MICROS_PRECISION))
  extends UnaryExpression with FoldableUnevaluable with ImplicitCastInputTypes {

  def this() = {
    this(Literal(TimeType.MICROS_PRECISION))
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(CURRENT_LIKE)

  override def nullable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    // Check foldability
    if (!child.foldable) {
      return DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("precision"),
          "inputType" -> toSQLType(child.dataType),
          "inputExpr" -> toSQLExpr(child)
        )
      )
    }

    // Evaluate
    val precisionValue = child.eval()
    if (precisionValue == null) {
      return DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "precision"))
    }

    // Check numeric range
    precisionValue match {
      case n: Number =>
        val p = n.intValue()
        if (p < TimeType.MIN_PRECISION || p > TimeType.MICROS_PRECISION) {
          return DataTypeMismatch(
            errorSubClass = "VALUE_OUT_OF_RANGE",
            messageParameters = Map(
              "exprName" -> toSQLId("precision"),
              "valueRange" -> s"[${TimeType.MIN_PRECISION}, ${TimeType.MICROS_PRECISION}]",
              "currentValue" -> toSQLValue(p, IntegerType)
            )
          )
        }
      case _ =>
        return DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(0),
            "requiredType" -> toSQLType(IntegerType),
            "inputSql" -> toSQLExpr(child),
            "inputType" -> toSQLType(child.dataType))
        )
    }
    TypeCheckSuccess
  }

  // Because checkInputDataTypes ensures the argument is foldable & valid,
  // we can directly evaluate here.
  lazy val precision: Int = child.eval().asInstanceOf[Number].intValue()

  override def dataType: DataType = TimeType(precision)

  override def prettyName: String = "current_time"

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(hour, minute, second) - Create time from hour, minute and second fields. For invalid inputs it will throw an error.",
  arguments = """
    Arguments:
      * hour - the hour to represent, from 0 to 23
      * minute - the minute to represent, from 0 to 59
      * second - the second to represent, from 0 to 59.999999
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(6, 30, 45.887);
       06:30:45.887
      > SELECT _FUNC_(NULL, 30, 0);
       NULL
  """,
  group = "datetime_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class MakeTime(
    hours: Expression,
    minutes: Expression,
    secsAndMicros: Expression)
  extends RuntimeReplaceable
    with ImplicitCastInputTypes
    with ExpectsInputTypes {

  // Accept `sec` as DecimalType to avoid loosing precision of microseconds while converting
  // it to the fractional part of `sec`. If `sec` is an IntegerType, it can be cast into decimal
  // safely because we use DecimalType(16, 6) which is wider than DecimalType(10, 0).
  override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType, IntegerType, DecimalType(16, 6))
  override def children: Seq[Expression] = Seq(hours, minutes, secsAndMicros)
  override def prettyName: String = "make_time"

  override def replacement: Expression = StaticInvoke(
    classOf[DateTimeUtils.type],
    TimeType(TimeType.MICROS_PRECISION),
    "timeToMicros",
    children,
    inputTypes
  )

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): MakeTime =
    copy(hours = newChildren(0), minutes = newChildren(1), secsAndMicros = newChildren(2))
}
