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

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.TreePattern.{TIME_WINDOW, TreePattern}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_DAY
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.types._

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_(time_column, window_duration[, slide_duration[, start_time]]) - Bucketize rows into one or more time windows given a timestamp specifying column.
      Window starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window [12:05,12:10) but not in [12:00,12:05).
      Windows can support microsecond precision. Windows in the order of months are not supported.
      See <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time">'Window Operations on Event Time'</a> in Structured Streaming guide doc for detailed explanation and examples.
  """,
  arguments = """
    Arguments:
      * time_column - The column or the expression to use as the timestamp for windowing by time. The time column must be of TimestampType.
      * window_duration - A string specifying the width of the window represented as "interval value".
        (See <a href="https://spark.apache.org/docs/latest/sql-ref-literals.html#interval-literal">Interval Literal</a> for more details.)
        Note that the duration is a fixed length of time, and does not vary over time according to a calendar.
      * slide_duration - A string specifying the sliding interval of the window represented as "interval value".
        A new window will be generated every `slide_duration`. Must be less than or equal to the `window_duration`.
        This duration is likewise absolute, and does not vary according to a calendar.
      * start_time - The offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals.
        For example, in order to have hourly tumbling windows that start 15 minutes past the hour,
        e.g. 12:15-13:15, 13:15-14:15... provide `start_time` as `15 minutes`.
  """,
  examples = """
    Examples:
      > SELECT a, window.start, window.end, count(*) as cnt FROM VALUES ('A1', '2021-01-01 00:00:00'), ('A1', '2021-01-01 00:04:30'), ('A1', '2021-01-01 00:06:00'), ('A2', '2021-01-01 00:01:00') AS tab(a, b) GROUP by a, _FUNC_(b, '5 minutes') ORDER BY a, start;
        A1	2021-01-01 00:00:00	2021-01-01 00:05:00	2
        A1	2021-01-01 00:05:00	2021-01-01 00:10:00	1
        A2	2021-01-01 00:00:00	2021-01-01 00:05:00	1
      > SELECT a, window.start, window.end, count(*) as cnt FROM VALUES ('A1', '2021-01-01 00:00:00'), ('A1', '2021-01-01 00:04:30'), ('A1', '2021-01-01 00:06:00'), ('A2', '2021-01-01 00:01:00') AS tab(a, b) GROUP by a, _FUNC_(b, '10 minutes', '5 minutes') ORDER BY a, start;
        A1	2020-12-31 23:55:00	2021-01-01 00:05:00	2
        A1	2021-01-01 00:00:00	2021-01-01 00:10:00	3
        A1	2021-01-01 00:05:00	2021-01-01 00:15:00	1
        A2	2020-12-31 23:55:00	2021-01-01 00:05:00	1
        A2	2021-01-01 00:00:00	2021-01-01 00:10:00	1
  """,
  group = "datetime_funcs",
  since = "2.0.0")
// scalastyle:on line.size.limit line.contains.tab
case class TimeWindow(
    timeColumn: Expression,
    windowDuration: Long,
    slideDuration: Long,
    startTime: Long) extends UnaryExpression
  with ImplicitCastInputTypes
  with Unevaluable
  with NonSQLExpression
  with QueryErrorsBase {

  //////////////////////////
  // SQL Constructors
  //////////////////////////

  def this(
      timeColumn: Expression,
      windowDuration: Expression,
      slideDuration: Expression,
      startTime: Expression) = {
    this(timeColumn, TimeWindow.parseExpression(windowDuration),
      TimeWindow.parseExpression(slideDuration), TimeWindow.parseExpression(startTime))
  }

  def this(timeColumn: Expression, windowDuration: Expression, slideDuration: Expression) = {
    this(timeColumn, TimeWindow.parseExpression(windowDuration),
      TimeWindow.parseExpression(slideDuration), 0)
  }

  def this(timeColumn: Expression, windowDuration: Expression) = {
    this(timeColumn, windowDuration, windowDuration)
  }

  private def inputTypeOnTimeColumn: AbstractDataType = {
    TypeCollection(
      AnyTimestampType,
      // Below two types cover both time window & session window, since they produce the same type
      // of output as window column.
      new StructType()
        .add(StructField("start", TimestampType))
        .add(StructField("end", TimestampType)),
      new StructType()
        .add(StructField("start", TimestampNTZType))
        .add(StructField("end", TimestampNTZType))
    )
  }

  // NOTE: if the window column is given as a time column, we resolve it to the point of time,
  // which resolves to either TimestampType or TimestampNTZType. That means, timeColumn may not
  // be "resolved", so it is safe to not rely on the data type of timeColumn directly.

  override def child: Expression = timeColumn
  override def inputTypes: Seq[AbstractDataType] = Seq(inputTypeOnTimeColumn)
  override def dataType: DataType = new StructType()
    .add(StructField("start", child.dataType))
    .add(StructField("end", child.dataType))
  override def prettyName: String = "window"
  final override val nodePatterns: Seq[TreePattern] = Seq(TIME_WINDOW)

  // This expression is replaced in the analyzer.
  override lazy val resolved = false

  /**
   * Validate the inputs for the window duration, slide duration, and start time in addition to
   * the input data type.
   */
  override def checkInputDataTypes(): TypeCheckResult = {
    val dataTypeCheck = super.checkInputDataTypes()
    if (dataTypeCheck.isSuccess) {
      if (windowDuration <= 0) {
        return DataTypeMismatch(
          errorSubClass = "VALUE_OUT_OF_RANGE",
          messageParameters = Map(
            "exprName" -> toSQLId("window_duration"),
            "valueRange" -> s"(0, ${Long.MaxValue}]",
            "currentValue" -> toSQLValue(windowDuration, LongType)
          )
        )
      }
      if (slideDuration <= 0) {
        return DataTypeMismatch(
          errorSubClass = "VALUE_OUT_OF_RANGE",
          messageParameters = Map(
            "exprName" -> toSQLId("slide_duration"),
            "valueRange" -> s"(0, ${Long.MaxValue}]",
            "currentValue" -> toSQLValue(slideDuration, LongType)
          )
        )
      }
      if (slideDuration > windowDuration) {
        return DataTypeMismatch(
          errorSubClass = "PARAMETER_CONSTRAINT_VIOLATION",
          messageParameters = Map(
            "leftExprName" -> toSQLId("slide_duration"),
            "leftExprValue" -> toSQLValue(slideDuration, LongType),
            "constraint" -> "<=",
            "rightExprName" -> toSQLId("window_duration"),
            "rightExprValue" -> toSQLValue(windowDuration, LongType)
          )
        )
      }
      if (startTime.abs >= slideDuration) {
        return DataTypeMismatch(
          errorSubClass = "PARAMETER_CONSTRAINT_VIOLATION",
          messageParameters = Map(
            "leftExprName" -> toSQLId("abs(start_time)"),
            "leftExprValue" -> toSQLValue(startTime.abs, LongType),
            "constraint" -> "<",
            "rightExprName" -> toSQLId("slide_duration"),
            "rightExprValue" -> toSQLValue(slideDuration, LongType)
          )
        )
      }
    }
    dataTypeCheck
  }

  override protected def withNewChildInternal(newChild: Expression): TimeWindow =
    copy(timeColumn = newChild)
}

object TimeWindow {
  val marker = "spark.timeWindow"

  /**
   * Parses the interval string for a valid time duration. CalendarInterval expects interval
   * strings to start with the string `interval`. For usability, we prepend `interval` to the string
   * if the user omitted it.
   *
   * @param interval The interval string
   * @return The interval duration in microseconds. SparkSQL casts TimestampType has microsecond
   *         precision.
   */
  def getIntervalInMicroSeconds(interval: String): Long = {
    val cal = IntervalUtils.fromIntervalString(interval)
    if (cal.months != 0) {
      throw new SparkIllegalArgumentException(
        errorClass = "_LEGACY_ERROR_TEMP_3231",
        messageParameters = Map("interval" -> interval))
    }
    Math.addExact(Math.multiplyExact(cal.days, MICROS_PER_DAY), cal.microseconds)
  }

  /**
   * Parses the duration expression to generate the long value for the original constructor so
   * that we can use `window` in SQL.
   */
  def parseExpression(expr: Expression): Long = expr match {
    case NonNullLiteral(s, _: StringType) => getIntervalInMicroSeconds(s.toString)
    case IntegerLiteral(i) => i.toLong
    case NonNullLiteral(l, LongType) => l.toString.toLong
    case _ => throw QueryCompilationErrors.invalidLiteralForWindowDurationError()
  }

  def apply(
      timeColumn: Expression,
      windowDuration: String,
      slideDuration: String,
      startTime: String): TimeWindow = {
    TimeWindow(timeColumn,
      getIntervalInMicroSeconds(windowDuration),
      getIntervalInMicroSeconds(slideDuration),
      getIntervalInMicroSeconds(startTime))
  }
}

/**
 * Expression used internally to convert the TimestampType to Long and back without losing
 * precision, i.e. in microseconds. Used in time windowing.
 */
case class PreciseTimestampConversion(
    child: Expression,
    fromType: DataType,
    toType: DataType) extends UnaryExpression with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = Seq(fromType)
  override def dataType: DataType = toType
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    ev.copy(code = eval.code +
      code"""boolean ${ev.isNull} = ${eval.isNull};
         |${CodeGenerator.javaType(dataType)} ${ev.value} = ${eval.value};
       """.stripMargin)
  }
  override def nullSafeEval(input: Any): Any = input

  override protected def withNewChildInternal(newChild: Expression): PreciseTimestampConversion =
    copy(child = newChild)
}
