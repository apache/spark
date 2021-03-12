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

import java.sql.Timestamp
import java.time.{DateTimeException, LocalDate, LocalDateTime, ZoneId}
import java.time.temporal.IsoFields
import java.util.Locale

import scala.util.control.NonFatal

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.SparkUpgradeException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, LegacyDateFormats, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.SIMPLE_DATE_FORMAT
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Common base class for time zone aware expressions.
 */
trait TimeZoneAwareExpression extends Expression {
  /** The expression is only resolved when the time zone has been set. */
  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess && timeZoneId.isDefined

  /** the timezone ID to be used to evaluate value. */
  def timeZoneId: Option[String]

  /** Returns a copy of this expression with the specified timeZoneId. */
  def withTimeZone(timeZoneId: String): TimeZoneAwareExpression

  @transient lazy val zoneId: ZoneId = DateTimeUtils.getZoneId(timeZoneId.get)
}

/**
 * Returns the current date at the start of query evaluation.
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_() - Returns the current date at the start of query evaluation. All calls of current_date within the same query return the same value.

    _FUNC_ - Returns the current date at the start of query evaluation.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_();
       2020-04-25
      > SELECT _FUNC_;
       2020-04-25
  """,
  note = """
    The syntax without braces has been supported since 2.0.1.
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class CurrentDate(timeZoneId: Option[String] = None)
  extends LeafExpression with TimeZoneAwareExpression with CodegenFallback {

  def this() = this(None)

  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = DateType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def eval(input: InternalRow): Any = currentDate(zoneId)

  override def prettyName: String = "current_date"
}

abstract class CurrentTimestampLike() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false
  override def dataType: DataType = TimestampType
  override def eval(input: InternalRow): Any = currentTimestamp()
}

/**
 * Returns the current timestamp at the start of query evaluation.
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_() - Returns the current timestamp at the start of query evaluation. All calls of current_timestamp within the same query return the same value.

    _FUNC_ - Returns the current timestamp at the start of query evaluation.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_();
       2020-04-25 15:49:11.914
      > SELECT _FUNC_;
       2020-04-25 15:49:11.914
  """,
  note = """
    The syntax without braces has been supported since 2.0.1.
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class CurrentTimestamp() extends CurrentTimestampLike {
  override def prettyName: String = "current_timestamp"
}

@ExpressionDescription(
  usage = "_FUNC_() - Returns the current timestamp at the start of query evaluation.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       2020-04-25 15:49:11.914
  """,
  group = "datetime_funcs",
  since = "1.6.0")
case class Now() extends CurrentTimestampLike {
  override def prettyName: String = "now"
}

/**
 * Expression representing the current batch time, which is used by StreamExecution to
 * 1. prevent optimizer from pushing this expression below a stateful operator
 * 2. allow IncrementalExecution to substitute this expression with a Literal(timestamp)
 *
 * There is no code generation since this expression should be replaced with a literal.
 */
case class CurrentBatchTimestamp(
    timestampMs: Long,
    dataType: DataType,
    timeZoneId: Option[String] = None)
  extends LeafExpression with TimeZoneAwareExpression with Nondeterministic with CodegenFallback {

  def this(timestampMs: Long, dataType: DataType) = this(timestampMs, dataType, None)

  override def nullable: Boolean = false

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def prettyName: String = "current_batch_timestamp"

  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  /**
   * Need to return literal value in order to support compile time expression evaluation
   * e.g., select(current_date())
   */
  override protected def evalInternal(input: InternalRow): Any = toLiteral.value

  def toLiteral: Literal = dataType match {
    case _: TimestampType =>
      Literal(DateTimeUtils.fromJavaTimestamp(new Timestamp(timestampMs)), TimestampType)
    case _: DateType => Literal(DateTimeUtils.millisToDays(timestampMs, zoneId), DateType)
  }
}

/**
 * Adds a number of days to startdate.
 */
@ExpressionDescription(
  usage = "_FUNC_(start_date, num_days) - Returns the date that is `num_days` after `start_date`.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-07-30', 1);
       2016-07-31
  """,
  group = "datetime_funcs",
  since = "1.5.0")
case class DateAdd(startDate: Expression, days: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def left: Expression = startDate
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] =
    Seq(DateType, TypeCollection(IntegerType, ShortType, ByteType))

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, d: Any): Any = {
    start.asInstanceOf[Int] + d.asInstanceOf[Number].intValue()
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (sd, d) => {
      s"""${ev.value} = $sd + $d;"""
    })
  }

  override def prettyName: String = "date_add"
}

/**
 * Subtracts a number of days to startdate.
 */
@ExpressionDescription(
  usage = "_FUNC_(start_date, num_days) - Returns the date that is `num_days` before `start_date`.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-07-30', 1);
       2016-07-29
  """,
  group = "datetime_funcs",
  since = "1.5.0")
case class DateSub(startDate: Expression, days: Expression)
  extends BinaryExpression with ExpectsInputTypes {
  override def left: Expression = startDate
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] =
    Seq(DateType, TypeCollection(IntegerType, ShortType, ByteType))

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, d: Any): Any = {
    start.asInstanceOf[Int] - d.asInstanceOf[Number].intValue()
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (sd, d) => {
      s"""${ev.value} = $sd - $d;"""
    })
  }

  override def prettyName: String = "date_sub"
}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns the hour component of the string/timestamp.",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30 12:58:59');
       12
  """,
  group = "datetime_funcs",
  since = "1.5.0")
case class Hour(child: Expression, timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(child: Expression) = this(child, None)

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getHours(timestamp.asInstanceOf[Long], zoneId)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getHours($c, $zid)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns the minute component of the string/timestamp.",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30 12:58:59');
       58
  """,
  group = "datetime_funcs",
  since = "1.5.0")
case class Minute(child: Expression, timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(child: Expression) = this(child, None)

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getMinutes(timestamp.asInstanceOf[Long], zoneId)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getMinutes($c, $zid)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns the second component of the string/timestamp.",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30 12:58:59');
       59
  """,
  group = "datetime_funcs",
  since = "1.5.0")
case class Second(child: Expression, timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(child: Expression) = this(child, None)

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getSeconds(timestamp.asInstanceOf[Long], zoneId)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getSeconds($c, $zid)")
  }
}

case class SecondWithFraction(child: Expression, timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(child: Expression) = this(child, None)

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  // 2 digits for seconds, and 6 digits for the fractional part with microsecond precision.
  override def dataType: DataType = DecimalType(8, 6)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getSecondsWithFraction(timestamp.asInstanceOf[Long], zoneId)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getSecondsWithFraction($c, $zid)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the day of year of the date/timestamp.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-04-09');
       100
  """,
  group = "datetime_funcs",
  since = "1.5.0")
case class DayOfYear(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getDayInYear(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getDayInYear($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the year component of the date/timestamp.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-07-30');
       2016
  """,
  group = "datetime_funcs",
  since = "1.5.0")
case class Year(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getYear(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getYear($c)")
  }
}

case class YearOfWeek(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getWeekBasedYear(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getWeekBasedYear($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the quarter of the year for date, in the range 1 to 4.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-08-31');
       3
  """,
  group = "datetime_funcs",
  since = "1.5.0")
case class Quarter(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getQuarter(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getQuarter($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the month component of the date/timestamp.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-07-30');
       7
  """,
  group = "datetime_funcs",
  since = "1.5.0")
case class Month(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getMonth(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getMonth($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the day of month of the date/timestamp.",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30');
       30
  """,
  since = "1.5.0")
case class DayOfMonth(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getDayOfMonth(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getDayOfMonth($c)")
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the day of the week for date/timestamp (1 = Sunday, 2 = Monday, ..., 7 = Saturday).",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30');
       5
  """,
  group = "datetime_funcs",
  since = "2.3.0")
// scalastyle:on line.size.limit
case class DayOfWeek(child: Expression) extends DayWeek {

  override protected def nullSafeEval(date: Any): Any = {
    val localDate = LocalDate.ofEpochDay(date.asInstanceOf[Int])
    localDate.getDayOfWeek.plus(1).getValue
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, days => {
      s"""
        ${ev.value} = java.time.LocalDate.ofEpochDay($days).getDayOfWeek().plus(1).getValue();
      """
    })
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30');
       3
  """,
  group = "datetime_funcs",
  since = "2.4.0")
// scalastyle:on line.size.limit
case class WeekDay(child: Expression) extends DayWeek {

  override protected def nullSafeEval(date: Any): Any = {
    val localDate = LocalDate.ofEpochDay(date.asInstanceOf[Int])
    localDate.getDayOfWeek.ordinal()
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, days => {
      s"""
         ${ev.value} = java.time.LocalDate.ofEpochDay($days).getDayOfWeek().ordinal();
      """
    })
  }
}

abstract class DayWeek extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the week of the year of the given date. A week is considered to start on a Monday and week 1 is the first week with >3 days.",
  examples = """
    Examples:
      > SELECT _FUNC_('2008-02-20');
       8
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class WeekOfYear(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    val localDate = LocalDate.ofEpochDay(date.asInstanceOf[Int])
    localDate.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, days => {
      s"""
         |${ev.value} = java.time.LocalDate.ofEpochDay($days).get(
         |  java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
       """.stripMargin
    })
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp, fmt) - Converts `timestamp` to a value of string in the format specified by the date format `fmt`.",
  arguments = """
    Arguments:
      * timestamp - A date/timestamp or string to be converted to the given format.
      * fmt - Date/time format pattern to follow. See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a> for valid date
              and time format patterns.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('2016-04-08', 'y');
       2016
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class DateFormatClass(left: Expression, right: Expression, timeZoneId: Option[String] = None)
  extends BinaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(left: Expression, right: Expression) = this(left, right, None)

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  @transient private lazy val formatter: Option[TimestampFormatter] = {
    if (right.foldable) {
      Option(right.eval()).map { format =>
        TimestampFormatter(
          format.toString,
          zoneId,
          legacyFormat = SIMPLE_DATE_FORMAT,
          isParsing = false)
      }
    } else None
  }

  override protected def nullSafeEval(timestamp: Any, format: Any): Any = {
    val tf = if (formatter.isEmpty) {
      TimestampFormatter(
        format.toString,
        zoneId,
        legacyFormat = SIMPLE_DATE_FORMAT,
        isParsing = false)
    } else {
      formatter.get
    }
    UTF8String.fromString(tf.format(timestamp.asInstanceOf[Long]))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    formatter.map { tf =>
      val timestampFormatter = ctx.addReferenceObj("timestampFormatter", tf)
      defineCodeGen(ctx, ev, (timestamp, _) => {
        s"""UTF8String.fromString($timestampFormatter.format($timestamp))"""
      })
    }.getOrElse {
      val tf = TimestampFormatter.getClass.getName.stripSuffix("$")
      val ldf = LegacyDateFormats.getClass.getName.stripSuffix("$")
      val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
      defineCodeGen(ctx, ev, (timestamp, format) => {
        s"""|UTF8String.fromString($tf$$.MODULE$$.apply(
            |  $format.toString(),
            |  $zid,
            |  $ldf$$.MODULE$$.SIMPLE_DATE_FORMAT(),
            |  false)
            |.format($timestamp))""".stripMargin
      })
    }
  }

  override def prettyName: String = "date_format"
}

/**
 * Converts time string with given pattern.
 * Deterministic version of [[UnixTimestamp]], must have at least one parameter.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timeExp[, format]) - Returns the UNIX timestamp of the given time.",
  arguments = """
    Arguments:
      * timeExp - A date/timestamp or string which is returned as a UNIX timestamp.
      * format - Date/time format pattern to follow. Ignored if `timeExp` is not a string.
                 Default value is "yyyy-MM-dd HH:mm:ss". See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a>
                 for valid date and time format patterns.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('2016-04-08', 'yyyy-MM-dd');
       1460098800
  """,
  group = "datetime_funcs",
  since = "1.6.0")
// scalastyle:on line.size.limit
case class ToUnixTimestamp(
    timeExp: Expression,
    format: Expression,
    timeZoneId: Option[String] = None)
  extends UnixTime {

  def this(timeExp: Expression, format: Expression) = this(timeExp, format, None)

  override def left: Expression = timeExp
  override def right: Expression = format

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  def this(time: Expression) = {
    this(time, Literal(TimestampFormatter.defaultPattern))
  }

  override def prettyName: String = "to_unix_timestamp"
}

// scalastyle:off line.size.limit
/**
 * Converts time string with given pattern to Unix time stamp (in seconds), returns null if fail.
 * See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a>.
 * Note that hive Language Manual says it returns 0 if fail, but in fact it returns null.
 * If the second parameter is missing, use "yyyy-MM-dd HH:mm:ss".
 * If no parameters provided, the first parameter will be current_timestamp.
 * If the first parameter is a Date or Timestamp instead of String, we will ignore the
 * second parameter.
 */
@ExpressionDescription(
  usage = "_FUNC_([timeExp[, format]]) - Returns the UNIX timestamp of current or specified time.",
  arguments = """
    Arguments:
      * timeExp - A date/timestamp or string. If not provided, this defaults to current time.
      * format - Date/time format pattern to follow. Ignored if `timeExp` is not a string.
                 Default value is "yyyy-MM-dd HH:mm:ss". See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html"> Datetime Patterns</a>
                 for valid date and time format patterns.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_();
       1476884637
      > SELECT _FUNC_('2016-04-08', 'yyyy-MM-dd');
       1460041200
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class UnixTimestamp(timeExp: Expression, format: Expression, timeZoneId: Option[String] = None)
  extends UnixTime {

  def this(timeExp: Expression, format: Expression) = this(timeExp, format, None)

  override def left: Expression = timeExp
  override def right: Expression = format

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  def this(time: Expression) = {
    this(time, Literal(TimestampFormatter.defaultPattern))
  }

  def this() = {
    this(CurrentTimestamp())
  }

  override def prettyName: String = "unix_timestamp"
}

abstract class ToTimestamp
  extends BinaryExpression with TimeZoneAwareExpression with ExpectsInputTypes {

  // The result of the conversion to timestamp is microseconds divided by this factor.
  // For example if the factor is 1000000, the result of the expression is in seconds.
  protected def downScaleFactor: Long

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, DateType, TimestampType), StringType)

  override def dataType: DataType = LongType
  override def nullable: Boolean = true

  private lazy val constFormat: UTF8String = right.eval().asInstanceOf[UTF8String]
  private lazy val formatter: TimestampFormatter =
    try {
      TimestampFormatter(
        constFormat.toString,
        zoneId,
        legacyFormat = SIMPLE_DATE_FORMAT,
        isParsing = true)
    } catch {
      case e: SparkUpgradeException => throw e
      case NonFatal(_) => null
    }

  override def eval(input: InternalRow): Any = {
    val t = left.eval(input)
    if (t == null) {
      null
    } else {
      left.dataType match {
        case DateType =>
          epochDaysToMicros(t.asInstanceOf[Int], zoneId) / downScaleFactor
        case TimestampType =>
          t.asInstanceOf[Long] / downScaleFactor
        case StringType if right.foldable =>
          if (constFormat == null || formatter == null) {
            null
          } else {
            try {
              formatter.parse(
                t.asInstanceOf[UTF8String].toString) / downScaleFactor
            } catch {
              case e: SparkUpgradeException => throw e
              case NonFatal(_) => null
            }
          }
        case StringType =>
          val f = right.eval(input)
          if (f == null) {
            null
          } else {
            val formatString = f.asInstanceOf[UTF8String].toString
            try {
              TimestampFormatter(
                formatString,
                zoneId,
                legacyFormat = SIMPLE_DATE_FORMAT,
                isParsing = true)
                .parse(t.asInstanceOf[UTF8String].toString) / downScaleFactor
            } catch {
              case e: SparkUpgradeException => throw e
              case NonFatal(_) => null
            }
          }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)
    left.dataType match {
      case StringType if right.foldable =>
        val df = classOf[TimestampFormatter].getName
        if (formatter == null) {
          ExprCode.forNullValue(dataType)
        } else {
          val formatterName = ctx.addReferenceObj("formatter", formatter, df)
          val eval1 = left.genCode(ctx)
          ev.copy(code = code"""
            ${eval1.code}
            boolean ${ev.isNull} = ${eval1.isNull};
            $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
            if (!${ev.isNull}) {
              try {
                ${ev.value} = $formatterName.parse(${eval1.value}.toString()) / $downScaleFactor;
              } catch (java.lang.IllegalArgumentException e) {
                ${ev.isNull} = true;
              } catch (java.text.ParseException e) {
                ${ev.isNull} = true;
              } catch (java.time.format.DateTimeParseException e) {
                ${ev.isNull} = true;
              } catch (java.time.DateTimeException e) {
                ${ev.isNull} = true;
              }
            }""")
        }
      case StringType =>
        val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
        val tf = TimestampFormatter.getClass.getName.stripSuffix("$")
        val ldf = LegacyDateFormats.getClass.getName.stripSuffix("$")
        nullSafeCodeGen(ctx, ev, (string, format) => {
          s"""
            try {
              ${ev.value} = $tf$$.MODULE$$.apply(
                $format.toString(),
                $zid,
                $ldf$$.MODULE$$.SIMPLE_DATE_FORMAT(),
                true)
              .parse($string.toString()) / $downScaleFactor;
            } catch (java.lang.IllegalArgumentException e) {
              ${ev.isNull} = true;
            } catch (java.text.ParseException e) {
              ${ev.isNull} = true;
            } catch (java.time.format.DateTimeParseException e) {
              ${ev.isNull} = true;
            } catch (java.time.DateTimeException e) {
              ${ev.isNull} = true;
            }
          """
        })
      case TimestampType =>
        val eval1 = left.genCode(ctx)
        ev.copy(code = code"""
          ${eval1.code}
          boolean ${ev.isNull} = ${eval1.isNull};
          $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = ${eval1.value} / $downScaleFactor;
          }""")
      case DateType =>
        val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        val eval1 = left.genCode(ctx)
        ev.copy(code = code"""
          ${eval1.code}
          boolean ${ev.isNull} = ${eval1.isNull};
          $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $dtu.epochDaysToMicros(${eval1.value}, $zid) / $downScaleFactor;
          }""")
    }
  }
}

abstract class UnixTime extends ToTimestamp {
  override val downScaleFactor: Long = MICROS_PER_SECOND
}

/**
 * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
 * representing the timestamp of that moment in the current system time zone in the given
 * format. If the format is missing, using format like "1970-01-01 00:00:00".
 * Note that hive Language Manual says it returns 0 if fail, but in fact it returns null.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(unix_time[, format]) - Returns `unix_time` in the specified `format`.",
  arguments = """
    Arguments:
      * unix_time - UNIX Timestamp to be converted to the provided format.
      * format - Date/time format pattern to follow. See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a>
                 for valid date and time format patterns. The 'yyyy-MM-dd HH:mm:ss' pattern is used if omitted.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0, 'yyyy-MM-dd HH:mm:ss');
       1969-12-31 16:00:00

      > SELECT _FUNC_(0);
       1969-12-31 16:00:00
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class FromUnixTime(sec: Expression, format: Expression, timeZoneId: Option[String] = None)
  extends BinaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(sec: Expression, format: Expression) = this(sec, format, None)

  override def left: Expression = sec
  override def right: Expression = format

  override def prettyName: String = "from_unixtime"

  def this(unix: Expression) = {
    this(unix, Literal(TimestampFormatter.defaultPattern))
  }

  override def dataType: DataType = StringType
  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  private lazy val constFormat: UTF8String = right.eval().asInstanceOf[UTF8String]
  private lazy val formatter: TimestampFormatter =
    try {
      TimestampFormatter(
        constFormat.toString,
        zoneId,
        legacyFormat = SIMPLE_DATE_FORMAT,
        isParsing = false)
    } catch {
      case e: SparkUpgradeException => throw e
      case NonFatal(_) => null
    }

  override def eval(input: InternalRow): Any = {
    val time = left.eval(input)
    if (time == null) {
      null
    } else {
      if (format.foldable) {
        if (constFormat == null || formatter == null) {
          null
        } else {
          try {
            UTF8String.fromString(formatter.format(time.asInstanceOf[Long] * MICROS_PER_SECOND))
          } catch {
            case e: SparkUpgradeException => throw e
            case NonFatal(_) => null
          }
        }
      } else {
        val f = format.eval(input)
        if (f == null) {
          null
        } else {
          try {
            UTF8String.fromString(
              TimestampFormatter(
                f.toString,
                zoneId,
                legacyFormat = SIMPLE_DATE_FORMAT,
                isParsing = false)
                .format(time.asInstanceOf[Long] * MICROS_PER_SECOND))
          } catch {
            case e: SparkUpgradeException => throw e
            case NonFatal(_) => null
          }
        }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val df = classOf[TimestampFormatter].getName
    if (format.foldable) {
      if (formatter == null) {
        ExprCode.forNullValue(StringType)
      } else {
        val formatterName = ctx.addReferenceObj("formatter", formatter, df)
        val t = left.genCode(ctx)
        ev.copy(code = code"""
          ${t.code}
          boolean ${ev.isNull} = ${t.isNull};
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            try {
              ${ev.value} = UTF8String.fromString($formatterName.format(${t.value} * 1000000L));
            } catch (java.lang.IllegalArgumentException e) {
              ${ev.isNull} = true;
            }
          }""")
      }
    } else {
      val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
      val tf = TimestampFormatter.getClass.getName.stripSuffix("$")
      val ldf = LegacyDateFormats.getClass.getName.stripSuffix("$")
      nullSafeCodeGen(ctx, ev, (seconds, f) => {
        s"""
        try {
          ${ev.value} = UTF8String.fromString(
            $tf$$.MODULE$$.apply($f.toString(), $zid, $ldf$$.MODULE$$.SIMPLE_DATE_FORMAT(), false)
              .format($seconds * 1000000L));
        } catch (java.lang.IllegalArgumentException e) {
          ${ev.isNull} = true;
        }"""
      })
    }
  }
}

/**
 * Returns the last day of the month which the date belongs to.
 */
@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the last day of the month which the date belongs to.",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-01-12');
       2009-01-31
  """,
  group = "datetime_funcs",
  since = "1.5.0")
case class LastDay(startDate: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def child: Expression = startDate

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = DateType

  override def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getLastDayOfMonth(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, sd => s"$dtu.getLastDayOfMonth($sd)")
  }

  override def prettyName: String = "last_day"
}

/**
 * Returns the first date which is later than startDate and named as dayOfWeek.
 * For example, NextDay(2015-07-27, Sunday) would return 2015-08-02, which is the first
 * Sunday later than 2015-07-27.
 *
 * Allowed "dayOfWeek" is defined in [[DateTimeUtils.getDayOfWeekFromString]].
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(start_date, day_of_week) - Returns the first date which is later than `start_date` and named as indicated.",
  examples = """
    Examples:
      > SELECT _FUNC_('2015-01-14', 'TU');
       2015-01-20
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class NextDay(startDate: Expression, dayOfWeek: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = dayOfWeek

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, StringType)

  override def dataType: DataType = DateType
  override def nullable: Boolean = true

  override def nullSafeEval(start: Any, dayOfW: Any): Any = {
    val dow = DateTimeUtils.getDayOfWeekFromString(dayOfW.asInstanceOf[UTF8String])
    if (dow == -1) {
      null
    } else {
      val sd = start.asInstanceOf[Int]
      DateTimeUtils.getNextDateForDayOfWeek(sd, dow)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (sd, dowS) => {
      val dateTimeUtilClass = DateTimeUtils.getClass.getName.stripSuffix("$")
      val dayOfWeekTerm = ctx.freshName("dayOfWeek")
      if (dayOfWeek.foldable) {
        val input = dayOfWeek.eval().asInstanceOf[UTF8String]
        if ((input eq null) || DateTimeUtils.getDayOfWeekFromString(input) == -1) {
          s"""
             |${ev.isNull} = true;
           """.stripMargin
        } else {
          val dayOfWeekValue = DateTimeUtils.getDayOfWeekFromString(input)
          s"""
             |${ev.value} = $dateTimeUtilClass.getNextDateForDayOfWeek($sd, $dayOfWeekValue);
           """.stripMargin
        }
      } else {
        s"""
           |int $dayOfWeekTerm = $dateTimeUtilClass.getDayOfWeekFromString($dowS);
           |if ($dayOfWeekTerm == -1) {
           |  ${ev.isNull} = true;
           |} else {
           |  ${ev.value} = $dateTimeUtilClass.getNextDateForDayOfWeek($sd, $dayOfWeekTerm);
           |}
         """.stripMargin
      }
    })
  }

  override def prettyName: String = "next_day"
}

/**
 * Adds an interval to timestamp.
 */
case class TimeAdd(start: Expression, interval: Expression, timeZoneId: Option[String] = None)
  extends BinaryExpression with TimeZoneAwareExpression with ExpectsInputTypes {

  def this(start: Expression, interval: Expression) = this(start, interval, None)

  override def left: Expression = start
  override def right: Expression = interval

  override def toString: String = s"$left + $right"
  override def sql: String = s"${left.sql} + ${right.sql}"
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, CalendarIntervalType)

  override def dataType: DataType = TimestampType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def nullSafeEval(start: Any, interval: Any): Any = {
    val itvl = interval.asInstanceOf[CalendarInterval]
    DateTimeUtils.timestampAddInterval(
      start.asInstanceOf[Long], itvl.months, itvl.days, itvl.microseconds, zoneId)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, i) => {
      s"""$dtu.timestampAddInterval($sd, $i.months, $i.days, $i.microseconds, $zid)"""
    })
  }
}

/**
 * Adds date and an interval.
 *
 * When ansi mode is on, the microseconds part of interval needs to be 0, otherwise a runtime
 * [[IllegalArgumentException]] will be raised.
 * When ansi mode is off, if the microseconds part of interval is 0, we perform date + interval
 * for better performance. if the microseconds part is not 0, then the date will be converted to a
 * timestamp to add with the whole interval parts.
 */
case class DateAddInterval(
    start: Expression,
    interval: Expression,
    timeZoneId: Option[String] = None,
    ansiEnabled: Boolean = SQLConf.get.ansiEnabled)
  extends BinaryExpression with ExpectsInputTypes with TimeZoneAwareExpression {

  override def left: Expression = start
  override def right: Expression = interval

  override def toString: String = s"$left + $right"
  override def sql: String = s"${left.sql} + ${right.sql}"
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, CalendarIntervalType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, interval: Any): Any = {
    val itvl = interval.asInstanceOf[CalendarInterval]
    if (ansiEnabled || itvl.microseconds == 0) {
      DateTimeUtils.dateAddInterval(start.asInstanceOf[Int], itvl)
    } else {
      val startTs = DateTimeUtils.epochDaysToMicros(start.asInstanceOf[Int], zoneId)
      val resultTs = DateTimeUtils.timestampAddInterval(
        startTs, itvl.months, itvl.days, itvl.microseconds, zoneId)
      DateTimeUtils.microsToEpochDays(resultTs, zoneId)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    nullSafeCodeGen(ctx, ev, (sd, i) => if (ansiEnabled) {
      s"""${ev.value} = $dtu.dateAddInterval($sd, $i);"""
    } else {
      val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
      val startTs = ctx.freshName("startTs")
      val resultTs = ctx.freshName("resultTs")
      s"""
         |if ($i.microseconds == 0) {
         |  ${ev.value} = $dtu.dateAddInterval($sd, $i);
         |} else {
         |  long $startTs = $dtu.epochDaysToMicros($sd, $zid);
         |  long $resultTs =
         |    $dtu.timestampAddInterval($startTs, $i.months, $i.days, $i.microseconds, $zid);
         |  ${ev.value} = $dtu.microsToEpochDays($resultTs, $zid);
         |}
         |""".stripMargin
    })
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))
}

/**
 * This is a common function for databases supporting TIMESTAMP WITHOUT TIMEZONE. This function
 * takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in UTC, and
 * renders that timestamp as a timestamp in the given time zone.
 *
 * However, timestamp in Spark represents number of microseconds from the Unix epoch, which is not
 * timezone-agnostic. So in Spark this function just shift the timestamp value from UTC timezone to
 * the given timezone.
 *
 * This function may return confusing result if the input is a string with timezone, e.g.
 * '2018-03-13T06:18:23+00:00'. The reason is that, Spark firstly cast the string to timestamp
 * according to the timezone in the string, and finally display the result by converting the
 * timestamp to string according to the session local timezone.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp, timezone) - Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC, and renders that time as a timestamp in the given time zone. For example, 'GMT+1' would yield '2017-07-14 03:40:00.0'.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-08-31', 'Asia/Seoul');
       2016-08-31 09:00:00
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class FromUTCTimestamp(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)
  override def dataType: DataType = TimestampType
  override def prettyName: String = "from_utc_timestamp"

  override def nullSafeEval(time: Any, timezone: Any): Any = {
    DateTimeUtils.fromUTCTime(time.asInstanceOf[Long],
      timezone.asInstanceOf[UTF8String].toString)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    if (right.foldable) {
      val tz = right.eval().asInstanceOf[UTF8String]
      if (tz == null) {
        ev.copy(code = code"""
           |boolean ${ev.isNull} = true;
           |long ${ev.value} = 0;
         """.stripMargin)
      } else {
        val tzClass = classOf[ZoneId].getName
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        val escapedTz = StringEscapeUtils.escapeJava(tz.toString)
        val tzTerm = ctx.addMutableState(tzClass, "tz",
          v => s"""$v = $dtu.getZoneId("$escapedTz");""")
        val utcTerm = "java.time.ZoneOffset.UTC"
        val eval = left.genCode(ctx)
        ev.copy(code = code"""
           |${eval.code}
           |boolean ${ev.isNull} = ${eval.isNull};
           |long ${ev.value} = 0;
           |if (!${ev.isNull}) {
           |  ${ev.value} = $dtu.convertTz(${eval.value}, $utcTerm, $tzTerm);
           |}
         """.stripMargin)
      }
    } else {
      defineCodeGen(ctx, ev, (timestamp, format) => {
        s"""$dtu.fromUTCTime($timestamp, $format.toString())"""
      })
    }
  }
}

/**
 * Subtracts an interval from timestamp.
 */
case class TimeSub(start: Expression, interval: Expression, timeZoneId: Option[String] = None)
  extends BinaryExpression with TimeZoneAwareExpression with ExpectsInputTypes {

  def this(start: Expression, interval: Expression) = this(start, interval, None)

  override def left: Expression = start
  override def right: Expression = interval

  override def toString: String = s"$left - $right"
  override def sql: String = s"${left.sql} - ${right.sql}"
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, CalendarIntervalType)

  override def dataType: DataType = TimestampType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def nullSafeEval(start: Any, interval: Any): Any = {
    val itvl = interval.asInstanceOf[CalendarInterval]
    DateTimeUtils.timestampAddInterval(
      start.asInstanceOf[Long], 0 - itvl.months, 0 - itvl.days, 0 - itvl.microseconds, zoneId)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, i) => {
      s"""$dtu.timestampAddInterval($sd, 0 - $i.months, 0 - $i.days, 0 - $i.microseconds, $zid)"""
    })
  }
}

/**
 * Returns the date that is num_months after start_date.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(start_date, num_months) - Returns the date that is `num_months` after `start_date`.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-08-31', 1);
       2016-09-30
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class AddMonths(startDate: Expression, numMonths: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = numMonths

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, months: Any): Any = {
    DateTimeUtils.dateAddMonths(start.asInstanceOf[Int], months.asInstanceOf[Int])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, m) => {
      s"""$dtu.dateAddMonths($sd, $m)"""
    })
  }

  override def prettyName: String = "add_months"
}

/**
 * Returns number of months between times `timestamp1` and `timestamp2`.
 * If `timestamp1` is later than `timestamp2`, then the result is positive.
 * If `timestamp1` and `timestamp2` are on the same day of month, or both
 * are the last day of month, time of day will be ignored. Otherwise, the
 * difference is calculated based on 31 days per month, and rounded to
 * 8 digits unless roundOff=false.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(timestamp1, timestamp2[, roundOff]) - If `timestamp1` is later than `timestamp2`, then the result
      is positive. If `timestamp1` and `timestamp2` are on the same day of month, or both
      are the last day of month, time of day will be ignored. Otherwise, the difference is
      calculated based on 31 days per month, and rounded to 8 digits unless roundOff=false.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('1997-02-28 10:30:00', '1996-10-30');
       3.94959677
      > SELECT _FUNC_('1997-02-28 10:30:00', '1996-10-30', false);
       3.9495967741935485
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class MonthsBetween(
    date1: Expression,
    date2: Expression,
    roundOff: Expression,
    timeZoneId: Option[String] = None)
  extends TernaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(date1: Expression, date2: Expression) = this(date1, date2, Literal.TrueLiteral, None)

  def this(date1: Expression, date2: Expression, roundOff: Expression) =
    this(date1, date2, roundOff, None)

  override def children: Seq[Expression] = Seq(date1, date2, roundOff)

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, TimestampType, BooleanType)

  override def dataType: DataType = DoubleType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def nullSafeEval(t1: Any, t2: Any, roundOff: Any): Any = {
    DateTimeUtils.monthsBetween(
      t1.asInstanceOf[Long], t2.asInstanceOf[Long], roundOff.asInstanceOf[Boolean], zoneId)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (d1, d2, roundOff) => {
      s"""$dtu.monthsBetween($d1, $d2, $roundOff, $zid)"""
    })
  }

  override def prettyName: String = "months_between"
}

/**
 * This is a common function for databases supporting TIMESTAMP WITHOUT TIMEZONE. This function
 * takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in the given
 * timezone, and renders that timestamp as a timestamp in UTC.
 *
 * However, timestamp in Spark represents number of microseconds from the Unix epoch, which is not
 * timezone-agnostic. So in Spark this function just shift the timestamp value from the given
 * timezone to UTC timezone.
 *
 * This function may return confusing result if the input is a string with timezone, e.g.
 * '2018-03-13T06:18:23+00:00'. The reason is that, Spark firstly cast the string to timestamp
 * according to the timezone in the string, and finally display the result by converting the
 * timestamp to string according to the session local timezone.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp, timezone) - Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in the given time zone, and renders that time as a timestamp in UTC. For example, 'GMT+1' would yield '2017-07-14 01:40:00.0'.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-08-31', 'Asia/Seoul');
       2016-08-30 15:00:00
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class ToUTCTimestamp(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)
  override def dataType: DataType = TimestampType
  override def prettyName: String = "to_utc_timestamp"

  override def nullSafeEval(time: Any, timezone: Any): Any = {
    DateTimeUtils.toUTCTime(time.asInstanceOf[Long],
      timezone.asInstanceOf[UTF8String].toString)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    if (right.foldable) {
      val tz = right.eval().asInstanceOf[UTF8String]
      if (tz == null) {
        ev.copy(code = code"""
           |boolean ${ev.isNull} = true;
           |long ${ev.value} = 0;
         """.stripMargin)
      } else {
        val tzClass = classOf[ZoneId].getName
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        val escapedTz = StringEscapeUtils.escapeJava(tz.toString)
        val tzTerm = ctx.addMutableState(tzClass, "tz",
          v => s"""$v = $dtu.getZoneId("$escapedTz");""")
        val utcTerm = "java.time.ZoneOffset.UTC"
        val eval = left.genCode(ctx)
        ev.copy(code = code"""
           |${eval.code}
           |boolean ${ev.isNull} = ${eval.isNull};
           |long ${ev.value} = 0;
           |if (!${ev.isNull}) {
           |  ${ev.value} = $dtu.convertTz(${eval.value}, $tzTerm, $utcTerm);
           |}
         """.stripMargin)
      }
    } else {
      defineCodeGen(ctx, ev, (timestamp, format) => {
        s"""$dtu.toUTCTime($timestamp, $format.toString())"""
      })
    }
  }
}

/**
 * Parses a column to a date based on the given format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(date_str[, fmt]) - Parses the `date_str` expression with the `fmt` expression to
      a date. Returns null with invalid input. By default, it follows casting rules to a date if
      the `fmt` is omitted.
  """,
  arguments = """
    Arguments:
      * date_str - A string to be parsed to date.
      * fmt - Date format pattern to follow. See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a> for valid
              date and time format patterns.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30 04:17:52');
       2009-07-30
      > SELECT _FUNC_('2016-12-31', 'yyyy-MM-dd');
       2016-12-31
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class ParseToDate(left: Expression, format: Option[Expression], child: Expression)
  extends RuntimeReplaceable {

  def this(left: Expression, format: Expression) {
      this(left, Option(format),
        Cast(Cast(UnixTimestamp(left, format), TimestampType), DateType))
  }

  def this(left: Expression) = {
    // backwards compatibility
    this(left, None, Cast(left, DateType))
  }

  override def flatArguments: Iterator[Any] = Iterator(left, format)
  override def sql: String = {
    if (format.isDefined) {
      s"$prettyName(${left.sql}, ${format.get.sql})"
    } else {
      s"$prettyName(${left.sql})"
    }
  }

  override def prettyName: String = "to_date"
}

/**
 * Parses a column to a timestamp based on the supplied format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(timestamp_str[, fmt]) - Parses the `timestamp_str` expression with the `fmt` expression
      to a timestamp. Returns null with invalid input. By default, it follows casting rules to
      a timestamp if the `fmt` is omitted.
  """,
  arguments = """
    Arguments:
      * timestamp_str - A string to be parsed to timestamp.
      * fmt - Timestamp format pattern to follow. See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a> for valid
              date and time format patterns.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('2016-12-31 00:12:00');
       2016-12-31 00:12:00
      > SELECT _FUNC_('2016-12-31', 'yyyy-MM-dd');
       2016-12-31 00:00:00
  """,
  group = "datetime_funcs",
  since = "2.2.0")
// scalastyle:on line.size.limit
case class ParseToTimestamp(left: Expression, format: Option[Expression], child: Expression)
  extends RuntimeReplaceable {

  def this(left: Expression, format: Expression) = {
    this(left, Option(format), GetTimestamp(left, format))
  }

  def this(left: Expression) = this(left, None, Cast(left, TimestampType))

  override def flatArguments: Iterator[Any] = Iterator(left, format)
  override def sql: String = {
    if (format.isDefined) {
      s"$prettyName(${left.sql}, ${format.get.sql})"
    } else {
      s"$prettyName(${left.sql})"
    }
  }

  override def prettyName: String = "to_timestamp"
  override def dataType: DataType = TimestampType
}

trait TruncInstant extends BinaryExpression with ImplicitCastInputTypes {
  val instant: Expression
  val format: Expression
  override def nullable: Boolean = true

  private lazy val truncLevel: Int =
    DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])

  /**
   * @param input internalRow (time)
   * @param minLevel Minimum level that can be used for truncation (e.g WEEK for Date input)
   * @param truncFunc function: (time, level) => time
   */
  protected def evalHelper(input: InternalRow, minLevel: Int)(
    truncFunc: (Any, Int) => Any): Any = {
    val level = if (format.foldable) {
      truncLevel
    } else {
      DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])
    }
    if (level < minLevel) {
      // unknown format or too small level
      null
    } else {
      val t = instant.eval(input)
      if (t == null) {
        null
      } else {
        truncFunc(t, level)
      }
    }
  }

  protected def codeGenHelper(
      ctx: CodegenContext,
      ev: ExprCode,
      minLevel: Int,
      orderReversed: Boolean = false)(
      truncFunc: (String, String) => String)
    : ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")

    val javaType = CodeGenerator.javaType(dataType)
    if (format.foldable) {
      if (truncLevel < minLevel) {
        ev.copy(code = code"""
          boolean ${ev.isNull} = true;
          $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};""")
      } else {
        val t = instant.genCode(ctx)
        val truncFuncStr = truncFunc(t.value, truncLevel.toString)
        ev.copy(code = code"""
          ${t.code}
          boolean ${ev.isNull} = ${t.isNull};
          $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $dtu.$truncFuncStr;
          }""")
      }
    } else {
      nullSafeCodeGen(ctx, ev, (left, right) => {
        val form = ctx.freshName("form")
        val (dateVal, fmt) = if (orderReversed) {
          (right, left)
        } else {
          (left, right)
        }
        val truncFuncStr = truncFunc(dateVal, form)
        s"""
          int $form = $dtu.parseTruncLevel($fmt);
          if ($form < $minLevel) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = $dtu.$truncFuncStr
          }
        """
      })
    }
  }
}

/**
 * Returns date truncated to the unit specified by the format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(date, fmt) - Returns `date` with the time portion of the day truncated to the unit specified by the format model `fmt`.
  """,
  arguments = """
     Arguments:
       * date - date value or valid date string
       * fmt - the format representing the unit to be truncated to
           - "YEAR", "YYYY", "YY" - truncate to the first date of the year that the `date` falls in
           - "QUARTER" - truncate to the first date of the quarter that the `date` falls in
           - "MONTH", "MM", "MON" - truncate to the first date of the month that the `date` falls in
           - "WEEK" - truncate to the Monday of the week that the `date` falls in
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('2019-08-04', 'week');
       2019-07-29
      > SELECT _FUNC_('2019-08-04', 'quarter');
       2019-07-01
      > SELECT _FUNC_('2009-02-12', 'MM');
       2009-02-01
      > SELECT _FUNC_('2015-10-27', 'YEAR');
       2015-01-01
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class TruncDate(date: Expression, format: Expression)
  extends TruncInstant {
  override def left: Expression = date
  override def right: Expression = format

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, StringType)
  override def dataType: DataType = DateType
  override def prettyName: String = "trunc"
  override val instant = date

  override def eval(input: InternalRow): Any = {
    evalHelper(input, minLevel = MIN_LEVEL_OF_DATE_TRUNC) { (d: Any, level: Int) =>
      DateTimeUtils.truncDate(d.asInstanceOf[Int], level)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    codeGenHelper(ctx, ev, minLevel = MIN_LEVEL_OF_DATE_TRUNC) {
      (date: String, fmt: String) => s"truncDate($date, $fmt);"
    }
  }
}

/**
 * Returns timestamp truncated to the unit specified by the format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(fmt, ts) - Returns timestamp `ts` truncated to the unit specified by the format model `fmt`.
  """,
  arguments = """
     Arguments:
       * fmt - the format representing the unit to be truncated to
           - "YEAR", "YYYY", "YY" - truncate to the first date of the year that the `ts` falls in, the time part will be zero out
           - "QUARTER" - truncate to the first date of the quarter that the `ts` falls in, the time part will be zero out
           - "MONTH", "MM", "MON" - truncate to the first date of the month that the `ts` falls in, the time part will be zero out
           - "WEEK" - truncate to the Monday of the week that the `ts` falls in, the time part will be zero out
           - "DAY", "DD" - zero out the time part
           - "HOUR" - zero out the minute and second with fraction part
           - "MINUTE"- zero out the second with fraction part
           - "SECOND" -  zero out the second fraction part
           - "MILLISECOND" - zero out the microseconds
           - "MICROSECOND" - everything remains
       * ts - datetime value or valid timestamp string
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('YEAR', '2015-03-05T09:32:05.359');
       2015-01-01 00:00:00
      > SELECT _FUNC_('MM', '2015-03-05T09:32:05.359');
       2015-03-01 00:00:00
      > SELECT _FUNC_('DD', '2015-03-05T09:32:05.359');
       2015-03-05 00:00:00
      > SELECT _FUNC_('HOUR', '2015-03-05T09:32:05.359');
       2015-03-05 09:00:00
      > SELECT _FUNC_('MILLISECOND', '2015-03-05T09:32:05.123456');
       2015-03-05 09:32:05.123
  """,
  group = "datetime_funcs",
  since = "2.3.0")
// scalastyle:on line.size.limit
case class TruncTimestamp(
    format: Expression,
    timestamp: Expression,
    timeZoneId: Option[String] = None)
  extends TruncInstant with TimeZoneAwareExpression {
  override def left: Expression = format
  override def right: Expression = timestamp

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, TimestampType)
  override def dataType: TimestampType = TimestampType
  override def prettyName: String = "date_trunc"
  override val instant = timestamp
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  def this(format: Expression, timestamp: Expression) = this(format, timestamp, None)

  override def eval(input: InternalRow): Any = {
    evalHelper(input, minLevel = MIN_LEVEL_OF_TIMESTAMP_TRUNC) { (t: Any, level: Int) =>
      DateTimeUtils.truncTimestamp(t.asInstanceOf[Long], level, zoneId)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    codeGenHelper(ctx, ev, minLevel = MIN_LEVEL_OF_TIMESTAMP_TRUNC, true) {
      (date: String, fmt: String) =>
        s"truncTimestamp($date, $fmt, $zid);"
    }
  }
}

/**
 * Returns the number of days from startDate to endDate.
 */
@ExpressionDescription(
  usage = "_FUNC_(endDate, startDate) - Returns the number of days from `startDate` to `endDate`.",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-31', '2009-07-30');
       1

      > SELECT _FUNC_('2009-07-30', '2009-07-31');
       -1
  """,
  group = "datetime_funcs",
  since = "1.5.0")
case class DateDiff(endDate: Expression, startDate: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = endDate
  override def right: Expression = startDate
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)
  override def dataType: DataType = IntegerType

  override def nullSafeEval(end: Any, start: Any): Any = {
    end.asInstanceOf[Int] - start.asInstanceOf[Int]
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (end, start) => s"$end - $start")
  }
}

/**
 * Gets timestamps from strings using given pattern.
 */
private case class GetTimestamp(
    left: Expression,
    right: Expression,
    timeZoneId: Option[String] = None)
  extends ToTimestamp {

  override val downScaleFactor = 1
  override def dataType: DataType = TimestampType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))
}

@ExpressionDescription(
  usage = "_FUNC_(year, month, day) - Create date from year, month and day fields.",
  arguments = """
    Arguments:
      * year - the year to represent, from 1 to 9999
      * month - the month-of-year to represent, from 1 (January) to 12 (December)
      * day - the day-of-month to represent, from 1 to 31
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(2013, 7, 15);
       2013-07-15
      > SELECT _FUNC_(2019, 13, 1);
       NULL
      > SELECT _FUNC_(2019, 7, NULL);
       NULL
      > SELECT _FUNC_(2019, 2, 30);
       NULL
  """,
  group = "datetime_funcs",
  since = "3.0.0")
case class MakeDate(year: Expression, month: Expression, day: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  override def children: Seq[Expression] = Seq(year, month, day)
  override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType, IntegerType, IntegerType)
  override def dataType: DataType = DateType
  override def nullable: Boolean = true

  override def nullSafeEval(year: Any, month: Any, day: Any): Any = {
    try {
      val ld = LocalDate.of(year.asInstanceOf[Int], month.asInstanceOf[Int], day.asInstanceOf[Int])
      localDateToDays(ld)
    } catch {
      case _: java.time.DateTimeException => null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    nullSafeCodeGen(ctx, ev, (year, month, day) => {
      s"""
      try {
        ${ev.value} = $dtu.localDateToDays(java.time.LocalDate.of($year, $month, $day));
      } catch (java.time.DateTimeException e) {
        ${ev.isNull} = true;
      }"""
    })
  }

  override def prettyName: String = "make_date"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(year, month, day, hour, min, sec[, timezone]) - Create timestamp from year, month, day, hour, min, sec and timezone fields.",
  arguments = """
    Arguments:
      * year - the year to represent, from 1 to 9999
      * month - the month-of-year to represent, from 1 (January) to 12 (December)
      * day - the day-of-month to represent, from 1 to 31
      * hour - the hour-of-day to represent, from 0 to 23
      * min - the minute-of-hour to represent, from 0 to 59
      * sec - the second-of-minute and its micro-fraction to represent, from
              0 to 60. If the sec argument equals to 60, the seconds field is set
              to 0 and 1 minute is added to the final timestamp.
      * timezone - the time zone identifier. For example, CET, UTC and etc.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(2014, 12, 28, 6, 30, 45.887);
       2014-12-28 06:30:45.887
      > SELECT _FUNC_(2014, 12, 28, 6, 30, 45.887, 'CET');
       2014-12-27 21:30:45.887
      > SELECT _FUNC_(2019, 6, 30, 23, 59, 60);
       2019-07-01 00:00:00
      > SELECT _FUNC_(2019, 13, 1, 10, 11, 12, 'PST');
       NULL
      > SELECT _FUNC_(null, 7, 22, 15, 30, 0);
       NULL
  """,
  group = "datetime_funcs",
  since = "3.0.0")
// scalastyle:on line.size.limit
case class MakeTimestamp(
    year: Expression,
    month: Expression,
    day: Expression,
    hour: Expression,
    min: Expression,
    sec: Expression,
    timezone: Option[Expression] = None,
    timeZoneId: Option[String] = None)
  extends SeptenaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(
      year: Expression,
      month: Expression,
      day: Expression,
      hour: Expression,
      min: Expression,
      sec: Expression) = {
    this(year, month, day, hour, min, sec, None, None)
  }

  def this(
      year: Expression,
      month: Expression,
      day: Expression,
      hour: Expression,
      min: Expression,
      sec: Expression,
      timezone: Expression) = {
    this(year, month, day, hour, min, sec, Some(timezone), None)
  }

  override def children: Seq[Expression] = Seq(year, month, day, hour, min, sec) ++ timezone
  // Accept `sec` as DecimalType to avoid loosing precision of microseconds while converting
  // them to the fractional part of `sec`.
  override def inputTypes: Seq[AbstractDataType] =
    Seq(IntegerType, IntegerType, IntegerType, IntegerType, IntegerType, DecimalType(8, 6)) ++
    timezone.map(_ => StringType)
  override def dataType: DataType = TimestampType
  override def nullable: Boolean = true

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  private def toMicros(
      year: Int,
      month: Int,
      day: Int,
      hour: Int,
      min: Int,
      secAndNanos: Decimal,
      zoneId: ZoneId): Any = {
    try {
      val secFloor = secAndNanos.floor
      val nanosPerSec = Decimal(NANOS_PER_SECOND, 10, 0)
      val nanos = ((secAndNanos - secFloor) * nanosPerSec).toInt
      val seconds = secFloor.toInt
      val ldt = if (seconds == 60) {
        if (nanos == 0) {
          // This case of sec = 60 and nanos = 0 is supported for compatibility with PostgreSQL
          LocalDateTime.of(year, month, day, hour, min, 0, 0).plusMinutes(1)
        } else {
          throw new DateTimeException("The fraction of sec must be zero. Valid range is [0, 60].")
        }
      } else {
        LocalDateTime.of(year, month, day, hour, min, seconds, nanos)
      }
      instantToMicros(ldt.atZone(zoneId).toInstant)
    } catch {
      case _: DateTimeException => null
    }
  }

  override def nullSafeEval(
      year: Any,
      month: Any,
      day: Any,
      hour: Any,
      min: Any,
      sec: Any,
      timezone: Option[Any]): Any = {
    val zid = timezone
      .map(tz => DateTimeUtils.getZoneId(tz.asInstanceOf[UTF8String].toString))
      .getOrElse(zoneId)
    toMicros(
      year.asInstanceOf[Int],
      month.asInstanceOf[Int],
      day.asInstanceOf[Int],
      hour.asInstanceOf[Int],
      min.asInstanceOf[Int],
      sec.asInstanceOf[Decimal],
      zid)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    val d = Decimal.getClass.getName.stripSuffix("$")
    nullSafeCodeGen(ctx, ev, (year, month, day, hour, min, secAndNanos, timezone) => {
      val zoneId = timezone.map(tz => s"$dtu.getZoneId(${tz}.toString())").getOrElse(zid)
      s"""
      try {
        org.apache.spark.sql.types.Decimal secFloor = $secAndNanos.floor();
        org.apache.spark.sql.types.Decimal nanosPerSec = $d$$.MODULE$$.apply(1000000000L, 10, 0);
        int nanos = (($secAndNanos.$$minus(secFloor)).$$times(nanosPerSec)).toInt();
        int seconds = secFloor.toInt();
        java.time.LocalDateTime ldt;
        if (seconds == 60) {
          if (nanos == 0) {
            ldt = java.time.LocalDateTime.of(
              $year, $month, $day, $hour, $min, 0, 0).plusMinutes(1);
          } else {
            throw new java.time.DateTimeException(
              "The fraction of sec must be zero. Valid range is [0, 60].");
          }
        } else {
          ldt = java.time.LocalDateTime.of($year, $month, $day, $hour, $min, seconds, nanos);
        }
        java.time.Instant instant = ldt.atZone($zoneId).toInstant();
        ${ev.value} = $dtu.instantToMicros(instant);
      } catch (java.time.DateTimeException e) {
        ${ev.isNull} = true;
      }"""
    })
  }

  override def prettyName: String = "make_timestamp"
}

object DatePart {

  def parseExtractField(
      extractField: String,
      source: Expression,
      errorHandleFunc: => Nothing): Expression = extractField.toUpperCase(Locale.ROOT) match {
    case "YEAR" | "Y" | "YEARS" | "YR" | "YRS" => Year(source)
    case "YEAROFWEEK" => YearOfWeek(source)
    case "QUARTER" | "QTR" => Quarter(source)
    case "MONTH" | "MON" | "MONS" | "MONTHS" => Month(source)
    case "WEEK" | "W" | "WEEKS" => WeekOfYear(source)
    case "DAY" | "D" | "DAYS" => DayOfMonth(source)
    case "DAYOFWEEK" | "DOW" => DayOfWeek(source)
    case "DAYOFWEEK_ISO" | "DOW_ISO" => Add(WeekDay(source), Literal(1))
    case "DOY" => DayOfYear(source)
    case "HOUR" | "H" | "HOURS" | "HR" | "HRS" => Hour(source)
    case "MINUTE" | "M" | "MIN" | "MINS" | "MINUTES" => Minute(source)
    case "SECOND" | "S" | "SEC" | "SECONDS" | "SECS" => SecondWithFraction(source)
    case _ => errorHandleFunc
  }

  def toEquivalentExpr(field: Expression, source: Expression): Expression = {
    if (!field.foldable) {
      throw new AnalysisException("The field parameter needs to be a foldable string value.")
    }
    val fieldEval = field.eval()
    if (fieldEval == null) {
      Literal(null, DoubleType)
    } else {
      val fieldStr = fieldEval.asInstanceOf[UTF8String].toString
      val errMsg = s"Literals of type '$fieldStr' are currently not supported " +
        s"for the ${source.dataType.catalogString} type."
      if (source.dataType == CalendarIntervalType) {
        ExtractIntervalPart.parseExtractField(
          fieldStr,
          source,
          throw new AnalysisException(errMsg))
      } else {
        DatePart.parseExtractField(fieldStr, source, throw new AnalysisException(errMsg))
      }
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(field, source) - Extracts a part of the date/timestamp or interval source.",
  arguments = """
    Arguments:
      * field - selects which part of the source should be extracted, and supported string values are as same as the fields of the equivalent function `EXTRACT`.
      * source - a date/timestamp or interval column from where `field` should be extracted
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('YEAR', TIMESTAMP '2019-08-12 01:00:00.123456');
       2019
      > SELECT _FUNC_('week', timestamp'2019-08-12 01:00:00.123456');
       33
      > SELECT _FUNC_('doy', DATE'2019-08-12');
       224
      > SELECT _FUNC_('SECONDS', timestamp'2019-10-01 00:00:01.000001');
       1.000001
      > SELECT _FUNC_('days', interval 1 year 10 months 5 days);
       5
      > SELECT _FUNC_('seconds', interval 5 hours 30 seconds 1 milliseconds 1 microseconds);
       30.001001
  """,
  note = """
    The _FUNC_ function is equivalent to the SQL-standard function `EXTRACT(field FROM source)`
  """,
  group = "datetime_funcs",
  since = "3.0.0")
// scalastyle:on line.size.limit
case class DatePart(field: Expression, source: Expression, child: Expression)
  extends RuntimeReplaceable {

  def this(field: Expression, source: Expression) = {
    this(field, source, DatePart.toEquivalentExpr(field, source))
  }

  override def flatArguments: Iterator[Any] = Iterator(field, source)
  override def sql: String = s"$prettyName(${field.sql}, ${source.sql})"
  override def prettyName: String = "date_part"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(field FROM source) - Extracts a part of the date/timestamp or interval source.",
  arguments = """
    Arguments:
      * field - selects which part of the source should be extracted
          - Supported string values of `field` for dates and timestamps are(case insensitive):
              - "YEAR", ("Y", "YEARS", "YR", "YRS") - the year field
              - "YEAROFWEEK" - the ISO 8601 week-numbering year that the datetime falls in. For example, 2005-01-02 is part of the 53rd week of year 2004, so the result is 2004
              - "QUARTER", ("QTR") - the quarter (1 - 4) of the year that the datetime falls in
              - "MONTH", ("MON", "MONS", "MONTHS") - the month field (1 - 12)
              - "WEEK", ("W", "WEEKS") - the number of the ISO 8601 week-of-week-based-year. A week is considered to start on a Monday and week 1 is the first week with >3 days. In the ISO week-numbering system, it is possible for early-January dates to be part of the 52nd or 53rd week of the previous year, and for late-December dates to be part of the first week of the next year. For example, 2005-01-02 is part of the 53rd week of year 2004, while 2012-12-31 is part of the first week of 2013
              - "DAY", ("D", "DAYS") - the day of the month field (1 - 31)
              - "DAYOFWEEK",("DOW") - the day of the week for datetime as Sunday(1) to Saturday(7)
              - "DAYOFWEEK_ISO",("DOW_ISO") - ISO 8601 based day of the week for datetime as Monday(1) to Sunday(7)
              - "DOY" - the day of the year (1 - 365/366)
              - "HOUR", ("H", "HOURS", "HR", "HRS") - The hour field (0 - 23)
              - "MINUTE", ("M", "MIN", "MINS", "MINUTES") - the minutes field (0 - 59)
              - "SECOND", ("S", "SEC", "SECONDS", "SECS") - the seconds field, including fractional parts
          - Supported string values of `field` for interval(which consists of `months`, `days`, `microseconds`) are(case insensitive):
              - "YEAR", ("Y", "YEARS", "YR", "YRS") - the total `months` / 12
              - "MONTH", ("MON", "MONS", "MONTHS") - the total `months` % 12
              - "DAY", ("D", "DAYS") - the `days` part of interval
              - "HOUR", ("H", "HOURS", "HR", "HRS") - how many hours the `microseconds` contains
              - "MINUTE", ("M", "MIN", "MINS", "MINUTES") - how many minutes left after taking hours from `microseconds`
              - "SECOND", ("S", "SEC", "SECONDS", "SECS") - how many second with fractions left after taking hours and minutes from `microseconds`
      * source - a date/timestamp or interval column from where `field` should be extracted
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(YEAR FROM TIMESTAMP '2019-08-12 01:00:00.123456');
       2019
      > SELECT _FUNC_(week FROM timestamp'2019-08-12 01:00:00.123456');
       33
      > SELECT _FUNC_(doy FROM DATE'2019-08-12');
       224
      > SELECT _FUNC_(SECONDS FROM timestamp'2019-10-01 00:00:01.000001');
       1.000001
      > SELECT _FUNC_(days FROM interval 1 year 10 months 5 days);
       5
      > SELECT _FUNC_(seconds FROM interval 5 hours 30 seconds 1 milliseconds 1 microseconds);
       30.001001
  """,
  note = """
    The _FUNC_ function is equivalent to `date_part(field, source)`.
  """,
  since = "3.0.0")
// scalastyle:on line.size.limit
case class Extract(field: Expression, source: Expression, child: Expression)
  extends RuntimeReplaceable {

  def this(field: Expression, source: Expression) = {
    this(field, source, DatePart.toEquivalentExpr(field, source))
  }

  override def flatArguments: Iterator[Any] = Iterator(field, source)
  override def sql: String = s"$prettyName(${field.sql} FROM ${source.sql})"
  override def prettyName: String = "extract"
}

/**
 * Returns the interval from startTimestamp to endTimestamp in which the `months` and `day` field
 * is set to 0 and the `microseconds` field is initialized to the microsecond difference
 * between the given timestamps.
 */
case class SubtractTimestamps(endTimestamp: Expression, startTimestamp: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def left: Expression = endTimestamp
  override def right: Expression = startTimestamp
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, TimestampType)
  override def dataType: DataType = CalendarIntervalType

  override def nullSafeEval(end: Any, start: Any): Any = {
    new CalendarInterval(0, 0, end.asInstanceOf[Long] - start.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (end, start) =>
      s"new org.apache.spark.unsafe.types.CalendarInterval(0, 0, $end - $start)")
  }
}

/**
 * Returns the interval from the `left` date (inclusive) to the `right` date (exclusive).
 */
case class SubtractDates(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)
  override def dataType: DataType = CalendarIntervalType

  override def nullSafeEval(leftDays: Any, rightDays: Any): Any = {
    DateTimeUtils.subtractDates(leftDays.asInstanceOf[Int], rightDays.asInstanceOf[Int])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (leftDays, rightDays) => {
      val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
      s"$dtu.subtractDates($leftDays, $rightDays)"
    })
  }
}

