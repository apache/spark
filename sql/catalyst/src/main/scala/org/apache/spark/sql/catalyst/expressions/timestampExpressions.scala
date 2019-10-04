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
import java.time.{DateTimeException, LocalDateTime, ZoneId}
import java.util.{Locale, TimeZone}

import scala.util.control.NonFatal

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Returns the current timestamp at the start of query evaluation.
 * All calls of current_timestamp within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current timestamp at the start of query evaluation.",
  since = "1.5.0")
case class CurrentTimestamp() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = TimestampType

  override def eval(input: InternalRow): Any = currentTimestamp()

  override def prettyName: String = "current_timestamp"
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
    case _: DateType => Literal(DateTimeUtils.millisToDays(timestampMs, timeZone), DateType)
  }
}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns the hour component of the string/timestamp.",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30 12:58:59');
       12
  """,
  since = "1.5.0")
case class Hour(child: Expression, timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(child: Expression) = this(child, None)

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getHours(timestamp.asInstanceOf[Long], timeZone)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getHours($c, $tz)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns the minute component of the string/timestamp.",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30 12:58:59');
       58
  """,
  since = "1.5.0")
case class Minute(child: Expression, timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(child: Expression) = this(child, None)

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getMinutes(timestamp.asInstanceOf[Long], timeZone)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getMinutes($c, $tz)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns the second component of the string/timestamp.",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30 12:58:59');
       59
  """,
  since = "1.5.0")
case class Second(child: Expression, timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(child: Expression) = this(child, None)

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getSeconds(timestamp.asInstanceOf[Long], timeZone)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getSeconds($c, $tz)")
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
    DateTimeUtils.getSecondsWithFraction(timestamp.asInstanceOf[Long], timeZone)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getSecondsWithFraction($c, $tz)")
  }
}

case class Milliseconds(child: Expression, timeZoneId: Option[String] = None)
  extends UnaryExpression with ImplicitCastInputTypes with TimeZoneAwareExpression {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
  // DecimalType is used here to not lose precision while converting microseconds to
  // the fractional part of milliseconds. Scale 3 is taken to have all microseconds as
  // the fraction. The precision 8 should cover 2 digits for seconds, 3 digits for
  // milliseconds and 3 digits for microseconds.
  override def dataType: DataType = DecimalType(8, 3)
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getMilliseconds(timestamp.asInstanceOf[Long], timeZone)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getMilliseconds($c, $tz)")
  }
}

case class Microseconds(child: Expression, timeZoneId: Option[String] = None)
  extends UnaryExpression with ImplicitCastInputTypes with TimeZoneAwareExpression {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
  override def dataType: DataType = IntegerType
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getMicroseconds(timestamp.asInstanceOf[Long], timeZone)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getMicroseconds($c, $tz)")
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp, fmt) - Converts `timestamp` to a value of string in the format specified by the date format `fmt`.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-04-08', 'y');
       2016
  """,
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
      Option(right.eval()).map(format => TimestampFormatter(format.toString, zoneId))
    } else None
  }

  override protected def nullSafeEval(timestamp: Any, format: Any): Any = {
    val tf = if (formatter.isEmpty) {
      TimestampFormatter(format.toString, zoneId)
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
      val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
      defineCodeGen(ctx, ev, (timestamp, format) => {
        s"""UTF8String.fromString($tf$$.MODULE$$.apply($format.toString(), $zid)
          .format($timestamp))"""
      })
    }
  }

  override def prettyName: String = "date_format"
}

/**
 * Adds an interval to timestamp.
 */
case class TimeAdd(start: Expression, interval: Expression, timeZoneId: Option[String] = None)
  extends BinaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

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
      start.asInstanceOf[Long], itvl.months, itvl.microseconds, zoneId)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, i) => {
      s"""$dtu.timestampAddInterval($sd, $i.months, $i.microseconds, $zid)"""
    })
  }
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
  since = "1.5.0",
  deprecated = """
    Deprecated since 3.0.0. See SPARK-25496.
  """)
// scalastyle:on line.size.limit
case class FromUTCTimestamp(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  if (!SQLConf.get.utcTimestampFuncEnabled) {
    throw new AnalysisException(s"The $prettyName function has been disabled since Spark 3.0." +
      s"Set ${SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key} to true to enable this function.")
  }

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
        val tzClass = classOf[TimeZone].getName
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        val escapedTz = StringEscapeUtils.escapeJava(tz.toString)
        val tzTerm = ctx.addMutableState(tzClass, "tz",
          v => s"""$v = $dtu.getTimeZone("$escapedTz");""")
        val utcTerm = "tzUTC"
        ctx.addImmutableStateIfNotExists(tzClass, utcTerm,
          v => s"""$v = $dtu.getTimeZone("UTC");""")
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
  extends BinaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

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
      start.asInstanceOf[Long], 0 - itvl.months, 0 - itvl.microseconds, zoneId)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, i) => {
      s"""$dtu.timestampAddInterval($sd, 0 - $i.months, 0 - $i.microseconds, $zid)"""
    })
  }
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
      t1.asInstanceOf[Long], t2.asInstanceOf[Long], roundOff.asInstanceOf[Boolean], timeZone)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (d1, d2, roundOff) => {
      s"""$dtu.monthsBetween($d1, $d2, $roundOff, $tz)"""
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
  since = "1.5.0",
  deprecated = """
    Deprecated since 3.0.0. See SPARK-25496.
  """)
// scalastyle:on line.size.limit
case class ToUTCTimestamp(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  if (!SQLConf.get.utcTimestampFuncEnabled) {
    throw new AnalysisException(s"The $prettyName function has been disabled since Spark 3.0. " +
      s"Set ${SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key} to true to enable this function.")
  }

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
        val tzClass = classOf[TimeZone].getName
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        val escapedTz = StringEscapeUtils.escapeJava(tz.toString)
        val tzTerm = ctx.addMutableState(tzClass, "tz",
          v => s"""$v = $dtu.getTimeZone("$escapedTz");""")
        val utcTerm = "tzUTC"
        ctx.addImmutableStateIfNotExists(tzClass, utcTerm,
          v => s"""$v = $dtu.getTimeZone("UTC");""")
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
 * Parses a column to a timestamp based on the supplied format.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(timestamp[, fmt]) - Parses the `timestamp` expression with the `fmt` expression to
      a timestamp. Returns null with invalid input. By default, it follows casting rules to
      a timestamp if the `fmt` is omitted.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('2016-12-31 00:12:00');
       2016-12-31 00:12:00
      > SELECT _FUNC_('2016-12-31', 'yyyy-MM-dd');
       2016-12-31 00:00:00
  """,
  since = "2.2.0")
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

/**
 * Returns timestamp truncated to the unit specified by the format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(fmt, ts) - Returns timestamp `ts` truncated to the unit specified by the format model `fmt`.
    `fmt` should be one of ["MILLENNIUM", "CENTURY", "DECADE", "YEAR", "YYYY", "YY",
                            "QUARTER", "MON", "MONTH", "MM", "WEEK", "DAY", "DD",
                            "HOUR", "MINUTE", "SECOND", "MILLISECOND", "MICROSECOND"]
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
      > SELECT _FUNC_('DECADE', '2015-03-05T09:32:05.123456');
       2010-01-01 00:00:00
      > SELECT _FUNC_('CENTURY', '2015-03-05T09:32:05.123456');
       2001-01-01 00:00:00
  """,
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
      DateTimeUtils.truncTimestamp(t.asInstanceOf[Long], level, timeZone)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    codeGenHelper(ctx, ev, minLevel = MIN_LEVEL_OF_TIMESTAMP_TRUNC, true) {
      (date: String, fmt: String) =>
        s"truncTimestamp($date, $fmt, $tz);"
    }
  }
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
      TimestampFormatter(constFormat.toString, zoneId)
    } catch {
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
              TimestampFormatter(formatString, zoneId).parse(
                t.asInstanceOf[UTF8String].toString) / downScaleFactor
            } catch {
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
        val locale = ctx.addReferenceObj("locale", Locale.US)
        val tf = TimestampFormatter.getClass.getName.stripSuffix("$")
        nullSafeCodeGen(ctx, ev, (string, format) => {
          s"""
            try {
              ${ev.value} = $tf$$.MODULE$$.apply($format.toString(), $zid, $locale)
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

/**
 * Converts time string with given pattern.
 * Deterministic version of [[UnixTimestamp]], must have at least one parameter.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr[, pattern]) - Returns the UNIX timestamp of the given time.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-04-08', 'yyyy-MM-dd');
       1460098800
  """,
  since = "1.6.0")
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
    this(time, Literal("uuuu-MM-dd HH:mm:ss"))
  }

  override def prettyName: String = "to_unix_timestamp"
}

/**
 * Converts time string with given pattern to Unix time stamp (in seconds), returns null if fail.
 * See [https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html].
 * Note that hive Language Manual says it returns 0 if fail, but in fact it returns null.
 * If the second parameter is missing, use "uuuu-MM-dd HH:mm:ss".
 * If no parameters provided, the first parameter will be current_timestamp.
 * If the first parameter is a Date or Timestamp instead of String, we will ignore the
 * second parameter.
 */
@ExpressionDescription(
  usage = "_FUNC_([expr[, pattern]]) - Returns the UNIX timestamp of current or specified time.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       1476884637
      > SELECT _FUNC_('2016-04-08', 'yyyy-MM-dd');
       1460041200
  """,
  since = "1.5.0")
case class UnixTimestamp(timeExp: Expression, format: Expression, timeZoneId: Option[String] = None)
  extends UnixTime {

  def this(timeExp: Expression, format: Expression) = this(timeExp, format, None)

  override def left: Expression = timeExp
  override def right: Expression = format

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  def this(time: Expression) = {
    this(time, Literal("uuuu-MM-dd HH:mm:ss"))
  }

  def this() = {
    this(CurrentTimestamp())
  }

  override def prettyName: String = "unix_timestamp"
}

/**
 * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
 * representing the timestamp of that moment in the current system time zone in the given
 * format. If the format is missing, using format like "1970-01-01 00:00:00".
 * Note that hive Language Manual says it returns 0 if fail, but in fact it returns null.
 */
@ExpressionDescription(
  usage = "_FUNC_(unix_time, format) - Returns `unix_time` in the specified `format`.",
  examples = """
    Examples:
      > SELECT _FUNC_(0, 'yyyy-MM-dd HH:mm:ss');
       1969-12-31 16:00:00
  """,
  since = "1.5.0")
case class FromUnixTime(sec: Expression, format: Expression, timeZoneId: Option[String] = None)
  extends BinaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(sec: Expression, format: Expression) = this(sec, format, None)

  override def left: Expression = sec
  override def right: Expression = format

  override def prettyName: String = "from_unixtime"

  def this(unix: Expression) = {
    this(unix, Literal("uuuu-MM-dd HH:mm:ss"))
  }

  override def dataType: DataType = StringType
  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  private lazy val constFormat: UTF8String = right.eval().asInstanceOf[UTF8String]
  private lazy val formatter: TimestampFormatter =
    try {
      TimestampFormatter(constFormat.toString, zoneId)
    } catch {
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
            case NonFatal(_) => null
          }
        }
      } else {
        val f = format.eval(input)
        if (f == null) {
          null
        } else {
          try {
            UTF8String.fromString(TimestampFormatter(f.toString, zoneId)
              .format(time.asInstanceOf[Long] * MICROS_PER_SECOND))
          } catch {
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
      val locale = ctx.addReferenceObj("locale", Locale.US)
      val tf = TimestampFormatter.getClass.getName.stripSuffix("$")
      nullSafeCodeGen(ctx, ev, (seconds, f) => {
        s"""
        try {
          ${ev.value} = UTF8String.fromString($tf$$.MODULE$$.apply($f.toString(), $zid, $locale).
            format($seconds * 1000000L));
        } catch (java.lang.IllegalArgumentException e) {
          ${ev.isNull} = true;
        }"""
      })
    }
  }
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

case class Epoch(child: Expression, timeZoneId: Option[String] = None)
    extends UnaryExpression with ImplicitCastInputTypes with TimeZoneAwareExpression {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
  // DecimalType is used to not lose precision while converting microseconds to
  // the fractional part of seconds. Scale 6 is taken to have all microseconds as
  // the fraction. The precision 20 should cover whole valid range of years [1, 9999]
  // plus negative years that can be used in some cases though are not officially supported.
  override def dataType: DataType = DecimalType(20, 6)
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getEpoch(timestamp.asInstanceOf[Long], zoneId)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    defineCodeGen(ctx, ev, c => s"$dtu.getEpoch($c, $zid)")
  }
}

/**
 * Returns the interval from startTimestamp to endTimestamp in which the `months` field
 * is set to 0 and the `microseconds` field is initialized to the microsecond difference
 * between the given timestamps.
 */
case class TimestampDiff(endTimestamp: Expression, startTimestamp: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = endTimestamp
  override def right: Expression = startTimestamp
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, TimestampType)
  override def dataType: DataType = CalendarIntervalType

  override def nullSafeEval(end: Any, start: Any): Any = {
    new CalendarInterval(0, end.asInstanceOf[Long] - start.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (end, start) =>
      s"new org.apache.spark.unsafe.types.CalendarInterval(0, $end - $start)")
  }
}
