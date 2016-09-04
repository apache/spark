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

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import scala.util.Try

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback,
  ExprCode}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Returns the current date at the start of query evaluation.
 * All calls of current_date within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current date at the start of query evaluation.")
case class CurrentDate() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = DateType

  override def eval(input: InternalRow): Any = {
    DateTimeUtils.millisToDays(System.currentTimeMillis())
  }

  override def prettyName: String = "current_date"
}

/**
 * Returns the current timestamp at the start of query evaluation.
 * All calls of current_timestamp within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current timestamp at the start of query evaluation.")
case class CurrentTimestamp() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = TimestampType

  override def eval(input: InternalRow): Any = {
    System.currentTimeMillis() * 1000L
  }

  override def prettyName: String = "current_timestamp"
}

/**
 * Adds a number of days to date/timestamp.
 */
@ExpressionDescription(
  usage = "_FUNC_(instant, num_days) - Returns the date/timestamp that is num_days after instant.",
  extended = "> SELECT _FUNC_('2016-07-30', 1);\n '2016-07-31'")
case class DateAdd(instant: Expression, days: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = instant
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DateType, TimestampType), IntegerType)

  override def dataType: DataType = instant.dataType

  override def nullSafeEval(start: Any, days: Any): Any = {
    (instant.dataType, start, days) match {
      case (_: DateType, startDate: Int, days: Int) =>
        startDate + days
      case (_: TimestampType, startTimestamp: Long, days: Int) =>
        DateTimeUtils.timestampAddDays(startTimestamp, days)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    instant.dataType match {
      case DateType => nullSafeCodeGen(ctx, ev, (sd, d) => s"""${ev.value} = $sd + $d;""")
      case TimestampType =>
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        defineCodeGen(ctx, ev, (sd, d) => s"""$dtu.timestampAddDays($sd, $d)""")
    }
  }

  override def prettyName: String = "date_add"
}

/**
 * Subtracts a number of days to date/timestamp.
 */
@ExpressionDescription(
  usage = "_FUNC_(instant, num_days) - Returns the date/timestamp that is num_days before instant.",
  extended = "> SELECT _FUNC_('2016-07-30', 1);\n '2016-07-29'")
case class DateSub(instant: Expression, days: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = instant
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DateType, TimestampType), IntegerType)

  override def dataType: DataType = instant.dataType

  override def nullSafeEval(start: Any, days: Any): Any = {
    (instant.dataType, start, days) match {
      case (_: DateType, startDate: Int, days: Int) =>
        startDate - days
      case (_: TimestampType, startTimestamp: Long, days: Int) =>
        DateTimeUtils.timestampAddDays(startTimestamp, -days)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    instant.dataType match {
      case DateType => nullSafeCodeGen(ctx, ev, (sd, d) => s"""${ev.value} = $sd - $d;""")
      case TimestampType =>
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        defineCodeGen(ctx, ev, (sd, d) => s"""$dtu.timestampAddDays($sd, -$d)""")
    }
  }

  override def prettyName: String = "date_sub"
}

@ExpressionDescription(
  usage = "_FUNC_(param) - Returns the hour component of the string/timestamp/interval.",
  extended = "> SELECT _FUNC_('2009-07-30 12:58:59');\n 12")
case class Hour(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getHours(timestamp.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getHours($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(param) - Returns the minute component of the string/timestamp/interval.",
  extended = "> SELECT _FUNC_('2009-07-30 12:58:59');\n 58")
case class Minute(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getMinutes(timestamp.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getMinutes($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(param) - Returns the second component of the string/timestamp/interval.",
  extended = "> SELECT _FUNC_('2009-07-30 12:58:59');\n 59")
case class Second(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getSeconds(timestamp.asInstanceOf[Long])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getSeconds($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(param) - Returns the day of year of date/timestamp.",
  extended = "> SELECT _FUNC_('2016-04-09');\n 100")
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
  usage = "_FUNC_(param) - Returns the year component of the date/timestamp/interval.",
  extended = "> SELECT _FUNC_('2016-07-30');\n 2016")
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

@ExpressionDescription(
  usage = "_FUNC_(param) - Returns the quarter of the year for date, in the range 1 to 4.")
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
  usage = "_FUNC_(param) - Returns the month component of the date/timestamp/interval",
  extended = "> SELECT _FUNC_('2016-07-30');\n 7")
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
  usage = "_FUNC_(param) - Returns the day of month of date/timestamp, or the day of interval.",
  extended = "> SELECT _FUNC_('2009-07-30');\n 30")
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

@ExpressionDescription(
  usage = "_FUNC_(param) - Returns the week of the year of the given date.",
  extended = "> SELECT _FUNC_('2008-02-20');\n 8")
case class WeekOfYear(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  @transient private lazy val c = {
    val c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    c.setFirstDayOfWeek(Calendar.MONDAY)
    c.setMinimalDaysInFirstWeek(4)
    c
  }

  override protected def nullSafeEval(date: Any): Any = {
    c.setTimeInMillis(date.asInstanceOf[Int] * 1000L * 3600L * 24L)
    c.get(Calendar.WEEK_OF_YEAR)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, time => {
      val cal = classOf[Calendar].getName
      val c = ctx.freshName("cal")
      ctx.addMutableState(cal, c,
        s"""
          $c = $cal.getInstance(java.util.TimeZone.getTimeZone("UTC"));
          $c.setFirstDayOfWeek($cal.MONDAY);
          $c.setMinimalDaysInFirstWeek(4);
         """)
      s"""
        $c.setTimeInMillis($time * 1000L * 3600L * 24L);
        ${ev.value} = $c.get($cal.WEEK_OF_YEAR);
      """
    })
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(date/timestamp/string, fmt) - Converts a date/timestamp/string to a value of string in the format specified by the date format fmt.",
  extended = "> SELECT _FUNC_('2016-04-08', 'y')\n '2016'")
// scalastyle:on line.size.limit
case class DateFormatClass(left: Expression, right: Expression) extends BinaryExpression
  with ImplicitCastInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)

  override protected def nullSafeEval(timestamp: Any, format: Any): Any = {
    val sdf = new SimpleDateFormat(format.toString)
    UTF8String.fromString(sdf.format(new java.util.Date(timestamp.asInstanceOf[Long] / 1000)))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sdf = classOf[SimpleDateFormat].getName
    defineCodeGen(ctx, ev, (timestamp, format) => {
      s"""UTF8String.fromString((new $sdf($format.toString()))
          .format(new java.util.Date($timestamp / 1000)))"""
    })
  }

  override def prettyName: String = "date_format"
}

/**
 * Converts time string with given pattern.
 * Deterministic version of [[UnixTimestamp]], must have at least one parameter.
 */
@ExpressionDescription(
  usage = "_FUNC_(date[, pattern]) - Returns the UNIX timestamp of the give time.")
case class ToUnixTimestamp(timeExp: Expression, format: Expression) extends UnixTime {
  override def left: Expression = timeExp
  override def right: Expression = format

  def this(time: Expression) = {
    this(time, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  override def prettyName: String = "to_unix_timestamp"
}

/**
 * Converts time string with given pattern.
 * (see [http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html])
 * to Unix time stamp (in seconds), returns null if fail.
 * Note that hive Language Manual says it returns 0 if fail, but in fact it returns null.
 * If the second parameter is missing, use "yyyy-MM-dd HH:mm:ss".
 * If no parameters provided, the first parameter will be current_timestamp.
 * If the first parameter is a Date or Timestamp instead of String, we will ignore the
 * second parameter.
 */
@ExpressionDescription(
  usage = "_FUNC_([date[, pattern]]) - Returns the UNIX timestamp of current or specified time.")
case class UnixTimestamp(timeExp: Expression, format: Expression) extends UnixTime {
  override def left: Expression = timeExp
  override def right: Expression = format

  def this(time: Expression) = {
    this(time, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  def this() = {
    this(CurrentTimestamp())
  }

  override def prettyName: String = "unix_timestamp"
}

abstract class UnixTime extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, DateType, TimestampType), StringType)

  override def dataType: DataType = LongType
  override def nullable: Boolean = true

  private lazy val constFormat: UTF8String = right.eval().asInstanceOf[UTF8String]
  private lazy val formatter: SimpleDateFormat =
    Try(new SimpleDateFormat(constFormat.toString)).getOrElse(null)

  override def eval(input: InternalRow): Any = {
    val t = left.eval(input)
    if (t == null) {
      null
    } else {
      left.dataType match {
        case DateType =>
          DateTimeUtils.daysToMillis(t.asInstanceOf[Int]) / 1000L
        case TimestampType =>
          t.asInstanceOf[Long] / 1000000L
        case StringType if right.foldable =>
          if (constFormat == null || formatter == null) {
            null
          } else {
            Try(formatter.parse(
              t.asInstanceOf[UTF8String].toString).getTime / 1000L).getOrElse(null)
          }
        case StringType =>
          val f = right.eval(input)
          if (f == null) {
            null
          } else {
            val formatString = f.asInstanceOf[UTF8String].toString
            Try(new SimpleDateFormat(formatString).parse(
              t.asInstanceOf[UTF8String].toString).getTime / 1000L).getOrElse(null)
          }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    left.dataType match {
      case StringType if right.foldable =>
        val sdf = classOf[SimpleDateFormat].getName
        if (formatter == null) {
          ExprCode("", "true", ctx.defaultValue(dataType))
        } else {
          val formatterName = ctx.addReferenceObj("formatter", formatter, sdf)
          val eval1 = left.genCode(ctx)
          ev.copy(code = s"""
            ${eval1.code}
            boolean ${ev.isNull} = ${eval1.isNull};
            ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
            if (!${ev.isNull}) {
              try {
                ${ev.value} = $formatterName.parse(${eval1.value}.toString()).getTime() / 1000L;
              } catch (java.text.ParseException e) {
                ${ev.isNull} = true;
              }
            }""")
        }
      case StringType =>
        val sdf = classOf[SimpleDateFormat].getName
        nullSafeCodeGen(ctx, ev, (string, format) => {
          s"""
            try {
              ${ev.value} =
                (new $sdf($format.toString())).parse($string.toString()).getTime() / 1000L;
            } catch (java.lang.IllegalArgumentException e) {
              ${ev.isNull} = true;
            } catch (java.text.ParseException e) {
              ${ev.isNull} = true;
            }
          """
        })
      case TimestampType =>
        val eval1 = left.genCode(ctx)
        ev.copy(code = s"""
          ${eval1.code}
          boolean ${ev.isNull} = ${eval1.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = ${eval1.value} / 1000000L;
          }""")
      case DateType =>
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        val eval1 = left.genCode(ctx)
        ev.copy(code = s"""
          ${eval1.code}
          boolean ${ev.isNull} = ${eval1.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $dtu.daysToMillis(${eval1.value}) / 1000L;
          }""")
    }
  }

  override def prettyName: String = "unix_time"
}

/**
 * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
 * representing the timestamp of that moment in the current system time zone in the given
 * format. If the format is missing, using format like "1970-01-01 00:00:00".
 * Note that hive Language Manual says it returns 0 if fail, but in fact it returns null.
 */
@ExpressionDescription(
  usage = "_FUNC_(unix_time, format) - Returns unix_time in the specified format",
  extended = "> SELECT _FUNC_(0, 'yyyy-MM-dd HH:mm:ss');\n '1970-01-01 00:00:00'")
case class FromUnixTime(sec: Expression, format: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = sec
  override def right: Expression = format

  override def prettyName: String = "from_unixtime"

  def this(unix: Expression) = {
    this(unix, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  override def dataType: DataType = StringType
  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  private lazy val constFormat: UTF8String = right.eval().asInstanceOf[UTF8String]
  private lazy val formatter: SimpleDateFormat =
    Try(new SimpleDateFormat(constFormat.toString)).getOrElse(null)

  override def eval(input: InternalRow): Any = {
    val time = left.eval(input)
    if (time == null) {
      null
    } else {
      if (format.foldable) {
        if (constFormat == null || formatter == null) {
          null
        } else {
          Try(UTF8String.fromString(formatter.format(
            new java.util.Date(time.asInstanceOf[Long] * 1000L)))).getOrElse(null)
        }
      } else {
        val f = format.eval(input)
        if (f == null) {
          null
        } else {
          Try(UTF8String.fromString(new SimpleDateFormat(
            f.asInstanceOf[UTF8String].toString).format(new java.util.Date(
              time.asInstanceOf[Long] * 1000L)))).getOrElse(null)
        }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sdf = classOf[SimpleDateFormat].getName
    if (format.foldable) {
      if (formatter == null) {
        ExprCode("", "true", "(UTF8String) null")
      } else {
        val formatterName = ctx.addReferenceObj("formatter", formatter, sdf)
        val t = left.genCode(ctx)
        ev.copy(code = s"""
          ${t.code}
          boolean ${ev.isNull} = ${t.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            try {
              ${ev.value} = UTF8String.fromString($formatterName.format(
                new java.util.Date(${t.value} * 1000L)));
            } catch (java.lang.IllegalArgumentException e) {
              ${ev.isNull} = true;
            }
          }""")
      }
    } else {
      nullSafeCodeGen(ctx, ev, (seconds, f) => {
        s"""
        try {
          ${ev.value} = UTF8String.fromString((new $sdf($f.toString())).format(
            new java.util.Date($seconds * 1000L)));
        } catch (java.lang.IllegalArgumentException e) {
          ${ev.isNull} = true;
        }""".stripMargin
      })
    }
  }
}

/**
 * Returns the last day of the month which the date belongs to.
 */
@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the last day of the month which the date belongs to.",
  extended = "> SELECT _FUNC_('2009-01-12');\n '2009-01-31'")
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
  usage = "_FUNC_(start_date, day_of_week) - Returns the first date which is later than start_date and named as indicated.",
  extended = "> SELECT _FUNC_('2015-01-14', 'TU');\n '2015-01-20'")
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
case class TimeAdd(start: Expression, interval: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = start
  override def right: Expression = interval

  override def toString: String = s"$left + $right"
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, CalendarIntervalType)

  override def dataType: DataType = TimestampType

  override def nullSafeEval(start: Any, interval: Any): Any = {
    val itvl = interval.asInstanceOf[CalendarInterval]
    DateTimeUtils.timestampAddInterval(
      start.asInstanceOf[Long], itvl.months, itvl.microseconds)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, i) => {
      s"""$dtu.timestampAddInterval($sd, $i.months, $i.microseconds)"""
    })
  }
}

/**
 * Assumes given timestamp is UTC and converts to given timezone.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp, string timezone) - Assumes given timestamp is UTC and converts to given timezone.")
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
      val tz = right.eval()
      if (tz == null) {
        ev.copy(code = s"""
           |boolean ${ev.isNull} = true;
           |long ${ev.value} = 0;
         """.stripMargin)
      } else {
        val tzTerm = ctx.freshName("tz")
        val utcTerm = ctx.freshName("utc")
        val tzClass = classOf[TimeZone].getName
        ctx.addMutableState(tzClass, tzTerm, s"""$tzTerm = $tzClass.getTimeZone("$tz");""")
        ctx.addMutableState(tzClass, utcTerm, s"""$utcTerm = $tzClass.getTimeZone("UTC");""")
        val eval = left.genCode(ctx)
        ev.copy(code = s"""
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
case class TimeSub(start: Expression, interval: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = start
  override def right: Expression = interval

  override def toString: String = s"$left - $right"
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, CalendarIntervalType)

  override def dataType: DataType = TimestampType

  override def nullSafeEval(start: Any, interval: Any): Any = {
    val itvl = interval.asInstanceOf[CalendarInterval]
    DateTimeUtils.timestampAddInterval(
      start.asInstanceOf[Long], 0 - itvl.months, 0 - itvl.microseconds)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, i) => {
      s"""$dtu.timestampAddInterval($sd, 0 - $i.months, 0 - $i.microseconds)"""
    })
  }
}

/**
 * Returns the date/timestamp that is num_months after instant.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(instant, num_months) - Returns the date/timestamp that is num_months after instant.",
  extended = "> SELECT _FUNC_('2016-08-31', 1);\n '2016-09-30'")
// scalastyle:on line.size.limit
case class AddMonths(instant: Expression, numMonths: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = instant
  override def right: Expression = numMonths

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DateType, TimestampType), IntegerType)

  override def dataType: DataType = instant.dataType

  override def nullSafeEval(start: Any, months: Any): Any = {
    (instant.dataType, start, months) match {
      case (_: DateType, startDate: Int, months: Int) =>
        DateTimeUtils.dateAddMonths(startDate, months)
      case (_: TimestampType, startTimestamp: Long, months: Int) =>
        DateTimeUtils.timestampAddInterval(startTimestamp, months, 0)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, m) => instant.dataType match {
      case DateType => s"""$dtu.dateAddMonths($sd, $m)"""
      case TimestampType => s"""$dtu.timestampAddInterval($sd, $m, 0)"""
    })
  }

  override def prettyName: String = "add_months"
}

/**
 * Returns number of months between dates date1 and date2.
 */
@ExpressionDescription(
  usage = "_FUNC_(date1, date2) - returns number of months between dates date1 and date2.",
  extended = "> SELECT _FUNC_('1997-02-28 10:30:00', '1996-10-30');\n 3.94959677")
case class MonthsBetween(date1: Expression, date2: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = date1
  override def right: Expression = date2

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, TimestampType)

  override def dataType: DataType = DoubleType

  override def nullSafeEval(t1: Any, t2: Any): Any = {
    DateTimeUtils.monthsBetween(t1.asInstanceOf[Long], t2.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (l, r) => {
      s"""$dtu.monthsBetween($l, $r)"""
    })
  }

  override def prettyName: String = "months_between"
}

/**
 * Assumes given timestamp is in given timezone and converts to UTC.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp, string timezone) - Assumes given timestamp is in given timezone and converts to UTC.")
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
      val tz = right.eval()
      if (tz == null) {
        ev.copy(code = s"""
           |boolean ${ev.isNull} = true;
           |long ${ev.value} = 0;
         """.stripMargin)
      } else {
        val tzTerm = ctx.freshName("tz")
        val utcTerm = ctx.freshName("utc")
        val tzClass = classOf[TimeZone].getName
        ctx.addMutableState(tzClass, tzTerm, s"""$tzTerm = $tzClass.getTimeZone("$tz");""")
        ctx.addMutableState(tzClass, utcTerm, s"""$utcTerm = $tzClass.getTimeZone("UTC");""")
        val eval = left.genCode(ctx)
        ev.copy(code = s"""
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
 * Returns the date part of a timestamp or string.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Extracts the date part of the date or datetime expression expr.",
  extended = "> SELECT _FUNC_('2009-07-30 04:17:52');\n '2009-07-30'")
case class ToDate(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  // Implicit casting of spark will accept string in both date and timestamp format, as
  // well as TimestampType.
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = DateType

  override def eval(input: InternalRow): Any = child.eval(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, d => d)
  }

  override def prettyName: String = "to_date"
}

/**
 * Returns timestamp truncated to the unit specified by the format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp, fmt) - Returns returns timestamp with the time portion truncated to the unit specified by the format model fmt.",
  extended = "> SELECT _FUNC_('2009-02-12', 'MM')\n '2009-02-01 00:00:00'\n> SELECT _FUNC_('2015-10-27', 'YEAR');\n '2015-01-01 00:00:00'")
// scalastyle:on line.size.limit
case class TruncDate(timestamp: Expression, format: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = timestamp
  override def right: Expression = format

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)

  override def dataType: DataType = TimestampType

  override def nullable: Boolean = true

  override def prettyName: String = "trunc"

  private lazy val truncLevel: Int =
    DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])

  override def eval(input: InternalRow): Any = {
    val level = if (format.foldable) {
      truncLevel
    } else {
      DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])
    }
    if (level == -1) {
      // unknown format
      null
    } else {
      val ts = timestamp.eval(input)
      if (ts == null) {
        null
      } else {
        DateTimeUtils.truncDate(ts.asInstanceOf[Long], level)
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")

    if (format.foldable) {
      if (truncLevel == -1) {
        ev.copy(code = s"""
          boolean ${ev.isNull} = true;
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};""")
      } else {
        val ts = timestamp.genCode(ctx)
        ev.copy(code = s"""
          ${ts.code}
          boolean ${ev.isNull} = ${ts.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $dtu.truncDate(${ts.value}, $truncLevel);
          }""")
      }
    } else {
      nullSafeCodeGen(ctx, ev, (dateVal, fmt) => {
        val form = ctx.freshName("form")
        s"""
          int $form = $dtu.parseTruncLevel($fmt);
          if ($form == -1) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = $dtu.truncDate($dateVal, $form);
          }
        """
      })
    }
  }
}

/**
 * Returns the number of days from startDate to endDate.
 */
@ExpressionDescription(
  usage = "_FUNC_(date1, date2) - Returns the number of days between date1 and date2.",
  extended = "> SELECT _FUNC_('2009-07-30', '2009-07-31');\n 1")
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
