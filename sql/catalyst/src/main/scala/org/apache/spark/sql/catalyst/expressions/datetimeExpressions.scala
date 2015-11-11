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

import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import scala.util.Try

/**
 * Returns the current date at the start of query evaluation.
 * All calls of current_date within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
case class CurrentDate() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = DateType

  override def eval(input: InternalRow): Any = {
    DateTimeUtils.millisToDays(System.currentTimeMillis())
  }
}

/**
 * Returns the current timestamp at the start of query evaluation.
 * All calls of current_timestamp within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
case class CurrentTimestamp() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = TimestampType

  override def eval(input: InternalRow): Any = {
    System.currentTimeMillis() * 1000L
  }
}

/**
 * Adds a number of days to startdate.
 */
case class DateAdd(startDate: Expression, days: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, d: Any): Any = {
    start.asInstanceOf[Int] + d.asInstanceOf[Int]
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (sd, d) => {
      s"""${ev.value} = $sd + $d;"""
    })
  }
}

/**
 * Subtracts a number of days to startdate.
 */
case class DateSub(startDate: Expression, days: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {
  override def left: Expression = startDate
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, d: Any): Any = {
    start.asInstanceOf[Int] - d.asInstanceOf[Int]
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (sd, d) => {
      s"""${ev.value} = $sd - $d;"""
    })
  }
}

case class Hour(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getHours(timestamp.asInstanceOf[Long])
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getHours($c)")
  }
}

case class Minute(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getMinutes(timestamp.asInstanceOf[Long])
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getMinutes($c)")
  }
}

case class Second(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getSeconds(timestamp.asInstanceOf[Long])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getSeconds($c)")
  }
}

case class DayOfYear(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getDayInYear(date.asInstanceOf[Int])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getDayInYear($c)")
  }
}


case class Year(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getYear(date.asInstanceOf[Int])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getYear($c)")
  }
}

case class Quarter(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getQuarter(date.asInstanceOf[Int])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getQuarter($c)")
  }
}

case class Month(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getMonth(date.asInstanceOf[Int])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getMonth($c)")
  }
}

case class DayOfMonth(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getDayOfMonth(date.asInstanceOf[Int])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getDayOfMonth($c)")
  }
}

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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
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

case class DateFormatClass(left: Expression, right: Expression) extends BinaryExpression
  with ImplicitCastInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)

  override protected def nullSafeEval(timestamp: Any, format: Any): Any = {
    val sdf = new SimpleDateFormat(format.toString)
    UTF8String.fromString(sdf.format(new java.util.Date(timestamp.asInstanceOf[Long] / 1000)))
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
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
case class ToUnixTimestamp(timeExp: Expression, format: Expression) extends UnixTime {
  override def left: Expression = timeExp
  override def right: Expression = format

  def this(time: Expression) = {
    this(time, Literal("yyyy-MM-dd HH:mm:ss"))
  }
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
case class UnixTimestamp(timeExp: Expression, format: Expression) extends UnixTime {
  override def left: Expression = timeExp
  override def right: Expression = format

  def this(time: Expression) = {
    this(time, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  def this() = {
    this(CurrentTimestamp())
  }
}

abstract class UnixTime extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, DateType, TimestampType), StringType)

  override def dataType: DataType = LongType

  private lazy val constFormat: UTF8String = right.eval().asInstanceOf[UTF8String]

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
          if (constFormat != null) {
            Try(new SimpleDateFormat(constFormat.toString).parse(
              t.asInstanceOf[UTF8String].toString).getTime / 1000L).getOrElse(null)
          } else {
            null
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    left.dataType match {
      case StringType if right.foldable =>
        val sdf = classOf[SimpleDateFormat].getName
        val fString = if (constFormat == null) null else constFormat.toString
        val formatter = ctx.freshName("formatter")
        if (fString == null) {
          s"""
            boolean ${ev.isNull} = true;
            ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          """
        } else {
          val eval1 = left.gen(ctx)
          s"""
            ${eval1.code}
            boolean ${ev.isNull} = ${eval1.isNull};
            ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
            if (!${ev.isNull}) {
              try {
                $sdf $formatter = new $sdf("$fString");
                ${ev.value} =
                  $formatter.parse(${eval1.value}.toString()).getTime() / 1000L;
              } catch (java.lang.Throwable e) {
                ${ev.isNull} = true;
              }
            }
          """
        }
      case StringType =>
        val sdf = classOf[SimpleDateFormat].getName
        nullSafeCodeGen(ctx, ev, (string, format) => {
          s"""
            try {
              ${ev.value} =
                (new $sdf($format.toString())).parse($string.toString()).getTime() / 1000L;
            } catch (java.lang.Throwable e) {
              ${ev.isNull} = true;
            }
          """
        })
      case TimestampType =>
        val eval1 = left.gen(ctx)
        s"""
          ${eval1.code}
          boolean ${ev.isNull} = ${eval1.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = ${eval1.value} / 1000000L;
          }
        """
      case DateType =>
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        val eval1 = left.gen(ctx)
        s"""
          ${eval1.code}
          boolean ${ev.isNull} = ${eval1.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $dtu.daysToMillis(${eval1.value}) / 1000L;
          }
        """
    }
  }
}

/**
 * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
 * representing the timestamp of that moment in the current system time zone in the given
 * format. If the format is missing, using format like "1970-01-01 00:00:00".
 * Note that hive Language Manual says it returns 0 if fail, but in fact it returns null.
 */
case class FromUnixTime(sec: Expression, format: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = sec
  override def right: Expression = format

  def this(unix: Expression) = {
    this(unix, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  private lazy val constFormat: UTF8String = right.eval().asInstanceOf[UTF8String]

  override def eval(input: InternalRow): Any = {
    val time = left.eval(input)
    if (time == null) {
      null
    } else {
      if (format.foldable) {
        if (constFormat == null) {
          null
        } else {
          Try(UTF8String.fromString(new SimpleDateFormat(constFormat.toString).format(
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val sdf = classOf[SimpleDateFormat].getName
    if (format.foldable) {
      if (constFormat == null) {
        s"""
          boolean ${ev.isNull} = true;
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
        """
      } else {
        val t = left.gen(ctx)
        s"""
          ${t.code}
          boolean ${ev.isNull} = ${t.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            try {
              ${ev.value} = UTF8String.fromString(new $sdf("${constFormat.toString}").format(
                new java.util.Date(${t.value} * 1000L)));
            } catch (java.lang.Throwable e) {
              ${ev.isNull} = true;
            }
          }
        """
      }
    } else {
      nullSafeCodeGen(ctx, ev, (seconds, f) => {
        s"""
        try {
          ${ev.value} = UTF8String.fromString((new $sdf($f.toString())).format(
            new java.util.Date($seconds * 1000L)));
        } catch (java.lang.Throwable e) {
          ${ev.isNull} = true;
        }""".stripMargin
      })
    }
  }
}

/**
 * Returns the last day of the month which the date belongs to.
 */
case class LastDay(startDate: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def child: Expression = startDate

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = DateType

  override def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getLastDayOfMonth(date.asInstanceOf[Int])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
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
case class NextDay(startDate: Expression, dayOfWeek: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = dayOfWeek

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, StringType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, dayOfW: Any): Any = {
    val dow = DateTimeUtils.getDayOfWeekFromString(dayOfW.asInstanceOf[UTF8String])
    if (dow == -1) {
      null
    } else {
      val sd = start.asInstanceOf[Int]
      DateTimeUtils.getNextDateForDayOfWeek(sd, dow)
    }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, i) => {
      s"""$dtu.timestampAddInterval($sd, $i.months, $i.microseconds)"""
    })
  }
}

/**
 * Assumes given timestamp is UTC and converts to given timezone.
 */
case class FromUTCTimestamp(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)
  override def dataType: DataType = TimestampType
  override def prettyName: String = "from_utc_timestamp"

  override def nullSafeEval(time: Any, timezone: Any): Any = {
    DateTimeUtils.fromUTCTime(time.asInstanceOf[Long],
      timezone.asInstanceOf[UTF8String].toString)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    if (right.foldable) {
      val tz = right.eval()
      if (tz == null) {
        s"""
           |boolean ${ev.isNull} = true;
           |long ${ev.value} = 0;
         """.stripMargin
      } else {
        val tzTerm = ctx.freshName("tz")
        val tzClass = classOf[TimeZone].getName
        ctx.addMutableState(tzClass, tzTerm, s"""$tzTerm = $tzClass.getTimeZone("$tz");""")
        val eval = left.gen(ctx)
        s"""
           |${eval.code}
           |boolean ${ev.isNull} = ${eval.isNull};
           |long ${ev.value} = 0;
           |if (!${ev.isNull}) {
           |  ${ev.value} = ${eval.value} +
           |   ${tzTerm}.getOffset(${eval.value} / 1000) * 1000L;
           |}
         """.stripMargin
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, i) => {
      s"""$dtu.timestampAddInterval($sd, 0 - $i.months, 0 - $i.microseconds)"""
    })
  }
}

/**
 * Returns the date that is num_months after start_date.
 */
case class AddMonths(startDate: Expression, numMonths: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = numMonths

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, months: Any): Any = {
    DateTimeUtils.dateAddMonths(start.asInstanceOf[Int], months.asInstanceOf[Int])
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, m) => {
      s"""$dtu.dateAddMonths($sd, $m)"""
    })
  }
}

/**
 * Returns number of months between dates date1 and date2.
 */
case class MonthsBetween(date1: Expression, date2: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = date1
  override def right: Expression = date2

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, TimestampType)

  override def dataType: DataType = DoubleType

  override def nullSafeEval(t1: Any, t2: Any): Any = {
    DateTimeUtils.monthsBetween(t1.asInstanceOf[Long], t2.asInstanceOf[Long])
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (l, r) => {
      s"""$dtu.monthsBetween($l, $r)"""
    })
  }
}

/**
 * Assumes given timestamp is in given timezone and converts to UTC.
 */
case class ToUTCTimestamp(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)
  override def dataType: DataType = TimestampType
  override def prettyName: String = "to_utc_timestamp"

  override def nullSafeEval(time: Any, timezone: Any): Any = {
    DateTimeUtils.toUTCTime(time.asInstanceOf[Long],
      timezone.asInstanceOf[UTF8String].toString)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    if (right.foldable) {
      val tz = right.eval()
      if (tz == null) {
        s"""
           |boolean ${ev.isNull} = true;
           |long ${ev.value} = 0;
         """.stripMargin
      } else {
        val tzTerm = ctx.freshName("tz")
        val tzClass = classOf[TimeZone].getName
        ctx.addMutableState(tzClass, tzTerm, s"""$tzTerm = $tzClass.getTimeZone("$tz");""")
        val eval = left.gen(ctx)
        s"""
           |${eval.code}
           |boolean ${ev.isNull} = ${eval.isNull};
           |long ${ev.value} = 0;
           |if (!${ev.isNull}) {
           |  ${ev.value} = ${eval.value} -
           |   ${tzTerm}.getOffset(${eval.value} / 1000) * 1000L;
           |}
         """.stripMargin
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
case class ToDate(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  // Implicit casting of spark will accept string in both date and timestamp format, as
  // well as TimestampType.
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = DateType

  override def eval(input: InternalRow): Any = child.eval(input)

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, d => d)
  }
}

/**
 * Returns date truncated to the unit specified by the format.
 */
case class TruncDate(date: Expression, format: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {
  override def left: Expression = date
  override def right: Expression = format

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, StringType)
  override def dataType: DataType = DateType
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
      val d = date.eval(input)
      if (d == null) {
        null
      } else {
        DateTimeUtils.truncDate(d.asInstanceOf[Int], level)
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")

    if (format.foldable) {
      if (truncLevel == -1) {
        s"""
          boolean ${ev.isNull} = true;
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
        """
      } else {
        val d = date.gen(ctx)
        s"""
          ${d.code}
          boolean ${ev.isNull} = ${d.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $dtu.truncDate(${d.value}, $truncLevel);
          }
        """
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
case class DateDiff(endDate: Expression, startDate: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = endDate
  override def right: Expression = startDate
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)
  override def dataType: DataType = IntegerType

  override def nullSafeEval(end: Any, start: Any): Any = {
    end.asInstanceOf[Int] - start.asInstanceOf[Int]
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (end, start) => s"$end - $start")
  }
}
