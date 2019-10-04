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

import java.time.{LocalDate, ZoneId}
import java.time.temporal.IsoFields
import java.util.{Locale, TimeZone}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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

  @transient lazy val timeZone: TimeZone = DateTimeUtils.getTimeZone(timeZoneId.get)
  @transient lazy val zoneId: ZoneId = DateTimeUtils.getZoneId(timeZoneId.get)
}

/**
 * Returns the current date at the start of query evaluation.
 * All calls of current_date within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current date at the start of query evaluation.",
  since = "1.5.0")
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
  since = "1.5.0")
case class DateAdd(startDate: Expression, days: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, d: Any): Any = {
    start.asInstanceOf[Int] + d.asInstanceOf[Int]
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
  since = "1.5.0")
case class DateSub(startDate: Expression, days: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {
  override def left: Expression = startDate
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, d: Any): Any = {
    start.asInstanceOf[Int] - d.asInstanceOf[Int]
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (sd, d) => {
      s"""${ev.value} = $sd - $d;"""
    })
  }

  override def prettyName: String = "date_sub"
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the day of year of the date/timestamp.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-04-09');
       100
  """,
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

case class IsoYear(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getIsoYear(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getIsoYear($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the quarter of the year for date, in the range 1 to 4.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-08-31');
       3
  """,
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
 * Parses a column to a date based on the given format.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(date_str[, fmt]) - Parses the `date_str` expression with the `fmt` expression to
      a date. Returns null with invalid input. By default, it follows casting rules to a date if
      the `fmt` is omitted.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30 04:17:52');
       2009-07-30
      > SELECT _FUNC_('2016-12-31', 'yyyy-MM-dd');
       2016-12-31
  """,
  since = "1.5.0")
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
    `fmt` should be one of ["week", "mon", "month", "mm", "quarter", "year", "yyyy", "yy", "decade", "century", "millennium"]
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
      > SELECT _FUNC_('2015-10-27', 'DECADE');
       2010-01-01
      > SELECT _FUNC_('1981-01-19', 'century');
       1901-01-01
      > SELECT _FUNC_('1981-01-19', 'millennium');
       1001-01-01
  """,
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

case class Millennium(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getMillennium(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getMillennium($c)")
  }
}

case class Century(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getCentury(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getCentury($c)")
  }
}

case class Decade(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getDecade(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getDecade($c)")
  }
}

object DatePart {

  def parseExtractField(
      extractField: String,
      source: Expression,
      errorHandleFunc: => Nothing): Expression = extractField.toUpperCase(Locale.ROOT) match {
    case "MILLENNIUM" | "MILLENNIA" | "MIL" | "MILS" => Millennium(source)
    case "CENTURY" | "CENTURIES" | "C" | "CENT" => Century(source)
    case "DECADE" | "DECADES" | "DEC" | "DECS" => Decade(source)
    case "YEAR" | "Y" | "YEARS" | "YR" | "YRS" => Year(source)
    case "ISOYEAR" => IsoYear(source)
    case "QUARTER" | "QTR" => Quarter(source)
    case "MONTH" | "MON" | "MONS" | "MONTHS" => Month(source)
    case "WEEK" | "W" | "WEEKS" => WeekOfYear(source)
    case "DAY" | "D" | "DAYS" => DayOfMonth(source)
    case "DAYOFWEEK" => DayOfWeek(source)
    case "DOW" => Subtract(DayOfWeek(source), Literal(1))
    case "ISODOW" => Add(WeekDay(source), Literal(1))
    case "DOY" => DayOfYear(source)
    case "HOUR" | "H" | "HOURS" | "HR" | "HRS" => Hour(source)
    case "MINUTE" | "M" | "MIN" | "MINS" | "MINUTES" => Minute(source)
    case "SECOND" | "S" | "SEC" | "SECONDS" | "SECS" => SecondWithFraction(source)
    case "MILLISECONDS" | "MSEC" | "MSECS" | "MILLISECON" | "MSECONDS" | "MS" =>
      Milliseconds(source)
    case "MICROSECONDS" | "USEC" | "USECS" | "USECONDS" | "MICROSECON" | "US" =>
      Microseconds(source)
    case "EPOCH" => Epoch(source)
    case _ => errorHandleFunc
  }
}

@ExpressionDescription(
  usage = "_FUNC_(field, source) - Extracts a part of the date/timestamp.",
  arguments = """
    Arguments:
      * field - selects which part of the source should be extracted. Supported string values are:
                ["MILLENNIUM", ("MILLENNIA", "MIL", "MILS"),
                 "CENTURY", ("CENTURIES", "C", "CENT"),
                 "DECADE", ("DECADES", "DEC", "DECS"),
                 "YEAR", ("Y", "YEARS", "YR", "YRS"),
                 "ISOYEAR",
                 "QUARTER", ("QTR"),
                 "MONTH", ("MON", "MONS", "MONTHS"),
                 "WEEK", ("W", "WEEKS"),
                 "DAY", ("D", "DAYS"),
                 "DAYOFWEEK",
                 "DOW",
                 "ISODOW",
                 "DOY",
                 "HOUR", ("H", "HOURS", "HR", "HRS"),
                 "MINUTE", ("M", "MIN", "MINS", "MINUTES"),
                 "SECOND", ("S", "SEC", "SECONDS", "SECS"),
                 "MILLISECONDS", ("MSEC", "MSECS", "MILLISECON", "MSECONDS", "MS"),
                 "MICROSECONDS", ("USEC", "USECS", "USECONDS", "MICROSECON", "US"),
                 "EPOCH"]
      * source - a date (or timestamp) column from where `field` should be extracted
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
  """,
  since = "3.0.0")
case class DatePart(field: Expression, source: Expression, child: Expression)
  extends RuntimeReplaceable {

  def this(field: Expression, source: Expression) {
    this(field, source, {
      if (!field.foldable) {
        throw new AnalysisException("The field parameter needs to be a foldable string value.")
      }
      val fieldEval = field.eval()
      if (fieldEval == null) {
        Literal(null, DoubleType)
      } else {
        val fieldStr = fieldEval.asInstanceOf[UTF8String].toString
        DatePart.parseExtractField(fieldStr, source, {
          throw new AnalysisException(s"Literals of type '$fieldStr' are currently not supported.")
        })
      }
    })
  }

  override def flatArguments: Iterator[Any] = Iterator(field, source)
  override def sql: String = s"$prettyName(${field.sql}, ${source.sql})"
  override def prettyName: String = "date_part"
}
