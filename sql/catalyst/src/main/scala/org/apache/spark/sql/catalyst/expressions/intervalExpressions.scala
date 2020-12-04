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

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.catalyst.util.IntervalUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

abstract class ExtractIntervalPart(
    child: Expression,
    val dataType: DataType,
    func: CalendarInterval => Any,
    funcName: String)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant with Serializable {

  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType)

  override protected def nullSafeEval(interval: Any): Any = {
    func(interval.asInstanceOf[CalendarInterval])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iu = IntervalUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$iu.$funcName($c)")
  }
}

case class ExtractIntervalYears(child: Expression)
  extends ExtractIntervalPart(child, IntegerType, getYears, "getYears")

case class ExtractIntervalMonths(child: Expression)
  extends ExtractIntervalPart(child, ByteType, getMonths, "getMonths")

case class ExtractIntervalDays(child: Expression)
  extends ExtractIntervalPart(child, IntegerType, getDays, "getDays")

case class ExtractIntervalHours(child: Expression)
  extends ExtractIntervalPart(child, LongType, getHours, "getHours")

case class ExtractIntervalMinutes(child: Expression)
  extends ExtractIntervalPart(child, ByteType, getMinutes, "getMinutes")

case class ExtractIntervalSeconds(child: Expression)
  extends ExtractIntervalPart(child, DecimalType(8, 6), getSeconds, "getSeconds")

object ExtractIntervalPart {

  def parseExtractField(
      extractField: String,
      source: Expression,
      errorHandleFunc: => Nothing): Expression = extractField.toUpperCase(Locale.ROOT) match {
    case "YEAR" | "Y" | "YEARS" | "YR" | "YRS" => ExtractIntervalYears(source)
    case "MONTH" | "MON" | "MONS" | "MONTHS" => ExtractIntervalMonths(source)
    case "DAY" | "D" | "DAYS" => ExtractIntervalDays(source)
    case "HOUR" | "H" | "HOURS" | "HR" | "HRS" => ExtractIntervalHours(source)
    case "MINUTE" | "M" | "MIN" | "MINS" | "MINUTES" => ExtractIntervalMinutes(source)
    case "SECOND" | "S" | "SEC" | "SECONDS" | "SECS" => ExtractIntervalSeconds(source)
    case _ => errorHandleFunc
  }
}

abstract class IntervalNumOperation(
    interval: Expression,
    num: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant with Serializable {
  override def left: Expression = interval
  override def right: Expression = num

  protected val operation: (CalendarInterval, Double) => CalendarInterval
  protected def operationName: String

  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType, DoubleType)
  override def dataType: DataType = CalendarIntervalType

  override def nullable: Boolean = true

  override def nullSafeEval(interval: Any, num: Any): Any = {
    operation(interval.asInstanceOf[CalendarInterval], num.asInstanceOf[Double])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iu = IntervalUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (interval, num) => s"$iu.$operationName($interval, $num)")
  }

  override def prettyName: String = operationName.stripSuffix("Exact") + "_interval"
}

case class MultiplyInterval(
    interval: Expression,
    num: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends IntervalNumOperation(interval, num) {

  override protected val operation: (CalendarInterval, Double) => CalendarInterval =
    if (failOnError) multiplyExact else multiply

  override protected def operationName: String = if (failOnError) "multiplyExact" else "multiply"
}

case class DivideInterval(
    interval: Expression,
    num: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends IntervalNumOperation(interval, num) {

  override protected val operation: (CalendarInterval, Double) => CalendarInterval =
    if (failOnError) divideExact else divide

  override protected def operationName: String = if (failOnError) "divideExact" else "divide"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(years, months, weeks, days, hours, mins, secs) - Make interval from years, months, weeks, days, hours, mins and secs.",
  arguments = """
    Arguments:
      * years - the number of years, positive or negative
      * months - the number of months, positive or negative
      * weeks - the number of weeks, positive or negative
      * days - the number of days, positive or negative
      * hours - the number of hours, positive or negative
      * mins - the number of minutes, positive or negative
      * secs - the number of seconds with the fractional part in microsecond precision.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(100, 11, 1, 1, 12, 30, 01.001001);
       100 years 11 months 8 days 12 hours 30 minutes 1.001001 seconds
      > SELECT _FUNC_(100, null, 3);
       NULL
      > SELECT _FUNC_(0, 1, 0, 1, 0, 0, 100.000001);
       1 months 1 days 1 minutes 40.000001 seconds
  """,
  since = "3.0.0")
// scalastyle:on line.size.limit
case class MakeInterval(
    years: Expression,
    months: Expression,
    weeks: Expression,
    days: Expression,
    hours: Expression,
    mins: Expression,
    secs: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends SeptenaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(
      years: Expression,
      months: Expression,
      weeks: Expression,
      days: Expression,
      hours: Expression,
      mins: Expression,
      sec: Expression) = {
    this(years, months, weeks, days, hours, mins, sec, SQLConf.get.ansiEnabled)
  }
  def this(
      years: Expression,
      months: Expression,
      weeks: Expression,
      days: Expression,
      hours: Expression,
      mins: Expression) = {
    this(years, months, weeks, days, hours, mins, Literal(Decimal(0, Decimal.MAX_LONG_DIGITS, 6)),
      SQLConf.get.ansiEnabled)
  }
  def this(
      years: Expression,
      months: Expression,
      weeks: Expression,
      days: Expression,
      hours: Expression) = {
    this(years, months, weeks, days, hours, Literal(0))
  }
  def this(years: Expression, months: Expression, weeks: Expression, days: Expression) =
    this(years, months, weeks, days, Literal(0))
  def this(years: Expression, months: Expression, weeks: Expression) =
    this(years, months, weeks, Literal(0))
  def this(years: Expression, months: Expression) = this(years, months, Literal(0))
  def this(years: Expression) = this(years, Literal(0))
  def this() = this(Literal(0))

  override def children: Seq[Expression] = Seq(years, months, weeks, days, hours, mins, secs)
  // Accept `secs` as DecimalType to avoid loosing precision of microseconds while converting
  // them to the fractional part of `secs`.
  override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType, IntegerType, IntegerType,
    IntegerType, IntegerType, IntegerType, DecimalType(Decimal.MAX_LONG_DIGITS, 6))
  override def dataType: DataType = CalendarIntervalType
  override def nullable: Boolean = if (failOnError) children.exists(_.nullable) else true

  override def nullSafeEval(
      year: Any,
      month: Any,
      week: Any,
      day: Any,
      hour: Any,
      min: Any,
      sec: Option[Any]): Any = {
    try {
      IntervalUtils.makeInterval(
        year.asInstanceOf[Int],
        month.asInstanceOf[Int],
        week.asInstanceOf[Int],
        day.asInstanceOf[Int],
        hour.asInstanceOf[Int],
        min.asInstanceOf[Int],
        sec.map(_.asInstanceOf[Decimal]).getOrElse(Decimal(0, Decimal.MAX_LONG_DIGITS, 6)))
    } catch {
      case _: ArithmeticException if !failOnError => null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (year, month, week, day, hour, min, sec) => {
      val iu = IntervalUtils.getClass.getName.stripSuffix("$")
      val secFrac = sec.getOrElse("0")
      val faileOnErrorBranch = if (failOnError) "throw e;" else s"${ev.isNull} = true;"
      s"""
        try {
          ${ev.value} = $iu.makeInterval($year, $month, $week, $day, $hour, $min, $secFrac);
        } catch (java.lang.ArithmeticException e) {
          $faileOnErrorBranch
        }
      """
    })
  }

  override def prettyName: String = "make_interval"
}
