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

import java.math.RoundingMode
import java.util.Locale

import com.google.common.math.{DoubleMath, IntMath, LongMath}

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
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
  extends ExtractIntervalPart(child, IntegerType, getYears, "getYears") {
  override protected def withNewChildInternal(newChild: Expression): ExtractIntervalYears =
    copy(child = newChild)
}

case class ExtractIntervalMonths(child: Expression)
  extends ExtractIntervalPart(child, ByteType, getMonths, "getMonths") {
  override protected def withNewChildInternal(newChild: Expression): ExtractIntervalMonths =
    copy(child = newChild)
}

case class ExtractIntervalDays(child: Expression)
  extends ExtractIntervalPart(child, IntegerType, getDays, "getDays") {
  override protected def withNewChildInternal(newChild: Expression): ExtractIntervalDays =
    copy(child = newChild)
}

case class ExtractIntervalHours(child: Expression)
  extends ExtractIntervalPart(child, LongType, getHours, "getHours") {
  override protected def withNewChildInternal(newChild: Expression): ExtractIntervalHours =
    copy(child = newChild)
}

case class ExtractIntervalMinutes(child: Expression)
  extends ExtractIntervalPart(child, ByteType, getMinutes, "getMinutes") {
  override protected def withNewChildInternal(newChild: Expression): ExtractIntervalMinutes =
    copy(child = newChild)
}

case class ExtractIntervalSeconds(child: Expression)
  extends ExtractIntervalPart(child, DecimalType(8, 6), getSeconds, "getSeconds") {
  override protected def withNewChildInternal(newChild: Expression): ExtractIntervalSeconds =
    copy(child = newChild)
}

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

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): MultiplyInterval =
    copy(interval = newLeft, num = newRight)
}

case class DivideInterval(
    interval: Expression,
    num: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends IntervalNumOperation(interval, num) {

  override protected val operation: (CalendarInterval, Double) => CalendarInterval =
    if (failOnError) divideExact else divide

  override protected def operationName: String = if (failOnError) "divideExact" else "divide"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): DivideInterval =
    copy(interval = newLeft, num = newRight)
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
  since = "3.0.0",
  group = "datetime_funcs")
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
      val failOnErrorBranch = if (failOnError) "throw e;" else s"${ev.isNull} = true;"
      s"""
        try {
          ${ev.value} = $iu.makeInterval($year, $month, $week, $day, $hour, $min, $secFrac);
        } catch (java.lang.ArithmeticException e) {
          $failOnErrorBranch
        }
      """
    })
  }

  override def prettyName: String = "make_interval"

  // Seq(years, months, weeks, days, hours, mins, secs)
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): MakeInterval =
    copy(
      years = newChildren(0),
      months = newChildren(1),
      weeks = newChildren(2),
      days = newChildren(3),
      hours = newChildren(4),
      mins = newChildren(5),
      secs = newChildren(6)
    )
}

// Multiply an year-month interval by a numeric
case class MultiplyYMInterval(
    interval: Expression,
    num: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant with Serializable {
  override def left: Expression = interval
  override def right: Expression = num

  override def inputTypes: Seq[AbstractDataType] = Seq(YearMonthIntervalType, NumericType)
  override def dataType: DataType = YearMonthIntervalType

  @transient
  private lazy val evalFunc: (Int, Any) => Any = right.dataType match {
    case ByteType | ShortType | IntegerType => (months: Int, num) =>
      Math.multiplyExact(months, num.asInstanceOf[Number].intValue())
    case LongType => (months: Int, num) =>
      Math.toIntExact(Math.multiplyExact(months, num.asInstanceOf[Long]))
    case FloatType | DoubleType => (months: Int, num) =>
      DoubleMath.roundToInt(months * num.asInstanceOf[Number].doubleValue(), RoundingMode.HALF_UP)
    case _: DecimalType => (months: Int, num) =>
      val decimalRes = ((new Decimal).set(months) * num.asInstanceOf[Decimal]).toJavaBigDecimal
      decimalRes.setScale(0, java.math.RoundingMode.HALF_UP).intValueExact()
  }

  override def nullSafeEval(interval: Any, num: Any): Any = {
    evalFunc(interval.asInstanceOf[Int], num)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = right.dataType match {
    case ByteType | ShortType | IntegerType =>
      defineCodeGen(ctx, ev, (m, n) => s"java.lang.Math.multiplyExact($m, $n)")
    case LongType =>
      val jlm = classOf[Math].getName
      defineCodeGen(ctx, ev, (m, n) => s"$jlm.toIntExact($jlm.multiplyExact($m, $n))")
    case FloatType | DoubleType =>
      val dm = classOf[DoubleMath].getName
      defineCodeGen(ctx, ev, (m, n) =>
        s"$dm.roundToInt($m * (double)$n, java.math.RoundingMode.HALF_UP)")
    case _: DecimalType =>
      defineCodeGen(ctx, ev, (m, n) =>
        s"((new Decimal()).set($m).$$times($n)).toJavaBigDecimal()" +
        ".setScale(0, java.math.RoundingMode.HALF_UP).intValueExact()")
  }

  override def toString: String = s"($left * $right)"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): MultiplyYMInterval =
    copy(interval = newLeft, num = newRight)
}

// Multiply a day-time interval by a numeric
case class MultiplyDTInterval(
    interval: Expression,
    num: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant with Serializable {
  override def left: Expression = interval
  override def right: Expression = num

  override def inputTypes: Seq[AbstractDataType] = Seq(DayTimeIntervalType, NumericType)
  override def dataType: DataType = DayTimeIntervalType

  @transient
  private lazy val evalFunc: (Long, Any) => Any = right.dataType match {
    case _: IntegralType => (micros: Long, num) =>
      Math.multiplyExact(micros, num.asInstanceOf[Number].longValue())
    case _: DecimalType => (micros: Long, num) =>
      val decimalRes = ((new Decimal).set(micros) * num.asInstanceOf[Decimal]).toJavaBigDecimal
      decimalRes.setScale(0, RoundingMode.HALF_UP).longValueExact()
    case _: FractionalType => (micros: Long, num) =>
      DoubleMath.roundToLong(micros * num.asInstanceOf[Number].doubleValue(), RoundingMode.HALF_UP)
  }

  override def nullSafeEval(interval: Any, num: Any): Any = {
    evalFunc(interval.asInstanceOf[Long], num)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = right.dataType match {
    case _: IntegralType =>
      defineCodeGen(ctx, ev, (m, n) => s"java.lang.Math.multiplyExact($m, $n)")
    case _: DecimalType =>
      defineCodeGen(ctx, ev, (m, n) =>
        s"((new Decimal()).set($m).$$times($n)).toJavaBigDecimal()" +
        ".setScale(0, java.math.RoundingMode.HALF_UP).longValueExact()")
    case _: FractionalType =>
      val dm = classOf[DoubleMath].getName
      defineCodeGen(ctx, ev, (m, n) =>
        s"$dm.roundToLong($m * (double)$n, java.math.RoundingMode.HALF_UP)")
  }

  override def toString: String = s"($left * $right)"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): MultiplyDTInterval =
    copy(interval = newLeft, num = newRight)
}

// Divide an year-month interval by a numeric
case class DivideYMInterval(
    interval: Expression,
    num: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant with Serializable {
  override def left: Expression = interval
  override def right: Expression = num

  override def inputTypes: Seq[AbstractDataType] = Seq(YearMonthIntervalType, NumericType)
  override def dataType: DataType = YearMonthIntervalType

  @transient
  private lazy val evalFunc: (Int, Any) => Any = right.dataType match {
    case LongType => (months: Int, num) =>
      // Year-month interval has `Int` as the internal type. The result of the divide operation
      // of `Int` by `Long` must fit to `Int`. So, the casting to `Int` cannot cause overflow.
      LongMath.divide(months, num.asInstanceOf[Long], RoundingMode.HALF_UP).toInt
    case _: IntegralType => (months: Int, num) =>
      IntMath.divide(months, num.asInstanceOf[Number].intValue(), RoundingMode.HALF_UP)
    case _: DecimalType => (months: Int, num) =>
      val decimalRes = ((new Decimal).set(months) / num.asInstanceOf[Decimal]).toJavaBigDecimal
      decimalRes.setScale(0, java.math.RoundingMode.HALF_UP).intValueExact()
    case _: FractionalType => (months: Int, num) =>
      DoubleMath.roundToInt(months / num.asInstanceOf[Number].doubleValue(), RoundingMode.HALF_UP)
  }

  override def nullSafeEval(interval: Any, num: Any): Any = {
    evalFunc(interval.asInstanceOf[Int], num)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = right.dataType match {
    case LongType =>
      val math = classOf[LongMath].getName
      val javaType = CodeGenerator.javaType(dataType)
      defineCodeGen(ctx, ev, (m, n) =>
        // Similarly to non-codegen code. The result of `divide(Int, Long, ...)` must fit to `Int`.
        // Casting to `Int` is safe here.
        s"($javaType)($math.divide($m, $n, java.math.RoundingMode.HALF_UP))")
    case _: IntegralType =>
      val math = classOf[IntMath].getName
      defineCodeGen(ctx, ev, (m, n) => s"$math.divide($m, $n, java.math.RoundingMode.HALF_UP)")
    case _: DecimalType =>
      defineCodeGen(ctx, ev, (m, n) =>
        s"((new Decimal()).set($m).$$div($n)).toJavaBigDecimal()" +
        ".setScale(0, java.math.RoundingMode.HALF_UP).intValueExact()")
    case _: FractionalType =>
      val math = classOf[DoubleMath].getName
      defineCodeGen(ctx, ev, (m, n) =>
        s"$math.roundToInt($m / (double)$n, java.math.RoundingMode.HALF_UP)")
  }

  override def toString: String = s"($left / $right)"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): DivideYMInterval =
    copy(interval = newLeft, num = newRight)
}

// Divide a day-time interval by a numeric
case class DivideDTInterval(
    interval: Expression,
    num: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant with Serializable {
  override def left: Expression = interval
  override def right: Expression = num

  override def inputTypes: Seq[AbstractDataType] = Seq(DayTimeIntervalType, NumericType)
  override def dataType: DataType = DayTimeIntervalType

  @transient
  private lazy val evalFunc: (Long, Any) => Any = right.dataType match {
    case _: IntegralType => (micros: Long, num) =>
      LongMath.divide(micros, num.asInstanceOf[Number].longValue(), RoundingMode.HALF_UP)
    case _: DecimalType => (micros: Long, num) =>
      val decimalRes = ((new Decimal).set(micros) / num.asInstanceOf[Decimal]).toJavaBigDecimal
      decimalRes.setScale(0, java.math.RoundingMode.HALF_UP).longValueExact()
    case _: FractionalType => (micros: Long, num) =>
      DoubleMath.roundToLong(micros / num.asInstanceOf[Number].doubleValue(), RoundingMode.HALF_UP)
  }

  override def nullSafeEval(interval: Any, num: Any): Any = {
    evalFunc(interval.asInstanceOf[Long], num)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = right.dataType match {
    case _: IntegralType =>
      val math = classOf[LongMath].getName
      defineCodeGen(ctx, ev, (m, n) => s"$math.divide($m, $n, java.math.RoundingMode.HALF_UP)")
    case _: DecimalType =>
      defineCodeGen(ctx, ev, (m, n) =>
        s"((new Decimal()).set($m).$$div($n)).toJavaBigDecimal()" +
        ".setScale(0, java.math.RoundingMode.HALF_UP).longValueExact()")
    case _: FractionalType =>
      val math = classOf[DoubleMath].getName
      defineCodeGen(ctx, ev, (m, n) =>
        s"$math.roundToLong($m / (double)$n, java.math.RoundingMode.HALF_UP)")
  }

  override def toString: String = s"($left / $right)"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): DivideDTInterval =
    copy(interval = newLeft, num = newRight)
}
