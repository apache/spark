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

import java.text.ParseException
import java.time.{DateTimeException, LocalDate, LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeParseException
import java.util.Locale

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.{SparkDateTimeException, SparkIllegalArgumentException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, FunctionRegistry}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, LegacyDateFormats, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.SIMPLE_DATE_FORMAT
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType.DAY
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Common base class for time zone aware expressions.
 */
trait TimeZoneAwareExpression extends Expression {
  /** The expression is only resolved when the time zone has been set. */
  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess && timeZoneId.isDefined

  final override val nodePatterns: Seq[TreePattern] =
    Seq(TIME_ZONE_AWARE_EXPRESSION) ++ nodePatternsInternal()

  // Subclasses can override this function to provide more TreePatterns.
  def nodePatternsInternal(): Seq[TreePattern] = Seq()

  /** the timezone ID to be used to evaluate value. */
  def timeZoneId: Option[String]

  /** Returns a copy of this expression with the specified timeZoneId. */
  def withTimeZone(timeZoneId: String): TimeZoneAwareExpression

  @transient lazy val zoneId: ZoneId = DateTimeUtils.getZoneId(timeZoneId.get)

  def zoneIdForType(dataType: DataType): ZoneId = dataType match {
    case _: TimestampNTZType => java.time.ZoneOffset.UTC
    case _ => zoneId
  }
}

private[catalyst] object TimeZoneAwareExpression {
  def convertExpressionToUnit(e: Expression, source: String): String = e match {
    case StringLiteral(unit) => unit
    case _ => throw QueryExecutionErrors.invalidDatetimeUnitError(source, e.sql)
  }
}

trait TimestampFormatterHelper extends TimeZoneAwareExpression {

  protected def formatString: Expression

  protected def isParsing: Boolean

  // Whether the timestamp formatter is for TimestampNTZType.
  // If yes, the formatter is always `Iso8601TimestampFormatter`.
  protected def forTimestampNTZ: Boolean = false

  @transient final protected lazy val formatterOption: Option[TimestampFormatter] =
    if (formatString.foldable) {
      Option(formatString.eval()).map(fmt => getFormatter(fmt.toString))
    } else None

  final protected def getFormatter(fmt: String): TimestampFormatter = {
    TimestampFormatter(
      format = fmt,
      zoneId = zoneId,
      legacyFormat = SIMPLE_DATE_FORMAT,
      isParsing = isParsing,
      forTimestampNTZ = forTimestampNTZ)
  }
}

@ExpressionDescription(
  usage = "_FUNC_() - Returns the current session local timezone.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       Asia/Shanghai
  """,
  group = "datetime_funcs",
  since = "3.1.0")
case class CurrentTimeZone() extends LeafExpression with Unevaluable {
  override def nullable: Boolean = false
  override def dataType: DataType = SQLConf.get.defaultStringType
  override def prettyName: String = "current_timezone"
  final override val nodePatterns: Seq[TreePattern] = Seq(CURRENT_LIKE)
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
  extends LeafExpression with TimeZoneAwareExpression with FoldableUnevaluable {
  def this() = this(None)
  override def nullable: Boolean = false
  override def dataType: DataType = DateType
  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(CURRENT_LIKE)
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def prettyName: String = "current_date"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_() - Returns the current date at the start of query evaluation. All calls of curdate within the same query return the same value.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_();
       2022-09-06
  """,
  group = "datetime_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
object CurDateExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    if (expressions.isEmpty) {
      CurrentDate()
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(
        funcName, Seq(0), expressions.length)
    }
  }
}

abstract class CurrentTimestampLike() extends LeafExpression with FoldableUnevaluable {
  override def nullable: Boolean = false
  override def dataType: DataType = TimestampType
  final override val nodePatterns: Seq[TreePattern] = Seq(CURRENT_LIKE)
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
 * Returns the current timestamp without time zone at the start of query evaluation.
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_() - Returns the current timestamp without time zone at the start of query evaluation. All calls of localtimestamp within the same query return the same value.

    _FUNC_ - Returns the current local date-time at the session time zone at the start of query evaluation.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_();
       2020-04-25 15:49:11.914
  """,
  group = "datetime_funcs",
  since = "3.4.0")
case class LocalTimestamp(timeZoneId: Option[String] = None) extends LeafExpression
  with TimeZoneAwareExpression with FoldableUnevaluable {
  def this() = this(None)
  override def nullable: Boolean = false
  override def dataType: DataType = TimestampNTZType
  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(CURRENT_LIKE)
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))
  override def prettyName: String = "localtimestamp"
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

  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(CURRENT_LIKE)

  override def prettyName: String = "current_batch_timestamp"

  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  /**
   * Need to return literal value in order to support compile time expression evaluation
   * e.g., select(current_date())
   */
  override protected def evalInternal(input: InternalRow): Any = toLiteral.value

  def toLiteral: Literal = {
    val timestampUs = millisToMicros(timestampMs)
    dataType match {
      case _: TimestampType => Literal(timestampUs, TimestampType)
      case _: TimestampNTZType =>
        Literal(convertTz(timestampUs, ZoneOffset.UTC, zoneId), TimestampNTZType)
      case _: DateType => Literal(microsToDays(timestampUs, zoneId), DateType)
    }
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
  override def nullIntolerant: Boolean = true
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

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): DateAdd = copy(startDate = newLeft, days = newRight)
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
  override def nullIntolerant: Boolean = true
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

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): DateSub = copy(startDate = newLeft, days = newRight)
}

trait GetTimeField extends UnaryExpression
  with TimeZoneAwareExpression with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true
  val func: (Long, ZoneId) => Any
  val funcName: String

  @transient protected lazy val zoneIdInEval: ZoneId = zoneIdForType(child.dataType)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyTimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    func(timestamp.asInstanceOf[Long], zoneIdInEval)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneIdInEval, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.$funcName($c, $zid)")
  }
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
case class Hour(child: Expression, timeZoneId: Option[String] = None) extends GetTimeField {
  def this(child: Expression) = this(child, None)
  override def withTimeZone(timeZoneId: String): Hour = copy(timeZoneId = Option(timeZoneId))
  override val func = DateTimeUtils.getHours
  override val funcName = "getHours"
  override protected def withNewChildInternal(newChild: Expression): Hour = copy(child = newChild)
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
case class Minute(child: Expression, timeZoneId: Option[String] = None) extends GetTimeField {
  def this(child: Expression) = this(child, None)
  override def withTimeZone(timeZoneId: String): Minute = copy(timeZoneId = Option(timeZoneId))
  override val func = DateTimeUtils.getMinutes
  override val funcName = "getMinutes"
  override protected def withNewChildInternal(newChild: Expression): Minute = copy(child = newChild)
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
case class Second(child: Expression, timeZoneId: Option[String] = None) extends GetTimeField {
  def this(child: Expression) = this(child, None)
  override def withTimeZone(timeZoneId: String): Second = copy(timeZoneId = Option(timeZoneId))
  override val func = DateTimeUtils.getSeconds
  override val funcName = "getSeconds"
  override protected def withNewChildInternal(newChild: Expression): Second =
    copy(child = newChild)
}

case class SecondWithFraction(child: Expression, timeZoneId: Option[String] = None)
  extends GetTimeField {
  def this(child: Expression) = this(child, None)
  // 2 digits for seconds, and 6 digits for the fractional part with microsecond precision.
  override def dataType: DataType = DecimalType(8, 6)
  override def withTimeZone(timeZoneId: String): SecondWithFraction =
    copy(timeZoneId = Option(timeZoneId))
  override val func = DateTimeUtils.getSecondsWithFraction
  override val funcName = "getSecondsWithFraction"
  override protected def withNewChildInternal(newChild: Expression): SecondWithFraction =
    copy(child = newChild)
}

trait GetDateField extends UnaryExpression with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true
  val func: Int => Any
  val funcName: String

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    func(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.$funcName($c)")
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
case class DayOfYear(child: Expression) extends GetDateField {
  override val func = DateTimeUtils.getDayInYear
  override val funcName = "getDayInYear"
  override protected def withNewChildInternal(newChild: Expression): DayOfYear =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(days) - Create date from the number of days since 1970-01-01.",
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       1970-01-02
  """,
  group = "datetime_funcs",
  since = "3.1.0")
case class DateFromUnixDate(child: Expression) extends UnaryExpression
  with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(input: Any): Any = input.asInstanceOf[Int]

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => c)

  override def prettyName: String = "date_from_unix_date"

  override protected def withNewChildInternal(newChild: Expression): DateFromUnixDate =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the number of days since 1970-01-01.",
  examples = """
    Examples:
      > SELECT _FUNC_(DATE("1970-01-02"));
       1
  """,
  group = "datetime_funcs",
  since = "3.1.0")
case class UnixDate(child: Expression) extends UnaryExpression
  with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override def nullSafeEval(input: Any): Any = input.asInstanceOf[Int]

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => c)

  override def prettyName: String = "unix_date"

  override protected def withNewChildInternal(newChild: Expression): UnixDate =
    copy(child = newChild)
}

abstract class IntegralToTimestampBase extends UnaryExpression
  with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  protected def upScaleFactor: Long

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegralType)

  override def dataType: DataType = TimestampType

  override def nullSafeEval(input: Any): Any = {
    Math.multiplyExact(input.asInstanceOf[Number].longValue(), upScaleFactor)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (upScaleFactor == 1) {
      defineCodeGen(ctx, ev, c => c)
    } else {
      defineCodeGen(ctx, ev, c => s"java.lang.Math.multiplyExact($c, ${upScaleFactor}L)")
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(seconds) - Creates timestamp from the number of seconds (can be fractional) since UTC epoch.",
  examples = """
    Examples:
      > SELECT _FUNC_(1230219000);
       2008-12-25 07:30:00
      > SELECT _FUNC_(1230219000.123);
       2008-12-25 07:30:00.123
  """,
  group = "datetime_funcs",
  since = "3.1.0")
// scalastyle:on line.size.limit
case class SecondsToTimestamp(child: Expression) extends UnaryExpression
  with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def dataType: DataType = TimestampType

  override def nullable: Boolean = child.dataType match {
    case _: FloatType | _: DoubleType => true
    case _ => child.nullable
  }

  @transient
  private lazy val evalFunc: Any => Any = child.dataType match {
    case _: IntegralType => input =>
      Math.multiplyExact(input.asInstanceOf[Number].longValue(), MICROS_PER_SECOND)
    case _: DecimalType => input =>
      val operand = new java.math.BigDecimal(MICROS_PER_SECOND)
      input.asInstanceOf[Decimal].toJavaBigDecimal.multiply(operand).longValueExact()
    case _: FloatType => input =>
      val f = input.asInstanceOf[Float]
      if (f.isNaN || f.isInfinite) null else (f.toDouble * MICROS_PER_SECOND).toLong
    case _: DoubleType => input =>
      val d = input.asInstanceOf[Double]
      if (d.isNaN || d.isInfinite) null else (d * MICROS_PER_SECOND).toLong
  }

  override def nullSafeEval(input: Any): Any = evalFunc(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = child.dataType match {
    case _: IntegralType =>
      defineCodeGen(ctx, ev, c => s"java.lang.Math.multiplyExact($c, ${MICROS_PER_SECOND}L)")
    case _: DecimalType =>
      val operand = s"new java.math.BigDecimal($MICROS_PER_SECOND)"
      defineCodeGen(ctx, ev, c => s"$c.toJavaBigDecimal().multiply($operand).longValueExact()")
    case other =>
      val castToDouble = if (other.isInstanceOf[FloatType]) "(double)" else ""
      nullSafeCodeGen(ctx, ev, c => {
        val typeStr = CodeGenerator.boxedType(other)
        s"""
           |if ($typeStr.isNaN($c) || $typeStr.isInfinite($c)) {
           |  ${ev.isNull} = true;
           |} else {
           |  ${ev.value} = (long)($castToDouble$c * $MICROS_PER_SECOND);
           |}
           |""".stripMargin
      })
  }

  override def prettyName: String = "timestamp_seconds"

  override protected def withNewChildInternal(newChild: Expression): SecondsToTimestamp =
    copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(milliseconds) - Creates timestamp from the number of milliseconds since UTC epoch.",
  examples = """
    Examples:
      > SELECT _FUNC_(1230219000123);
       2008-12-25 07:30:00.123
  """,
  group = "datetime_funcs",
  since = "3.1.0")
// scalastyle:on line.size.limit
case class MillisToTimestamp(child: Expression)
  extends IntegralToTimestampBase {

  override def upScaleFactor: Long = MICROS_PER_MILLIS

  override def prettyName: String = "timestamp_millis"

  override protected def withNewChildInternal(newChild: Expression): MillisToTimestamp =
    copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(microseconds) - Creates timestamp from the number of microseconds since UTC epoch.",
  examples = """
    Examples:
      > SELECT _FUNC_(1230219000123123);
       2008-12-25 07:30:00.123123
  """,
  group = "datetime_funcs",
  since = "3.1.0")
// scalastyle:on line.size.limit
case class MicrosToTimestamp(child: Expression)
  extends IntegralToTimestampBase {

  override def upScaleFactor: Long = 1L

  override def prettyName: String = "timestamp_micros"

  override protected def withNewChildInternal(newChild: Expression): MicrosToTimestamp =
    copy(child = newChild)
}

abstract class TimestampToLongBase extends UnaryExpression
  with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  protected def scaleFactor: Long

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = LongType

  override def nullSafeEval(input: Any): Any = {
    Math.floorDiv(input.asInstanceOf[Number].longValue(), scaleFactor)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (scaleFactor == 1) {
      defineCodeGen(ctx, ev, c => c)
    } else {
      defineCodeGen(ctx, ev, c => s"java.lang.Math.floorDiv($c, ${scaleFactor}L)")
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns the number of seconds since 1970-01-01 00:00:00 UTC. Truncates higher levels of precision.",
  examples = """
    Examples:
      > SELECT _FUNC_(TIMESTAMP('1970-01-01 00:00:01Z'));
       1
  """,
  group = "datetime_funcs",
  since = "3.1.0")
// scalastyle:on line.size.limit
case class UnixSeconds(child: Expression) extends TimestampToLongBase {
  override def scaleFactor: Long = MICROS_PER_SECOND

  override def prettyName: String = "unix_seconds"

  override protected def withNewChildInternal(newChild: Expression): UnixSeconds =
    copy(child = newChild)
}

// Internal expression used to get the raw UTC timestamp in pandas API on Spark.
// This is to work around casting timestamp_ntz to long disallowed by ANSI.
case class CastTimestampNTZToLong(child: Expression) extends TimestampToLongBase {
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampNTZType)

  override def scaleFactor: Long = MICROS_PER_SECOND

  override def prettyName: String = "cast_timestamp_ntz_to_long"

  override protected def withNewChildInternal(newChild: Expression): CastTimestampNTZToLong =
    copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns the number of milliseconds since 1970-01-01 00:00:00 UTC. Truncates higher levels of precision.",
  examples = """
    Examples:
      > SELECT _FUNC_(TIMESTAMP('1970-01-01 00:00:01Z'));
       1000
  """,
  group = "datetime_funcs",
  since = "3.1.0")
// scalastyle:on line.size.limit
case class UnixMillis(child: Expression) extends TimestampToLongBase {
  override def scaleFactor: Long = MICROS_PER_MILLIS

  override def prettyName: String = "unix_millis"

  override protected def withNewChildInternal(newChild: Expression): UnixMillis =
    copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns the number of microseconds since 1970-01-01 00:00:00 UTC.",
  examples = """
    Examples:
      > SELECT _FUNC_(TIMESTAMP('1970-01-01 00:00:01Z'));
       1000000
  """,
  group = "datetime_funcs",
  since = "3.1.0")
// scalastyle:on line.size.limit
case class UnixMicros(child: Expression) extends TimestampToLongBase {
  override def scaleFactor: Long = 1L

  override def prettyName: String = "unix_micros"

  override protected def withNewChildInternal(newChild: Expression): UnixMicros =
    copy(child = newChild)
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
case class Year(child: Expression) extends GetDateField {
  override val func = DateTimeUtils.getYear
  override val funcName = "getYear"
  override protected def withNewChildInternal(newChild: Expression): Year =
    copy(child = newChild)
}

case class YearOfWeek(child: Expression) extends GetDateField {
  override val func = DateTimeUtils.getWeekBasedYear
  override val funcName = "getWeekBasedYear"
  override protected def withNewChildInternal(newChild: Expression): YearOfWeek =
    copy(child = newChild)
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
case class Quarter(child: Expression) extends GetDateField {
  override val func = DateTimeUtils.getQuarter
  override val funcName = "getQuarter"
  override protected def withNewChildInternal(newChild: Expression): Quarter =
    copy(child = newChild)
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
case class Month(child: Expression) extends GetDateField {
  override val func = DateTimeUtils.getMonth
  override val funcName = "getMonth"
  override protected def withNewChildInternal(newChild: Expression): Month = copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the day of month of the date/timestamp.",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-07-30');
       30
  """,
  group = "datetime_funcs",
  since = "1.5.0")
case class DayOfMonth(child: Expression) extends GetDateField {
  override val func = DateTimeUtils.getDayOfMonth
  override val funcName = "getDayOfMonth"
  override protected def withNewChildInternal(newChild: Expression): DayOfMonth =
    copy(child = newChild)
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
case class DayOfWeek(child: Expression) extends GetDateField {
  override val func = DateTimeUtils.getDayOfWeek
  override val funcName = "getDayOfWeek"
  override protected def withNewChildInternal(newChild: Expression): DayOfWeek =
    copy(child = newChild)
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
case class WeekDay(child: Expression) extends GetDateField {
  override val func = DateTimeUtils.getWeekDay
  override val funcName = "getWeekDay"
  override protected def withNewChildInternal(newChild: Expression): WeekDay =
    copy(child = newChild)
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
case class WeekOfYear(child: Expression) extends GetDateField {
  override val func = DateTimeUtils.getWeekOfYear
  override val funcName = "getWeekOfYear"
  override protected def withNewChildInternal(newChild: Expression): WeekOfYear =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the three-letter abbreviated month name from the given date.",
  examples = """
    Examples:
      > SELECT _FUNC_('2008-02-20');
       Feb
  """,
  group = "datetime_funcs",
  since = "4.0.0")
case class MonthName(child: Expression) extends GetDateField {
  override val func = DateTimeUtils.getMonthName
  override val funcName = "getMonthName"
  override def dataType: DataType = StringType
  override protected def withNewChildInternal(newChild: Expression): MonthName =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the three-letter abbreviated day name from the given date.",
  examples = """
    Examples:
      > SELECT _FUNC_(DATE('2008-02-20'));
       Wed
  """,
  group = "datetime_funcs",
  since = "4.0.0")
case class DayName(child: Expression) extends GetDateField {
  override val func = DateTimeUtils.getDayName
  override val funcName = "getDayName"

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)
  override def dataType: DataType = SQLConf.get.defaultStringType
  override protected def withNewChildInternal(newChild: Expression): DayName =
    copy(child = newChild)
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
  extends BinaryExpression with TimestampFormatterHelper with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true

  def this(left: Expression, right: Expression) = this(left, right, None)

  override def dataType: DataType = SQLConf.get.defaultStringType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TimestampType, StringTypeWithCollation(supportsTrimCollation = true))

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def nullSafeEval(timestamp: Any, format: Any): Any = {
    val formatter = formatterOption.getOrElse(getFormatter(format.toString))
    UTF8String.fromString(formatter.format(timestamp.asInstanceOf[Long]))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    formatterOption.map { tf =>
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

  override protected def formatString: Expression = right

  override protected def isParsing: Boolean = false

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): DateFormatClass =
    copy(left = newLeft, right = newRight)
}

/**
 * Converts time string with given pattern.
 * Deterministic version of [[UnixTimestamp]], must have at least one parameter.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timeExp[, fmt]) - Returns the UNIX timestamp of the given time.",
  arguments = """
    Arguments:
      * timeExp - A date/timestamp or string which is returned as a UNIX timestamp.
      * fmt - Date/time format pattern to follow. Ignored if `timeExp` is not a string.
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
    timeZoneId: Option[String] = None,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends UnixTime {

  def this(timeExp: Expression, format: Expression) =
    this(timeExp, format, None, SQLConf.get.ansiEnabled)

  override def left: Expression = timeExp
  override def right: Expression = format

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  def this(time: Expression) = {
    this(time, Literal(TimestampFormatter.defaultPattern()))
  }

  override def prettyName: String = "to_unix_timestamp"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): ToUnixTimestamp =
    copy(timeExp = newLeft, format = newRight)
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
  usage = "_FUNC_([timeExp[, fmt]]) - Returns the UNIX timestamp of current or specified time.",
  arguments = """
    Arguments:
      * timeExp - A date/timestamp or string. If not provided, this defaults to current time.
      * fmt - Date/time format pattern to follow. Ignored if `timeExp` is not a string.
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
case class UnixTimestamp(
    timeExp: Expression,
    format: Expression,
    timeZoneId: Option[String] = None,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends UnixTime {

  def this(timeExp: Expression, format: Expression) =
    this(timeExp, format, None, SQLConf.get.ansiEnabled)

  override def left: Expression = timeExp
  override def right: Expression = format

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  def this(time: Expression) = {
    this(time, Literal(TimestampFormatter.defaultPattern()))
  }

  def this() = {
    this(CurrentTimestamp())
  }

  override def prettyName: String = "unix_timestamp"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): UnixTimestamp =
    copy(timeExp = newLeft, format = newRight)
}

/**
 * Gets a timestamp from a string or a date.
 */
case class GetTimestamp(
    left: Expression,
    right: Expression,
    override val dataType: DataType,
    override val suggestedFuncOnFail: String = "try_to_timestamp",
    timeZoneId: Option[String] = None,
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends ToTimestamp {

  override val forTimestampNTZ: Boolean = dataType == TimestampNTZType

  override protected def downScaleFactor: Long = 1

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Some(timeZoneId))

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}


/**
 * Parses a column to a timestamp without time zone based on the supplied format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(timestamp_str[, fmt]) - Parses the `timestamp_str` expression with the `fmt` expression
      to a timestamp without time zone. Returns null with invalid input. By default, it follows casting rules to
      a timestamp if the `fmt` is omitted.
  """,
  arguments = """
    Arguments:
      * timestamp_str - A string to be parsed to timestamp without time zone.
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
  since = "3.4.0")
// scalastyle:on line.size.limit
object ParseToTimestampNTZExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 1 || numArgs == 2) {
      ParseToTimestamp(expressions(0), expressions.drop(1).lastOption, TimestampNTZType)
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(1, 2), numArgs)
    }
  }
}

/**
 * Parses a column to a timestamp with local time zone based on the supplied format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(timestamp_str[, fmt]) - Parses the `timestamp_str` expression with the `fmt` expression
      to a timestamp with local time zone. Returns null with invalid input. By default, it follows casting rules to
      a timestamp if the `fmt` is omitted.
  """,
  arguments = """
    Arguments:
      * timestamp_str - A string to be parsed to timestamp with local time zone.
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
  since = "3.4.0")
// scalastyle:on line.size.limit
object ParseToTimestampLTZExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 1 || numArgs == 2) {
      ParseToTimestamp(expressions(0), expressions.drop(1).lastOption, TimestampType)
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(1, 2), numArgs)
    }
  }
}

/**
 * * Parses a column to a timestamp based on the supplied format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(timestamp_str[, fmt]) - Parses the `timestamp_str` expression with the `fmt` expression
      to a timestamp. The function always returns null on an invalid input with/without ANSI SQL
      mode enabled. By default, it follows casting rules to a timestamp if the `fmt` is omitted.
      The result data type is consistent with the value of configuration `spark.sql.timestampType`.
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
      > SELECT _FUNC_('foo', 'yyyy-MM-dd');
       NULL
  """,
  group = "datetime_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
object TryToTimestampExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 1 || numArgs == 2) {
      ParseToTimestamp(
        expressions.head,
        expressions.drop(1).lastOption,
        SQLConf.get.timestampType,
        failOnError = false)
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(1, 2), numArgs)
    }
  }
}

abstract class ToTimestamp
  extends BinaryExpression with TimestampFormatterHelper with ExpectsInputTypes {

  val suggestedFuncOnFail: String = "try_to_timestamp"
  def failOnError: Boolean

  // The result of the conversion to timestamp is microseconds divided by this factor.
  // For example if the factor is 1000000, the result of the expression is in seconds.
  protected def downScaleFactor: Long

  override protected def formatString: Expression = right
  override protected def isParsing = true

  override def forTimestampNTZ: Boolean = left.dataType == TimestampNTZType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(
        StringTypeWithCollation(supportsTrimCollation = true),
        DateType,
        TimestampType,
        TimestampNTZType),
      StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = LongType
  override def nullable: Boolean = if (failOnError) children.exists(_.nullable) else true

  private def isParseError(e: Throwable): Boolean = e match {
    case _: DateTimeParseException |
         _: DateTimeException |
         _: ParseException => true
    case _ => false
  }

  override def eval(input: InternalRow): Any = {
    val t = left.eval(input)
    if (t == null) {
      null
    } else {
      left.dataType match {
        case DateType =>
          daysToMicros(t.asInstanceOf[Int], zoneId) / downScaleFactor
        case TimestampType | TimestampNTZType =>
          t.asInstanceOf[Long] / downScaleFactor
        case _: StringType =>
          val fmt = right.eval(input)
          if (fmt == null) {
            null
          } else {
            val formatter = formatterOption.getOrElse(getFormatter(fmt.toString))
            try {
              if (forTimestampNTZ) {
                formatter.parseWithoutTimeZone(t.asInstanceOf[UTF8String].toString)
              } else {
                formatter.parse(t.asInstanceOf[UTF8String].toString) / downScaleFactor
              }
            } catch {
              case e: DateTimeException if failOnError =>
                throw QueryExecutionErrors.ansiDateTimeParseError(e, suggestedFuncOnFail)
              case e: ParseException if failOnError =>
                throw QueryExecutionErrors.ansiDateTimeParseError(e, suggestedFuncOnFail)
              case e if isParseError(e) => null
            }
          }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)
    val parseErrorBranch: String = if (failOnError) {
      s"throw QueryExecutionErrors.ansiDateTimeParseError(e, \"${suggestedFuncOnFail}\");"
    } else {
      s"${ev.isNull} = true;"
    }
    val parseMethod = if (forTimestampNTZ) {
      "parseWithoutTimeZone"
    } else {
      "parse"
    }
    val downScaleCode = if (forTimestampNTZ) {
      ""
    } else {
      s"/ $downScaleFactor"
    }

    left.dataType match {
      case _: StringType => formatterOption.map { fmt =>
        val df = classOf[TimestampFormatter].getName
        val formatterName = ctx.addReferenceObj("formatter", fmt, df)
        nullSafeCodeGen(ctx, ev, (datetimeStr, _) =>
          s"""
             |try {
             |  ${ev.value} = $formatterName.$parseMethod($datetimeStr.toString()) $downScaleCode;
             |} catch (java.time.DateTimeException e) {
             |  ${parseErrorBranch}
             |} catch (java.text.ParseException e) {
             |  ${parseErrorBranch}
             |}
             |""".stripMargin)
      }.getOrElse {
        val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
        val tf = TimestampFormatter.getClass.getName.stripSuffix("$")
        val ldf = LegacyDateFormats.getClass.getName.stripSuffix("$")
        val timestampFormatter = ctx.freshName("timestampFormatter")
        nullSafeCodeGen(ctx, ev, (string, format) =>
          s"""
             |$tf $timestampFormatter = $tf$$.MODULE$$.apply(
             |  $format.toString(),
             |  $zid,
             |  $ldf$$.MODULE$$.SIMPLE_DATE_FORMAT(),
             |  true);
             |try {
             |  ${ev.value} = $timestampFormatter.$parseMethod($string.toString()) $downScaleCode;
             |} catch (java.time.DateTimeException e) {
             |    ${parseErrorBranch}
             |} catch (java.text.ParseException e) {
             |    ${parseErrorBranch}
             |}
             |""".stripMargin)
      }
      case TimestampType | TimestampNTZType =>
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
            ${ev.value} = $dtu.daysToMicros(${eval1.value}, $zid) / $downScaleFactor;
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
 * Note that Hive Language Manual says it returns 0 if fail, but in fact it returns null.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(unix_time[, fmt]) - Returns `unix_time` in the specified `fmt`.",
  arguments = """
    Arguments:
      * unix_time - UNIX Timestamp to be converted to the provided format.
      * fmt - Date/time format pattern to follow. See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a>
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
  extends BinaryExpression with TimestampFormatterHelper with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true

  def this(sec: Expression, format: Expression) = this(sec, format, None)

  override def left: Expression = sec
  override def right: Expression = format

  override def prettyName: String = "from_unixtime"

  def this(unix: Expression) = {
    this(unix, Literal(TimestampFormatter.defaultPattern()))
  }

  override def dataType: DataType = SQLConf.get.defaultStringType
  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(LongType, StringTypeWithCollation(supportsTrimCollation = true))

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def nullSafeEval(seconds: Any, format: Any): Any = {
    val fmt = formatterOption.getOrElse(getFormatter(format.toString))
    UTF8String.fromString(fmt.format(seconds.asInstanceOf[Long] * MICROS_PER_SECOND))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    formatterOption.map { f =>
      val formatterName = ctx.addReferenceObj("formatter", f)
      defineCodeGen(ctx, ev, (seconds, _) =>
        s"UTF8String.fromString($formatterName.format($seconds * 1000000L))")
    }.getOrElse {
      val tf = TimestampFormatter.getClass.getName.stripSuffix("$")
      val ldf = LegacyDateFormats.getClass.getName.stripSuffix("$")
      val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
      defineCodeGen(ctx, ev, (seconds, format) =>
        s"""
           |UTF8String.fromString(
           |  $tf$$.MODULE$$.apply($format.toString(),
           |  $zid,
           |  $ldf$$.MODULE$$.SIMPLE_DATE_FORMAT(),
           |  false).format($seconds * 1000000L))
           |""".stripMargin)
    }
  }

  override protected def formatString: Expression = format

  override protected def isParsing: Boolean = false

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): FromUnixTime =
    copy(sec = newLeft, format = newRight)
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
case class LastDay(startDate: Expression)
  extends UnaryExpression with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true

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

  override protected def withNewChildInternal(newChild: Expression): LastDay =
    copy(startDate = newChild)
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
  usage =
    """_FUNC_(start_date, day_of_week) - Returns the first date which is later than `start_date` and named as indicated.
      The function returns NULL if at least one of the input parameters is NULL.
      When both of the input parameters are not NULL and day_of_week is an invalid input,
      the function throws SparkIllegalArgumentException if `spark.sql.ansi.enabled` is set to true, otherwise NULL.
      """,
  examples = """
    Examples:
      > SELECT _FUNC_('2015-01-14', 'TU');
       2015-01-20
  """,
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class NextDay(
    startDate: Expression,
    dayOfWeek: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends BinaryExpression with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true

  override def left: Expression = startDate
  override def right: Expression = dayOfWeek

  def this(left: Expression, right: Expression) = this(left, right, SQLConf.get.ansiEnabled)

  override def inputTypes: Seq[AbstractDataType] =
    Seq(DateType, StringTypeWithCollation(supportsTrimCollation = true))

  override def dataType: DataType = DateType
  override def nullable: Boolean = true

  override def nullSafeEval(start: Any, dayOfW: Any): Any = {
    try {
      val dow = DateTimeUtils.getDayOfWeekFromString(dayOfW.asInstanceOf[UTF8String])
      val sd = start.asInstanceOf[Int]
      DateTimeUtils.getNextDateForDayOfWeek(sd, dow)
    } catch {
      case e: SparkIllegalArgumentException =>
        if (failOnError) {
          throw e
        } else {
          null
        }
    }
  }

  private def dateTimeUtilClass: String = DateTimeUtils.getClass.getName.stripSuffix("$")

  private def nextDayGenCode(
      ev: ExprCode,
      dayOfWeekTerm: String,
      sd: String,
      dowS: String): String = {
    val failOnErrorBranch = if (failOnError) {
      "throw e;"
    } else {
      s"${ev.isNull} = true;"
    }
    s"""
     |try {
     |  int $dayOfWeekTerm = $dateTimeUtilClass.getDayOfWeekFromString($dowS);
     |  ${ev.value} = $dateTimeUtilClass.getNextDateForDayOfWeek($sd, $dayOfWeekTerm);
     |} catch (org.apache.spark.SparkIllegalArgumentException e) {
     |  $failOnErrorBranch
     |}
     |""".stripMargin
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (sd, dowS) => {
      val dayOfWeekTerm = ctx.freshName("dayOfWeek")
      if (dayOfWeek.foldable) {
        val input = dayOfWeek.eval().asInstanceOf[UTF8String]
        if (input eq null) {
          s"""${ev.isNull} = true;"""
        } else {
          try {
            val dayOfWeekValue = DateTimeUtils.getDayOfWeekFromString(input)
            s"${ev.value} = $dateTimeUtilClass.getNextDateForDayOfWeek($sd, $dayOfWeekValue);"
          } catch {
            case _: SparkIllegalArgumentException => nextDayGenCode(ev, dayOfWeekTerm, sd, dowS)
          }
        }
      } else {
        nextDayGenCode(ev, dayOfWeekTerm, sd, dowS)
      }
    })
  }

  override def prettyName: String = "next_day"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): NextDay =
    copy(startDate = newLeft, dayOfWeek = newRight)
}

/**
 * Adds an interval to timestamp.
 */
case class TimeAdd(start: Expression, interval: Expression, timeZoneId: Option[String] = None)
  extends BinaryExpression with TimeZoneAwareExpression with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  def this(start: Expression, interval: Expression) = this(start, interval, None)

  override def left: Expression = start
  override def right: Expression = interval

  override def toString: String = s"$left + $right"
  override def sql: String = s"${left.sql} + ${right.sql}"
  override def inputTypes: Seq[AbstractDataType] =
    Seq(AnyTimestampType, TypeCollection(CalendarIntervalType, DayTimeIntervalType))

  override def dataType: DataType = start.dataType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  @transient private lazy val zoneIdInEval: ZoneId = zoneIdForType(left.dataType)

  override def nullSafeEval(start: Any, interval: Any): Any = right.dataType match {
    case _: DayTimeIntervalType =>
      timestampAddDayTime(start.asInstanceOf[Long], interval.asInstanceOf[Long], zoneIdInEval)
    case CalendarIntervalType =>
      val i = interval.asInstanceOf[CalendarInterval]
      timestampAddInterval(start.asInstanceOf[Long], i.months, i.days, i.microseconds, zoneIdInEval)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneIdInEval, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    interval.dataType match {
      case _: DayTimeIntervalType =>
        defineCodeGen(ctx, ev, (sd, dt) => s"""$dtu.timestampAddDayTime($sd, $dt, $zid)""")
      case CalendarIntervalType =>
        defineCodeGen(ctx, ev, (sd, i) => {
          s"""$dtu.timestampAddInterval($sd, $i.months, $i.days, $i.microseconds, $zid)"""
        })
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): TimeAdd =
    copy(start = newLeft, interval = newRight)
}

/**
 * Subtract an interval from timestamp or date, which is only used to give a pretty sql string
 * for `datetime - interval` operations
 */
case class DatetimeSub(
    start: Expression,
    interval: Expression,
    replacement: Expression) extends RuntimeReplaceable with InheritAnalysisRules {

  override def parameters: Seq[Expression] = Seq(start, interval)

  override def makeSQLString(childrenSQL: Seq[String]): String = {
    childrenSQL.mkString(" - ")
  }

  override def toString: String = s"$start - $interval"

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(replacement = newChild)
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
  override def nullIntolerant: Boolean = true

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
      val startTs = DateTimeUtils.daysToMicros(start.asInstanceOf[Int], zoneId)
      val resultTs = DateTimeUtils.timestampAddInterval(
        startTs, itvl.months, itvl.days, itvl.microseconds, zoneId)
      DateTimeUtils.microsToDays(resultTs, zoneId)
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
         |  long $startTs = $dtu.daysToMicros($sd, $zid);
         |  long $resultTs =
         |    $dtu.timestampAddInterval($startTs, $i.months, $i.days, $i.microseconds, $zid);
         |  ${ev.value} = $dtu.microsToDays($resultTs, $zid);
         |}
         |""".stripMargin
    })
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): DateAddInterval =
    copy(start = newLeft, interval = newRight)
}

sealed trait UTCTimestamp extends BinaryExpression with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true
  val func: (Long, String) => Long
  val funcName: String

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TimestampType, StringTypeWithCollation(supportsTrimCollation = true))
  override def dataType: DataType = TimestampType

  override def nullSafeEval(time: Any, timezone: Any): Any = {
    func(time.asInstanceOf[Long], timezone.asInstanceOf[UTF8String].toString)
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
        val (fromTz, toTz) = this match {
          case _: FromUTCTimestamp => (utcTerm, tzTerm)
          case _: ToUTCTimestamp => (tzTerm, utcTerm)
        }
        val eval = left.genCode(ctx)
        ev.copy(code = code"""
           |${eval.code}
           |boolean ${ev.isNull} = ${eval.isNull};
           |long ${ev.value} = 0;
           |if (!${ev.isNull}) {
           |  ${ev.value} = $dtu.convertTz(${eval.value}, $fromTz, $toTz);
           |}
         """.stripMargin)
      }
    } else {
      defineCodeGen(ctx, ev, (timestamp, format) => {
        s"""$dtu.$funcName($timestamp, $format.toString())"""
      })
    }
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
  group = "datetime_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class FromUTCTimestamp(left: Expression, right: Expression) extends UTCTimestamp {
  override val func = DateTimeUtils.fromUTCTime
  override val funcName: String = "fromUTCTime"
  override val prettyName: String = "from_utc_timestamp"
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): FromUTCTimestamp =
    copy(left = newLeft, right = newRight)
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
case class ToUTCTimestamp(left: Expression, right: Expression) extends UTCTimestamp {
  override val func = DateTimeUtils.toUTCTime
  override val funcName: String = "toUTCTime"
  override val prettyName: String = "to_utc_timestamp"
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): ToUTCTimestamp =
    copy(left = newLeft, right = newRight)
}

abstract class AddMonthsBase extends BinaryExpression with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true
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
case class AddMonths(startDate: Expression, numMonths: Expression) extends AddMonthsBase {
  override def left: Expression = startDate
  override def right: Expression = numMonths

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def prettyName: String = "add_months"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): AddMonths =
    copy(startDate = newLeft, numMonths = newRight)
}

// Adds the year-month interval to the date
case class DateAddYMInterval(date: Expression, interval: Expression) extends AddMonthsBase {
  override def left: Expression = date
  override def right: Expression = interval

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, YearMonthIntervalType)

  override def toString: String = s"$left + $right"
  override def sql: String = s"${left.sql} + ${right.sql}"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): DateAddYMInterval =
    copy(date = newLeft, interval = newRight)
}

// Adds the year-month interval to the timestamp
case class TimestampAddYMInterval(
    timestamp: Expression,
    interval: Expression,
    timeZoneId: Option[String] = None)
  extends BinaryExpression with TimeZoneAwareExpression with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  def this(timestamp: Expression, interval: Expression) = this(timestamp, interval, None)

  override def left: Expression = timestamp
  override def right: Expression = interval

  override def toString: String = s"$left + $right"
  override def sql: String = s"${left.sql} + ${right.sql}"
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyTimestampType, YearMonthIntervalType)

  override def dataType: DataType = timestamp.dataType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  @transient private lazy val zoneIdInEval: ZoneId = zoneIdForType(left.dataType)

  override def nullSafeEval(micros: Any, months: Any): Any = {
    timestampAddMonths(micros.asInstanceOf[Long], months.asInstanceOf[Int], zoneIdInEval)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneIdInEval, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (micros, months) => {
      s"""$dtu.timestampAddMonths($micros, $months, $zid)"""
    })
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): TimestampAddYMInterval =
    copy(timestamp = newLeft, interval = newRight)
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
  override def nullIntolerant: Boolean = true

  def this(date1: Expression, date2: Expression) = this(date1, date2, Literal.TrueLiteral, None)

  def this(date1: Expression, date2: Expression, roundOff: Expression) =
    this(date1, date2, roundOff, None)

  override def first: Expression = date1
  override def second: Expression = date2
  override def third: Expression = roundOff

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

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): MonthsBetween =
    copy(date1 = newFirst, date2 = newSecond, roundOff = newThird)
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
case class ParseToDate(
    left: Expression,
    format: Option[Expression],
    timeZoneId: Option[String] = None,
    ansiEnabled: Boolean = SQLConf.get.ansiEnabled)
  extends RuntimeReplaceable with ImplicitCastInputTypes with TimeZoneAwareExpression {

  override lazy val replacement: Expression = format.map { f =>
    Cast(GetTimestamp(left, f, TimestampType, "try_to_date", timeZoneId, ansiEnabled), DateType,
      timeZoneId, EvalMode.fromBoolean(ansiEnabled))
  }.getOrElse(Cast(left, DateType, timeZoneId,
    EvalMode.fromBoolean(ansiEnabled))) // backwards compatibility

  def this(left: Expression, format: Expression) = {
    this(left, Option(format))
  }

  def this(left: Expression) = {
    this(left, None)
  }

  override def prettyName: String = "to_date"

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Some(timeZoneId))

  override def nodePatternsInternal(): Seq[TreePattern] = Seq(RUNTIME_REPLACEABLE)

  override def children: Seq[Expression] = left +: format.toSeq

  override def inputTypes: Seq[AbstractDataType] = {
    // Note: ideally this function should only take string input, but we allow more types here to
    // be backward compatible.
    TypeCollection(
      StringTypeWithCollation(supportsTrimCollation = true),
        DateType,
        TimestampType,
        TimestampNTZType) +:
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

/**
 * Parses a column to a timestamp based on the supplied format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(timestamp_str[, fmt]) - Parses the `timestamp_str` expression with the `fmt` expression
      to a timestamp. Returns null with invalid input. By default, it follows casting rules to
      a timestamp if the `fmt` is omitted. The result data type is consistent with the value of
      configuration `spark.sql.timestampType`.
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
case class ParseToTimestamp(
    left: Expression,
    format: Option[Expression],
    override val dataType: DataType,
    timeZoneId: Option[String] = None,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends RuntimeReplaceable with ImplicitCastInputTypes with TimeZoneAwareExpression {

  override lazy val replacement: Expression = format.map { f =>
    GetTimestamp(left, f, dataType, "try_to_timestamp", timeZoneId, failOnError = failOnError)
  }.getOrElse(Cast(left, dataType, timeZoneId, ansiEnabled = failOnError))

  def this(left: Expression, format: Expression) = {
    this(left, Option(format), SQLConf.get.timestampType)
  }

  def this(left: Expression) =
    this(left, None, SQLConf.get.timestampType)

  override def nodeName: String = "to_timestamp"

  override def nodePatternsInternal(): Seq[TreePattern] = Seq(RUNTIME_REPLACEABLE)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Some(timeZoneId))

  override def children: Seq[Expression] = left +: format.toSeq

  override def inputTypes: Seq[AbstractDataType] = {
    // Note: ideally this function should only take string input, but we allow more types here to
    // be backward compatible.
    val types = Seq(
      StringTypeWithCollation(
        supportsTrimCollation = true),
        DateType,
        TimestampType,
        TimestampNTZType)
    TypeCollection(
      (if (dataType.isInstanceOf[TimestampType]) types :+ NumericType else types): _*
    ) +: format.map(_ => StringTypeWithCollation(supportsTrimCollation = true)).toSeq
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
      DateTimeUtils.parseTruncLevel(format.eval(input).asInstanceOf[UTF8String])
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

  override def inputTypes: Seq[AbstractDataType] =
    Seq(DateType, StringTypeWithCollation(supportsTrimCollation = true))
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

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): TruncDate =
    copy(date = newLeft, format = newRight)
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

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCollation(supportsTrimCollation = true), TimestampType)
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

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): TruncTimestamp =
    copy(format = newLeft, timestamp = newRight)
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
  override def nullIntolerant: Boolean = true

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

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): DateDiff =
    copy(endDate = newLeft, startDate = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(year, month, day) - Create date from year, month and day fields. If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.",
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
      > SELECT _FUNC_(2019, 7, NULL);
       NULL
  """,
  group = "datetime_funcs",
  since = "3.0.0")
// scalastyle:on line.size.limit
case class MakeDate(
    year: Expression,
    month: Expression,
    day: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends TernaryExpression with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true

  def this(year: Expression, month: Expression, day: Expression) =
    this(year, month, day, SQLConf.get.ansiEnabled)

  override def first: Expression = year
  override def second: Expression = month
  override def third: Expression = day
  override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType, IntegerType, IntegerType)
  override def dataType: DataType = DateType
  override def nullable: Boolean = if (failOnError) children.exists(_.nullable) else true

  override def nullSafeEval(year: Any, month: Any, day: Any): Any = {
    try {
      val ld = LocalDate.of(year.asInstanceOf[Int], month.asInstanceOf[Int], day.asInstanceOf[Int])
      localDateToDays(ld)
    } catch {
      case e: java.time.DateTimeException =>
        if (failOnError) throw QueryExecutionErrors.ansiDateTimeArgumentOutOfRange(e) else null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    val failOnErrorBranch = if (failOnError) {
      "throw QueryExecutionErrors.ansiDateTimeArgumentOutOfRange(e);"
    } else {
      s"${ev.isNull} = true;"
    }
    nullSafeCodeGen(ctx, ev, (year, month, day) => {
      s"""
      try {
        ${ev.value} = $dtu.localDateToDays(java.time.LocalDate.of($year, $month, $day));
      } catch (java.time.DateTimeException e) {
        $failOnErrorBranch
      }"""
    })
  }

  override def prettyName: String = "make_date"

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): MakeDate =
    copy(year = newFirst, month = newSecond, day = newThird)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(year, month, day, hour, min, sec) - Create local date-time from year, month, day, hour, min, sec fields. If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.",
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
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(2014, 12, 28, 6, 30, 45.887);
       2014-12-28 06:30:45.887
      > SELECT _FUNC_(2019, 6, 30, 23, 59, 60);
       2019-07-01 00:00:00
      > SELECT _FUNC_(null, 7, 22, 15, 30, 0);
       NULL
  """,
  group = "datetime_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
object MakeTimestampNTZExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 6) {
      MakeTimestamp(
        expressions(0),
        expressions(1),
        expressions(2),
        expressions(3),
        expressions(4),
        expressions(5),
        dataType = TimestampNTZType)
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(6), numArgs)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(year, month, day, hour, min, sec) - Try to create local date-time from year, month, day, hour, min, sec fields. The function returns NULL on invalid inputs.",
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
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(2014, 12, 28, 6, 30, 45.887);
       2014-12-28 06:30:45.887
      > SELECT _FUNC_(2019, 6, 30, 23, 59, 60);
       2019-07-01 00:00:00
      > SELECT _FUNC_(null, 7, 22, 15, 30, 0);
       NULL
      > SELECT _FUNC_(2024, 13, 22, 15, 30, 0);
       NULL
  """,
  group = "datetime_funcs",
  since = "4.0.0")
// scalastyle:on line.size.limit
object TryMakeTimestampNTZExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 6) {
      MakeTimestamp(
        expressions(0),
        expressions(1),
        expressions(2),
        expressions(3),
        expressions(4),
        expressions(5),
        dataType = TimestampNTZType,
        failOnError = false)
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(6), numArgs)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(year, month, day, hour, min, sec[, timezone]) - Create the current timestamp with local time zone from year, month, day, hour, min, sec and timezone fields. If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.",
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
      > SELECT _FUNC_(null, 7, 22, 15, 30, 0);
       NULL
  """,
  group = "datetime_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
object MakeTimestampLTZExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 6 || numArgs == 7) {
      MakeTimestamp(
        expressions(0),
        expressions(1),
        expressions(2),
        expressions(3),
        expressions(4),
        expressions(5),
        expressions.drop(6).lastOption,
        dataType = TimestampType)
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(6), numArgs)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(year, month, day, hour, min, sec[, timezone]) - Try to create the current timestamp with local time zone from year, month, day, hour, min, sec and timezone fields. The function returns NULL on invalid inputs.",
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
      > SELECT _FUNC_(null, 7, 22, 15, 30, 0);
       NULL
      > SELECT _FUNC_(2024, 13, 22, 15, 30, 0);
       NULL
  """,
  group = "datetime_funcs",
  since = "4.0.0")
// scalastyle:on line.size.limit
object TryMakeTimestampLTZExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 6 || numArgs == 7) {
      MakeTimestamp(
        expressions(0),
        expressions(1),
        expressions(2),
        expressions(3),
        expressions(4),
        expressions(5),
        expressions.drop(6).lastOption,
        dataType = TimestampType,
        failOnError = false)
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(6), numArgs)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(year, month, day, hour, min, sec[, timezone]) - Create timestamp from year, month, day, hour, min, sec and timezone fields. The result data type is consistent with the value of configuration `spark.sql.timestampType`. If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.",
  arguments = """
    Arguments:
      * year - the year to represent, from 1 to 9999
      * month - the month-of-year to represent, from 1 (January) to 12 (December)
      * day - the day-of-month to represent, from 1 to 31
      * hour - the hour-of-day to represent, from 0 to 23
      * min - the minute-of-hour to represent, from 0 to 59
      * sec - the second-of-minute and its micro-fraction to represent, from 0 to 60.
              The value can be either an integer like 13 , or a fraction like 13.123.
              If the sec argument equals to 60, the seconds field is set
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
      > SELECT _FUNC_(2019, 6, 30, 23, 59, 1);
       2019-06-30 23:59:01
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
    timeZoneId: Option[String] = None,
    failOnError: Boolean = SQLConf.get.ansiEnabled,
    override val dataType: DataType = SQLConf.get.timestampType)
  extends SeptenaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true

  def this(
      year: Expression,
      month: Expression,
      day: Expression,
      hour: Expression,
      min: Expression,
      sec: Expression) = {
    this(year, month, day, hour, min, sec, None, None, SQLConf.get.ansiEnabled,
      SQLConf.get.timestampType)
  }

  def this(
      year: Expression,
      month: Expression,
      day: Expression,
      hour: Expression,
      min: Expression,
      sec: Expression,
      timezone: Expression) = {
    this(year, month, day, hour, min, sec, Some(timezone), None, SQLConf.get.ansiEnabled,
      SQLConf.get.timestampType)
  }

  override def children: Seq[Expression] = Seq(year, month, day, hour, min, sec) ++ timezone
  // Accept `sec` as DecimalType to avoid loosing precision of microseconds while converting
  // them to the fractional part of `sec`. For accepts IntegerType as `sec` and integer can be
  // casted into decimal safely, we use DecimalType(16, 6) which is wider than DecimalType(10, 0).
  override def inputTypes: Seq[AbstractDataType] =
    Seq(IntegerType, IntegerType, IntegerType, IntegerType, IntegerType, DecimalType(16, 6)) ++
    timezone.map(_ => StringTypeWithCollation(supportsTrimCollation = true))
  override def nullable: Boolean = if (failOnError) children.exists(_.nullable) else true

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  private def toMicros(
      year: Int,
      month: Int,
      day: Int,
      hour: Int,
      min: Int,
      secAndMicros: Decimal,
      zoneId: ZoneId): Any = {
    try {
      assert(secAndMicros.scale == 6,
        s"Seconds fraction must have 6 digits for microseconds but got ${secAndMicros.scale}")
      val unscaledSecFrac = secAndMicros.toUnscaledLong
      val totalMicros = unscaledSecFrac.toInt // 8 digits cannot overflow Int
      val seconds = Math.floorDiv(totalMicros, MICROS_PER_SECOND.toInt)
      val nanos = Math.floorMod(totalMicros, MICROS_PER_SECOND.toInt) * NANOS_PER_MICROS.toInt
      val ldt = if (seconds == 60) {
        if (nanos == 0) {
          // This case of sec = 60 and nanos = 0 is supported for compatibility with PostgreSQL
          LocalDateTime.of(year, month, day, hour, min, 0, 0).plusMinutes(1)
        } else {
          throw QueryExecutionErrors.invalidFractionOfSecondError(secAndMicros.toDouble)
        }
      } else {
        LocalDateTime.of(year, month, day, hour, min, seconds, nanos)
      }
      if (dataType == TimestampType) {
        instantToMicros(ldt.atZone(zoneId).toInstant)
      } else {
        localDateTimeToMicros(ldt)
      }
    } catch {
      case e: SparkDateTimeException if failOnError => throw e
      case e: DateTimeException if failOnError =>
        throw QueryExecutionErrors.ansiDateTimeArgumentOutOfRange(e)
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
    val failOnErrorBranch = if (failOnError) {
      "throw QueryExecutionErrors.ansiDateTimeArgumentOutOfRange(e);"
    } else {
      s"${ev.isNull} = true;"
    }
    val failOnSparkErrorBranch = if (failOnError) "throw e;" else s"${ev.isNull} = true;"
    nullSafeCodeGen(ctx, ev, (year, month, day, hour, min, secAndNanos, timezone) => {
      val zoneId = timezone.map(tz => s"$dtu.getZoneId(${tz}.toString())").getOrElse(zid)
      val toMicrosCode = if (dataType == TimestampType) {
        s"""
           |java.time.Instant instant = ldt.atZone($zoneId).toInstant();
           |${ev.value} = $dtu.instantToMicros(instant);
           |""".stripMargin
      } else {
        s"${ev.value} = $dtu.localDateTimeToMicros(ldt);"
      }
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
            throw QueryExecutionErrors.invalidFractionOfSecondError($secAndNanos.toDouble());
          }
        } else {
          ldt = java.time.LocalDateTime.of($year, $month, $day, $hour, $min, seconds, nanos);
        }
        $toMicrosCode
      } catch (org.apache.spark.SparkDateTimeException e) {
        $failOnSparkErrorBranch
      } catch (java.time.DateTimeException e) {
        $failOnErrorBranch
      }"""
    })
  }

  override def nodeName: String = "make_timestamp"

//  override def children: Seq[Expression] = Seq(year, month, day, hour, min, sec) ++ timezone
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): MakeTimestamp = {
    val timezoneOpt = if (timezone.isDefined) Some(newChildren(6)) else None
    copy(
      year = newChildren(0),
      month = newChildren(1),
      day = newChildren(2),
      hour = newChildren(3),
      min = newChildren(4),
      sec = newChildren(5),
      timezone = timezoneOpt)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(year, month, day, hour, min, sec[, timezone]) - Try to create a timestamp from year, month, day, hour, min, sec and timezone fields. The result data type is consistent with the value of configuration `spark.sql.timestampType`. The function returns NULL on invalid inputs.",
  arguments = """
    Arguments:
      * year - the year to represent, from 1 to 9999
      * month - the month-of-year to represent, from 1 (January) to 12 (December)
      * day - the day-of-month to represent, from 1 to 31
      * hour - the hour-of-day to represent, from 0 to 23
      * min - the minute-of-hour to represent, from 0 to 59
      * sec - the second-of-minute and its micro-fraction to represent, from 0 to 60.
              The value can be either an integer like 13 , or a fraction like 13.123.
              If the sec argument equals to 60, the seconds field is set
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
      > SELECT _FUNC_(2019, 6, 30, 23, 59, 1);
       2019-06-30 23:59:01
      > SELECT _FUNC_(null, 7, 22, 15, 30, 0);
       NULL
      > SELECT _FUNC_(2024, 13, 22, 15, 30, 0);
       NULL
  """,
  group = "datetime_funcs",
  since = "4.0.0")
// scalastyle:on line.size.limit
case class TryMakeTimestamp(
    year: Expression,
    month: Expression,
    day: Expression,
    hour: Expression,
    min: Expression,
    sec: Expression,
    timezone: Option[Expression],
    timeZoneId: Option[String],
    replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {

  private def this(
      year: Expression,
      month: Expression,
      day: Expression,
      hour: Expression,
      min: Expression,
      sec: Expression,
      timezone: Option[Expression]) = this(year, month, day, hour, min, sec, timezone, None,
      MakeTimestamp(year, month, day, hour, min, sec, timezone, None, failOnError = false))

  def this(
      year: Expression,
      month: Expression,
      day: Expression,
      hour: Expression,
      min: Expression,
      sec: Expression,
      timezone: Expression) = this(year, month, day, hour, min, sec, Some(timezone))

  def this(
      year: Expression,
      month: Expression,
      day: Expression,
      hour: Expression,
      min: Expression,
      sec: Expression) = this(year, month, day, hour, min, sec, None)

  override def prettyName: String = "try_make_timestamp"

  override def parameters: Seq[Expression] = Seq(
    year, month, day, hour, min, sec)

  override protected def withNewChildInternal(newChild: Expression): TryMakeTimestamp = {
    copy(replacement = newChild)
  }
}

object DatePart {

  def parseExtractField(
      extractField: String,
      source: Expression): Expression = extractField.toUpperCase(Locale.ROOT) match {
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
    case _ =>
      throw QueryCompilationErrors.literalTypeUnsupportedForSourceTypeError(extractField, source)
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
      > SELECT _FUNC_('days', interval 5 days 3 hours 7 minutes);
       5
      > SELECT _FUNC_('seconds', interval 5 hours 30 seconds 1 milliseconds 1 microseconds);
       30.001001
      > SELECT _FUNC_('MONTH', INTERVAL '2021-11' YEAR TO MONTH);
       11
      > SELECT _FUNC_('MINUTE', INTERVAL '123 23:55:59.002001' DAY TO SECOND);
       55
  """,
  note = """
    The _FUNC_ function is equivalent to the SQL-standard function `EXTRACT(field FROM source)`
  """,
  group = "datetime_funcs",
  since = "3.0.0")
// scalastyle:on line.size.limit
object DatePartExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 2) {
      val field = expressions(0)
      val source = expressions(1)
      Extract(field, source, Extract.createExpr(funcName, field, source))
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(2), numArgs)
    }
  }
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
      > SELECT _FUNC_(days FROM interval 5 days 3 hours 7 minutes);
       5
      > SELECT _FUNC_(seconds FROM interval 5 hours 30 seconds 1 milliseconds 1 microseconds);
       30.001001
      > SELECT _FUNC_(MONTH FROM INTERVAL '2021-11' YEAR TO MONTH);
       11
      > SELECT _FUNC_(MINUTE FROM INTERVAL '123 23:55:59.002001' DAY TO SECOND);
       55
  """,
  note = """
    The _FUNC_ function is equivalent to `date_part(field, source)`.
  """,
  group = "datetime_funcs",
  since = "3.0.0")
// scalastyle:on line.size.limit
case class Extract(field: Expression, source: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {

  def this(field: Expression, source: Expression) =
    this(field, source, Extract.createExpr("extract", field, source))

  override def parameters: Seq[Expression] = Seq(field, source)

  override def makeSQLString(childrenSQL: Seq[String]): String = {
    getTagValue(FunctionRegistry.FUNC_ALIAS) match {
      case Some("date_part") => s"$prettyName(${childrenSQL.mkString(", ")})"
      case _ => s"$prettyName(${childrenSQL.mkString(" FROM ")})"
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(replacement = newChild)
  }
}

object Extract {
  def createExpr(funcName: String, field: Expression, source: Expression): Expression = {
    // both string and null literals are allowed.
    if ((field.dataType.isInstanceOf[StringType] || field.dataType == NullType) && field.foldable) {
      val fieldStr = field.eval().asInstanceOf[UTF8String]
      if (fieldStr == null) {
        Literal(null, DoubleType)
      } else {
        source.dataType match {
          case _: AnsiIntervalType | CalendarIntervalType =>
            ExtractIntervalPart.parseExtractField(fieldStr.toString, source)
          case _ =>
            DatePart.parseExtractField(fieldStr.toString, source)
        }
      }
    } else {
      throw QueryCompilationErrors.nonFoldableArgumentError(funcName, "field", StringType)
    }
  }
}

/**
 * Returns the interval from `right` to `left` timestamps.
 *   - When the SQL config `spark.sql.legacy.interval.enabled` is `true`,
 *     it returns `CalendarIntervalType` in which the months` and `day` field is set to 0 and
 *     the `microseconds` field is initialized to the microsecond difference between
 *     the given timestamps.
 *   - Otherwise the expression returns `DayTimeIntervalType` with the difference in microseconds
 *     between given timestamps.
 */
case class SubtractTimestamps(
    left: Expression,
    right: Expression,
    legacyInterval: Boolean,
    timeZoneId: Option[String] = None)
  extends BinaryExpression
  with TimeZoneAwareExpression
  with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  def this(endTimestamp: Expression, startTimestamp: Expression) =
    this(endTimestamp, startTimestamp, SQLConf.get.legacyIntervalEnabled)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyTimestampType, AnyTimestampType)
  override def dataType: DataType =
    if (legacyInterval) CalendarIntervalType else DayTimeIntervalType()

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  @transient private lazy val zoneIdInEval: ZoneId = zoneIdForType(left.dataType)

  @transient
  private lazy val evalFunc: (Long, Long) => Any = if (legacyInterval) {
    (leftMicros, rightMicros) =>
      new CalendarInterval(0, 0, leftMicros - rightMicros)
  } else {
    (leftMicros, rightMicros) =>
      subtractTimestamps(leftMicros, rightMicros, zoneIdInEval)
  }

  override def nullSafeEval(leftMicros: Any, rightMicros: Any): Any = {
    evalFunc(leftMicros.asInstanceOf[Long], rightMicros.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = if (legacyInterval) {
    defineCodeGen(ctx, ev, (end, start) =>
      s"new org.apache.spark.unsafe.types.CalendarInterval(0, 0, $end - $start)")
  } else {
    val zid = ctx.addReferenceObj("zoneId", zoneIdInEval, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (l, r) => s"""$dtu.subtractTimestamps($l, $r, $zid)""")
  }

  override def toString: String = s"($left - $right)"
  override def sql: String = s"(${left.sql} - ${right.sql})"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): SubtractTimestamps =
    copy(left = newLeft, right = newRight)
}

object SubtractTimestamps {
  def apply(left: Expression, right: Expression): SubtractTimestamps = {
    new SubtractTimestamps(left, right)
  }
}

/**
 * Returns the interval from the `left` date (inclusive) to the `right` date (exclusive).
 *   - When the SQL config `spark.sql.legacy.interval.enabled` is `true`,
 *     it returns `CalendarIntervalType` in which the `microseconds` field is set to 0 and
 *     the `months` and `days` fields are initialized to the difference between the given dates.
 *   - Otherwise the expression returns `DayTimeIntervalType` with the difference in days
 *     between the given dates.
 */
case class SubtractDates(
    left: Expression,
    right: Expression,
    legacyInterval: Boolean)
  extends BinaryExpression with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true

  def this(left: Expression, right: Expression) =
    this(left, right, SQLConf.get.legacyIntervalEnabled)

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)
  override def dataType: DataType = {
    if (legacyInterval) CalendarIntervalType else DayTimeIntervalType(DAY)
  }

  @transient
  private lazy val evalFunc: (Int, Int) => Any = if (legacyInterval) {
    (leftDays: Int, rightDays: Int) => subtractDates(leftDays, rightDays)
  } else {
    (leftDays: Int, rightDays: Int) =>
      Math.multiplyExact(Math.subtractExact(leftDays, rightDays), MICROS_PER_DAY)
  }

  override def nullSafeEval(leftDays: Any, rightDays: Any): Any = {
    evalFunc(leftDays.asInstanceOf[Int], rightDays.asInstanceOf[Int])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = if (legacyInterval) {
    defineCodeGen(ctx, ev, (leftDays, rightDays) => {
      val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
      s"$dtu.subtractDates($leftDays, $rightDays)"
    })
  } else {
    val m = classOf[Math].getName
    defineCodeGen(ctx, ev, (leftDays, rightDays) =>
      s"$m.multiplyExact($m.subtractExact($leftDays, $rightDays), ${MICROS_PER_DAY}L)")
  }

  override def toString: String = s"($left - $right)"
  override def sql: String = s"(${left.sql} - ${right.sql})"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): SubtractDates =
    copy(left = newLeft, right = newRight)
}

object SubtractDates {
  def apply(left: Expression, right: Expression): SubtractDates = new SubtractDates(left, right)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_([sourceTz, ]targetTz, sourceTs) - Converts the timestamp without time zone `sourceTs` from the `sourceTz` time zone to `targetTz`. ",
  arguments = """
    Arguments:
      * sourceTz - the time zone for the input timestamp.
                   If it is missed, the current session time zone is used as the source time zone.
      * targetTz - the time zone to which the input timestamp should be converted
      * sourceTs - a timestamp without time zone
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Europe/Brussels', 'America/Los_Angeles', timestamp_ntz'2021-12-06 00:00:00');
       2021-12-05 15:00:00
      > SELECT _FUNC_('Europe/Brussels', timestamp_ntz'2021-12-05 15:00:00');
       2021-12-06 00:00:00
  """,
  group = "datetime_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
case class ConvertTimezone(
    sourceTz: Expression,
    targetTz: Expression,
    sourceTs: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {
  override def nullIntolerant: Boolean = true

  def this(targetTz: Expression, sourceTs: Expression) =
    this(CurrentTimeZone(), targetTz, sourceTs)

  override def first: Expression = sourceTz
  override def second: Expression = targetTz
  override def third: Expression = sourceTs

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true),
      TimestampNTZType)
  override def dataType: DataType = TimestampNTZType

  override def nullSafeEval(srcTz: Any, tgtTz: Any, micros: Any): Any = {
    DateTimeUtils.convertTimestampNtzToAnotherTz(
      srcTz.asInstanceOf[UTF8String].toString,
      tgtTz.asInstanceOf[UTF8String].toString,
      micros.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (srcTz, tgtTz, micros) =>
      s"""$dtu.convertTimestampNtzToAnotherTz($srcTz.toString(), $tgtTz.toString(), $micros)""")
  }

  override def prettyName: String = "convert_timezone"

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): ConvertTimezone = {
    copy(sourceTz = newFirst, targetTz = newSecond, sourceTs = newThird)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(unit, quantity, timestamp) - Adds the specified number of units to the given timestamp.",
  arguments = """
    Arguments:
      * unit - this indicates the units of datetime that you want to add.
        Supported string values of `unit` are (case insensitive):
          - "YEAR"
          - "QUARTER" - 3 months
          - "MONTH"
          - "WEEK" - 7 days
          - "DAY", "DAYOFYEAR"
          - "HOUR"
          - "MINUTE"
          - "SECOND"
          - "MILLISECOND"
          - "MICROSECOND"
      * quantity - this is the number of units of time that you want to add.
      * timestamp - this is a timestamp (w/ or w/o timezone) to which you want to add.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(HOUR, 8, timestamp_ntz'2022-02-11 20:30:00');
       2022-02-12 04:30:00
      > SELECT _FUNC_(MONTH, 1, timestamp_ltz'2022-01-31 00:00:00');
       2022-02-28 00:00:00
      > SELECT _FUNC_(SECOND, -10, date'2022-01-01');
       2021-12-31 23:59:50
      > SELECT _FUNC_(YEAR, 10, timestamp'2000-01-01 01:02:03.123456');
       2010-01-01 01:02:03.123456
  """,
  group = "datetime_funcs",
  since = "3.3.0")
// scalastyle:on line.size.limit
case class TimestampAdd(
    unit: String,
    quantity: Expression,
    timestamp: Expression,
    timeZoneId: Option[String] = None)
  extends BinaryExpression
  with ImplicitCastInputTypes
  with TimeZoneAwareExpression {
  override def nullIntolerant: Boolean = true

  def this(unit: String, quantity: Expression, timestamp: Expression) =
    this(unit, quantity, timestamp, None)

  def this(unit: Expression, quantity: Expression, timestamp: Expression) =
    this(
      TimeZoneAwareExpression.convertExpressionToUnit(unit, "timestamp_add"),
      quantity,
      timestamp)

  override def left: Expression = quantity
  override def right: Expression = timestamp

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, AnyTimestampType)
  override def dataType: DataType = timestamp.dataType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  @transient private lazy val zoneIdInEval: ZoneId = zoneIdForType(timestamp.dataType)

  override def nullSafeEval(q: Any, micros: Any): Any = {
    DateTimeUtils.timestampAdd(unit, q.asInstanceOf[Long], micros.asInstanceOf[Long], zoneIdInEval)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    val zid = ctx.addReferenceObj("zoneId", zoneIdInEval, classOf[ZoneId].getName)
    defineCodeGen(ctx, ev, (q, micros) =>
      s"""$dtu.timestampAdd("$unit", $q, $micros, $zid)""")
  }

  override def prettyName: String = "timestampadd"

  override def sql: String = {
    val childrenSQL = (unit +: children.map(_.sql)).mkString(", ")
    s"$prettyName($childrenSQL)"
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): TimestampAdd = {
    copy(quantity = newLeft, timestamp = newRight)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(unit, startTimestamp, endTimestamp) - Gets the difference between the timestamps `endTimestamp` and `startTimestamp` in the specified units by truncating the fraction part.",
  arguments = """
    Arguments:
      * unit - this indicates the units of the difference between the given timestamps.
        Supported string values of `unit` are (case insensitive):
          - "YEAR"
          - "QUARTER" - 3 months
          - "MONTH"
          - "WEEK" - 7 days
          - "DAY"
          - "HOUR"
          - "MINUTE"
          - "SECOND"
          - "MILLISECOND"
          - "MICROSECOND"
      * startTimestamp - A timestamp which the expression subtracts from `endTimestamp`.
      * endTimestamp - A timestamp from which the expression subtracts `startTimestamp`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(HOUR, timestamp_ntz'2022-02-11 20:30:00', timestamp_ntz'2022-02-12 04:30:00');
       8
      > SELECT _FUNC_(MONTH, timestamp_ltz'2022-01-01 00:00:00', timestamp_ltz'2022-02-28 00:00:00');
       1
      > SELECT _FUNC_(SECOND, date'2022-01-01', timestamp'2021-12-31 23:59:50');
       -10
      > SELECT _FUNC_(YEAR, timestamp'2000-01-01 01:02:03.123456', timestamp'2010-01-01 01:02:03.123456');
       10
  """,
  group = "datetime_funcs",
  since = "3.3.0")
// scalastyle:on line.size.limit
case class TimestampDiff(
    unit: String,
    startTimestamp: Expression,
    endTimestamp: Expression,
    timeZoneId: Option[String] = None)
  extends BinaryExpression
  with ImplicitCastInputTypes
  with TimeZoneAwareExpression {
  override def nullIntolerant: Boolean = true

  def this(unit: String, startTimestamp: Expression, endTimestamp: Expression) =
    this(unit, startTimestamp, endTimestamp, None)

  def this(unit: Expression, startTimestamp: Expression, endTimestamp: Expression) =
    this(
      TimeZoneAwareExpression.convertExpressionToUnit(unit, "timestamp_diff"),
      startTimestamp,
      endTimestamp)

  override def left: Expression = startTimestamp
  override def right: Expression = endTimestamp

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, TimestampType)
  override def dataType: DataType = LongType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  @transient private lazy val zoneIdInEval: ZoneId = zoneIdForType(endTimestamp.dataType)

  override def nullSafeEval(startMicros: Any, endMicros: Any): Any = {
    DateTimeUtils.timestampDiff(
      unit,
      startMicros.asInstanceOf[Long],
      endMicros.asInstanceOf[Long],
      zoneIdInEval)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    val zid = ctx.addReferenceObj("zoneId", zoneIdInEval, classOf[ZoneId].getName)
    defineCodeGen(ctx, ev, (s, e) =>
      s"""$dtu.timestampDiff("$unit", $s, $e, $zid)""")
  }

  override def prettyName: String = "timestampdiff"

  override def sql: String = {
    val childrenSQL = (unit +: children.map(_.sql)).mkString(", ")
    s"$prettyName($childrenSQL)"
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): TimestampDiff = {
    copy(startTimestamp = newLeft, endTimestamp = newRight)
  }
}
