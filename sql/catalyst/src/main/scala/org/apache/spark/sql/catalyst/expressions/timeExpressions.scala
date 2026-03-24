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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.util.TimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Extracts the microsecond component from a TIME value.
 * Returns an integer in the range 0-999999.
 */
@ExpressionDescription(
  usage = "_FUNC_(time) - Returns the microsecond component of the time (0-999999).",
  examples = """
    Examples:
      > SELECT _FUNC_(TIME '14:30:45.123456');
       123456
      > SELECT _FUNC_(TIME '00:00:00');
       0
  """,
  since = "4.0.0",
  group = "datetime_funcs")
case class MicrosecondOfTime(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimeType)
  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(time: Any): Any = {
    TimeUtils.getMicrosecond(time.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, time =>
      s"${TimeUtils.getClass.getName.stripSuffix("$$")}.getMicrosecond($time)")
  }

  override protected def withNewChildInternal(newChild: Expression): MicrosecondOfTime =
    copy(child = newChild)
}

/**
 * Returns the current time at the start of query evaluation.
 * The time is evaluated once per query and remains constant throughout execution.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current time at the start of query evaluation.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       14:30:45.123456
  """,
  since = "4.0.0",
  group = "datetime_funcs")
case class CurrentTime(timeZoneId: Option[String] = None)
  extends LeafExpression with TimeZoneAwareExpression with CodegenFallback {

  def this() = this(None)

  override def nullable: Boolean = false
  override def dataType: DataType = TimeType
  override def foldable: Boolean = false
  override lazy val deterministic: Boolean = false

  override def eval(input: InternalRow): Any = {
    TimeUtils.currentTime(zoneId)
  }

  override def withTimeZone(timeZoneId: String): CurrentTime =
    copy(timeZoneId = Option(timeZoneId))

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): CurrentTime =
    copy()
}

/**
 * Creates a TIME value from hour, minute, second, and optional microsecond components.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(hour, minute, second[, microsecond]) - Creates a time from hour, minute, second,
    and optional microsecond components.
  """,
  arguments = """
    Arguments:
      * hour - Hour component (0-23)
      * minute - Minute component (0-59)
      * second - Second component (0-59)
      * microsecond - Microsecond component (0-999999), optional, defaults to 0
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(14, 30, 45);
       14:30:45.000000
      > SELECT _FUNC_(14, 30, 45, 123456);
       14:30:45.123456
      > SELECT _FUNC_(0, 0, 0);
       00:00:00.000000
  """,
  since = "4.0.0",
  group = "datetime_funcs")
case class MakeTime(
    hour: Expression,
    minute: Expression,
    sec: Expression,
    microsecond: Expression = Literal(0))
  extends QuaternaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(hour: Expression, minute: Expression, sec: Expression) =
    this(hour, minute, sec, Literal(0))

  override def first: Expression = hour
  override def second: Expression = minute
  override def third: Expression = sec
  override def fourth: Expression = microsecond

  override def inputTypes: Seq[AbstractDataType] =
    Seq(IntegerType, IntegerType, IntegerType, IntegerType)

  override def dataType: DataType = TimeType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (hour.foldable && minute.foldable && sec.foldable && microsecond.foldable) {
      try {
        val h = hour.eval().asInstanceOf[Int]
        val m = minute.eval().asInstanceOf[Int]
        val s = sec.eval().asInstanceOf[Int]
        val us = microsecond.eval().asInstanceOf[Int]

        if (h < 0 || h > 23) {
          return TypeCheckResult.TypeCheckFailure(s"Hour must be in range [0, 23], got $h")
        }
        if (m < 0 || m > 59) {
          return TypeCheckResult.TypeCheckFailure(s"Minute must be in range [0, 59], got $m")
        }
        if (s < 0 || s > 59) {
          return TypeCheckResult.TypeCheckFailure(s"Second must be in range [0, 59], got $s")
        }
        if (us < 0 || us >= TimeUtils.MICROS_PER_SECOND) {
          return TypeCheckResult.TypeCheckFailure(
            s"Microsecond must be in range [0, 999999], got $us")
        }
      } catch {
        case _: Exception => // Will be caught at runtime
      }
    }
    TypeCheckResult.TypeCheckSuccess
  }

  override protected def nullSafeEval(h: Any, m: Any, s: Any, us: Any): Any = {
    try {
      TimeUtils.makeTime(
        h.asInstanceOf[Int],
        m.asInstanceOf[Int],
        s.asInstanceOf[Int],
        us.asInstanceOf[Int])
    } catch {
      case _: IllegalArgumentException => null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (h, m, s, us) => {
      val timeUtils = TimeUtils.getClass.getName.stripSuffix("$")
      s"""
         |try {
         |  ${ev.value} = $timeUtils.makeTime($h, $m, $s, $us);
         |} catch (IllegalArgumentException e) {
         |  ${ev.isNull} = true;
         |}
       """.stripMargin
    })
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): MakeTime =
    copy(hour = newFirst, minute = newSecond, sec = newThird, microsecond = newFourth)
}

/**
 * Converts a TIME value to the number of milliseconds since midnight.
 */
@ExpressionDescription(
  usage = "_FUNC_(time) - Returns the number of milliseconds since midnight for a TIME value.",
  examples = """
    Examples:
      > SELECT _FUNC_(TIME '14:30:00.5');
       52200500
  """,
  since = "4.0.0",
  group = "datetime_funcs")
case class TimeToMillis(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimeType)
  override def dataType: DataType = LongType

  override protected def nullSafeEval(time: Any): Any = {
    time.asInstanceOf[Long] / 1000L
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, time => s"$time / 1000L")
  }

  override protected def withNewChildInternal(newChild: Expression): TimeToMillis =
    copy(child = newChild)
}

/**
 * Creates a TIME value from the number of milliseconds since midnight.
 */
@ExpressionDescription(
  usage = "_FUNC_(millis) - Creates a TIME value from the number of milliseconds since midnight.",
  examples = """
    Examples:
      > SELECT _FUNC_(52200500);
       14:30:00.500000
  """,
  since = "4.0.0",
  group = "datetime_funcs")
case class TimeFromMillis(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)
  override def dataType: DataType = TimeType

  override protected def nullSafeEval(millis: Any): Any = {
    val micros = millis.asInstanceOf[Long] * 1000L
    if (TimeUtils.isValidTime(micros)) micros else null
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val timeUtils = TimeUtils.getClass.getName.stripSuffix("$")
    nullSafeCodeGen(ctx, ev, millis =>
      s"""
         |long micros = $millis * 1000L;
         |if ($timeUtils.isValidTime(micros)) {
         |  ${ev.value} = micros;
         |} else {
         |  ${ev.isNull} = true;
         |}
       """.stripMargin)
  }

  override protected def withNewChildInternal(newChild: Expression): TimeFromMillis =
    copy(child = newChild)
}

/**
 * Converts a TIME value to the number of microseconds since midnight.
 */
@ExpressionDescription(
  usage = "_FUNC_(time) - Returns the number of microseconds since midnight for a TIME value.",
  examples = """
    Examples:
      > SELECT _FUNC_(TIME '14:30:00.5');
       52200500000
  """,
  since = "4.0.0",
  group = "datetime_funcs")
case class TimeToMicros(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimeType)
  override def dataType: DataType = LongType

  override protected def nullSafeEval(time: Any): Any = time

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, time => time)
  }

  override protected def withNewChildInternal(newChild: Expression): TimeToMicros =
    copy(child = newChild)
}

/**
 * Creates a TIME value from the number of microseconds since midnight.
 */
@ExpressionDescription(
  usage = "_FUNC_(micros) - Creates a TIME value from the number of microseconds since midnight.",
  examples = """
    Examples:
      > SELECT _FUNC_(52200500000);
       14:30:00.500000
  """,
  since = "4.0.0",
  group = "datetime_funcs")
case class TimeFromMicros(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)
  override def dataType: DataType = TimeType

  override protected def nullSafeEval(micros: Any): Any = {
    val m = micros.asInstanceOf[Long]
    if (TimeUtils.isValidTime(m)) m else null
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val timeUtils = TimeUtils.getClass.getName.stripSuffix("$")
    nullSafeCodeGen(ctx, ev, micros =>
      s"""
         |if ($timeUtils.isValidTime($micros)) {
         |  ${ev.value} = $micros;
         |} else {
         |  ${ev.isNull} = true;
         |}
       """.stripMargin)
  }

  override protected def withNewChildInternal(newChild: Expression): TimeFromMicros =
    copy(child = newChild)
}

/**
 * Converts a TIME value to a formatted string representation.
 */
@ExpressionDescription(
  usage = "_FUNC_(time, pattern) - Converts a TIME value to a formatted string.",
  examples = """
    Examples:
      > SELECT _FUNC_(TIME '14:30:45', 'HH:mm:ss');
       14:30:45
      > SELECT _FUNC_(TIME '14:30:45', 'hh:mm:ss a');
       02:30:45 PM
  """,
  since = "4.0.0",
  group = "datetime_funcs")
case class TimeFormat(
    left: Expression,
    right: Expression,
    timeZoneId: Option[String] = None)
  extends BinaryExpression
  with TimeZoneAwareExpression
  with ImplicitCastInputTypes
  with NullIntolerant {

  def this(left: Expression, right: Expression) = this(left, right, None)

  override def inputTypes: Seq[AbstractDataType] = Seq(TimeType, StringType)
  override def dataType: DataType = StringType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  @transient private lazy val formatterOption: Option[java.time.format.DateTimeFormatter] =
    if (right.foldable) {
      Option(right.eval()).map { fmt =>
        java.time.format.DateTimeFormatter.ofPattern(fmt.toString)
      }
    } else None

  override protected def nullSafeEval(time: Any, pattern: Any): Any = {
    val fmt = pattern.toString
    val formatter = formatterOption.getOrElse(java.time.format.DateTimeFormatter.ofPattern(fmt))
    val localTime = TimeUtils.microsToLocalTime(time.asInstanceOf[Long])
    val localDateTime = localTime.atDate(java.time.LocalDate.ofEpochDay(0))
    UTF8String.fromString(localDateTime.format(formatter))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[java.time.ZoneId].getName)
    val timeUtils = TimeUtils.getClass.getName.stripSuffix("$")
    nullSafeCodeGen(ctx, ev, (time, pattern) => {
      s"""
         |java.time.format.DateTimeFormatter formatter =
         |  java.time.format.DateTimeFormatter.ofPattern($pattern.toString());
         |java.time.LocalTime localTime = $timeUtils.microsToLocalTime($time);
         |java.time.LocalDateTime localDateTime =
         |  localTime.atDate(java.time.LocalDate.ofEpochDay(0));
         |${ev.value} = UTF8String.fromString(localDateTime.format(formatter));
       """.stripMargin
    })
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): TimeFormat =
    copy(left = newLeft, right = newRight)
}
