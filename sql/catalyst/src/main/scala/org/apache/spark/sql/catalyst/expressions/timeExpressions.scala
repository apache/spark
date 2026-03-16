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
