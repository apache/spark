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

package org.apache.spark.sql.execution.streaming.operators.stateful.join

import java.time.{LocalDateTime, ZoneId}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeConstants.{MICROS_PER_DAY, MICROS_PER_MINUTE, MICROS_PER_SECOND}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

class MatchingScanWindowSuite extends SparkFunSuite {

  private val leftTime = AttributeReference("leftTime", TimestampType)()
  private val expiry = AttributeReference("expiry", TimestampType)()
  private val leftValue = AttributeReference("leftValue", LongType)()
  private val leftInput = Seq(leftTime, expiry, leftValue)
  private val rightTime = AttributeReference("rightTime", TimestampType)()
  private val rightValue = AttributeReference("rightValue", LongType)()
  private val rightInput = Seq(rightTime, rightValue)

  private val utc = Some("UTC")
  private def seconds(s: Double): Long = (s * MICROS_PER_SECOND).toLong
  private def minutes(m: Long): Literal = Literal(m * MICROS_PER_MINUTE, DayTimeIntervalType())
  private def plus(t: Expression, interval: Expression, zone: Option[String] = utc) =
    TimestampAddInterval(t, interval, zone)
  private def long(e: Expression): Expression = Cast(e, LongType, utc)

  private def leftRow(time: Any, expiryTime: Any = null, value: Any = null): InternalRow =
    InternalRow(time, expiryTime, value)
  private def rightRow(time: Any, value: Any = null): InternalRow = InternalRow(time, value)

  /** The window a row of the input reads on `column`, or None if it reads every row. */
  private def window(
      condition: Expression,
      input: Seq[Attribute],
      column: Attribute,
      row: InternalRow): Option[(Long, Long)] =
    MatchingScanWindow(Some(condition), input, column).flatMap(_.rangeFor(row))

  test("q9 shape: rows of both sides bound the other side's indexed column") {
    val condition = rightTime >= leftTime && rightTime <= expiry
    // A left row reads right rows from its own time to its expiry.
    assert(window(condition, leftInput, rightTime, leftRow(seconds(100), seconds(200))) ===
      Some((seconds(100), seconds(200))))
    // The left side is ordered by expiry, so a right row reads left rows open at the top.
    assert(window(condition, rightInput, expiry, rightRow(seconds(150))) ===
      Some((seconds(150), Long.MaxValue)))
    // If the left side were ordered by leftTime, the same row would read up to its own time.
    assert(window(condition, rightInput, leftTime, rightRow(seconds(150))) ===
      Some((Long.MinValue, seconds(150))))
  }

  test("BETWEEN, equality and strict comparisons give inclusive bounds") {
    val between = Between(rightTime, leftTime, expiry)
    assert(window(between, leftInput, rightTime, leftRow(seconds(1), seconds(2))) ===
      Some((seconds(1), seconds(2))))
    assert(window(rightTime > leftTime && rightTime < expiry, leftInput, rightTime,
      leftRow(seconds(1), seconds(2))) === Some((seconds(1), seconds(2))))
    assert(window(EqualTo(leftTime, rightTime), rightInput, leftTime, rightRow(seconds(3))) ===
      Some((seconds(3), seconds(3))))
    // Bounds of several conjuncts intersect.
    val twoLower = rightTime >= leftTime && rightTime >= expiry && rightTime <= plus(leftTime,
      minutes(10))
    assert(window(twoLower, leftInput, rightTime, leftRow(seconds(0), seconds(60))) ===
      Some((seconds(60), seconds(600))))
  }

  test("shifts without days or months are inverted exactly on the indexed side") {
    val condition = rightTime >= plus(leftTime, minutes(1)) &&
      plus(rightTime, minutes(2)) <= plus(leftTime, minutes(12))
    assert(window(condition, leftInput, rightTime, leftRow(seconds(0))) ===
      Some((seconds(60), seconds(600))))
    assert(window(condition, rightInput, leftTime, rightRow(seconds(600))) ===
      Some((seconds(0), seconds(540))))
    val calendar = Literal(new CalendarInterval(0, 0, MICROS_PER_SECOND), CalendarIntervalType)
    assert(window(plus(leftTime, calendar) >= rightTime, rightInput, leftTime,
      rightRow(seconds(10))) === Some((seconds(9), Long.MaxValue)))
    val add = TimestampAdd("MINUTE", Literal(5), leftTime, utc)
    assert(window(add >= rightTime, rightInput, leftTime, rightRow(seconds(600))) ===
      Some((seconds(300), Long.MaxValue)))
  }

  test("days and months give no bound on the indexed side but are exact on the probing side") {
    val zone = "America/Los_Angeles"
    val oneDay = Literal(MICROS_PER_DAY, DayTimeIntervalType())
    val condition = rightTime >= leftTime && rightTime <= plus(leftTime, oneDay, Some(zone))
    // Daylight saving time ends on 2026-11-01 in Los Angeles, so that day has 25 hours.
    val start = DateTimeUtils.instantToMicros(
      LocalDateTime.of(2026, 10, 31, 12, 0).atZone(ZoneId.of(zone)).toInstant)
    assert(window(condition, leftInput, rightTime, leftRow(start)) ===
      Some((start, start + 25L * 3600 * MICROS_PER_SECOND)))
    // A right row cannot invert leftTime + 1 day, so only rightTime >= leftTime bounds it.
    assert(window(condition, rightInput, leftTime, rightRow(start)) ===
      Some((Long.MinValue, start)))
    val calendarDay = Literal(new CalendarInterval(0, 1, 0), CalendarIntervalType)
    val month = Literal(new CalendarInterval(1, 0, 0), CalendarIntervalType)
    val yearMonth = Literal(1, YearMonthIntervalType())
    Seq(calendarDay, month).foreach { interval =>
      assert(window(plus(leftTime, interval) >= rightTime, rightInput, leftTime,
        rightRow(start)) === None)
    }
    assert(window(TimestampAddYMInterval(leftTime, yearMonth, utc) >= rightTime, rightInput,
      leftTime, rightRow(start)) === None)
    assert(window(TimestampAdd("DAY", Literal(1), leftTime, utc) >= rightTime, rightInput,
      leftTime, rightRow(start)) === None)
  }

  test("casts to BIGINT bound whole seconds on either side") {
    // floor(rightTime) >= floor(leftTime) + 60 and <= floor(leftTime) + 120
    val condition = long(rightTime) >= long(leftTime) + 60L &&
      long(rightTime) <= long(leftTime) + 120L
    // A left row at 100.6 seconds matches right rows from 160 seconds to just below 221.
    assert(window(condition, leftInput, rightTime, leftRow(seconds(100.6))) ===
      Some((seconds(160), seconds(221) - 1)))
    // A right row at 160.2 seconds matches left rows from 40 seconds to just below 101.
    assert(window(condition, rightInput, leftTime, rightRow(seconds(160.2))) ===
      Some((seconds(40), seconds(101) - 1)))
    // A cast of a shifted time.
    val shifted = long(rightTime) >= long(plus(leftTime, minutes(1)))
    assert(window(shifted, rightInput, leftTime, rightRow(seconds(160.2))) ===
      Some((Long.MinValue, seconds(101) - 1)))
    // A difference of casts.
    val difference = long(leftTime) - long(rightTime) > 2L
    assert(window(difference, leftInput, rightTime, leftRow(seconds(10.5))) ===
      Some((Long.MinValue, seconds(9) - 1)))
    assert(window(difference, rightInput, leftTime, rightRow(seconds(10.5))) ===
      Some((seconds(12), Long.MaxValue)))
  }

  test("other casts and functions of the indexed column give no bound") {
    val row = rightRow(seconds(10))
    Seq[Expression](
      Cast(leftTime, DoubleType, utc) >= Cast(rightTime, DoubleType, utc),
      Cast(leftTime, IntegerType, utc) >= Cast(rightTime, IntegerType, utc),
      Cast(leftTime, DateType, utc) >= Cast(rightTime, DateType, utc),
      Cast(Cast(long(leftTime), TimestampType, utc), TimestampType, utc) >= rightTime,
      Coalesce(Seq(leftTime, expiry)) >= rightTime,
      plus(leftTime, Cast(rightValue, DayTimeIntervalType(), utc)) >= rightTime,
      leftTime >= rightTime || leftTime <= rightTime,
      EqualNullSafe(leftTime, rightTime)
    ).foreach { condition =>
      assert(window(condition, rightInput, leftTime, row) === None, condition.sql)
    }
    // A bound that reads the indexed side too, or is not deterministic.
    assert(window(leftTime >= Greatest(Seq(rightTime, expiry)), rightInput, leftTime, row) ===
      None)
    val random = If(Rand(0) < 2.0, rightTime, rightTime)
    assert(window(leftTime >= random, rightInput, leftTime, row) === None)
    // Only a TIMESTAMP column can be scanned by range.
    assert(MatchingScanWindow(Some(leftValue >= rightValue), rightInput, leftValue).isEmpty)
  }

  test("a NULL, failing or overflowing bound leaves its end open") {
    val condition = rightTime >= leftTime && rightTime <= expiry
    assert(window(condition, leftInput, rightTime, leftRow(seconds(1), null)) ===
      Some((seconds(1), Long.MaxValue)))
    assert(window(condition, leftInput, rightTime, leftRow(null, null)) === None)

    val divideByZero = long(rightTime) >= IntegralDivide(leftValue, Literal(0L), EvalMode.ANSI)
    assert(window(divideByZero && rightTime <= expiry, leftInput, rightTime,
      leftRow(seconds(1), seconds(2), 5L)) === Some((Long.MinValue, seconds(2))))

    // Seconds beyond the microsecond range, and a bound shifted past it.
    assert(window(long(rightTime) >= leftValue, leftInput, rightTime,
      leftRow(seconds(1), null, Long.MaxValue / 10)) === None)
    assert(window(rightTime <= plus(leftTime, minutes(-1)), rightInput, leftTime,
      rightRow(Long.MaxValue - 1)) === None)
    // The rewritten integral bound overflows.
    assert(window(Add(long(rightTime), leftValue, EvalMode.ANSI) >= Long.MinValue, leftInput,
      rightTime, leftRow(seconds(1), null, 1L)) === None)
  }

  test("integral sums are inverted only when they cannot wrap around") {
    def sum(x: Expression, y: Expression, mode: EvalMode.Value): Expression = Add(x, y, mode)
    val row = leftRow(seconds(100))
    Seq(EvalMode.ANSI, EvalMode.TRY, EvalMode.LEGACY).foreach { mode =>
      // The cast's seconds and a small constant cannot overflow, in any mode.
      assert(window(sum(long(rightTime), Literal(10L), mode) >= long(leftTime), leftInput,
        rightTime, row) === Some((seconds(90), Long.MaxValue)), mode)
    }
    // A column of this input has no known magnitude, so the legacy mode gives no bound.
    assert(window(sum(long(rightTime), leftValue, EvalMode.LEGACY) >= long(leftTime), leftInput,
      rightTime, leftRow(seconds(100), null, 10L)) === None)
    assert(window(sum(long(rightTime), leftValue, EvalMode.ANSI) >= long(leftTime), leftInput,
      rightTime, leftRow(seconds(100), null, 10L)) === Some((seconds(90), Long.MaxValue)))
    assert(window(sum(long(rightTime), Literal(Long.MaxValue), EvalMode.LEGACY) >= 0L,
      leftInput, rightTime, row) === None)
  }
}
