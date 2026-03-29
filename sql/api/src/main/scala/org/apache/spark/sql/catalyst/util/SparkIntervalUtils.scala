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
package org.apache.spark.sql.catalyst.util

import java.time.{Duration, Period}
import java.time.temporal.ChronoUnit

import scala.collection.mutable

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.IntervalStringStyles.{ANSI_STYLE, HIVE_STYLE, IntervalStyle}
import org.apache.spark.sql.types.{DayTimeIntervalType => DT, YearMonthIntervalType => YM}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

trait SparkIntervalUtils {
  protected val MAX_DAY: Long = Long.MaxValue / MICROS_PER_DAY
  protected val MAX_HOUR: Long = Long.MaxValue / MICROS_PER_HOUR
  protected val MAX_MINUTE: Long = Long.MaxValue / MICROS_PER_MINUTE
  protected val MAX_SECOND: Long = Long.MaxValue / MICROS_PER_SECOND
  protected val MIN_SECOND: Long = Long.MinValue / MICROS_PER_SECOND

  // The amount of seconds that can cause overflow in the conversion to microseconds
  private final val minDurationSeconds = Math.floorDiv(Long.MinValue, MICROS_PER_SECOND)

  /**
   * Converts this duration to the total length in microseconds. <p> If this duration is too large
   * to fit in a [[Long]] microseconds, then an exception is thrown. <p> If this duration has
   * greater than microsecond precision, then the conversion will drop any excess precision
   * information as though the amount in nanoseconds was subject to integer division by one
   * thousand.
   *
   * @return
   *   The total length of the duration in microseconds
   * @throws ArithmeticException
   *   If numeric overflow occurs
   */
  def durationToMicros(duration: Duration): Long = {
    durationToMicros(duration, DT.SECOND)
  }

  def durationToMicros(duration: Duration, endField: Byte): Long = {
    val seconds = duration.getSeconds
    val micros = if (seconds == minDurationSeconds) {
      val microsInSeconds = (minDurationSeconds + 1) * MICROS_PER_SECOND
      val nanoAdjustment = duration.getNano
      assert(
        0 <= nanoAdjustment && nanoAdjustment < NANOS_PER_SECOND,
        "Duration.getNano() must return the adjustment to the seconds field " +
          "in the range from 0 to 999999999 nanoseconds, inclusive.")
      Math.addExact(microsInSeconds, (nanoAdjustment - NANOS_PER_SECOND) / NANOS_PER_MICROS)
    } else {
      val microsInSeconds = Math.multiplyExact(seconds, MICROS_PER_SECOND)
      Math.addExact(microsInSeconds, duration.getNano / NANOS_PER_MICROS)
    }

    endField match {
      case DT.DAY => micros - micros % MICROS_PER_DAY
      case DT.HOUR => micros - micros % MICROS_PER_HOUR
      case DT.MINUTE => micros - micros % MICROS_PER_MINUTE
      case DT.SECOND => micros
    }
  }

  /**
   * Gets the total number of months in this period. <p> This returns the total number of months
   * in the period by multiplying the number of years by 12 and adding the number of months. <p>
   *
   * @return
   *   The total number of months in the period, may be negative
   * @throws ArithmeticException
   *   If numeric overflow occurs
   */
  def periodToMonths(period: Period): Int = {
    periodToMonths(period, YM.MONTH)
  }

  def periodToMonths(period: Period, endField: Byte): Int = {
    val monthsInYears = Math.multiplyExact(period.getYears, MONTHS_PER_YEAR)
    val months = Math.addExact(monthsInYears, period.getMonths)
    if (endField == YM.YEAR) {
      months - months % MONTHS_PER_YEAR
    } else {
      months
    }
  }

  /**
   * Obtains a [[Duration]] representing a number of microseconds.
   *
   * @param micros
   *   The number of microseconds, positive or negative
   * @return
   *   A [[Duration]], not null
   */
  def microsToDuration(micros: Long): Duration = Duration.of(micros, ChronoUnit.MICROS)

  /**
   * Obtains a [[Period]] representing a number of months. The days unit will be zero, and the
   * years and months units will be normalized.
   *
   * <p> The months unit is adjusted to have an absolute value < 12, with the years unit being
   * adjusted to compensate. For example, the method returns "2 years and 3 months" for the 27
   * input months. <p> The sign of the years and months units will be the same after
   * normalization. For example, -13 months will be converted to "-1 year and -1 month".
   *
   * @param months
   *   The number of months, positive or negative
   * @return
   *   The period of months, not null
   */
  def monthsToPeriod(months: Int): Period = Period.ofMonths(months).normalized()

  /**
   * Converts a string to [[CalendarInterval]] case-insensitively.
   *
   * @throws IllegalArgumentException
   *   if the input string is not in valid interval format.
   */
  def stringToInterval(input: UTF8String): CalendarInterval = {
    import ParseState._
    if (input == null) {
      throw new SparkIllegalArgumentException(
        errorClass = "INVALID_INTERVAL_FORMAT.INPUT_IS_NULL",
        messageParameters = Map("input" -> "null"))
    }
    // scalastyle:off caselocale .toLowerCase
    val s = input.trimAll().toLowerCase
    // scalastyle:on
    val bytes = s.getBytes
    if (bytes.isEmpty) {
      throw new SparkIllegalArgumentException(
        errorClass = "INVALID_INTERVAL_FORMAT.INPUT_IS_EMPTY",
        messageParameters = Map("input" -> input.toString))
    }
    var state = PREFIX
    var i = 0
    var currentValue: Long = 0
    var isNegative: Boolean = false
    var months: Int = 0
    var days: Int = 0
    var microseconds: Long = 0
    var fractionScale: Int = 0
    val initialFractionScale = (NANOS_PER_SECOND / 10).toInt
    var fraction: Int = 0
    var pointPrefixed: Boolean = false

    def trimToNextState(b: Byte, next: ParseState): Unit = {
      if (Character.isWhitespace(b)) {
        i += 1
      } else {
        state = next
      }
    }

    def currentWord: String = {
      val sep = "\\s+"
      val strings = s.toString.split(sep)
      val lenRight = s.substring(i, s.numBytes()).toString.split(sep).length
      strings(strings.length - lenRight)
    }

    while (i < bytes.length) {
      val b = bytes(i)
      state match {
        case PREFIX =>
          if (s.startsWith(intervalStr)) {
            if (s.numBytes() == intervalStr.numBytes()) {
              throw new SparkIllegalArgumentException(
                errorClass = "INVALID_INTERVAL_FORMAT.INPUT_IS_EMPTY",
                messageParameters = Map("input" -> input.toString))
            } else if (!Character.isWhitespace(bytes(i + intervalStr.numBytes()))) {
              throw new SparkIllegalArgumentException(
                errorClass = "INVALID_INTERVAL_FORMAT.INVALID_PREFIX",
                messageParameters = Map("input" -> input.toString, "prefix" -> currentWord))
            } else {
              i += intervalStr.numBytes() + 1
            }
          }
          state = TRIM_BEFORE_SIGN
        case TRIM_BEFORE_SIGN => trimToNextState(b, SIGN)
        case SIGN =>
          currentValue = 0
          fraction = 0
          // We preset next state from SIGN to TRIM_BEFORE_VALUE. If we meet '.' in the SIGN state,
          // it means that the interval value we deal with here is a numeric with only fractional
          // part, such as '.11 second', which can be parsed to 0.11 seconds. In this case, we need
          // to reset next state to `VALUE_FRACTIONAL_PART` to go parse the fraction part of the
          // interval value.
          state = TRIM_BEFORE_VALUE
          // We preset the scale to an invalid value to track fraction presence in the UNIT_BEGIN
          // state. If we meet '.', the scale become valid for the VALUE_FRACTIONAL_PART state.
          fractionScale = -1
          pointPrefixed = false
          b match {
            case '-' =>
              isNegative = true
              i += 1
            case '+' =>
              isNegative = false
              i += 1
            case _ if '0' <= b && b <= '9' =>
              isNegative = false
            case '.' =>
              isNegative = false
              fractionScale = initialFractionScale
              pointPrefixed = true
              i += 1
              state = VALUE_FRACTIONAL_PART
            case _ =>
              throw new SparkIllegalArgumentException(
                errorClass = "INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER",
                messageParameters = Map("input" -> input.toString, "number" -> currentWord))
          }
        case TRIM_BEFORE_VALUE => trimToNextState(b, VALUE)
        case VALUE =>
          b match {
            case _ if '0' <= b && b <= '9' =>
              try {
                currentValue = Math.addExact(Math.multiplyExact(10, currentValue), (b - '0'))
              } catch {
                case e: ArithmeticException =>
                  throw new SparkIllegalArgumentException(
                    errorClass = "INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION",
                    messageParameters = Map("input" -> input.toString))
              }
            case _ if Character.isWhitespace(b) => state = TRIM_BEFORE_UNIT
            case '.' =>
              fractionScale = initialFractionScale
              state = VALUE_FRACTIONAL_PART
            case _ =>
              throw new SparkIllegalArgumentException(
                errorClass = "INVALID_INTERVAL_FORMAT.INVALID_VALUE",
                messageParameters = Map("input" -> input.toString, "value" -> currentWord))
          }
          i += 1
        case VALUE_FRACTIONAL_PART =>
          if ('0' <= b && b <= '9' && fractionScale > 0) {
            fraction += (b - '0') * fractionScale
            fractionScale /= 10
          } else if (Character.isWhitespace(b) &&
            (!pointPrefixed || fractionScale < initialFractionScale)) {
            fraction /= NANOS_PER_MICROS.toInt
            state = TRIM_BEFORE_UNIT
          } else if ('0' <= b && b <= '9') {
            throw new SparkIllegalArgumentException(
              errorClass = "INVALID_INTERVAL_FORMAT.INVALID_PRECISION",
              messageParameters = Map("input" -> input.toString, "value" -> currentWord))
          } else {
            throw new SparkIllegalArgumentException(
              errorClass = "INVALID_INTERVAL_FORMAT.INVALID_VALUE",
              messageParameters = Map("input" -> input.toString, "value" -> currentWord))
          }
          i += 1
        case TRIM_BEFORE_UNIT => trimToNextState(b, UNIT_BEGIN)
        case UNIT_BEGIN =>
          // Checks that only seconds can have the fractional part
          if (b != 's' && fractionScale >= 0) {
            throw new SparkIllegalArgumentException(
              errorClass = "INVALID_INTERVAL_FORMAT.INVALID_FRACTION",
              messageParameters = Map("input" -> input.toString, "unit" -> currentWord))
          }
          if (isNegative) {
            currentValue = -currentValue
            fraction = -fraction
          }
          try {
            b match {
              case 'y' if s.matchAt(yearStr, i) =>
                val monthsInYears = Math.multiplyExact(MONTHS_PER_YEAR, currentValue)
                months = Math.toIntExact(Math.addExact(months, monthsInYears))
                i += yearStr.numBytes()
              case 'w' if s.matchAt(weekStr, i) =>
                val daysInWeeks = Math.multiplyExact(DAYS_PER_WEEK, currentValue)
                days = Math.toIntExact(Math.addExact(days, daysInWeeks))
                i += weekStr.numBytes()
              case 'd' if s.matchAt(dayStr, i) =>
                days = Math.addExact(days, Math.toIntExact(currentValue))
                i += dayStr.numBytes()
              case 'h' if s.matchAt(hourStr, i) =>
                val hoursUs = Math.multiplyExact(currentValue, MICROS_PER_HOUR)
                microseconds = Math.addExact(microseconds, hoursUs)
                i += hourStr.numBytes()
              case 's' if s.matchAt(secondStr, i) =>
                val secondsUs = Math.multiplyExact(currentValue, MICROS_PER_SECOND)
                microseconds = Math.addExact(Math.addExact(microseconds, secondsUs), fraction)
                i += secondStr.numBytes()
              case 'm' =>
                if (s.matchAt(monthStr, i)) {
                  months = Math.addExact(months, Math.toIntExact(currentValue))
                  i += monthStr.numBytes()
                } else if (s.matchAt(minuteStr, i)) {
                  val minutesUs = Math.multiplyExact(currentValue, MICROS_PER_MINUTE)
                  microseconds = Math.addExact(microseconds, minutesUs)
                  i += minuteStr.numBytes()
                } else if (s.matchAt(millisStr, i)) {
                  val millisUs = SparkDateTimeUtils.millisToMicros(currentValue)
                  microseconds = Math.addExact(microseconds, millisUs)
                  i += millisStr.numBytes()
                } else if (s.matchAt(microsStr, i)) {
                  microseconds = Math.addExact(microseconds, currentValue)
                  i += microsStr.numBytes()
                } else {
                  throw new SparkIllegalArgumentException(
                    errorClass = "INVALID_INTERVAL_FORMAT.INVALID_UNIT",
                    messageParameters = Map("input" -> input.toString, "unit" -> currentWord))
                }
              case _ =>
                throw new SparkIllegalArgumentException(
                  errorClass = "INVALID_INTERVAL_FORMAT.INVALID_UNIT",
                  messageParameters = Map("input" -> input.toString, "unit" -> currentWord))
            }
          } catch {
            case e: ArithmeticException =>
              throw new SparkIllegalArgumentException(
                errorClass = "INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION",
                messageParameters = Map("input" -> input.toString))
          }
          state = UNIT_SUFFIX
        case UNIT_SUFFIX =>
          b match {
            case 's' => state = UNIT_END
            case _ if Character.isWhitespace(b) => state = TRIM_BEFORE_SIGN
            case _ =>
              throw new SparkIllegalArgumentException(
                errorClass = "INVALID_INTERVAL_FORMAT.INVALID_UNIT",
                messageParameters = Map("input" -> input.toString, "unit" -> currentWord))
          }
          i += 1
        case UNIT_END =>
          if (Character.isWhitespace(b)) {
            i += 1
            state = TRIM_BEFORE_SIGN
          } else {
            throw new SparkIllegalArgumentException(
              errorClass = "INVALID_INTERVAL_FORMAT.INVALID_UNIT",
              messageParameters = Map("input" -> input.toString, "unit" -> currentWord))
          }
      }
    }

    val result = state match {
      case UNIT_SUFFIX | UNIT_END | TRIM_BEFORE_SIGN =>
        new CalendarInterval(months, days, microseconds)
      case TRIM_BEFORE_VALUE =>
        throw new SparkIllegalArgumentException(
          errorClass = "INVALID_INTERVAL_FORMAT.MISSING_NUMBER",
          messageParameters = Map("input" -> input.toString, "word" -> currentWord))
      case VALUE | VALUE_FRACTIONAL_PART =>
        throw new SparkIllegalArgumentException(
          errorClass = "INVALID_INTERVAL_FORMAT.MISSING_UNIT",
          messageParameters = Map("input" -> input.toString, "word" -> currentWord))
      case _ =>
        throw new SparkIllegalArgumentException(
          errorClass = "INVALID_INTERVAL_FORMAT.UNKNOWN_PARSING_ERROR",
          messageParameters = Map("input" -> input.toString, "word" -> currentWord))
    }

    result
  }

  /**
   * Converts an year-month interval as a number of months to its textual representation which
   * conforms to the ANSI SQL standard.
   *
   * @param months
   *   The number of months, positive or negative
   * @param style
   *   The style of textual representation of the interval
   * @param startField
   *   The start field (YEAR or MONTH) which the interval comprises of.
   * @param endField
   *   The end field (YEAR or MONTH) which the interval comprises of.
   * @return
   *   Year-month interval string
   */
  def toYearMonthIntervalString(
      months: Int,
      style: IntervalStyle,
      startField: Byte,
      endField: Byte): String = {
    var sign = ""
    var absMonths: Long = months
    if (months < 0) {
      sign = "-"
      absMonths = -absMonths
    }
    val year = s"$sign${absMonths / MONTHS_PER_YEAR}"
    val yearAndMonth = s"$year-${absMonths % MONTHS_PER_YEAR}"
    style match {
      case ANSI_STYLE =>
        val formatBuilder = new StringBuilder("INTERVAL '")
        if (startField == endField) {
          startField match {
            case YM.YEAR => formatBuilder.append(s"$year' YEAR")
            case YM.MONTH => formatBuilder.append(s"$months' MONTH")
          }
        } else {
          formatBuilder.append(s"$yearAndMonth' YEAR TO MONTH")
        }
        formatBuilder.toString
      case HIVE_STYLE => s"$yearAndMonth"
    }
  }

  /**
   * Converts a day-time interval as a number of microseconds to its textual representation which
   * conforms to the ANSI SQL standard.
   *
   * @param micros
   *   The number of microseconds, positive or negative
   * @param style
   *   The style of textual representation of the interval
   * @param startField
   *   The start field (DAY, HOUR, MINUTE, SECOND) which the interval comprises of.
   * @param endField
   *   The end field (DAY, HOUR, MINUTE, SECOND) which the interval comprises of.
   * @return
   *   Day-time interval string
   */
  def toDayTimeIntervalString(
      micros: Long,
      style: IntervalStyle,
      startField: Byte,
      endField: Byte): String = {
    var sign = ""
    var rest = micros
    // scalastyle:off caselocale
    val from = DT.fieldToString(startField).toUpperCase
    val to = DT.fieldToString(endField).toUpperCase
    // scalastyle:on caselocale
    val prefix = "INTERVAL '"
    val postfix = s"' ${if (startField == endField) from else s"$from TO $to"}"

    if (micros < 0) {
      if (micros == Long.MinValue) {
        // Especial handling of minimum `Long` value because negate op overflows `Long`.
        // seconds = 106751991 * (24 * 60 * 60) + 4 * 60 * 60 + 54 = 9223372036854
        // microseconds = -9223372036854000000L-775808 == Long.MinValue
        val baseStr = "-106751991 04:00:54.775808000"
        val minIntervalString = style match {
          case ANSI_STYLE =>
            val firstStr = startField match {
              case DT.DAY => s"-$MAX_DAY"
              case DT.HOUR => s"-$MAX_HOUR"
              case DT.MINUTE => s"-$MAX_MINUTE"
              case DT.SECOND => s"-$MAX_SECOND.775808"
            }
            val followingStr = if (startField == endField) {
              ""
            } else {
              val substrStart = startField match {
                case DT.DAY => 10
                case DT.HOUR => 13
                case DT.MINUTE => 16
              }
              val substrEnd = endField match {
                case DT.HOUR => 13
                case DT.MINUTE => 16
                case DT.SECOND => 26
              }
              baseStr.substring(substrStart, substrEnd)
            }

            s"$prefix$firstStr$followingStr$postfix"
          case HIVE_STYLE => baseStr
        }
        return minIntervalString
      } else {
        sign = "-"
        rest = -rest
      }
    }
    val intervalString = style match {
      case ANSI_STYLE =>
        val formatBuilder = new mutable.StringBuilder(sign)
        val formatArgs = new mutable.ArrayBuffer[Long]()
        startField match {
          case DT.DAY =>
            formatBuilder.append(rest / MICROS_PER_DAY)
            rest %= MICROS_PER_DAY
          case DT.HOUR =>
            formatBuilder.append("%02d")
            formatArgs.append(rest / MICROS_PER_HOUR)
            rest %= MICROS_PER_HOUR
          case DT.MINUTE =>
            formatBuilder.append("%02d")
            formatArgs.append(rest / MICROS_PER_MINUTE)
            rest %= MICROS_PER_MINUTE
          case DT.SECOND =>
            val leadZero = if (rest < 10 * MICROS_PER_SECOND) "0" else ""
            formatBuilder.append(
              s"$leadZero" +
                s"${java.math.BigDecimal.valueOf(rest, 6).stripTrailingZeros.toPlainString}")
        }

        if (startField < DT.HOUR && DT.HOUR <= endField) {
          formatBuilder.append(" %02d")
          formatArgs.append(rest / MICROS_PER_HOUR)
          rest %= MICROS_PER_HOUR
        }
        if (startField < DT.MINUTE && DT.MINUTE <= endField) {
          formatBuilder.append(":%02d")
          formatArgs.append(rest / MICROS_PER_MINUTE)
          rest %= MICROS_PER_MINUTE
        }
        if (startField < DT.SECOND && DT.SECOND <= endField) {
          val leadZero = if (rest < 10 * MICROS_PER_SECOND) "0" else ""
          formatBuilder.append(
            s":$leadZero${java.math.BigDecimal.valueOf(rest, 6).stripTrailingZeros.toPlainString}")
        }
        s"$prefix${formatBuilder.toString.format(formatArgs.toSeq: _*)}$postfix"
      case HIVE_STYLE =>
        val secondsWithFraction = rest % MICROS_PER_MINUTE
        rest /= MICROS_PER_MINUTE
        val minutes = rest % MINUTES_PER_HOUR
        rest /= MINUTES_PER_HOUR
        val hours = rest % HOURS_PER_DAY
        val days = rest / HOURS_PER_DAY
        val seconds = secondsWithFraction / MICROS_PER_SECOND
        val nanos = (secondsWithFraction % MICROS_PER_SECOND) * NANOS_PER_MICROS
        f"$sign$days $hours%02d:$minutes%02d:$seconds%02d.$nanos%09d"
    }
    intervalString
  }

  protected def unitToUtf8(unit: String): UTF8String = {
    UTF8String.fromString(unit)
  }

  protected val intervalStr: UTF8String = unitToUtf8("interval")

  protected val yearStr: UTF8String = unitToUtf8("year")
  protected val monthStr: UTF8String = unitToUtf8("month")
  protected val weekStr: UTF8String = unitToUtf8("week")
  protected val dayStr: UTF8String = unitToUtf8("day")
  protected val hourStr: UTF8String = unitToUtf8("hour")
  protected val minuteStr: UTF8String = unitToUtf8("minute")
  protected val secondStr: UTF8String = unitToUtf8("second")
  protected val millisStr: UTF8String = unitToUtf8("millisecond")
  protected val microsStr: UTF8String = unitToUtf8("microsecond")
  protected val nanosStr: UTF8String = unitToUtf8("nanosecond")

  private object ParseState extends Enumeration {
    type ParseState = Value

    val PREFIX, TRIM_BEFORE_SIGN, SIGN, TRIM_BEFORE_VALUE, VALUE, VALUE_FRACTIONAL_PART,
        TRIM_BEFORE_UNIT, UNIT_BEGIN, UNIT_SUFFIX, UNIT_END = Value
  }
}

object SparkIntervalUtils extends SparkIntervalUtils

// The style of textual representation of intervals
object IntervalStringStyles extends Enumeration {
  type IntervalStyle = Value
  val ANSI_STYLE, HIVE_STYLE = Value
}
