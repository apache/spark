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

import org.apache.spark.sql.catalyst.util.DateTimeConstants.{DAYS_PER_WEEK, MICROS_PER_HOUR, MICROS_PER_MINUTE, MICROS_PER_SECOND, MONTHS_PER_YEAR, NANOS_PER_MICROS, NANOS_PER_SECOND}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

trait SparkIntervalUtils {
  /**
   * Converts a string to [[CalendarInterval]] case-insensitively.
   *
   * @throws IllegalArgumentException if the input string is not in valid interval format.
   */
  def stringToInterval(input: UTF8String): CalendarInterval = {
    import ParseState._
    def throwIAE(msg: String, e: Exception = null) = {
      throw new IllegalArgumentException(s"Error parsing '$input' to interval, $msg", e)
    }

    if (input == null) {
      throwIAE("interval string cannot be null")
    }
    // scalastyle:off caselocale .toLowerCase
    val s = input.trimAll().toLowerCase
    // scalastyle:on
    val bytes = s.getBytes
    if (bytes.isEmpty) {
      throwIAE("interval string cannot be empty")
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
              throwIAE("interval string cannot be empty")
            } else if (!Character.isWhitespace(bytes(i + intervalStr.numBytes()))) {
              throwIAE(s"invalid interval prefix $currentWord")
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
            case _ => throwIAE( s"unrecognized number '$currentWord'")
          }
        case TRIM_BEFORE_VALUE => trimToNextState(b, VALUE)
        case VALUE =>
          b match {
            case _ if '0' <= b && b <= '9' =>
              try {
                currentValue = Math.addExact(Math.multiplyExact(10, currentValue), (b - '0'))
              } catch {
                case e: ArithmeticException => throwIAE(e.getMessage, e)
              }
            case _ if Character.isWhitespace(b) => state = TRIM_BEFORE_UNIT
            case '.' =>
              fractionScale = initialFractionScale
              state = VALUE_FRACTIONAL_PART
            case _ => throwIAE(s"invalid value '$currentWord'")
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
            throwIAE(s"interval can only support nanosecond precision, '$currentWord' is out" +
              s" of range")
          } else {
            throwIAE(s"invalid value '$currentWord'")
          }
          i += 1
        case TRIM_BEFORE_UNIT => trimToNextState(b, UNIT_BEGIN)
        case UNIT_BEGIN =>
          // Checks that only seconds can have the fractional part
          if (b != 's' && fractionScale >= 0) {
            throwIAE(s"'$currentWord' cannot have fractional part")
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
                } else throwIAE(s"invalid unit '$currentWord'")
              case _ => throwIAE(s"invalid unit '$currentWord'")
            }
          } catch {
            case e: ArithmeticException => throwIAE(e.getMessage, e)
          }
          state = UNIT_SUFFIX
        case UNIT_SUFFIX =>
          b match {
            case 's' => state = UNIT_END
            case _ if Character.isWhitespace(b) => state = TRIM_BEFORE_SIGN
            case _ => throwIAE(s"invalid unit '$currentWord'")
          }
          i += 1
        case UNIT_END =>
          if (Character.isWhitespace(b) ) {
            i += 1
            state = TRIM_BEFORE_SIGN
          } else {
            throwIAE(s"invalid unit '$currentWord'")
          }
      }
    }

    val result = state match {
      case UNIT_SUFFIX | UNIT_END | TRIM_BEFORE_SIGN =>
        new CalendarInterval(months, days, microseconds)
      case TRIM_BEFORE_VALUE => throwIAE(s"expect a number after '$currentWord' but hit EOL")
      case VALUE | VALUE_FRACTIONAL_PART =>
        throwIAE(s"expect a unit name after '$currentWord' but hit EOL")
      case _ => throwIAE(s"unknown error when parsing '$currentWord'")
    }

    result
  }

  protected def unitToUtf8(unit: String): UTF8String = {
    UTF8String.fromString(unit)
  }

  protected val intervalStr = unitToUtf8("interval")

  protected val yearStr = unitToUtf8("year")
  protected val monthStr = unitToUtf8("month")
  protected val weekStr = unitToUtf8("week")
  protected val dayStr = unitToUtf8("day")
  protected val hourStr = unitToUtf8("hour")
  protected val minuteStr = unitToUtf8("minute")
  protected val secondStr = unitToUtf8("second")
  protected val millisStr = unitToUtf8("millisecond")
  protected val microsStr = unitToUtf8("microsecond")
  protected val nanosStr = unitToUtf8("nanosecond")


  private object ParseState extends Enumeration {
    type ParseState = Value

    val PREFIX,
    TRIM_BEFORE_SIGN,
    SIGN,
    TRIM_BEFORE_VALUE,
    VALUE,
    VALUE_FRACTIONAL_PART,
    TRIM_BEFORE_UNIT,
    UNIT_BEGIN,
    UNIT_SUFFIX,
    UNIT_END = Value
  }
}

object SparkIntervalUtils extends SparkIntervalUtils
