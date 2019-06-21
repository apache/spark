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

package org.apache.spark.sql.types

import java.io.Serializable
import java.util.Locale
import java.util.regex.Pattern


/**
 * The internal representation of interval type.
 */

object CalendarInterval {
  val MICROS_PER_MILLI = 1000L
  val MICROS_PER_SECOND: Long = MICROS_PER_MILLI * 1000
  val MICROS_PER_MINUTE: Long = MICROS_PER_SECOND * 60
  val MICROS_PER_HOUR: Long = MICROS_PER_MINUTE * 60
  val MICROS_PER_DAY: Long = MICROS_PER_HOUR * 24
  val MICROS_PER_WEEK: Long = MICROS_PER_DAY * 7

  /**
    * A function to generate regex which matches interval string's unit part like "3 years".
    *
    * First, we can leave out some units in interval string, and we only care about the value of
    * unit, so here we use non-capturing group to wrap the actual regex.
    * At the beginning of the actual regex, we should match spaces before the unit part.
    * Next is the number part, starts with an optional "-" to represent negative value. We use
    * capturing group to wrap this part as we need the value later.
    * Finally is the unit name, ends with an optional "s".
    */
  private def unitRegex(unit: String) = "(?:\\s+(-?\\d+)\\s+" + unit + "s?)?"

  private val p = Pattern.compile("interval" + unitRegex("year") + unitRegex("month") +
    unitRegex("week") + unitRegex("day") + unitRegex("hour") + unitRegex("minute") +
    unitRegex("second") + unitRegex("millisecond") + unitRegex("microsecond"))
  private val yearMonthPattern = Pattern.compile("^(?:['|\"])?([+|-])?(\\d+)-(\\d+)(?:['|\"])?$")
  private val dayTimePattern = Pattern.compile("^(?:['|\"])?([+|-])?((\\d+) )?" +
    "(\\d+):(\\d+):(\\d+)(\\.(\\d+))?(?:['|\"])?$")
  private val quoteTrimPattern = Pattern.compile("^(?:['|\"])?(.*?)(?:['|\"])?$")

  private def toLong(s: String) = if (s == null) 0L
  else s.toLong

  /**
    * Convert a string to CalendarInterval. Return null if the input string is not a valid interval.
    * This method is case-sensitive and all characters in the input string should be in lower case.
    */
  def fromString(s: String): CalendarInterval = {
    if (s == null) return null
    val m = p.matcher(s.trim)
    if (!m.matches || s == "interval") null
    else {
      val months = toLong(m.group(1)) * 12 + toLong(m.group(2))
      var microseconds = toLong(m.group(3)) * MICROS_PER_WEEK
      microseconds += toLong(m.group(4)) * MICROS_PER_DAY
      microseconds += toLong(m.group(5)) * MICROS_PER_HOUR
      microseconds += toLong(m.group(6)) * MICROS_PER_MINUTE
      microseconds += toLong(m.group(7)) * MICROS_PER_SECOND
      microseconds += toLong(m.group(8)) * MICROS_PER_MILLI
      microseconds += toLong(m.group(9))
      new CalendarInterval(months.toInt, microseconds)
    }
  }

  /**
    * Convert a string to CalendarInterval. Unlike fromString, this method is case-insensitive and
    * will throw IllegalArgumentException when the input string is not a valid interval.
    *
    * @throws IllegalArgumentException if the string is not a valid internal.
    */
  def fromCaseInsensitiveString(s: String): CalendarInterval = {
    if (s == null || s.trim.isEmpty) {
      throw new IllegalArgumentException("Interval cannot be null or blank.")
    }
    val sInLowerCase = s.trim.toLowerCase(Locale.ROOT)
    val interval = if (sInLowerCase.startsWith("interval ")) sInLowerCase
    else "interval " + sInLowerCase
    val cal = fromString(interval)
    if (cal == null) throw new IllegalArgumentException("Invalid interval: " + s)
    cal
  }

  @throws[IllegalArgumentException]
  def toLongWithRange(fieldName: String, s: String, minValue: Long, maxValue: Long): Long = {
    var result = 0L
    if (s != null) {
      result = s.toLong
      if (result < minValue || result > maxValue) {
        throw new IllegalArgumentException(
          s"${fieldName} ${result} outside range [${minValue}, ${maxValue}]")
      }
    }
    result
  }

  /**
    * Parse YearMonth string in form: [-]YYYY-MM
    *
    * adapted from HiveIntervalYearMonth.valueOf
    */
  @throws[IllegalArgumentException]
  def fromYearMonthString(s: String): CalendarInterval = {
    var result : CalendarInterval = null
    if (s == null) {
      throw new IllegalArgumentException("Interval year-month string was null")
    }

    val m = yearMonthPattern.matcher(s.trim)
    if (!m.matches) {
      throw new IllegalArgumentException(
        "Interval string does not match year-month format of 'y-m': " + s)
    }
    else try {
      val sign = if (m.group(1) != null && m.group(1) == "-") -1
      else 1
      val years = toLongWithRange("year", m.group(2), 0, Integer.MAX_VALUE).toInt
      val months = toLongWithRange("month", m.group(3), 0, 11).toInt
      result = new CalendarInterval(sign * (years * 12 + months), 0)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          "Error parsing interval year-month string: " + e.getMessage, e)
    }
    result
  }

  /**
    * Parse dayTime string in form: [-]d HH:mm:ss.nnnnnnnnn and [-]HH:mm:ss.nnnnnnnnn
    *
    * adapted from HiveIntervalDayTime.valueOf
    */
  @throws[IllegalArgumentException]
  def fromDayTimeString(s: String): CalendarInterval = {
    var result : CalendarInterval = null
    if (s == null) throw new IllegalArgumentException("Interval day-time string was null")
    val m = dayTimePattern.matcher(s.trim)
    if (!m.matches) {
      throw new IllegalArgumentException(
        "Interval string does not match day-time format of 'd h:m:s.n': " + s)
    }
    else try {
      val sign = if (m.group(1) != null && m.group(1) == "-") -1
      else 1
      val days = if (m.group(2) == null) 0
      else toLongWithRange("day", m.group(3), 0, Integer.MAX_VALUE)
      val hours = toLongWithRange("hour", m.group(4), 0, 23)
      val minutes = toLongWithRange("minute", m.group(5), 0, 59)
      val seconds = toLongWithRange("second", m.group(6), 0, 59)
      // Hive allow nanosecond precision interval
      val nanos = toLongWithRange("nanosecond", m.group(8), 0L, 999999999L)
      result = new CalendarInterval(0, sign * (days * MICROS_PER_DAY +
        hours * MICROS_PER_HOUR + minutes * MICROS_PER_MINUTE +
        seconds * MICROS_PER_SECOND + nanos / 1000L))
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          "Error parsing interval day-time string: " + e.getMessage, e)
    }
    result
  }

  @throws[IllegalArgumentException]
  def fromSingleUnitString(unit: String, s: String): CalendarInterval = {
    var result : CalendarInterval = null
    if (s == null) {
      throw new IllegalArgumentException(String.format("Interval %s string was null", unit))
    }

    val m = quoteTrimPattern.matcher(s.trim)
    if (!m.matches) {
      throw new IllegalArgumentException(
        "Interval string does not match day-time format of 'd h:m:s.n': " + s)
    }
    else try
      unit match {
        case "year" =>
          val year = toLongWithRange("year", m.group(1),
            Integer.MIN_VALUE / 12, Integer.MAX_VALUE / 12).toInt
          result = new CalendarInterval(year * 12, 0L)
        case "month" =>
          val month = toLongWithRange("month", m.group(1),
            Integer.MIN_VALUE, Integer.MAX_VALUE).toInt
          result = new CalendarInterval(month, 0L)
        case "week" =>
          val week = toLongWithRange("week", m.group(1),
            Long.MinValue / MICROS_PER_WEEK, Long.MaxValue / MICROS_PER_WEEK)
          result = new CalendarInterval(0, week * MICROS_PER_WEEK)
        case "day" =>
          val day = toLongWithRange("day", m.group(1),
            Long.MinValue / MICROS_PER_DAY, Long.MaxValue / MICROS_PER_DAY)
          result = new CalendarInterval(0, day * MICROS_PER_DAY)
        case "hour" =>
          val hour = toLongWithRange("hour", m.group(1),
            Long.MinValue / MICROS_PER_HOUR, Long.MaxValue / MICROS_PER_HOUR)
          result = new CalendarInterval(0, hour * MICROS_PER_HOUR)
        case "minute" =>
          val minute = toLongWithRange("minute", m.group(1),
            Long.MinValue / MICROS_PER_MINUTE, Long.MaxValue / MICROS_PER_MINUTE)
          result = new CalendarInterval(0, minute * MICROS_PER_MINUTE)
        case "second" =>
          val micros = parseSecondNano(m.group(1))
          result = new CalendarInterval(0, micros)
        case "millisecond" =>
          val millisecond = toLongWithRange("millisecond", m.group(1),
            Long.MinValue / MICROS_PER_MILLI, Long.MaxValue / MICROS_PER_MILLI)
          result = new CalendarInterval(0, millisecond * MICROS_PER_MILLI)
        case "microsecond" =>
          val micros = m.group(1).toLong
          result = new CalendarInterval(0, micros)
      }
    catch {
      case e: Exception =>
        throw new IllegalArgumentException("Error parsing interval string: " + e.getMessage, e)
    }
    result
  }

  /**
    * Parse second_nano string in ss.nnnnnnnnn format to microseconds
    */
  @throws[IllegalArgumentException]
  def parseSecondNano(secondNano: String): Long = {
    val parts = secondNano.split("\\.")
    if (parts.length == 1) {
      toLongWithRange("second", parts(0), Long.MinValue/ MICROS_PER_SECOND,
        Long.MaxValue / MICROS_PER_SECOND) * MICROS_PER_SECOND
    }
    else if (parts.length == 2) {
      val seconds = if (parts(0) == "") 0L
      else {
        toLongWithRange("second", parts(0), Long.MinValue / MICROS_PER_SECOND,
          Long.MaxValue / MICROS_PER_SECOND)
      }
      val nanos = toLongWithRange("nanosecond", parts(1), 0L, 999999999L)
      seconds * MICROS_PER_SECOND + nanos / 1000L
    }
    else {
      throw new IllegalArgumentException(
        "Interval string does not match second-nano format of ss.nnnnnnnnn")
    }
  }
}

final class CalendarInterval(val months: Int, val microseconds: Long) extends Serializable {
  def milliseconds: Long = this.microseconds / CalendarInterval.MICROS_PER_MILLI

  def add(that: CalendarInterval): CalendarInterval = {
    val months = this.months + that.months
    val microseconds = this.microseconds + that.microseconds
    new CalendarInterval(months, microseconds)
  }

  def subtract(that: CalendarInterval): CalendarInterval = {
    val months = this.months - that.months
    val microseconds = this.microseconds - that.microseconds
    new CalendarInterval(months, microseconds)
  }

  def negate : CalendarInterval = new CalendarInterval(-this.months, -this.microseconds)

  override def equals(other: Any): Boolean = {
    if (this equals other) return true
    if (other == null || !other.isInstanceOf[CalendarInterval]) return false
    val o = other.asInstanceOf[CalendarInterval]
    this.months == o.months && this.microseconds == o.microseconds
  }

  override def hashCode: Int = 31 * months + microseconds.toInt

  override def toString: String = {
    val sb = new StringBuilder("interval")
    if (months != 0) {
      appendUnit(sb, months / 12, "year")
      appendUnit(sb, months % 12, "month")
    }
    if (microseconds != 0) {
      var rest = microseconds
      appendUnit(sb, rest / CalendarInterval.MICROS_PER_WEEK, "week")
      rest %= CalendarInterval.MICROS_PER_WEEK
      appendUnit(sb, rest / CalendarInterval.MICROS_PER_DAY, "day")
      rest %= CalendarInterval.MICROS_PER_DAY
      appendUnit(sb, rest / CalendarInterval.MICROS_PER_HOUR, "hour")
      rest %= CalendarInterval.MICROS_PER_HOUR
      appendUnit(sb, rest / CalendarInterval.MICROS_PER_MINUTE, "minute")
      rest %= CalendarInterval.MICROS_PER_MINUTE
      appendUnit(sb, rest / CalendarInterval.MICROS_PER_SECOND, "second")
      rest %= CalendarInterval.MICROS_PER_SECOND
      appendUnit(sb, rest / CalendarInterval.MICROS_PER_MILLI, "millisecond")
      rest %= CalendarInterval.MICROS_PER_MILLI
      appendUnit(sb, rest, "microsecond")
    }
    else if (months == 0) sb.append(" 0 microseconds")
    sb.toString
  }

  private def appendUnit(sb: StringBuilder, value: Long, unit: String): Unit = {
    if (value != 0) sb.append(' ').append(value).append(' ').append(unit).append('s')
  }
}
