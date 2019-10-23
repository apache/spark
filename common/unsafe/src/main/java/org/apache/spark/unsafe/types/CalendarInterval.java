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

package org.apache.spark.unsafe.types;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The internal representation of interval type.
 */
public final class CalendarInterval implements Serializable {
  public static final long MICROS_PER_MILLI = 1000L;
  public static final long MICROS_PER_SECOND = MICROS_PER_MILLI * 1000;
  public static final long MICROS_PER_MINUTE = MICROS_PER_SECOND * 60;
  public static final long MICROS_PER_HOUR = MICROS_PER_MINUTE * 60;
  public static final long MICROS_PER_DAY = MICROS_PER_HOUR * 24;
  public static final long MICROS_PER_WEEK = MICROS_PER_DAY * 7;

  private static Pattern yearMonthPattern = Pattern.compile(
    "^([+|-])?(\\d+)-(\\d+)$");

  private static Pattern dayTimePattern = Pattern.compile(
    "^([+|-])?((\\d+) )?((\\d+):)?(\\d+):(\\d+)(\\.(\\d+))?$");

  public static long toLongWithRange(String fieldName,
      String s, long minValue, long maxValue) throws IllegalArgumentException {
    long result = 0;
    if (s != null) {
      result = Long.parseLong(s);
      if (result < minValue || result > maxValue) {
        throw new IllegalArgumentException(String.format("%s %d outside range [%d, %d]",
          fieldName, result, minValue, maxValue));
      }
    }
    return result;
  }

  /**
   * Parse YearMonth string in form: [-]YYYY-MM
   *
   * adapted from HiveIntervalYearMonth.valueOf
   */
  public static CalendarInterval fromYearMonthString(String s) throws IllegalArgumentException {
    CalendarInterval result = null;
    if (s == null) {
      throw new IllegalArgumentException("Interval year-month string was null");
    }
    s = s.trim();
    Matcher m = yearMonthPattern.matcher(s);
    if (!m.matches()) {
      throw new IllegalArgumentException(
        "Interval string does not match year-month format of 'y-m': " + s);
    } else {
      try {
        int sign = m.group(1) != null && m.group(1).equals("-") ? -1 : 1;
        int years = (int) toLongWithRange("year", m.group(2), 0, Integer.MAX_VALUE);
        int months = (int) toLongWithRange("month", m.group(3), 0, 11);
        result = new CalendarInterval(sign * (years * 12 + months), 0);
      } catch (Exception e) {
        throw new IllegalArgumentException(
          "Error parsing interval year-month string: " + e.getMessage(), e);
      }
    }
    return result;
  }

  /**
   * Parse dayTime string in form: [-]d HH:mm:ss.nnnnnnnnn and [-]HH:mm:ss.nnnnnnnnn
   *
   * adapted from HiveIntervalDayTime.valueOf
   */
  public static CalendarInterval fromDayTimeString(String s) throws IllegalArgumentException {
    return fromDayTimeString(s, "day", "second");
  }

  /**
   * Parse dayTime string in form: [-]d HH:mm:ss.nnnnnnnnn and [-]HH:mm:ss.nnnnnnnnn
   *
   * adapted from HiveIntervalDayTime.valueOf.
   * Below interval conversion patterns are supported:
   * - DAY TO (HOUR|MINUTE|SECOND)
   * - HOUR TO (MINUTE|SECOND)
   * - MINUTE TO SECOND
   */
  public static CalendarInterval fromDayTimeString(String s, String from, String to)
      throws IllegalArgumentException {
    CalendarInterval result = null;
    if (s == null) {
      throw new IllegalArgumentException("Interval day-time string was null");
    }
    s = s.trim();
    Matcher m = dayTimePattern.matcher(s);
    if (!m.matches()) {
      throw new IllegalArgumentException(
        "Interval string does not match day-time format of 'd h:m:s.n': " + s);
    } else {
      try {
        int sign = m.group(1) != null && m.group(1).equals("-") ? -1 : 1;
        long days = m.group(2) == null ? 0 : toLongWithRange("day", m.group(3),
          0, Integer.MAX_VALUE);
        long hours = 0;
        long minutes;
        long seconds = 0;
        if (m.group(5) != null || from.equals("minute")) { // 'HH:mm:ss' or 'mm:ss minute'
          hours = toLongWithRange("hour", m.group(5), 0, 23);
          minutes = toLongWithRange("minute", m.group(6), 0, 59);
          seconds = toLongWithRange("second", m.group(7), 0, 59);
        } else if (m.group(8) != null){ // 'mm:ss.nn'
          minutes = toLongWithRange("minute", m.group(6), 0, 59);
          seconds = toLongWithRange("second", m.group(7), 0, 59);
        } else { // 'HH:mm'
          hours = toLongWithRange("hour", m.group(6), 0, 23);
          minutes = toLongWithRange("second", m.group(7), 0, 59);
        }
        // Hive allow nanosecond precision interval
        String nanoStr = m.group(9) == null ? null : (m.group(9) + "000000000").substring(0, 9);
        long nanos = toLongWithRange("nanosecond", nanoStr, 0L, 999999999L);
        switch (to) {
          case "hour":
            minutes = 0;
            seconds = 0;
            nanos = 0;
            break;
          case "minute":
            seconds = 0;
            nanos = 0;
            break;
          case "second":
            // No-op
            break;
          default:
            throw new IllegalArgumentException(
              String.format("Cannot support (interval '%s' %s to %s) expression", s, from, to));
        }
        result = new CalendarInterval(0, sign * (
          days * MICROS_PER_DAY + hours * MICROS_PER_HOUR + minutes * MICROS_PER_MINUTE +
          seconds * MICROS_PER_SECOND + nanos / 1000L));
      } catch (Exception e) {
        throw new IllegalArgumentException(
          "Error parsing interval day-time string: " + e.getMessage(), e);
      }
    }
    return result;
  }

  public static CalendarInterval fromUnitStrings(String[] units, String[] values)
      throws IllegalArgumentException {
    assert units.length == values.length;
    int months = 0;
    long microseconds = 0;

    for (int i = 0; i < units.length; i++) {
      try {
        switch (units[i]) {
          case "year":
            months = Math.addExact(months, Math.multiplyExact(Integer.parseInt(values[i]), 12));
            break;
          case "month":
            months = Math.addExact(months, Integer.parseInt(values[i]));
            break;
          case "week":
            microseconds = Math.addExact(
              microseconds,
              Math.multiplyExact(Long.parseLong(values[i]), MICROS_PER_WEEK));
            break;
          case "day":
            microseconds = Math.addExact(
              microseconds,
              Math.multiplyExact(Long.parseLong(values[i]), MICROS_PER_DAY));
            break;
          case "hour":
            microseconds = Math.addExact(
              microseconds,
              Math.multiplyExact(Long.parseLong(values[i]), MICROS_PER_HOUR));
            break;
          case "minute":
            microseconds = Math.addExact(
              microseconds,
              Math.multiplyExact(Long.parseLong(values[i]), MICROS_PER_MINUTE));
            break;
          case "second": {
            microseconds = Math.addExact(microseconds, parseSecondNano(values[i]));
            break;
          }
          case "millisecond":
            microseconds = Math.addExact(
              microseconds,
              Math.multiplyExact(Long.parseLong(values[i]), MICROS_PER_MILLI));
            break;
          case "microsecond":
            microseconds = Math.addExact(microseconds, Long.parseLong(values[i]));
            break;
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Error parsing interval string: " + e.getMessage(), e);
      }
    }
    return new CalendarInterval(months, microseconds);
  }

  /**
   * Parse second_nano string in ss.nnnnnnnnn format to microseconds
   */
  public static long parseSecondNano(String secondNano) throws IllegalArgumentException {
    String[] parts = secondNano.split("\\.");
    if (parts.length == 1) {
      return toLongWithRange("second", parts[0], Long.MIN_VALUE / MICROS_PER_SECOND,
        Long.MAX_VALUE / MICROS_PER_SECOND) * MICROS_PER_SECOND;

    } else if (parts.length == 2) {
      long seconds = parts[0].equals("") ? 0L : toLongWithRange("second", parts[0],
        Long.MIN_VALUE / MICROS_PER_SECOND, Long.MAX_VALUE / MICROS_PER_SECOND);
      long nanos = toLongWithRange("nanosecond", parts[1], 0L, 999999999L);
      return seconds * MICROS_PER_SECOND + nanos / 1000L;

    } else {
      throw new IllegalArgumentException(
        "Interval string does not match second-nano format of ss.nnnnnnnnn");
    }
  }

  public final int months;
  public final long microseconds;

  public long milliseconds() {
    return this.microseconds / MICROS_PER_MILLI;
  }

  public CalendarInterval(int months, long microseconds) {
    this.months = months;
    this.microseconds = microseconds;
  }

  public CalendarInterval add(CalendarInterval that) {
    int months = this.months + that.months;
    long microseconds = this.microseconds + that.microseconds;
    return new CalendarInterval(months, microseconds);
  }

  public CalendarInterval subtract(CalendarInterval that) {
    int months = this.months - that.months;
    long microseconds = this.microseconds - that.microseconds;
    return new CalendarInterval(months, microseconds);
  }

  public CalendarInterval negate() {
    return new CalendarInterval(-this.months, -this.microseconds);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || !(other instanceof CalendarInterval)) return false;

    CalendarInterval o = (CalendarInterval) other;
    return this.months == o.months && this.microseconds == o.microseconds;
  }

  @Override
  public int hashCode() {
    return 31 * months + (int) microseconds;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("interval");

    if (months != 0) {
      appendUnit(sb, months / 12, "year");
      appendUnit(sb, months % 12, "month");
    }

    if (microseconds != 0) {
      long rest = microseconds;
      appendUnit(sb, rest / MICROS_PER_WEEK, "week");
      rest %= MICROS_PER_WEEK;
      appendUnit(sb, rest / MICROS_PER_DAY, "day");
      rest %= MICROS_PER_DAY;
      appendUnit(sb, rest / MICROS_PER_HOUR, "hour");
      rest %= MICROS_PER_HOUR;
      appendUnit(sb, rest / MICROS_PER_MINUTE, "minute");
      rest %= MICROS_PER_MINUTE;
      appendUnit(sb, rest / MICROS_PER_SECOND, "second");
      rest %= MICROS_PER_SECOND;
      appendUnit(sb, rest / MICROS_PER_MILLI, "millisecond");
      rest %= MICROS_PER_MILLI;
      appendUnit(sb, rest, "microsecond");
    } else if (months == 0) {
      sb.append(" 0 microseconds");
    }

    return sb.toString();
  }

  private void appendUnit(StringBuilder sb, long value, String unit) {
    if (value != 0) {
      sb.append(' ').append(value).append(' ').append(unit).append('s');
    }
  }
}
