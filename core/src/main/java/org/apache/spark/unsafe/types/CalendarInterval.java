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
  private static String unitRegex(String unit) {
    return "(?:\\s+(-?\\d+)\\s+" + unit + "s?)?";
  }

  private static Pattern p = Pattern.compile("interval" + unitRegex("year") + unitRegex("month") +
    unitRegex("week") + unitRegex("day") + unitRegex("hour") + unitRegex("minute") +
    unitRegex("second") + unitRegex("millisecond") + unitRegex("microsecond"));

  private static Pattern yearMonthPattern =
    Pattern.compile("^(?:['|\"])?([+|-])?(\\d+)-(\\d+)(?:['|\"])?$");

  private static Pattern dayTimePattern =
    Pattern.compile("^(?:['|\"])?([+|-])?(\\d+) (\\d+):(\\d+):(\\d+)(\\.(\\d+))?(?:['|\"])?$");

  private static Pattern quoteTrimPattern = Pattern.compile("^(?:['|\"])?(.*?)(?:['|\"])?$");

  private static long toLong(String s) {
    if (s == null) {
      return 0;
    } else {
      return Long.valueOf(s);
    }
  }

  public static CalendarInterval fromString(String s) {
    if (s == null) {
      return null;
    }
    s = s.trim();
    Matcher m = p.matcher(s);
    if (!m.matches() || s.equals("interval")) {
      return null;
    } else {
      long months = toLong(m.group(1)) * 12 + toLong(m.group(2));
      long microseconds = toLong(m.group(3)) * MICROS_PER_WEEK;
      microseconds += toLong(m.group(4)) * MICROS_PER_DAY;
      microseconds += toLong(m.group(5)) * MICROS_PER_HOUR;
      microseconds += toLong(m.group(6)) * MICROS_PER_MINUTE;
      microseconds += toLong(m.group(7)) * MICROS_PER_SECOND;
      microseconds += toLong(m.group(8)) * MICROS_PER_MILLI;
      microseconds += toLong(m.group(9));
      return new CalendarInterval((int) months, microseconds);
    }
  }

  public static long toLongWithRange(String fieldName,
      String s, long minValue, long maxValue) throws IllegalArgumentException {
    long result = 0;
    if (s != null) {
      result = Long.valueOf(s);
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
   * Parse dayTime string in form: [-]d HH:mm:ss.nnnnnnnnn
   *
   * adapted from HiveIntervalDayTime.valueOf
   */
  public static CalendarInterval fromDayTimeString(String s) throws IllegalArgumentException {
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
        long days = toLongWithRange("day", m.group(2), 0, Integer.MAX_VALUE);
        long hours = toLongWithRange("hour", m.group(3), 0, 23);
        long minutes = toLongWithRange("minute", m.group(4), 0, 59);
        long seconds = toLongWithRange("second", m.group(5), 0, 59);
        // Hive allow nanosecond precision interval
        long nanos = toLongWithRange("nanosecond", m.group(7), 0L, 999999999L);
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

  public static CalendarInterval fromSingleUnitString(String unit, String s)
      throws IllegalArgumentException {

    CalendarInterval result = null;
    if (s == null) {
      throw new IllegalArgumentException(String.format("Interval %s string was null", unit));
    }
    s = s.trim();
    Matcher m = quoteTrimPattern.matcher(s);
    if (!m.matches()) {
      throw new IllegalArgumentException(
        "Interval string does not match day-time format of 'd h:m:s.n': " + s);
    } else {
      try {
        if (unit.equals("year")) {
          int year = (int) toLongWithRange("year", m.group(1),
            Integer.MIN_VALUE / 12, Integer.MAX_VALUE / 12);
          result = new CalendarInterval(year * 12, 0L);

        } else if (unit.equals("month")) {
          int month = (int) toLongWithRange("month", m.group(1),
            Integer.MIN_VALUE, Integer.MAX_VALUE);
          result = new CalendarInterval(month, 0L);

        } else if (unit.equals("day")) {
          long day = toLongWithRange("day", m.group(1),
            Long.MIN_VALUE / MICROS_PER_DAY, Long.MAX_VALUE / MICROS_PER_DAY);
          result = new CalendarInterval(0, day * MICROS_PER_DAY);

        } else if (unit.equals("hour")) {
          long hour = toLongWithRange("hour", m.group(1),
            Long.MIN_VALUE / MICROS_PER_HOUR, Long.MAX_VALUE / MICROS_PER_HOUR);
          result = new CalendarInterval(0, hour * MICROS_PER_HOUR);

        } else if (unit.equals("minute")) {
          long minute = toLongWithRange("minute", m.group(1),
            Long.MIN_VALUE / MICROS_PER_MINUTE, Long.MAX_VALUE / MICROS_PER_MINUTE);
          result = new CalendarInterval(0, minute * MICROS_PER_MINUTE);

        } else if (unit.equals("second")) {
          long micros = parseSecondNano(m.group(1));
          result = new CalendarInterval(0, micros);
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Error parsing interval string: " + e.getMessage(), e);
      }
    }
    return result;
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
    }

    return sb.toString();
  }

  private void appendUnit(StringBuilder sb, long value, String unit) {
    if (value != 0) {
      sb.append(" " + value + " " + unit + "s");
    }
  }
}
