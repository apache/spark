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
import java.time.Duration;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

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

  public final int months;
  public final int days;
  public final long microseconds;

  public long milliseconds() {
    return this.microseconds / MICROS_PER_MILLI;
  }

  public CalendarInterval(int months, int days, long microseconds) {
    this.months = months;
    this.days = days;
    this.microseconds = microseconds;
  }

  public CalendarInterval add(CalendarInterval that) {
    int months = this.months + that.months;
    int days = this.days + that.days;
    long microseconds = this.microseconds + that.microseconds;
    return new CalendarInterval(months, days, microseconds);
  }

  public CalendarInterval subtract(CalendarInterval that) {
    int months = this.months - that.months;
    int days = this.days - that.days;
    long microseconds = this.microseconds - that.microseconds;
    return new CalendarInterval(months, days, microseconds);
  }

  public CalendarInterval negate() {
    return new CalendarInterval(-this.months, -this.days, -this.microseconds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CalendarInterval that = (CalendarInterval) o;
    return months == that.months &&
      days == that.days &&
      microseconds == that.microseconds;
  }

  @Override
  public int hashCode() {
    return Objects.hash(months, days, microseconds);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("interval");

    if (months != 0) {
      appendUnit(sb, months / 12, "year");
      appendUnit(sb, months % 12, "month");
    }

    if (days != 0) {
      appendUnit(sb, days / 7, "week");
      appendUnit(sb, days % 7, "day");
    }

    if (microseconds != 0) {
      long rest = microseconds;
      appendUnit(sb, rest / MICROS_PER_HOUR, "hour");
      rest %= MICROS_PER_HOUR;
      appendUnit(sb, rest / MICROS_PER_MINUTE, "minute");
      rest %= MICROS_PER_MINUTE;
      appendUnit(sb, rest / MICROS_PER_SECOND, "second");
      rest %= MICROS_PER_SECOND;
      appendUnit(sb, rest / MICROS_PER_MILLI, "millisecond");
      rest %= MICROS_PER_MILLI;
      appendUnit(sb, rest, "microsecond");
    } else if (months == 0 && days == 0) {
      sb.append(" 0 microseconds");
    }

    return sb.toString();
  }

  private void appendUnit(StringBuilder sb, long value, String unit) {
    if (value != 0) {
      sb.append(' ').append(value).append(' ').append(unit).append('s');
    }
  }

  /**
   * Extracts the date part of the interval.
   * @return an instance of {@code java.time.Period} based on the months and days fields
   *         of the given interval, not null.
   */
  public Period period() { return Period.of(0, months, days); }

  /**
   * Extracts the time part of the interval.
   * @return an instance of {@code java.time.Duration} based on the microseconds field
   *         of the given interval, not null.
   * @throws ArithmeticException if a numeric overflow occurs
   */
  public Duration duration() { return Duration.of(microseconds, ChronoUnit.MICROS); }
}
