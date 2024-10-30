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

import org.apache.spark.annotation.Unstable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import static org.apache.spark.sql.catalyst.util.DateTimeConstants.*;

/**
 * The class representing calendar intervals. The calendar interval is stored internally in
 * three components:
 * <ul>
 *   <li>an integer value representing the number of `months` in this interval,</li>
 *   <li>an integer value representing the number of `days` in this interval,</li>
 *   <li>a long value representing the number of `microseconds` in this interval.</li>
 * </ul>
 *
 * The `months` and `days` are not units of time with a constant length (unlike hours, seconds), so
 * they are two separated fields from microseconds. One month may be equal to 28, 29, 30 or 31 days
 * and one day may be equal to 23, 24 or 25 hours (daylight saving).
 *
 * @since 3.0.0
 */
@Unstable
public final class CalendarInterval implements Serializable, Comparable<CalendarInterval> {
  // NOTE: If you're moving or renaming this file, you should also update Unidoc configuration
  // specified in 'SparkBuild.scala'.
  public final int months;
  public final int days;
  public final long microseconds;

  // CalendarInterval is represented by months, days and microseconds. Months and days are not
  // units of time with a constant length (unlike hours, seconds), so they are two separated fields
  // from microseconds. One month may be equal to 29, 30 or 31 days and one day may be equal to
  // 23, 24 or 25 hours (daylight saving)
  public CalendarInterval(int months, int days, long microseconds) {
    this.months = months;
    this.days = days;
    this.microseconds = microseconds;
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
    if (months == 0 && days == 0 && microseconds == 0) {
      return "0 seconds";
    }

    StringBuilder sb = new StringBuilder();

    if (months != 0) {
      appendUnit(sb, months / 12, "years");
      appendUnit(sb, months % 12, "months");
    }

    appendUnit(sb, days, "days");

    if (microseconds != 0) {
      long rest = microseconds;
      appendUnit(sb, rest / MICROS_PER_HOUR, "hours");
      rest %= MICROS_PER_HOUR;
      appendUnit(sb, rest / MICROS_PER_MINUTE, "minutes");
      rest %= MICROS_PER_MINUTE;
      if (rest != 0) {
        String s = BigDecimal.valueOf(rest, 6).stripTrailingZeros().toPlainString();
        sb.append(s).append(" seconds ");
      }
    }

    sb.setLength(sb.length() - 1);
    return sb.toString();
  }

  private void appendUnit(StringBuilder sb, long value, String unit) {
    if (value != 0) {
      sb.append(value).append(' ').append(unit).append(' ');
    }
  }

  /**
   * Extracts the date part of the interval.
   * @return an instance of {@code java.time.Period} based on the months and days fields
   *         of the given interval, not null.
   */
  public Period extractAsPeriod() { return Period.of(0, months, days); }

  /**
   * Extracts the time part of the interval.
   * @return an instance of {@code java.time.Duration} based on the microseconds field
   *         of the given interval, not null.
   * @throws ArithmeticException if a numeric overflow occurs
   */
  public Duration extractAsDuration() { return Duration.of(microseconds, ChronoUnit.MICROS); }

  /**
   * This method is not used to order CalendarInterval instances, as they are not orderable and
   * cannot be used in a ORDER BY statement.
   * Instead, it is used to find identical interval instances for aggregation purposes.
   * It compares the 'months', 'days', and 'microseconds' fields of this CalendarInterval
   * with another instance. The comparison is done first on the 'months', then on the 'days',
   * and finally on the 'microseconds'.
   *
   * @param o The CalendarInterval instance to compare with.
   * @return Zero if this object is equal to the specified object, and non-zero otherwise
   */
  @Override
  public int compareTo(CalendarInterval o) {
    if (this.months != o.months) {
      return Integer.compare(this.months, o.months);
    } else if (this.days != o.days) {
      return Integer.compare(this.days, o.days);
    } else {
      return Long.compare(this.microseconds, o.microseconds);
    }
  }
}
