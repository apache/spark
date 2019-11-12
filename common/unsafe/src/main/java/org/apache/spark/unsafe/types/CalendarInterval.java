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

import static org.apache.spark.sql.catalyst.util.DateTimeConstants.*;

/**
 * The internal representation of interval type.
 */
public final class CalendarInterval implements Serializable, Comparable<CalendarInterval> {
  public final int months;
  public final int days;
  public final long microseconds;

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
  public int compareTo(CalendarInterval that) {
    long thisAdjustDays =
      this.microseconds / MICROS_PER_DAY + this.days + this.months * DAYS_PER_MONTH;
    long thatAdjustDays =
      that.microseconds / MICROS_PER_DAY + that.days + that.months * DAYS_PER_MONTH;
    long daysDiff = thisAdjustDays - thatAdjustDays;
    if (daysDiff == 0) {
      long msDiff = (this.microseconds % MICROS_PER_DAY) - (that.microseconds % MICROS_PER_DAY);
      if (msDiff == 0) {
        return 0;
      } else if (msDiff > 0) {
        return 1;
      } else {
        return -1;
      }
    } else if (daysDiff > 0){
      return 1;
    } else {
      return -1;
    }
  }

  @Override
  public String toString() {
    return "CalendarInterval(months= " + months + ", days = " + days + ", microsecond = " + microseconds + ")";
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
}
