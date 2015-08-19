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

/**
 * The internal representation of interval type.
 */
public final class CalendarInterval extends Interval {
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
      appendYearMonth(sb, months);
    }
    if (microseconds != 0) {
      appendDayTime(sb, microseconds);
    }
    return sb.toString();
  }
}
