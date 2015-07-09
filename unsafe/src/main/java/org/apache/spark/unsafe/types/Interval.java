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

/**
 * The internal representation of interval type.
 */
public final class Interval implements Serializable {
  public static final long MICROS_PER_MILLI = 1000L;
  public static final long MICROS_PER_SECOND = MICROS_PER_MILLI * 1000;
  public static final long MICROS_PER_MINUTE = MICROS_PER_SECOND * 60;
  public static final long MICROS_PER_HOUR = MICROS_PER_MINUTE * 60;
  public static final long MICROS_PER_DAY = MICROS_PER_HOUR * 24;
  public static final long MICROS_PER_WEEK = MICROS_PER_DAY * 7;

  public final int months;
  public final long microseconds;

  public Interval(int months, long microseconds) {
    this.months = months;
    this.microseconds = microseconds;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || !(other instanceof Interval)) return false;

    Interval o = (Interval) other;
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
