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

package org.apache.spark.sql.catalyst.expressions;

import java.time.DateTimeException;
import java.time.LocalDate;

import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.IntervalUtils;
import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;

/**
 * Static helpers shared by date/time/interval expression {@code doGenCode}
 * paths (and their corresponding eval paths). The methods here wrap the
 * {@code DateTimeUtils} / {@code IntervalUtils} routines whose checked
 * exceptions need to be translated into the user-facing ANSI errors.
 */
public final class DateTimeExpressionUtils {

  private DateTimeExpressionUtils() {}

  /**
   * Builds a day count for {@code MakeDate(year, month, day)} in ANSI mode.
   * Only the {@link DateTimeException} thrown by
   * {@link LocalDate#of(int, int, int)} for invalid year/month/day is caught
   * and converted to {@code ansiDateTimeArgumentOutOfRange}. Any other
   * exception thrown by {@code DateTimeUtils.localDateToDays} (e.g. a
   * day-count overflow from its internal {@code toIntExact}) propagates to
   * the caller unchanged.
   */
  public static int makeDateExact(int year, int month, int day) {
    try {
      return DateTimeUtils.localDateToDays(LocalDate.of(year, month, day));
    } catch (DateTimeException e) {
      throw QueryExecutionErrors.ansiDateTimeArgumentOutOfRange(e);
    }
  }

  /**
   * Builds a {@link CalendarInterval} for
   * {@code MakeInterval(years, months, weeks, days, hours, mins, secs)} in
   * ANSI mode. Throws a {@code SparkArithmeticException} if any of the
   * intermediate {@code Math.*Exact} calls overflows.
   */
  public static CalendarInterval makeIntervalExact(
      int years, int months, int weeks, int days,
      int hours, int mins, Decimal secs) {
    try {
      return IntervalUtils.makeInterval(years, months, weeks, days, hours, mins, secs);
    } catch (ArithmeticException e) {
      throw QueryExecutionErrors.arithmeticOverflowError(e.getMessage(), "", null);
    }
  }
}
