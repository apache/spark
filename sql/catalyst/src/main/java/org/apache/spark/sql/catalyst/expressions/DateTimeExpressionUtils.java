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

import java.text.ParseException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.apache.spark.SparkDateTimeException;
import org.apache.spark.sql.catalyst.util.DateTimeConstants;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.IntervalUtils;
import org.apache.spark.sql.catalyst.util.TimestampFormatter;
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

  /**
   * Builds the microsecond count for
   * {@code MakeTimestamp(year, month, day, hour, min, secAndMicros[, timezone])}.
   * {@code secAndMicros} carries the whole seconds plus the microsecond fraction
   * (scale 6); a value of {@code 60} seconds with no fraction is accepted for
   * PostgreSQL compatibility and rolls over to the next minute. When
   * {@code timestampNTZ} is {@code true} the result is the local-time micros
   * (no zone applied); otherwise {@code zoneId} is used to resolve the instant.
   *
   * <p>This is the shared, exception-raising core used by both the eval and
   * codegen paths. It throws {@link SparkDateTimeException} for an invalid
   * fraction-of-second and {@link DateTimeException} for an invalid
   * year/month/day/hour/min combination; callers decide how to translate those.
   */
  public static long makeTimestampMicros(
      int year, int month, int day, int hour, int min,
      Decimal secAndMicros, ZoneId zoneId, boolean timestampNTZ) {
    assert secAndMicros.scale() == 6 :
      "Seconds fraction must have 6 digits for microseconds but got " + secAndMicros.scale();
    // 8 digits cannot overflow Int.
    int totalMicros = (int) secAndMicros.toUnscaledLong();
    int microsPerSecond = (int) DateTimeConstants.MICROS_PER_SECOND;
    int nanosPerMicros = (int) DateTimeConstants.NANOS_PER_MICROS;
    int seconds = Math.floorDiv(totalMicros, microsPerSecond);
    int nanos = Math.floorMod(totalMicros, microsPerSecond) * nanosPerMicros;
    LocalDateTime ldt;
    if (seconds == 60) {
      if (nanos == 0) {
        // This case of sec = 60 and nanos = 0 is supported for compatibility with PostgreSQL.
        ldt = LocalDateTime.of(year, month, day, hour, min, 0, 0).plusMinutes(1);
      } else {
        throw QueryExecutionErrors.invalidFractionOfSecondError(secAndMicros.toDouble());
      }
    } else {
      ldt = LocalDateTime.of(year, month, day, hour, min, seconds, nanos);
    }
    if (timestampNTZ) {
      return DateTimeUtils.localDateTimeToMicros(ldt);
    } else {
      return DateTimeUtils.instantToMicros(ldt.atZone(zoneId).toInstant());
    }
  }

  /**
   * ANSI ({@code failOnError = true}) variant of {@link #makeTimestampMicros}: a
   * {@link SparkDateTimeException} (e.g. an invalid fraction of second) is
   * rethrown as-is to preserve its message, while any other
   * {@link DateTimeException} is translated to {@code ansiDateTimeArgumentOutOfRange}.
   * {@code SparkDateTimeException} is caught first because it is itself a
   * {@link DateTimeException}.
   */
  public static long makeTimestampExact(
      int year, int month, int day, int hour, int min,
      Decimal secAndMicros, ZoneId zoneId, boolean timestampNTZ) {
    try {
      return makeTimestampMicros(year, month, day, hour, min, secAndMicros, zoneId, timestampNTZ);
    } catch (SparkDateTimeException e) {
      throw e;
    } catch (DateTimeException e) {
      throw QueryExecutionErrors.ansiDateTimeArgumentOutOfRange(e);
    }
  }

  /**
   * Parses {@code input} to a timestamp for {@code ToTimestamp} expressions
   * (e.g. {@code to_timestamp}) in ANSI mode ({@code failOnError = true}).
   * For a TIMESTAMP_NTZ result the formatter's {@code parseWithoutTimeZone} is
   * used and {@code downScaleFactor} is not applied; otherwise the parsed micros
   * are divided by {@code downScaleFactor}. A {@link DateTimeException} (which
   * also covers {@code DateTimeParseException}) or a {@link ParseException} is
   * translated to {@code ansiDateTimeParseError} carrying the suggested
   * fall-back function; any other exception (e.g. {@code IllegalStateException})
   * propagates unchanged.
   */
  public static long parseToTimestampExact(
      TimestampFormatter formatter,
      String input,
      long downScaleFactor,
      boolean forTimestampNTZ,
      String suggestedFuncOnFail) {
    try {
      if (forTimestampNTZ) {
        return formatter.parseWithoutTimeZone(input);
      } else {
        return formatter.parse(input) / downScaleFactor;
      }
    } catch (DateTimeException | ParseException e) {
      throw QueryExecutionErrors.ansiDateTimeParseError(e, suggestedFuncOnFail);
    }
  }
}
