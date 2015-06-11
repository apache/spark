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

import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class DateUtils {
  public static final int MILLIS_PER_DAY = 86400000;

  public static final long HUNDRED_NANOS_PER_SECOND = 10000000L;

  // Java TimeZone has no mention of thread safety. Use thread local instance to be safe.
  private static ThreadLocal<TimeZone> LOCAL_TIMEZONE = new ThreadLocal<TimeZone>() {
    @Override
    protected TimeZone initialValue() {
      return Calendar.getInstance().getTimeZone();
    }
  };

  // `SimpleDateFormat` is not thread-safe.
  private static ThreadLocal<DateFormat> DATE_FORMAT = new ThreadLocal<DateFormat>() {
    @Override
    protected DateFormat initialValue() {
      return new SimpleDateFormat("yyyy-MM-dd");
    }
  };

  private static int javaDateToDays(Date d) {
    return millisToDays(d.getTime());
  }

  // we should use the exact day as Int, for example, (year, month, day) -> day
  public static int millisToDays(long millisLocal) {
    return (int) ((millisLocal + LOCAL_TIMEZONE.get().getOffset(millisLocal)) / MILLIS_PER_DAY);
  }

  public static long toMillisSinceEpoch(int days) {
    long millisUtc = ((long) days) * MILLIS_PER_DAY;
    return millisUtc - LOCAL_TIMEZONE.get().getOffset(millisUtc);
  }

  public static int fromJavaDate(Date date) {
    return javaDateToDays(date);
  }

  public static Date toJavaDate(int daysSinceEpoch) {
    return new Date(toMillisSinceEpoch(daysSinceEpoch));
  }

  public static String toString(int days) {
    return DATE_FORMAT.get().format(toJavaDate(days));
  }

  public static java.util.Date stringToTime(String s) throws ParseException {
    if (!s.contains("T")) {
      // JDBC escape string
      return s.contains(" ") ? Timestamp.valueOf(s) : Date.valueOf(s);
    } else if (s.endsWith("Z")) {
      // this is zero timezone of ISO8601
      return stringToTime(s.substring(0, s.length() - 1) + "GMT-00:00");
    } else if (!s.contains("GMT")) {
      // timezone with ISO8601
      int inset = "+00.00".length();
      String s0 = s.substring(0, s.length() - inset);
      String s1 = s.substring(s.length() - inset, s.length());
      if (s0.substring(s0.lastIndexOf(':')).contains(".")) {
        return stringToTime(s0 + "GMT" + s1);
      } else {
        return stringToTime(s0 + ".0GMT" + s1);
      }
    } else {
      // ISO8601 with GMT insert
      SimpleDateFormat ISO8601GMT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz");
      return ISO8601GMT.parse(s);
    }
  }

  /**
   * Return a java.sql.Timestamp from number of 100ns since epoch
   */
  public static Timestamp toJavaTimestamp(long num100ns) {
    // setNanos() will overwrite the millisecond part, so the milliseconds should be
    // cut off at seconds
    long seconds = num100ns / HUNDRED_NANOS_PER_SECOND;
    long nanos = num100ns % HUNDRED_NANOS_PER_SECOND;
    // setNanos() can not accept negative value
    if (nanos < 0) {
      nanos += HUNDRED_NANOS_PER_SECOND;
      seconds -= 1;
    }
    Timestamp t = new Timestamp(seconds * 1000);
    t.setNanos(((int) nanos) * 100);
    return t;
  }

  /**
   * Return the number of 100ns since epoch from java.sql.Timestamp.
   */
  public static long fromJavaTimestamp(Timestamp t) {
    if (t != null) {
      return t.getTime() * 10000L + (((long) t.getNanos()) / 100) % 10000L;
    } else {
      return 0L;
    }
  }
}
