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

package org.apache.spark.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkException;

// Replicating code from SparkIntervalUtils so code in the 'common' space can work with
// year-month intervals.
public class DayTimeIntervalUtils {
  private static final byte DAY = 0;
  private static final byte HOUR = 1;
  private static final byte MINUTE = 2;
  private static final byte SECOND = 3;
  private static final long HOURS_PER_DAY = 24;
  private static final long MINUTES_PER_HOUR = 60;
  private static final long SECONDS_PER_MINUTE = 60;
  private static final long MILLIS_PER_SECOND = 1000;
  private static final long MICROS_PER_MILLIS = 1000;
  private static final long MICROS_PER_SECOND = MICROS_PER_MILLIS * MILLIS_PER_SECOND;
  private static final long MICROS_PER_MINUTE = SECONDS_PER_MINUTE * MICROS_PER_SECOND;
  private static final long MICROS_PER_HOUR = MINUTES_PER_HOUR * MICROS_PER_MINUTE;
  private static final long MICROS_PER_DAY = HOURS_PER_DAY * MICROS_PER_HOUR;
  private static final long MAX_HOUR = Long.MAX_VALUE / MICROS_PER_HOUR;
  private static final long MAX_MINUTE = Long.MAX_VALUE / MICROS_PER_MINUTE;
  private static final long MAX_SECOND = Long.MAX_VALUE / MICROS_PER_SECOND;

  public static String fieldToString(byte field) throws SparkException {
    if (field == DAY) {
      return "DAY";
    } else if (field == HOUR) {
      return "HOUR";
    } else if (field == MINUTE) {
      return "MINUTE";
    } else if (field == SECOND) {
      return "SECOND";
    } else {
      throw new SparkException("Invalid field in day-time interval: " + field +
              ". Supported fields are: DAY, HOUR, MINUTE, SECOND");
    }
  }

  // Used to convert microseconds representing a day-time interval with given start and end fields
  // to its ANSI SQL string representation. Throws a SparkException if startField or endField are
  // out of bounds.
  public static String toDayTimeIntervalANSIString(long micros, byte startField, byte endField)
          throws SparkException {
    String sign = "";
    long rest = micros;
    String from = fieldToString(startField).toUpperCase();
    String to = fieldToString(endField).toUpperCase();
    String prefix = "INTERVAL '";
    String postfix = startField == endField ? "' " + from : "' " + from + " TO " + to;
    if (micros < 0) {
      if (micros == Long.MIN_VALUE) {
        // Especial handling of minimum `Long` value because negate op overflows `Long`.
        // seconds = 106751991 * (24 * 60 * 60) + 4 * 60 * 60 + 54 = 9223372036854
        // microseconds = -9223372036854000000L-775808 == Long.MinValue
        String baseStr = "-106751991 04:00:54.775808000";
        long MAX_DAY = Long.MAX_VALUE / MICROS_PER_DAY;
        String firstStr = "-" + (startField == DAY ? Long.toString(MAX_DAY) :
                (startField == HOUR ? Long.toString(MAX_HOUR) :
                        (startField == MINUTE ? Long.toString(MAX_MINUTE) :
                                MAX_SECOND + ".775808")));
        if (startField == endField) {
          return prefix + firstStr + postfix;
        } else {
          int substrStart = startField == DAY ? 10 : (startField == HOUR ? 13 : 16);
          int substrEnd = endField == HOUR ? 13 : (endField == MINUTE ? 16 : 26);
          return prefix + firstStr + baseStr.substring(substrStart, substrEnd) + postfix;
        }
      } else {
        sign = "-";
        rest = -rest;
      }
    }
    StringBuilder formatBuilder = new StringBuilder(sign);
    List<Long> formatArgs = new ArrayList<>();
    if (startField == DAY) {
      formatBuilder.append(rest / MICROS_PER_DAY);
      rest %= MICROS_PER_DAY;
    } else if (startField == HOUR) {
      formatBuilder.append("%02d");
      formatArgs.add(rest / MICROS_PER_HOUR);
      rest %= MICROS_PER_HOUR;
    } else if (startField == MINUTE) {
      formatBuilder.append("%02d");
      formatArgs.add(rest / MICROS_PER_MINUTE);
      rest %= MICROS_PER_MINUTE;
    } else if (startField == SECOND) {
      String leadZero = rest < 10 * MICROS_PER_SECOND ? "0" : "";
      formatBuilder.append(leadZero).append(BigDecimal.valueOf(rest, 6)
              .stripTrailingZeros().toPlainString());
    }

    if (startField < HOUR && HOUR <= endField) {
      formatBuilder.append(" %02d");
      formatArgs.add(rest / MICROS_PER_HOUR);
      rest %= MICROS_PER_HOUR;
    }
    if (startField < MINUTE && MINUTE <= endField) {
      formatBuilder.append(":%02d");
      formatArgs.add(rest / MICROS_PER_MINUTE);
      rest %= MICROS_PER_MINUTE;
    }
    if (startField < SECOND && SECOND <= endField) {
      String leadZero = rest < 10 * MICROS_PER_SECOND ? "0" : "";
      formatBuilder.append(":").append(leadZero).append(BigDecimal.valueOf(rest, 6)
              .stripTrailingZeros().toPlainString());
    }
    return prefix + String.format(formatBuilder.toString(), formatArgs.toArray()) + postfix;
  }
}
