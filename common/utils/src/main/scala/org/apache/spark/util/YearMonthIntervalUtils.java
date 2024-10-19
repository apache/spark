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

// Replicating code from SparkIntervalUtils so code in the 'common' space can work with
// year-month intervals.
public class YearMonthIntervalUtils {
  private static byte YEAR = 0;
  private static byte MONTH = 1;
  private static int MONTHS_PER_YEAR = 12;

  // Used to convert months representing a year-month interval with given start and end fields
  // to its ANSI SQL string representation.
  public static String toYearMonthIntervalANSIString(int months, byte startField, byte endField) {
    String sign = "";
    long absMonths = months;
    if (months < 0) {
      sign = "-";
      absMonths = -absMonths;
    }
    String year = sign + Long.toString(absMonths / MONTHS_PER_YEAR);
    String yearAndMonth = year + "-" + Long.toString(absMonths % MONTHS_PER_YEAR);
    StringBuilder formatBuilder = new StringBuilder("INTERVAL '");
    if (startField == endField) {
      if (startField == YEAR) {
        formatBuilder.append(year + "' YEAR");
      } else {
        formatBuilder.append(Integer.toString(months) + "' MONTH");
      }
    } else {
      formatBuilder.append(yearAndMonth + "' YEAR TO MONTH");
    }
    return formatBuilder.toString();
  }
}
