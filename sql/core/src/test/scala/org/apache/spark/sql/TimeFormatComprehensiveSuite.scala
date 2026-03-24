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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSparkSession

class TimeFormatComprehensiveSuite extends QueryTest with SharedSparkSession {

  test("TIME_FORMAT - basic 24-hour and 12-hour format") {
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'HH:mm:ss')"), Row("14:30:45"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'hh:mm:ss a')"), Row("02:30:45 PM"))
    checkAnswer(sql("SELECT time_format(TIME'09:15:30', 'hh:mm:ss a')"), Row("09:15:30 AM"))
  }

  test("TIME_FORMAT - different precisions") {
    checkAnswer(sql("SELECT time_format(TIME'14:30:45.123', 'HH:mm:ss.SSS')"),
      Row("14:30:45.123"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45.123456', 'HH:mm:ss.SSSSSS')"),
      Row("14:30:45.123456"))
  }

  test("TIME_FORMAT - various format patterns") {
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'HH:mm')"), Row("14:30"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'HH')"), Row("14"))
    checkAnswer(sql("SELECT time_format(TIME'09:05:00', 'H:mm a')"), Row("9:05 AM"))
  }

  test("TIME_FORMAT - edge cases (midnight, noon, end of day)") {
    checkAnswer(sql("SELECT time_format(TIME'00:00:00', 'HH:mm:ss')"), Row("00:00:00"))
    checkAnswer(sql("SELECT time_format(TIME'00:00:00', 'hh:mm:ss a')"), Row("12:00:00 AM"))
    checkAnswer(sql("SELECT time_format(TIME'12:00:00', 'HH:mm:ss')"), Row("12:00:00"))
    checkAnswer(sql("SELECT time_format(TIME'12:00:00', 'hh:mm:ss a')"), Row("12:00:00 PM"))
    checkAnswer(sql("SELECT time_format(TIME'23:59:59', 'HH:mm:ss')"), Row("23:59:59"))
    checkAnswer(sql("SELECT time_format(TIME'23:59:59.999999', 'HH:mm:ss.SSSSSS')"),
      Row("23:59:59.999999"))
  }

  test("TIME_FORMAT - null handling") {
    checkAnswer(sql("SELECT time_format(NULL, 'HH:mm:ss')"), Row(null))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', NULL)"), Row(null))
    checkAnswer(sql("SELECT time_format(CAST(NULL AS TIME), 'HH:mm:ss')"), Row(null))
  }

  test("TIME_FORMAT - custom formats and separators") {
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'hh-mm-ss a')"), Row("02-30-45 PM"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'HH.mm.ss')"), Row("14.30.45"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'HH_mm_ss')"), Row("14_30_45"))
  }

  test("TIME_FORMAT - text literals and verbose formats") {
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', \"'Time:' HH:mm:ss\")"),
      Row("Time: 14:30:45"))
    val verbosePattern = "\"H 'hours' mm 'mins' ss 'seconds' SSS 'milliseconds'\""
    checkAnswer(sql(s"SELECT time_format(TIME'02:20:30.040', $verbosePattern)"),
      Row("2 hours 20 mins 30 seconds 040 milliseconds"))
    val verbosePattern2 = "\"H 'hours,' mm 'minutes and' ss 'seconds'\""
    checkAnswer(sql(s"SELECT time_format(TIME'14:05:15', $verbosePattern2)"),
      Row("14 hours, 05 minutes and 15 seconds"))
  }

  test("TIME_FORMAT - AM/PM edge cases") {
    checkAnswer(sql("SELECT time_format(TIME'00:00:00', 'a')"), Row("AM"))
    checkAnswer(sql("SELECT time_format(TIME'11:59:59', 'a')"), Row("AM"))
    checkAnswer(sql("SELECT time_format(TIME'12:00:00', 'a')"), Row("PM"))
    checkAnswer(sql("SELECT time_format(TIME'13:00:00', 'a')"), Row("PM"))
    checkAnswer(sql("SELECT time_format(TIME'23:59:59', 'a')"), Row("PM"))
  }

  test("TIME_FORMAT - hour edge cases for 12-hour format") {
    checkAnswer(sql("SELECT time_format(TIME'00:00:00', 'hh:mm a')"), Row("12:00 AM"))
    checkAnswer(sql("SELECT time_format(TIME'00:30:00', 'hh:mm a')"), Row("12:30 AM"))
    checkAnswer(sql("SELECT time_format(TIME'01:00:00', 'hh:mm a')"), Row("01:00 AM"))
    checkAnswer(sql("SELECT time_format(TIME'11:00:00', 'hh:mm a')"), Row("11:00 AM"))
    checkAnswer(sql("SELECT time_format(TIME'12:00:00', 'hh:mm a')"), Row("12:00 PM"))
    checkAnswer(sql("SELECT time_format(TIME'13:00:00', 'hh:mm a')"), Row("01:00 PM"))
  }

  test("TIME_FORMAT - H vs HH") {
    checkAnswer(sql("SELECT time_format(TIME'09:15:30', 'H:mm:ss')"), Row("9:15:30"))
    checkAnswer(sql("SELECT time_format(TIME'09:15:30', 'HH:mm:ss')"), Row("09:15:30"))
    checkAnswer(sql("SELECT time_format(TIME'09:15:30', 'h:mm a')"), Row("9:15 AM"))
    checkAnswer(sql("SELECT time_format(TIME'09:15:30', 'hh:mm a')"), Row("09:15 AM"))
  }

  test("TIME_FORMAT - precision variations") {
    checkAnswer(sql("SELECT time_format(TIME'14:30:45.1', 'HH:mm:ss.S')"), Row("14:30:45.1"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45.12', 'HH:mm:ss.SS')"), Row("14:30:45.12"))
  }

  test("TIME_FORMAT - with COALESCE and CONCAT") {
    checkAnswer(sql("SELECT coalesce(time_format(NULL, 'HH:mm'), 'N/A')"), Row("N/A"))
    checkAnswer(sql("SELECT coalesce(time_format(TIME'14:30:45', NULL), 'N/A')"), Row("N/A"))
    val concatExpr = "concat('The time is ', time_format(TIME'14:30:45', 'hh:mm a'))"
    checkAnswer(sql(s"SELECT $concatExpr"), Row("The time is 02:30 PM"))
  }

  test("TIME_FORMAT - mixed 24-hour and 12-hour in one output") {
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'HH:mm:ss (hh:mm:ss a)')"),
      Row("14:30:45 (02:30:45 PM)"))
    checkAnswer(sql("SELECT time_format(TIME'09:15:30', 'HH:mm (h:mm a)')"),
      Row("09:15 (9:15 AM)"))
    val mixedPattern = "'24hr:' HH:mm '12hr:' hh:mm a"
    checkAnswer(sql(s"""SELECT time_format(TIME'23:45:00', "$mixedPattern")"""),
      Row("24hr: 23:45 12hr: 11:45 PM"))
    checkAnswer(sql("SELECT time_format(TIME'00:30:00', 'HH:mm:ss = hh:mm:ss a')"),
      Row("00:30:00 = 12:30:00 AM"))
    val mixedPattern2 = "HH:mm:ss '(24h)' / hh:mm:ss a '(12h)'"
    checkAnswer(sql(s"""SELECT time_format(TIME'14:30:45', "$mixedPattern2")"""),
      Row("14:30:45 (24h) / 02:30:45 PM (12h)"))
  }

  test("TIME_FORMAT - multiple formats in single query") {
    val df = sql("""
      SELECT
        time_format(TIME'14:30:45.123456', 'HH:mm:ss') as format_24hr,
        time_format(TIME'14:30:45.123456', 'hh:mm:ss a') as format_12hr,
        time_format(TIME'14:30:45.123456', 'HH:mm:ss.SSSSSS') as format_with_micros
    """)
    checkAnswer(df, Row("14:30:45", "02:30:45 PM", "14:30:45.123456"))
  }

  test("TIME_FORMAT - special characters and patterns") {
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', \"HH:mm:ss 'UTC'\")"), Row("14:30:45 UTC"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', \"'['HH:mm:ss']'\")"), Row("[14:30:45]"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'HH|mm|ss')"), Row("14|30|45"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'HH-mm-ss')"), Row("14-30-45"))
  }

  test("TIME_FORMAT - month pattern (MM)") {
    // MM should return epoch month (01) if we use LocalDateTime at epoch
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'HH:MM:ss')"), Row("14:01:45"))
  }

  test("TIME_FORMAT - subsecond precision edge cases") {
    checkAnswer(sql("SELECT time_format(TIME'14:30:45.000', 'HH:mm:ss.SSS')"),
      Row("14:30:45.000"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45.999', 'HH:mm:ss.SSS')"),
      Row("14:30:45.999"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45.001', 'HH:mm:ss.SSS')"),
      Row("14:30:45.001"))

    checkAnswer(sql("SELECT time_format(TIME'14:30:45.000000', 'HH:mm:ss.SSSSSS')"),
      Row("14:30:45.000000"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45.999999', 'HH:mm:ss.SSSSSS')"),
      Row("14:30:45.999999"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45.000001', 'HH:mm:ss.SSSSSS')"),
      Row("14:30:45.000001"))
  }

  test("TIME_FORMAT - CAST with formatted output") {
    checkAnswer(sql("SELECT cast(time_format(TIME'14:30:45', 'HH:mm:ss') as string)"),
      Row("14:30:45"))
  }
}
