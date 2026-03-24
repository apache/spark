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

/**
 * Verification tests for TIME functions added in PR #53320 (SPARK-54588).
 */
class TimeTypePRSupportSuite extends QueryTest with SharedSparkSession {

  test("time_to_millis and time_from_millis") {
    checkAnswer(sql("SELECT time_to_millis(time_from_millis(52200500))"), Row(52200500L))
    checkAnswer(sql("SELECT time_from_millis(time_to_millis(TIME'14:30:00.5'))"),
      Row(java.time.LocalTime.of(14, 30, 0, 500000000)))
  }

  test("time_to_micros and time_from_micros") {
    checkAnswer(sql("SELECT time_to_micros(time_from_micros(52200500000L))"), Row(52200500000L))
    checkAnswer(sql("SELECT time_from_micros(time_to_micros(TIME'14:30:00.5'))"),
      Row(java.time.LocalTime.of(14, 30, 0, 500000000)))
  }

  test("time_format - basic formats") {
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'HH:mm:ss')"), Row("14:30:45"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', 'hh:mm:ss a')"), Row("02:30:45 PM"))
    checkAnswer(sql("SELECT time_format(TIME'09:15:30', 'hh:mm:ss a')"), Row("09:15:30 AM"))
  }

  test("time_format - with subseconds") {
    checkAnswer(sql("SELECT time_format(TIME'14:30:45.123', 'HH:mm:ss.SSS')"), Row("14:30:45.123"))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45.123456', 'HH:mm:ss.SSSSSS')"),
      Row("14:30:45.123456"))
  }

  test("time_format - edge cases") {
    // Midnight
    checkAnswer(sql("SELECT time_format(TIME'00:00:00', 'HH:mm:ss')"), Row("00:00:00"))
    checkAnswer(sql("SELECT time_format(TIME'00:00:00', 'hh:mm a')"), Row("12:00 AM"))

    // Noon
    checkAnswer(sql("SELECT time_format(TIME'12:00:00', 'HH:mm:ss')"), Row("12:00:00"))
    checkAnswer(sql("SELECT time_format(TIME'12:00:00', 'hh:mm a')"), Row("12:00 PM"))

    // End of day
    checkAnswer(sql("SELECT time_format(TIME'23:59:59.999999', 'HH:mm:ss.SSSSSS')"),
      Row("23:59:59.999999"))
  }

  test("time_format - null handling") {
    checkAnswer(sql("SELECT time_format(NULL, 'HH:mm:ss')"), Row(null))
    checkAnswer(sql("SELECT time_format(TIME'14:30:45', NULL)"), Row(null))
  }
}
