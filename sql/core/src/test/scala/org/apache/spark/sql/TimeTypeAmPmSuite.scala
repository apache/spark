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

class TimeTypeAmPmSuite extends QueryTest with SharedSparkSession {

  test("CAST string with AM/PM to TIME") {
    // This is expected to fail with current regex in TimeUtils.stringToTime
    checkAnswer(sql("SELECT CAST('02:30:45 PM' AS TIME)"),
      Row(java.time.LocalTime.of(14, 30, 45)))
  }

  test("CAST string with AM/PM (lowercase) to TIME") {
    checkAnswer(sql("SELECT CAST('02:30:45 pm' AS TIME)"),
      Row(java.time.LocalTime.of(14, 30, 45)))
  }
}
