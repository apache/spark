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

import java.sql.Date

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.scalatest.BeforeAndAfterAll

class DatetimeExpressionsSuite extends QueryTest with BeforeAndAfterAll {
  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext

  test("function current_date") {
    // Date constructor would keep the original millis, we need to align it with begin of day.
    checkAnswer(ctx.sql("""SELECT CURRENT_DATE()"""),
      Row(new Date(DateTimeUtils.daysToMillis(
        DateTimeUtils.millisToDays(System.currentTimeMillis())))))
  }

  test("function current_timestamp") {
    // Execution in one query should return the same value
    checkAnswer(ctx.sql("""SELECT CURRENT_TIMESTAMP() = CURRENT_TIMESTAMP()"""),
      Row(true))
    // By the time we run check, current timestamp has been different.
    // So we just check the date part.
    checkAnswer(ctx.sql("""SELECT CAST(CURRENT_TIMESTAMP() AS DATE)"""),
      Row(new Date(DateTimeUtils.daysToMillis(
        DateTimeUtils.millisToDays(System.currentTimeMillis())))))
  }

}