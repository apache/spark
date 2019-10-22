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

package org.apache.spark.sql.catalyst.util

import java.util.concurrent.TimeUnit

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.IntervalUtils._
import org.apache.spark.unsafe.types.CalendarInterval

class IntervalUtilsSuite extends SparkFunSuite with Matchers {
  test("interval duration") {
    def duration(s: String, daysPerMonth: Int, unit: TimeUnit): Long = {
      getDuration(CalendarInterval.fromString(s), daysPerMonth, unit)
    }

    assert(duration("0 seconds", 31, TimeUnit.MILLISECONDS) === 0)
    assert(duration("1 month", 31, TimeUnit.DAYS) === 31)
    assert(duration("1 microsecond", 30, TimeUnit.MICROSECONDS) === 1)
    assert(duration("1 month -30 days", 31, TimeUnit.DAYS) === 1)

    try {
      duration(Integer.MAX_VALUE + " month", 31, TimeUnit.SECONDS)
      fail("Expected to throw an exception for the invalid input")
    } catch {
      case e: ArithmeticException =>
        assert(e.getMessage.contains("overflow"))
    }
  }

  test("negative interval") {
    def isNegative(s: String, daysPerMonth: Int): Boolean = {
      IntervalUtils.isNegative(CalendarInterval.fromString(s), daysPerMonth)
    }

    assert(isNegative("-1 months", 28))
    assert(isNegative("-1 microsecond", 30))
    assert(isNegative("-1 month 30 days", 31))
    assert(isNegative("2 months -61 days", 30))
    assert(isNegative("-1 year -2 seconds", 30))
    assert(!isNegative("0 months", 28))
    assert(!isNegative("1 year -360 days", 31))
    assert(!isNegative("-1 year 380 days", 31))

  }
}
