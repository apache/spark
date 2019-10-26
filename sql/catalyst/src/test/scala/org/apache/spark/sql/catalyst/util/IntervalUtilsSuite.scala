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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.IntervalUtils.fromString
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.unsafe.types.CalendarInterval._

class IntervalUtilsSuite extends SparkFunSuite {

  test("fromString: basic") {
    testSingleUnit("YEAR", 3, 36, 0, 0)
    testSingleUnit("Month", 3, 3, 0, 0)
    testSingleUnit("Week", 3, 0, 21, 0)
    testSingleUnit("DAY", 3, 0, 3, 0)
    testSingleUnit("HouR", 3, 0, 0, 3 * MICROS_PER_HOUR)
    testSingleUnit("MiNuTe", 3, 0, 0, 3 * MICROS_PER_MINUTE)
    testSingleUnit("Second", 3, 0, 0, 3 * MICROS_PER_SECOND)
    testSingleUnit("MilliSecond", 3, 0, 0, 3 * MICROS_PER_MILLI)
    testSingleUnit("MicroSecond", 3, 0, 0, 3)

    for (input <- Seq(null, "", " ")) {
      try {
        fromString(input)
        fail("Expected to throw an exception for the invalid input")
      } catch {
        case e: IllegalArgumentException =>
          val msg = e.getMessage
          if (input == null) {
            assert(msg.contains("cannot be null"))
          }
      }
    }

    for (input <- Seq("interval", "interval1 day", "foo", "foo 1 day")) {
      try {
        fromString(input)
        fail("Expected to throw an exception for the invalid input")
      } catch {
        case e: IllegalArgumentException =>
          val msg = e.getMessage
          assert(msg.contains("Invalid interval string"))
      }
    }
  }

  test("fromString: random order field") {
    val input = "1 day 1 year"
    val result = new CalendarInterval(12, 1, 0)
    assert(fromString(input) == result)
  }

  test("fromString: duplicated fields") {
    val input = "1 day 1 day"
    val result = new CalendarInterval(0, 2, 0)
    assert(fromString(input) == result)
  }

  test("fromString: value with +/-") {
    val input = "+1 year -1 day"
    val result = new CalendarInterval(12, -1, 0)
    assert(fromString(input) == result)
  }

  private def testSingleUnit(
      unit: String, number: Int, months: Int, days: Int, microseconds: Long): Unit = {
    for (prefix <- Seq("interval ", "")) {
      val input1 = prefix + number + " " + unit
      val input2 = prefix + number + " " + unit + "s"
      val result = new CalendarInterval(months, days, microseconds)
      assert(fromString(input1) == result)
      assert(fromString(input2) == result)
    }
  }
}
