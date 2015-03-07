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
package org.apache.spark.util.expression.quantity

import org.scalatest.FunSuite

class TimeAsMSQuantitySuite extends FunSuite {

  test("ms") {
    assert( TimeAsMS(1, "ms").toMs == 1)
  }

  test("seconds") {
    assert( TimeAsMS(1, "seconds").toMs == 1000)
  }

  test("minutes") {
    assert( TimeAsMS(1, "minutes").toMs == 1000 * 60)
  }

  test("hours") {
    assert( TimeAsMS(1, "hours").toMs == 1000 * 60 * 60)
  }

  test("days") {
    assert( TimeAsMS(1, "days").toMs == 1000 * 60 * 60 * 24)
  }

  test("weeks") {
    assert( TimeAsMS(1, "weeks").toMs == 1000 * 60 * 60 * 24 * 7)
  }

  test("weeks as Seconds") {
    assert( TimeAsMS(1, "weeks").toSecs == 60 * 60 * 24 * 7)
  }

  test("weeks as Minutes") {
    assert( TimeAsMS(1, "weeks").toMins ==  60 * 24 * 7)
  }

  test("weeks as hours") {
    assert( TimeAsMS(1, "weeks").toHours == 24 * 7)
  }

  test("weeks as days") {
    assert( TimeAsMS(1, "weeks").toDays == 7)
  }

  test("weeks as weeks") {
    assert( TimeAsMS(1, "weeks").toWeeks == 1)
  }
}
