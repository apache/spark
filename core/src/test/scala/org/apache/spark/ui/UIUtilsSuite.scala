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


package org.apache.spark.ui

import java.util.concurrent.TimeUnit

import org.scalatest.FunSuite
import org.scalatest.Matchers

class UIUtilsSuite extends FunSuite with Matchers{

  test("shortTimeUnitString") {
    assert("ns" === UIUtils.shortTimeUnitString(TimeUnit.NANOSECONDS))
    assert("us" === UIUtils.shortTimeUnitString(TimeUnit.MICROSECONDS))
    assert("ms" === UIUtils.shortTimeUnitString(TimeUnit.MILLISECONDS))
    assert("sec" === UIUtils.shortTimeUnitString(TimeUnit.SECONDS))
    assert("min" === UIUtils.shortTimeUnitString(TimeUnit.MINUTES))
    assert("hrs" === UIUtils.shortTimeUnitString(TimeUnit.HOURS))
    assert("days" === UIUtils.shortTimeUnitString(TimeUnit.DAYS))
  }

  test("normalizeDuration") {
    verifyNormalizedTime(900, TimeUnit.MILLISECONDS, 900)
    verifyNormalizedTime(1.0, TimeUnit.SECONDS, 1000)
    verifyNormalizedTime(1.0, TimeUnit.MINUTES, 60 * 1000)
    verifyNormalizedTime(1.0, TimeUnit.HOURS, 60 * 60 * 1000)
    verifyNormalizedTime(1.0, TimeUnit.DAYS, 24 * 60 * 60 * 1000)
  }

  private def verifyNormalizedTime(
      expectedTime: Double, expectedUnit: TimeUnit, input: Long): Unit = {
    val (time, unit) = UIUtils.normalizeDuration(input)
    time should be (expectedTime +- 1E-6)
    unit should be (expectedUnit)
  }
}
