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

package org.apache.spark.streaming.ui

import java.util.TimeZone
import java.util.concurrent.TimeUnit

import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.ui.{UIUtils => SparkUIUtils}

class UIUtilsSuite extends SparkFunSuite with Matchers {

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

  test("convertToTimeUnit") {
    verifyConvertToTimeUnit(60.0 * 1000 * 1000 * 1000, 60 * 1000, TimeUnit.NANOSECONDS)
    verifyConvertToTimeUnit(60.0 * 1000 * 1000, 60 * 1000, TimeUnit.MICROSECONDS)
    verifyConvertToTimeUnit(60 * 1000, 60 * 1000, TimeUnit.MILLISECONDS)
    verifyConvertToTimeUnit(60, 60 * 1000, TimeUnit.SECONDS)
    verifyConvertToTimeUnit(1, 60 * 1000, TimeUnit.MINUTES)
    verifyConvertToTimeUnit(1.0 / 60, 60 * 1000, TimeUnit.HOURS)
    verifyConvertToTimeUnit(1.0 / 60 / 24, 60 * 1000, TimeUnit.DAYS)
  }

  private def verifyConvertToTimeUnit(
      expectedTime: Double, milliseconds: Long, unit: TimeUnit): Unit = {
    val convertedTime = UIUtils.convertToTimeUnit(milliseconds, unit)
    convertedTime should be (expectedTime +- 1E-6)
  }

  test("formatBatchTime") {
    val tzForTest = TimeZone.getTimeZone("America/Los_Angeles")
    val batchTime = 1431637480452L // Thu May 14 14:04:40 PDT 2015
    assert("2015/05/14 14:04:40" ===
      SparkUIUtils.formatBatchTime(batchTime, 1000, timezone = tzForTest))
    assert("2015/05/14 14:04:40.452" ===
      SparkUIUtils.formatBatchTime(batchTime, 999, timezone = tzForTest))
    assert("14:04:40" ===
      SparkUIUtils.formatBatchTime(batchTime, 1000, false, timezone = tzForTest))
    assert("14:04:40.452" ===
      SparkUIUtils.formatBatchTime(batchTime, 999, false, timezone = tzForTest))
  }
}
