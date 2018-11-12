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

package org.apache.spark.sql.catalyst.expressions

import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.LongType

class TimeWindowSuite extends SparkFunSuite with ExpressionEvalHelper with PrivateMethodTester {

  test("time window is unevaluable") {
    intercept[UnsupportedOperationException] {
      evaluateWithoutCodegen(TimeWindow(Literal(10L), "1 second", "1 second", "0 second"))
    }
  }

  private def checkErrorMessage(msg: String, value: String): Unit = {
    val validDuration = "10 second"
    val validTime = "5 second"
    val e1 = intercept[IllegalArgumentException] {
      TimeWindow(Literal(10L), value, validDuration, validTime).windowDuration
    }
    val e2 = intercept[IllegalArgumentException] {
      TimeWindow(Literal(10L), validDuration, value, validTime).slideDuration
    }
    val e3 = intercept[IllegalArgumentException] {
      TimeWindow(Literal(10L), validDuration, validDuration, value).startTime
    }
    Seq(e1, e2, e3).foreach { e =>
      e.getMessage.contains(msg)
    }
  }

  test("blank intervals throw exception") {
    for (blank <- Seq(null, " ", "\n", "\t")) {
      checkErrorMessage(
        "The window duration, slide duration and start time cannot be null or blank.", blank)
    }
  }

  test("invalid intervals throw exception") {
    checkErrorMessage(
      "did not correspond to a valid interval string.", "2 apples")
  }

  test("intervals greater than a month throws exception") {
    checkErrorMessage(
      "Intervals greater than or equal to a month is not supported (1 month).", "1 month")
  }

  test("interval strings work with and without 'interval' prefix and return microseconds") {
    val validDuration = "10 second"
    for ((text, seconds) <- Seq(
      ("1 second", 1000000), // 1e6
      ("1 minute", 60000000), // 6e7
      ("2 hours", 7200000000L))) { // 72e9
      assert(TimeWindow(Literal(10L), text, validDuration, "0 seconds").windowDuration === seconds)
      assert(TimeWindow(Literal(10L), "interval " + text, validDuration, "0 seconds").windowDuration
        === seconds)
    }
  }

  test("SPARK-21590: Start time works with negative values and return microseconds") {
    val validDuration = "10 minutes"
    for ((text, seconds) <- Seq(
      ("-10 seconds", -10000000), // -1e7
      ("-1 minute", -60000000),
      ("-1 hour", -3600000000L))) { // -6e7
      assert(TimeWindow(Literal(10L), validDuration, validDuration, "interval " + text).startTime
        === seconds)
      assert(TimeWindow(Literal(10L), validDuration, validDuration, text).startTime
        === seconds)
    }
  }

  private val parseExpression = PrivateMethod[Long]('parseExpression)

  test("parse sql expression for duration in microseconds - string") {
    val dur = TimeWindow.invokePrivate(parseExpression(Literal("5 seconds")))
    assert(dur.isInstanceOf[Long])
    assert(dur === 5000000)
  }

  test("parse sql expression for duration in microseconds - integer") {
    val dur = TimeWindow.invokePrivate(parseExpression(Literal(100)))
    assert(dur.isInstanceOf[Long])
    assert(dur === 100)
  }

  test("parse sql expression for duration in microseconds - long") {
    val dur = TimeWindow.invokePrivate(parseExpression(Literal.create(2L << 52, LongType)))
    assert(dur.isInstanceOf[Long])
    assert(dur === (2L << 52))
  }

  test("parse sql expression for duration in microseconds - invalid interval") {
    intercept[IllegalArgumentException] {
      TimeWindow.invokePrivate(parseExpression(Literal("2 apples")))
    }
  }

  test("parse sql expression for duration in microseconds - invalid expression") {
    intercept[AnalysisException] {
      TimeWindow.invokePrivate(parseExpression(Rand(123)))
    }
  }

  test("SPARK-16837: TimeWindow.apply equivalent to TimeWindow constructor") {
    val slideLength = "1 second"
    for (windowLength <- Seq("10 second", "1 minute", "2 hours")) {
      val applyValue = TimeWindow(Literal(10L), windowLength, slideLength, "0 seconds")
      val constructed = new TimeWindow(Literal(10L),
        Literal(windowLength),
        Literal(slideLength),
        Literal("0 seconds"))
      assert(applyValue == constructed)
    }
  }
}
