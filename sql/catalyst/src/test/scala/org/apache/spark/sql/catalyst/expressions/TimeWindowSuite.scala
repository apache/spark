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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException

class TimeWindowSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("time window is unevaluable") {
    intercept[UnsupportedOperationException] {
      evaluate(TimeWindow(Literal(10L), "1 second", "1 second", "0 second"))
    }
  }

  test("invalid intervals throw exception") {
    val validDuration = "10 second"
    val validTime = "5 second"
    def checkErrorMessage(msg: String, value: String): Unit = {
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
    for (blank <- Seq(null, " ", "\n", "\t")) {
      checkErrorMessage(
        "The window duration, slide duration and start time cannot be null or blank.", blank)
    }
    checkErrorMessage(
      "did not correspond to a valid interval string.", "2 apples")
  }

  test("interval strings work with and without 'interval' prefix and returns seconds") {
    val validDuration = "10 second"
    for ((text, seconds) <- Seq(("1 second", 1), ("1 minute", 60), ("2 hours", 7200))) {
      assert(TimeWindow(Literal(10L), text, validDuration, "0 seconds").windowDuration === seconds)
      assert(TimeWindow(Literal(10L), "interval " + text, validDuration, "0 seconds").windowDuration
        === seconds)
    }
  }
}
