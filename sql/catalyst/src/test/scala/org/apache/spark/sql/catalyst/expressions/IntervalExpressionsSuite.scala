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

import scala.language.implicitConversions

import org.apache.spark.SparkFunSuite
import org.apache.spark.unsafe.types.CalendarInterval.fromString

class IntervalExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("multiply") {
    def multiply(interval: String, num: Long): Expression = {
      MultiplyInterval(Literal(fromString(interval)), Literal(num))
    }
    checkEvaluation(multiply("0 seconds", 10), fromString("0 seconds"))
    checkEvaluation(multiply("10 hours", 0), fromString("0 hours"))
    checkEvaluation(
      multiply("12 months 1 microseconds", 2),
      fromString("2 years 2 microseconds"))
    checkEvaluation(
      multiply("-5 year 3 seconds", 3),
      fromString("-15 years 9 seconds"))
    checkEvaluation(multiply("2 months", Int.MaxValue), null)
    checkEvaluation(multiply("2 days", Long.MaxValue), null)
  }
}