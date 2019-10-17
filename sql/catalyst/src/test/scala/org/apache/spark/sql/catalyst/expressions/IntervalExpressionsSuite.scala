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
import org.apache.spark.unsafe.types.CalendarInterval.fromString

class IntervalExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("multiply") {
    def multiply(interval: String, num: Double): Expression = {
      MultiplyInterval(Literal(fromString(interval)), Literal(num))
    }
    checkEvaluation(multiply("0 seconds", 10), fromString("0 seconds"))
    checkEvaluation(multiply("10 hours", 0), fromString("0 hours"))
    checkEvaluation(multiply("12 months 1 microseconds", 2), fromString("2 years 2 microseconds"))
    checkEvaluation(multiply("-5 year 3 seconds", 3), fromString("-15 years 9 seconds"))
    checkEvaluation(multiply("1 year 1 second", 0.5), fromString("6 months 500 milliseconds"))
    checkEvaluation(
      multiply("-100 years -1 millisecond", 0.5),
      fromString("-50 years -500 microseconds"))
    checkEvaluation(
      multiply("2 months 4 seconds", -0.5),
      fromString("-1 months -2 seconds"))
    checkEvaluation(
      multiply("1 month 2 microseconds", 1.5),
      fromString("1 months 15 days 3 microseconds"))
    checkEvaluation(multiply("2 months", Int.MaxValue), null)
  }

  test("divide") {
    def divide(interval: String, num: Double): Expression = {
      DivideInterval(Literal(fromString(interval)), Literal(num))
    }
    checkEvaluation(divide("0 seconds", 10), fromString("0 seconds"))
    checkEvaluation(
      divide("12 months 3 milliseconds", 2),
      fromString("6 months 1 milliseconds 500 microseconds"))
    checkEvaluation(
      divide("-5 year 3 seconds", 3),
      fromString("-1 years -8 months 1 seconds"))
    checkEvaluation(
      divide("6 years -7 seconds", 3),
      fromString("2 years -2 seconds -333 milliseconds -333 microseconds"))
    checkEvaluation(
      divide("2 years -8 seconds", 0.5),
      fromString("4 years -16 seconds"))
    checkEvaluation(
      divide("-1 month 2 microseconds", -0.25),
      fromString("4 months -8 microseconds"))
    checkEvaluation(
      divide("1 month 3 microsecond", 1.5),
      fromString("20 days 2 microseconds"))
    checkEvaluation(divide("2 months", 0), null)
  }
}
