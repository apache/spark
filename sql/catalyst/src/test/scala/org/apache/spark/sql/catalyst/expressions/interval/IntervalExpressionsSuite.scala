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

package org.apache.spark.sql.catalyst.expressions.interval

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, Literal}
import org.apache.spark.unsafe.types.CalendarInterval

class IntervalExpressionsSuite  extends SparkFunSuite with ExpressionEvalHelper {
  implicit def interval(s: String): Literal = {
    Literal(CalendarInterval.fromString("interval " + s))
  }

  test("millennium") {
    checkEvaluation(Millennium("0 years"), 0)
    checkEvaluation(Millennium("9999 years"), 9)

    checkEvaluation(Millennium("1000 years"), 1)
    checkEvaluation(Millennium("-2000 years"), -2)

    // Microseconds part must not be taken into account
    checkEvaluation(Millennium("999 years 400 days"), 0)

    // Millennium must be taken from years and months
    checkEvaluation(Millennium("999 years 12 months"), 1)
    checkEvaluation(Millennium("1000 years -1 months"), 0)
  }

  test("century") {
    checkEvaluation(Century("0 years"), 0)
    checkEvaluation(Century("9999 years"), 99)

    checkEvaluation(Century("1000 years"), 10)
    checkEvaluation(Century("-2000 years"), -20)

    // Microseconds part must not be taken into account
    checkEvaluation(Century("99 years 400 days"), 0)

    // Century must be taken from years and months
    checkEvaluation(Century("99 years 12 months"), 1)
    checkEvaluation(Century("100 years -1 months"), 0)
  }

  test("decade") {
    checkEvaluation(Decade("0 years"), 0)
    checkEvaluation(Decade("9999 years"), 999)

    checkEvaluation(Decade("1000 years"), 100)
    checkEvaluation(Decade("-2000 years"), -200)

    // Microseconds part must not be taken into account
    checkEvaluation(Decade("9 years 400 days"), 0)

    // Decade must be taken from years and months
    checkEvaluation(Decade("9 years 12 months"), 1)
    checkEvaluation(Decade("10 years -1 months"), 0)
  }

  test("year") {
    checkEvaluation(Year("0 years"), 0)
    checkEvaluation(Year("9999 years"), 9999)

    checkEvaluation(Year("1000 years"), 1000)
    checkEvaluation(Year("-2000 years"), -2000)

    // Microseconds part must not be taken into account
    checkEvaluation(Year("9 years 400 days"), 9)

    // Year must be taken from years and months
    checkEvaluation(Year("9 years 12 months"), 10)
    checkEvaluation(Year("10 years -1 months"), 9)
  }

  test("quarter") {
    checkEvaluation(Quarter("0 months"), 1)
    checkEvaluation(Quarter("1 months"), 1)
    checkEvaluation(Quarter("-1 months"), 1)
    checkEvaluation(Quarter("2 months"), 1)
    checkEvaluation(Quarter("-2 months"), 1)
    checkEvaluation(Quarter("1 years -1 months"), 4)
    checkEvaluation(Quarter("-1 years 1 months"), -2)
    checkEvaluation(Quarter("2 years 3 months"), 2)
    checkEvaluation(Quarter("-2 years -3 months"), 0)
    checkEvaluation(Quarter("9999 years"), 1)
  }
}
