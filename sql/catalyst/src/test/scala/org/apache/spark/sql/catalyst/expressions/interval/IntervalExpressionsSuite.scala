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

import scala.language.implicitConversions

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
    checkEvaluation(Quarter("0 months"), 1.toByte)
    checkEvaluation(Quarter("1 months"), 1.toByte)
    checkEvaluation(Quarter("-1 months"), 1.toByte)
    checkEvaluation(Quarter("2 months"), 1.toByte)
    checkEvaluation(Quarter("-2 months"), 1.toByte)
    checkEvaluation(Quarter("1 years -1 months"), 4.toByte)
    checkEvaluation(Quarter("-1 years 1 months"), -2.toByte)
    checkEvaluation(Quarter("2 years 3 months"), 2.toByte)
    checkEvaluation(Quarter("-2 years -3 months"), 0.toByte)
    checkEvaluation(Quarter("9999 years"), 1.toByte)
  }

  test("month") {
    checkEvaluation(Month("0 year"), 0.toByte)
    for (m <- -24 to 24) {
      checkEvaluation(Month(s"$m months"), (m % 12).toByte)
    }
    checkEvaluation(Month("1 year 10 months"), 10.toByte)
    checkEvaluation(Month("-2 year -10 months"), -10.toByte)
    checkEvaluation(Month("9999 years"), 0.toByte)
  }

  test("day") {
    checkEvaluation(Day("0 days"), 0L)
    checkEvaluation(Day("1 days 100 seconds"), 1L)
    checkEvaluation(Day("-1 days -100 seconds"), -1L)
    checkEvaluation(Day("-365 days"), -365L)
    checkEvaluation(Day("365 days"), 365L)
    // Years and months must not be taken into account
    checkEvaluation(Day("100 year 10 months 5 days"), 5L)
    checkEvaluation(Day(
      "9999 years 11 months 31 days 11 hours 59 minutes 59 seconds"), 31L)
  }

  test("hour") {
    checkEvaluation(Hour("0 hours"), 0.toByte)
    checkEvaluation(Hour("1 hour"), 1.toByte)
    checkEvaluation(Hour("-1 hour"), -1.toByte)
    checkEvaluation(Hour("23 hours"), 23.toByte)
    checkEvaluation(Hour("-23 hours"), -23.toByte)
    // Years and months must not be taken into account
    checkEvaluation(Hour("100 year 10 months 10 hours"), 10.toByte)
    checkEvaluation(Hour(
      "9999 years 11 months 31 days 11 hours 59 minutes 59 seconds"), 11.toByte)
  }
}
