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
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval.fromString

class IntervalExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  implicit def interval(s: String): Literal = {
    Literal(fromString("interval " + s))
  }

  test("millenniums") {
    checkEvaluation(ExtractIntervalMillenniums("0 years"), 0)
    checkEvaluation(ExtractIntervalMillenniums("9999 years"), 9)
    checkEvaluation(ExtractIntervalMillenniums("1000 years"), 1)
    checkEvaluation(ExtractIntervalMillenniums("-2000 years"), -2)
    // Microseconds part must not be taken into account
    checkEvaluation(ExtractIntervalMillenniums("999 years 400 days"), 0)
    // Millennium must be taken from years and months
    checkEvaluation(ExtractIntervalMillenniums("999 years 12 months"), 1)
    checkEvaluation(ExtractIntervalMillenniums("1000 years -1 months"), 0)
  }

  test("centuries") {
    checkEvaluation(ExtractIntervalCenturies("0 years"), 0)
    checkEvaluation(ExtractIntervalCenturies("9999 years"), 99)
    checkEvaluation(ExtractIntervalCenturies("1000 years"), 10)
    checkEvaluation(ExtractIntervalCenturies("-2000 years"), -20)
    // Microseconds part must not be taken into account
    checkEvaluation(ExtractIntervalCenturies("99 years 400 days"), 0)
    // Century must be taken from years and months
    checkEvaluation(ExtractIntervalCenturies("99 years 12 months"), 1)
    checkEvaluation(ExtractIntervalCenturies("100 years -1 months"), 0)
  }

  test("decades") {
    checkEvaluation(ExtractIntervalDecades("0 years"), 0)
    checkEvaluation(ExtractIntervalDecades("9999 years"), 999)
    checkEvaluation(ExtractIntervalDecades("1000 years"), 100)
    checkEvaluation(ExtractIntervalDecades("-2000 years"), -200)
    // Microseconds part must not be taken into account
    checkEvaluation(ExtractIntervalDecades("9 years 400 days"), 0)
    // Decade must be taken from years and months
    checkEvaluation(ExtractIntervalDecades("9 years 12 months"), 1)
    checkEvaluation(ExtractIntervalDecades("10 years -1 months"), 0)
  }

  test("years") {
    checkEvaluation(ExtractIntervalYears("0 years"), 0)
    checkEvaluation(ExtractIntervalYears("9999 years"), 9999)
    checkEvaluation(ExtractIntervalYears("1000 years"), 1000)
    checkEvaluation(ExtractIntervalYears("-2000 years"), -2000)
    // Microseconds part must not be taken into account
    checkEvaluation(ExtractIntervalYears("9 years 400 days"), 9)
    // Year must be taken from years and months
    checkEvaluation(ExtractIntervalYears("9 years 12 months"), 10)
    checkEvaluation(ExtractIntervalYears("10 years -1 months"), 9)
  }

  test("quarters") {
    checkEvaluation(ExtractIntervalQuarters("0 months"), 1.toByte)
    checkEvaluation(ExtractIntervalQuarters("1 months"), 1.toByte)
    checkEvaluation(ExtractIntervalQuarters("-1 months"), 1.toByte)
    checkEvaluation(ExtractIntervalQuarters("2 months"), 1.toByte)
    checkEvaluation(ExtractIntervalQuarters("-2 months"), 1.toByte)
    checkEvaluation(ExtractIntervalQuarters("1 years -1 months"), 4.toByte)
    checkEvaluation(ExtractIntervalQuarters("-1 years 1 months"), -2.toByte)
    checkEvaluation(ExtractIntervalQuarters("2 years 3 months"), 2.toByte)
    checkEvaluation(ExtractIntervalQuarters("-2 years -3 months"), 0.toByte)
    checkEvaluation(ExtractIntervalQuarters("9999 years"), 1.toByte)
  }

  test("months") {
    checkEvaluation(ExtractIntervalMonths("0 year"), 0.toByte)
    for (m <- -24 to 24) {
      checkEvaluation(ExtractIntervalMonths(s"$m months"), (m % 12).toByte)
    }
    checkEvaluation(ExtractIntervalMonths("1 year 10 months"), 10.toByte)
    checkEvaluation(ExtractIntervalMonths("-2 year -10 months"), -10.toByte)
    checkEvaluation(ExtractIntervalMonths("9999 years"), 0.toByte)
  }

  private val largeInterval: String = "9999 years 11 months " +
    "31 days 11 hours 59 minutes 59 seconds 999 milliseconds 999 microseconds"

  test("days") {
    checkEvaluation(ExtractIntervalDays("0 days"), 0L)
    checkEvaluation(ExtractIntervalDays("1 days 100 seconds"), 1L)
    checkEvaluation(ExtractIntervalDays("-1 days -100 seconds"), -1L)
    checkEvaluation(ExtractIntervalDays("-365 days"), -365L)
    checkEvaluation(ExtractIntervalDays("365 days"), 365L)
    // Years and months must not be taken into account
    checkEvaluation(ExtractIntervalDays("100 year 10 months 5 days"), 5L)
    checkEvaluation(ExtractIntervalDays(largeInterval), 31L)
  }

  test("hours") {
    checkEvaluation(ExtractIntervalHours("0 hours"), 0.toByte)
    checkEvaluation(ExtractIntervalHours("1 hour"), 1.toByte)
    checkEvaluation(ExtractIntervalHours("-1 hour"), -1.toByte)
    checkEvaluation(ExtractIntervalHours("23 hours"), 23.toByte)
    checkEvaluation(ExtractIntervalHours("-23 hours"), -23.toByte)
    // Years and months must not be taken into account
    checkEvaluation(ExtractIntervalHours("100 year 10 months 10 hours"), 10.toByte)
    checkEvaluation(ExtractIntervalHours(largeInterval), 11.toByte)
  }

  test("minutes") {
    checkEvaluation(ExtractIntervalMinutes("0 minute"), 0.toByte)
    checkEvaluation(ExtractIntervalMinutes("1 minute"), 1.toByte)
    checkEvaluation(ExtractIntervalMinutes("-1 minute"), -1.toByte)
    checkEvaluation(ExtractIntervalMinutes("59 minute"), 59.toByte)
    checkEvaluation(ExtractIntervalMinutes("-59 minute"), -59.toByte)
    // Years and months must not be taken into account
    checkEvaluation(ExtractIntervalMinutes("100 year 10 months 10 minutes"), 10.toByte)
    checkEvaluation(ExtractIntervalMinutes(largeInterval), 59.toByte)
  }

  test("seconds") {
    checkEvaluation(ExtractIntervalSeconds("0 second"), Decimal(0, 8, 6))
    checkEvaluation(ExtractIntervalSeconds("1 second"), Decimal(1.0, 8, 6))
    checkEvaluation(ExtractIntervalSeconds("-1 second"), Decimal(-1.0, 8, 6))
    checkEvaluation(ExtractIntervalSeconds("1 minute 59 second"), Decimal(59.0, 8, 6))
    checkEvaluation(ExtractIntervalSeconds("-59 minutes -59 seconds"), Decimal(-59.0, 8, 6))
    // Years and months must not be taken into account
    checkEvaluation(ExtractIntervalSeconds("100 year 10 months 10 seconds"), Decimal(10.0, 8, 6))
    checkEvaluation(ExtractIntervalSeconds(largeInterval), Decimal(59.999999, 8, 6))
    checkEvaluation(
      ExtractIntervalSeconds("10 seconds 1 milliseconds 1 microseconds"),
      Decimal(10001001, 8, 6))
    checkEvaluation(ExtractIntervalSeconds("61 seconds 1 microseconds"), Decimal(1000001, 8, 6))
  }

  test("milliseconds") {
    checkEvaluation(ExtractIntervalMilliseconds("0 milliseconds"), Decimal(0, 8, 3))
    checkEvaluation(ExtractIntervalMilliseconds("1 milliseconds"), Decimal(1.0, 8, 3))
    checkEvaluation(ExtractIntervalMilliseconds("-1 milliseconds"), Decimal(-1.0, 8, 3))
    checkEvaluation(
      ExtractIntervalMilliseconds("1 second 999 milliseconds"),
      Decimal(1999.0, 8, 3))
    checkEvaluation(
      ExtractIntervalMilliseconds("999 milliseconds 1 microsecond"),
      Decimal(999.001, 8, 3))
    checkEvaluation(
      ExtractIntervalMilliseconds("-1 second -999 milliseconds"),
      Decimal(-1999.0, 8, 3))
    // Years and months must not be taken into account
    checkEvaluation(ExtractIntervalMilliseconds("100 year 1 millisecond"), Decimal(1.0, 8, 3))
    checkEvaluation(ExtractIntervalMilliseconds(largeInterval), Decimal(59999.999, 8, 3))
  }

  test("microseconds") {
    checkEvaluation(ExtractIntervalMicroseconds("0 microseconds"), 0L)
    checkEvaluation(ExtractIntervalMicroseconds("1 microseconds"), 1L)
    checkEvaluation(ExtractIntervalMicroseconds("-1 microseconds"), -1L)
    checkEvaluation(ExtractIntervalMicroseconds("1 second 999 microseconds"), 1000999L)
    checkEvaluation(ExtractIntervalMicroseconds("999 milliseconds 1 microseconds"), 999001L)
    checkEvaluation(ExtractIntervalMicroseconds("-1 second -999 microseconds"), -1000999L)
    // Years and months must not be taken into account
    checkEvaluation(ExtractIntervalMicroseconds("11 year 1 microseconds"), 1L)
    checkEvaluation(ExtractIntervalMicroseconds(largeInterval), 59999999L)
  }

  test("epoch") {
    checkEvaluation(ExtractIntervalEpoch("0 months"), Decimal(0.0, 18, 6))
    checkEvaluation(ExtractIntervalEpoch("10000 years"), Decimal(315576000000.0, 18, 6))
    checkEvaluation(ExtractIntervalEpoch("1 year"), Decimal(31557600.0, 18, 6))
    checkEvaluation(ExtractIntervalEpoch("-1 year"), Decimal(-31557600.0, 18, 6))
    checkEvaluation(
      ExtractIntervalEpoch("1 second 1 millisecond 1 microsecond"),
      Decimal(1.001001, 18, 6))
  }

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
