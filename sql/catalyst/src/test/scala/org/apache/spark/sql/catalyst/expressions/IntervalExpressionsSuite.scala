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
import org.apache.spark.unsafe.types.CalendarInterval

class IntervalExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  implicit def interval(s: String): Literal = {
    Literal(CalendarInterval.fromString("interval " + s))
  }

  test("millenniums") {
    checkEvaluation(IntervalMillenniums("0 years"), 0)
    checkEvaluation(IntervalMillenniums("9999 years"), 9)
    checkEvaluation(IntervalMillenniums("1000 years"), 1)
    checkEvaluation(IntervalMillenniums("-2000 years"), -2)
    // Microseconds part must not be taken into account
    checkEvaluation(IntervalMillenniums("999 years 400 days"), 0)
    // Millennium must be taken from years and months
    checkEvaluation(IntervalMillenniums("999 years 12 months"), 1)
    checkEvaluation(IntervalMillenniums("1000 years -1 months"), 0)
  }

  test("centuries") {
    checkEvaluation(IntervalCenturies("0 years"), 0)
    checkEvaluation(IntervalCenturies("9999 years"), 99)
    checkEvaluation(IntervalCenturies("1000 years"), 10)
    checkEvaluation(IntervalCenturies("-2000 years"), -20)
    // Microseconds part must not be taken into account
    checkEvaluation(IntervalCenturies("99 years 400 days"), 0)
    // Century must be taken from years and months
    checkEvaluation(IntervalCenturies("99 years 12 months"), 1)
    checkEvaluation(IntervalCenturies("100 years -1 months"), 0)
  }

  test("decades") {
    checkEvaluation(IntervalDecades("0 years"), 0)
    checkEvaluation(IntervalDecades("9999 years"), 999)
    checkEvaluation(IntervalDecades("1000 years"), 100)
    checkEvaluation(IntervalDecades("-2000 years"), -200)
    // Microseconds part must not be taken into account
    checkEvaluation(IntervalDecades("9 years 400 days"), 0)
    // Decade must be taken from years and months
    checkEvaluation(IntervalDecades("9 years 12 months"), 1)
    checkEvaluation(IntervalDecades("10 years -1 months"), 0)
  }

  test("years") {
    checkEvaluation(IntervalYears("0 years"), 0)
    checkEvaluation(IntervalYears("9999 years"), 9999)
    checkEvaluation(IntervalYears("1000 years"), 1000)
    checkEvaluation(IntervalYears("-2000 years"), -2000)
    // Microseconds part must not be taken into account
    checkEvaluation(IntervalYears("9 years 400 days"), 9)
    // Year must be taken from years and months
    checkEvaluation(IntervalYears("9 years 12 months"), 10)
    checkEvaluation(IntervalYears("10 years -1 months"), 9)
  }

  test("quarters") {
    checkEvaluation(IntervalQuarters("0 months"), 1.toByte)
    checkEvaluation(IntervalQuarters("1 months"), 1.toByte)
    checkEvaluation(IntervalQuarters("-1 months"), 1.toByte)
    checkEvaluation(IntervalQuarters("2 months"), 1.toByte)
    checkEvaluation(IntervalQuarters("-2 months"), 1.toByte)
    checkEvaluation(IntervalQuarters("1 years -1 months"), 4.toByte)
    checkEvaluation(IntervalQuarters("-1 years 1 months"), -2.toByte)
    checkEvaluation(IntervalQuarters("2 years 3 months"), 2.toByte)
    checkEvaluation(IntervalQuarters("-2 years -3 months"), 0.toByte)
    checkEvaluation(IntervalQuarters("9999 years"), 1.toByte)
  }

  test("months") {
    checkEvaluation(IntervalMonths("0 year"), 0.toByte)
    for (m <- -24 to 24) {
      checkEvaluation(IntervalMonths(s"$m months"), (m % 12).toByte)
    }
    checkEvaluation(IntervalMonths("1 year 10 months"), 10.toByte)
    checkEvaluation(IntervalMonths("-2 year -10 months"), -10.toByte)
    checkEvaluation(IntervalMonths("9999 years"), 0.toByte)
  }

  private val largeInterval: String = "9999 years 11 months " +
    "31 days 11 hours 59 minutes 59 seconds 999 milliseconds 999 microseconds"

  test("days") {
    checkEvaluation(IntervalDays("0 days"), 0L)
    checkEvaluation(IntervalDays("1 days 100 seconds"), 1L)
    checkEvaluation(IntervalDays("-1 days -100 seconds"), -1L)
    checkEvaluation(IntervalDays("-365 days"), -365L)
    checkEvaluation(IntervalDays("365 days"), 365L)
    // Years and months must not be taken into account
    checkEvaluation(IntervalDays("100 year 10 months 5 days"), 5L)
    checkEvaluation(IntervalDays(largeInterval), 31L)
  }

  test("hours") {
    checkEvaluation(IntervalHours("0 hours"), 0.toByte)
    checkEvaluation(IntervalHours("1 hour"), 1.toByte)
    checkEvaluation(IntervalHours("-1 hour"), -1.toByte)
    checkEvaluation(IntervalHours("23 hours"), 23.toByte)
    checkEvaluation(IntervalHours("-23 hours"), -23.toByte)
    // Years and months must not be taken into account
    checkEvaluation(IntervalHours("100 year 10 months 10 hours"), 10.toByte)
    checkEvaluation(IntervalHours(largeInterval), 11.toByte)
  }

  test("minutes") {
    checkEvaluation(IntervalMinutes("0 minute"), 0.toByte)
    checkEvaluation(IntervalMinutes("1 minute"), 1.toByte)
    checkEvaluation(IntervalMinutes("-1 minute"), -1.toByte)
    checkEvaluation(IntervalMinutes("59 minute"), 59.toByte)
    checkEvaluation(IntervalMinutes("-59 minute"), -59.toByte)
    // Years and months must not be taken into account
    checkEvaluation(IntervalMinutes("100 year 10 months 10 minutes"), 10.toByte)
    checkEvaluation(IntervalMinutes(largeInterval), 59.toByte)
  }

  test("seconds") {
    checkEvaluation(IntervalSeconds("0 second"), Decimal(0, 8, 6))
    checkEvaluation(IntervalSeconds("1 second"), Decimal(1.0, 8, 6))
    checkEvaluation(IntervalSeconds("-1 second"), Decimal(-1.0, 8, 6))
    checkEvaluation(IntervalSeconds("1 minute 59 second"), Decimal(59.0, 8, 6))
    checkEvaluation(IntervalSeconds("-59 minutes -59 seconds"), Decimal(-59.0, 8, 6))
    // Years and months must not be taken into account
    checkEvaluation(IntervalSeconds("100 year 10 months 10 seconds"), Decimal(10.0, 8, 6))
    checkEvaluation(IntervalSeconds(largeInterval), Decimal(59.999999, 8, 6))
    checkEvaluation(
      IntervalSeconds("10 seconds 1 milliseconds 1 microseconds"),
      Decimal(10001001, 8, 6))
    checkEvaluation(IntervalSeconds("61 seconds 1 microseconds"), Decimal(1000001, 8, 6))
  }

  test("milliseconds") {
    checkEvaluation(IntervalMilliseconds("0 milliseconds"), Decimal(0, 8, 3))
    checkEvaluation(IntervalMilliseconds("1 milliseconds"), Decimal(1.0, 8, 3))
    checkEvaluation(IntervalMilliseconds("-1 milliseconds"), Decimal(-1.0, 8, 3))
    checkEvaluation(IntervalMilliseconds("1 second 999 milliseconds"), Decimal(1999.0, 8, 3))
    checkEvaluation(
      IntervalMilliseconds("999 milliseconds 1 microsecond"),
      Decimal(999.001, 8, 3))
    checkEvaluation(IntervalMilliseconds("-1 second -999 milliseconds"), Decimal(-1999.0, 8, 3))
    // Years and months must not be taken into account
    checkEvaluation(IntervalMilliseconds("100 year 1 millisecond"), Decimal(1.0, 8, 3))
    checkEvaluation(IntervalMilliseconds(largeInterval), Decimal(59999.999, 8, 3))
  }

  test("microseconds") {
    checkEvaluation(IntervalMicroseconds("0 microseconds"), 0L)
    checkEvaluation(IntervalMicroseconds("1 microseconds"), 1L)
    checkEvaluation(IntervalMicroseconds("-1 microseconds"), -1L)
    checkEvaluation(IntervalMicroseconds("1 second 999 microseconds"), 1000999L)
    checkEvaluation(IntervalMicroseconds("999 milliseconds 1 microseconds"), 999001L)
    checkEvaluation(IntervalMicroseconds("-1 second -999 microseconds"), -1000999L)
    // Years and months must not be taken into account
    checkEvaluation(IntervalMicroseconds("11 year 1 microseconds"), 1L)
    checkEvaluation(IntervalMicroseconds(largeInterval), 59999999L)
  }

  test("epoch") {
    checkEvaluation(IntervalEpoch("0 months"), Decimal(0.0, 18, 6))
    checkEvaluation(IntervalEpoch("10000 years"), Decimal(315569520000.0, 18, 6))
    checkEvaluation(IntervalEpoch("1 year"), Decimal(31556952.0, 18, 6))
    checkEvaluation(IntervalEpoch("-1 year"), Decimal(-31556952.0, 18, 6))
    checkEvaluation(
      IntervalEpoch("1 second 1 millisecond 1 microsecond"),
      Decimal(1.001001, 18, 6))
  }
}
