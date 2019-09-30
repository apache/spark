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
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval

class IntervalExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  implicit def interval(s: String): Literal = {
    Literal(CalendarInterval.fromString("interval " + s))
  }

  test("millenniums") {
    checkEvaluation(Millenniums("0 years"), 0)
    checkEvaluation(Millenniums("9999 years"), 9)
    checkEvaluation(Millenniums("1000 years"), 1)
    checkEvaluation(Millenniums("-2000 years"), -2)
    // Microseconds part must not be taken into account
    checkEvaluation(Millenniums("999 years 400 days"), 0)
    // Millennium must be taken from years and months
    checkEvaluation(Millenniums("999 years 12 months"), 1)
    checkEvaluation(Millenniums("1000 years -1 months"), 0)
  }

  test("centuries") {
    checkEvaluation(Centuries("0 years"), 0)
    checkEvaluation(Centuries("9999 years"), 99)
    checkEvaluation(Centuries("1000 years"), 10)
    checkEvaluation(Centuries("-2000 years"), -20)
    // Microseconds part must not be taken into account
    checkEvaluation(Centuries("99 years 400 days"), 0)
    // Century must be taken from years and months
    checkEvaluation(Centuries("99 years 12 months"), 1)
    checkEvaluation(Centuries("100 years -1 months"), 0)
  }

  test("decades") {
    checkEvaluation(Decades("0 years"), 0)
    checkEvaluation(Decades("9999 years"), 999)
    checkEvaluation(Decades("1000 years"), 100)
    checkEvaluation(Decades("-2000 years"), -200)
    // Microseconds part must not be taken into account
    checkEvaluation(Decades("9 years 400 days"), 0)
    // Decade must be taken from years and months
    checkEvaluation(Decades("9 years 12 months"), 1)
    checkEvaluation(Decades("10 years -1 months"), 0)
  }

  test("years") {
    checkEvaluation(Years("0 years"), 0)
    checkEvaluation(Years("9999 years"), 9999)
    checkEvaluation(Years("1000 years"), 1000)
    checkEvaluation(Years("-2000 years"), -2000)
    // Microseconds part must not be taken into account
    checkEvaluation(Years("9 years 400 days"), 9)
    // Year must be taken from years and months
    checkEvaluation(Years("9 years 12 months"), 10)
    checkEvaluation(Years("10 years -1 months"), 9)
  }

  test("quarters") {
    checkEvaluation(Quarters("0 months"), 1.toByte)
    checkEvaluation(Quarters("1 months"), 1.toByte)
    checkEvaluation(Quarters("-1 months"), 1.toByte)
    checkEvaluation(Quarters("2 months"), 1.toByte)
    checkEvaluation(Quarters("-2 months"), 1.toByte)
    checkEvaluation(Quarters("1 years -1 months"), 4.toByte)
    checkEvaluation(Quarters("-1 years 1 months"), -2.toByte)
    checkEvaluation(Quarters("2 years 3 months"), 2.toByte)
    checkEvaluation(Quarters("-2 years -3 months"), 0.toByte)
    checkEvaluation(Quarters("9999 years"), 1.toByte)
  }

  test("months") {
    checkEvaluation(Months("0 year"), 0.toByte)
    for (m <- -24 to 24) {
      checkEvaluation(Months(s"$m months"), (m % 12).toByte)
    }
    checkEvaluation(Months("1 year 10 months"), 10.toByte)
    checkEvaluation(Months("-2 year -10 months"), -10.toByte)
    checkEvaluation(Months("9999 years"), 0.toByte)
  }

  private val largeInterval: String = "9999 years 11 months " +
    "31 days 11 hours 59 minutes 59 seconds 999 milliseconds 999 microseconds"

  test("days") {
    checkEvaluation(Days("0 days"), 0L)
    checkEvaluation(Days("1 days 100 seconds"), 1L)
    checkEvaluation(Days("-1 days -100 seconds"), -1L)
    checkEvaluation(Days("-365 days"), -365L)
    checkEvaluation(Days("365 days"), 365L)
    // Years and months must not be taken into account
    checkEvaluation(Days("100 year 10 months 5 days"), 5L)
    checkEvaluation(Days(largeInterval), 31L)
  }

  test("hours") {
    checkEvaluation(Hours("0 hours"), 0.toByte)
    checkEvaluation(Hours("1 hour"), 1.toByte)
    checkEvaluation(Hours("-1 hour"), -1.toByte)
    checkEvaluation(Hours("23 hours"), 23.toByte)
    checkEvaluation(Hours("-23 hours"), -23.toByte)
    // Years and months must not be taken into account
    checkEvaluation(Hours("100 year 10 months 10 hours"), 10.toByte)
    checkEvaluation(Hours(largeInterval), 11.toByte)
  }

  test("minutes") {
    checkEvaluation(Minutes("0 minute"), 0.toByte)
    checkEvaluation(Minutes("1 minute"), 1.toByte)
    checkEvaluation(Minutes("-1 minute"), -1.toByte)
    checkEvaluation(Minutes("59 minute"), 59.toByte)
    checkEvaluation(Minutes("-59 minute"), -59.toByte)
    // Years and months must not be taken into account
    checkEvaluation(Minutes("100 year 10 months 10 minutes"), 10.toByte)
    checkEvaluation(Minutes(largeInterval), 59.toByte)
  }

  test("seconds") {
    checkEvaluation(Seconds("0 second"), Decimal(0, 8, 6))
    checkEvaluation(Seconds("1 second"), Decimal(1.0, 8, 6))
    checkEvaluation(Seconds("-1 second"), Decimal(-1.0, 8, 6))
    checkEvaluation(Seconds("1 minute 59 second"), Decimal(59.0, 8, 6))
    checkEvaluation(Seconds("-59 minutes -59 seconds"), Decimal(-59.0, 8, 6))
    // Years and months must not be taken into account
    checkEvaluation(Seconds("100 year 10 months 10 seconds"), Decimal(10.0, 8, 6))
    checkEvaluation(Seconds(largeInterval), Decimal(59.999999, 8, 6))
    checkEvaluation(Seconds("10 seconds 1 milliseconds 1 microseconds"), Decimal(10001001, 8, 6))
    checkEvaluation(Seconds("61 seconds 1 microseconds"), Decimal(1000001, 8, 6))
  }

  test("milliseconds") {
    checkEvaluation(Milliseconds("0 milliseconds"), Decimal(0, 8, 3))
    checkEvaluation(Milliseconds("1 milliseconds"), Decimal(1.0, 8, 3))
    checkEvaluation(Milliseconds("-1 milliseconds"), Decimal(-1.0, 8, 3))
    checkEvaluation(Milliseconds("1 second 999 milliseconds"), Decimal(1999.0, 8, 3))
    checkEvaluation(Milliseconds("999 milliseconds 1 microsecond"), Decimal(999.001, 8, 3))
    checkEvaluation(Milliseconds("-1 second -999 milliseconds"), Decimal(-1999.0, 8, 3))
    // Years and months must not be taken into account
    checkEvaluation(Milliseconds("100 year 1 millisecond"), Decimal(1.0, 8, 3))
    checkEvaluation(Milliseconds(largeInterval), Decimal(59999.999, 8, 3))
  }

  test("microseconds") {
    checkEvaluation(Microseconds("0 microseconds"), 0L)
    checkEvaluation(Microseconds("1 microseconds"), 1L)
    checkEvaluation(Microseconds("-1 microseconds"), -1L)
    checkEvaluation(Microseconds("1 second 999 microseconds"), 1000999L)
    checkEvaluation(Microseconds("999 milliseconds 1 microseconds"), 999001L)
    checkEvaluation(Microseconds("-1 second -999 microseconds"), -1000999L)
    // Years and months must not be taken into account
    checkEvaluation(Microseconds("11 year 1 microseconds"), 1L)
    checkEvaluation(Microseconds(largeInterval), 59999999L)
  }

  test("epoch") {
    checkEvaluation(Epoch("0 months"), Decimal(0.0, 18, 6))
    checkEvaluation(Epoch("10000 years"), Decimal(315576000000.0, 18, 6))
    checkEvaluation(Epoch("1 year"), Decimal(31557600.0, 18, 6))
    checkEvaluation(Epoch("-1 year"), Decimal(-31557600.0, 18, 6))
    checkEvaluation(Epoch("1 second 1 millisecond 1 microsecond"), Decimal(1.001001, 18, 6))
  }
}
