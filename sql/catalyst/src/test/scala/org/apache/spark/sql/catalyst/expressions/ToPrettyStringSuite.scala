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
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC_OPT
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class ToPrettyStringSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("CalendarInterval as pretty strings") {
    checkEvaluation(
      ToPrettyString(Cast(Literal("interval -3 month 1 day 7 hours"), CalendarIntervalType)),
      "-3 months 1 days 7 hours")
  }

  test("Binary as pretty strings") {
    checkEvaluation(ToPrettyString(Cast(Literal("abcdef"), BinaryType)), "[61 62 63 64 65 66]")
  }

  test("Date as pretty strings") {
    checkEvaluation(ToPrettyString(Cast(Literal("1980-12-17"), DateType, UTC_OPT)), "1980-12-17")
  }

  test("Timestamp as pretty strings") {
    checkEvaluation(
      ToPrettyString(Cast(Literal("2012-11-30 09:19:00"), TimestampType, UTC_OPT)),
      "2012-11-30 01:19:00")
  }

  test("TimestampNTZ as pretty strings") {
    checkEvaluation(ToPrettyString(Literal(1L, TimestampNTZType)), "1970-01-01 00:00:00.000001")
  }

  test("Array as pretty strings") {
    checkEvaluation(ToPrettyString(Literal.create(Array(1, 2, 3, 4, 5))), "[1, 2, 3, 4, 5]")
  }

  test("Map as pretty strings") {
    checkEvaluation(
      ToPrettyString(Literal.create(Map(1 -> "a", 2 -> "b", 3 -> "c"))),
      "{1 -> a, 2 -> b, 3 -> c}")
  }

  test("Struct as pretty strings") {
    checkEvaluation(ToPrettyString(Literal.create((1, "a", 0.1))), "{1, a, 0.1}")
    checkEvaluation(
      ToPrettyString(Literal.create(Tuple2[String, String](null, null))),
      "{NULL, NULL}"
    )
  }

  test("YearMonthInterval as pretty strings") {
    checkEvaluation(
      ToPrettyString(Cast(Literal("INTERVAL '1-0' YEAR TO MONTH"), YearMonthIntervalType())),
      "INTERVAL '1-0' YEAR TO MONTH")
  }

  test("DayTimeInterval as pretty strings") {
    checkEvaluation(
      ToPrettyString(Cast(Literal("INTERVAL '1 2:03:04' DAY TO SECOND"), DayTimeIntervalType())),
      "INTERVAL '1 02:03:04' DAY TO SECOND")
  }

  test("Decimal as pretty strings") {
    checkEvaluation(
      ToPrettyString(Cast(Literal(1234.65), DecimalType(6, 2))), "1234.65")
  }

  test("String as pretty strings") {
    checkEvaluation(ToPrettyString(Literal("s")), "s")
  }

  test("Char as pretty strings") {
    checkEvaluation(ToPrettyString(Literal.create('a', CharType(5))), "a")
    withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true") {
      checkEvaluation(ToPrettyString(Literal.create('a', CharType(5))), "a    ")
    }
  }

  test("Byte as pretty strings") {
    checkEvaluation(ToPrettyString(Cast(Literal(8), ByteType)), "8")
  }

  test("Short as pretty strings") {
    checkEvaluation(ToPrettyString(Cast(Literal(8), ShortType)), "8")
  }

  test("Int as pretty strings") {
    checkEvaluation(ToPrettyString(Literal(1)), "1")
  }

  test("Long as pretty strings") {
    checkEvaluation(ToPrettyString(Literal(1L)), "1")
  }

  test("Float as pretty strings") {
    checkEvaluation(ToPrettyString(Cast(Literal(8), FloatType)), "8.0")
  }

  test("Double as pretty strings") {
    checkEvaluation(ToPrettyString(Cast(Literal(8), DoubleType)), "8.0")
  }

  test("Boolean as pretty strings") {
    checkEvaluation(ToPrettyString(Literal(false)), "false")
    checkEvaluation(ToPrettyString(Literal(true)), "true")
  }

  test("Variant as pretty strings") {
    val v = new VariantVal(Array[Byte](1, 2, 3), Array[Byte](1, 1))
    checkEvaluation(ToPrettyString(Literal(v)), UTF8String.fromString(v.toString))
  }

  test("sql method is equalivalent to child's sql") {
    val child = Literal(1)
    val prettyString = ToPrettyString(child)
    assert(prettyString.sql === child.sql)
  }

  test("Time as pretty strings") {
    checkEvaluation(ToPrettyString(Literal(1000L, TimeType())), "00:00:00.001")
    checkEvaluation(ToPrettyString(Literal(1L, TimeType())), "00:00:00.000001")
    checkEvaluation(ToPrettyString(Literal(
      (23 * 3600 + 59 * 60 + 59) * 1000000L, TimeType())), "23:59:59")
  }
}
