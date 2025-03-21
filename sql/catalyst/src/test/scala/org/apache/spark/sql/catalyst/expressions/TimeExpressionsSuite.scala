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

import java.time.LocalTime

import org.apache.spark.{SparkDateTimeException, SparkFunSuite}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.types.{Decimal, DecimalType, IntegerType, StringType}

class TimeExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("ParseToTime") {
    checkEvaluation(new ToTime(Literal("00:00:00"), Literal.create(null)), null)
    checkEvaluation(new ToTime(Literal("00:00:00"), NonFoldableLiteral(null, StringType)), null)
    checkEvaluation(new ToTime(Literal(null, StringType), Literal("HH:mm:ss")), null)

    checkEvaluation(new ToTime(Literal("00:00:00")), localTime())
    checkEvaluation(new ToTime(Literal("23-59-00.000999"), Literal("HH-mm-ss.SSSSSS")),
      localTime(23, 59, 0, 999))
    checkEvaluation(
      new ToTime(Literal("12.00.59.90909"), NonFoldableLiteral("HH.mm.ss.SSSSS")),
      localTime(12, 0, 59, 909090))
    checkEvaluation(
      new ToTime(NonFoldableLiteral(" 12:00.909 "), Literal(" HH:mm.SSS ")),
      localTime(12, 0, 0, 909000))
    checkEvaluation(
      new ToTime(
        NonFoldableLiteral("12 hours 123 millis"),
        NonFoldableLiteral("HH 'hours' SSS 'millis'")),
      localTime(12, 0, 0, 123000))

    checkErrorInExpression[SparkDateTimeException](
      expression = new ToTime(Literal("100:50")),
      condition = "CANNOT_PARSE_TIME",
      parameters = Map("input" -> "'100:50'", "format" -> "'HH:mm:ss.SSSSSS'"))
    checkErrorInExpression[SparkDateTimeException](
      expression = new ToTime(Literal("100:50"), Literal("mm:HH")),
      condition = "CANNOT_PARSE_TIME",
      parameters = Map("input" -> "'100:50'", "format" -> "'mm:HH'"))
  }

  test("creating values of TimeType via make_time") {
    // basic case
    checkEvaluation(
      MakeTime(Literal(13), Literal.create(2, IntegerType),
        Literal(Decimal(BigDecimal(23.5), 16, 6))),
      LocalTime.of(13, 2, 23, 500000000))

    // null cases
    checkEvaluation(
      MakeTime(Literal.create(null, IntegerType), Literal(18),
        Literal(Decimal(BigDecimal(23.5), 16, 6))),
      null)
    checkEvaluation(
      MakeTime(Literal(13), Literal.create(null, IntegerType),
        Literal(Decimal(BigDecimal(23.5), 16, 6))),
      null)
    checkEvaluation(MakeTime(Literal(13), Literal(18),
      Literal.create(null, DecimalType(16, 6))), null)

    // Invalid cases
    val errorCode = "DATETIME_FIELD_OUT_OF_BOUNDS.WITHOUT_SUGGESTION"
    checkErrorInExpression[SparkDateTimeException](
      MakeTime(Literal(25), Literal(2), Literal(Decimal(BigDecimal(23.5), 16, 6))),
      errorCode,
      Map("rangeMessage" -> "Invalid value for HourOfDay (valid values 0 - 23): 25")
    )
    checkErrorInExpression[SparkDateTimeException](
      MakeTime(Literal(23), Literal(-1), Literal(Decimal(BigDecimal(23.5), 16, 6))),
      errorCode,
      Map("rangeMessage" -> "Invalid value for MinuteOfHour (valid values 0 - 59): -1")
    )
    checkErrorInExpression[SparkDateTimeException](
      MakeTime(Literal(23), Literal(12), Literal(Decimal(BigDecimal(100.5), 16, 6))),
      errorCode,
      Map("rangeMessage" -> "Invalid value for SecondOfMinute (valid values 0 - 59): 100")
    )
  }
}
