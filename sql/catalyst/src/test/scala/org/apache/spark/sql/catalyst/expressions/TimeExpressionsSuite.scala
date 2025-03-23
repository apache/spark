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

import org.apache.spark.{SPARK_DOC_ROOT, SparkDateTimeException, SparkFunSuite}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.types.{StringType, TimeType}

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

  test("MinuteExpressionBuilder") {
    // Empty expressions list
    checkError(
      exception = intercept[AnalysisException] {
        MinuteExpressionBuilder.build("minute", Seq.empty)
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`minute`",
        "expectedNum" -> "> 0",
        "actualNum" -> "0",
        "docroot" -> SPARK_DOC_ROOT)
    )

    // test TIME-typed child should build MinutesOfTime
    val timeExpr = Literal(localTime(12, 58, 59), TimeType())
    val builtExprForTime = MinuteExpressionBuilder.build("minute", Seq(timeExpr))
    assert(builtExprForTime.isInstanceOf[MinutesOfTime])
    assert(builtExprForTime.asInstanceOf[MinutesOfTime].child eq timeExpr)

    // test non TIME-typed child should build Minute
    val tsExpr = Literal("2009-07-30 12:58:59")
    val builtExprForTs = MinuteExpressionBuilder.build("minute", Seq(tsExpr))
    assert(builtExprForTs.isInstanceOf[Minute])
    assert(builtExprForTs.asInstanceOf[Minute].child eq tsExpr)
  }

  test("Minute with TIME type") {
    // A few test times in microseconds since midnight:
    //   time in microseconds -> expected minute
    val testTimes = Seq(
      localTime() -> 0,
      localTime(1) -> 0,
      localTime(0, 59) -> 59,
      localTime(14, 30) -> 30,
      localTime(12, 58, 59) -> 58,
      localTime(23, 0, 1) -> 0,
      localTime(23, 59, 59, 999999) -> 59
    )

    // Create a literal with TimeType() for each test microsecond value
    // evaluate MinutesOfTime(...), and check that the result matches the expected minute.
    testTimes.foreach { case (micros, expectedMinute) =>
      checkEvaluation(
        MinutesOfTime(Literal(micros, TimeType())),
        expectedMinute)
    }

    // Verify NULL handling
    checkEvaluation(
      MinutesOfTime(Literal.create(null, TimeType(TimeType.MICROS_PRECISION))),
      null
    )
  }
}
