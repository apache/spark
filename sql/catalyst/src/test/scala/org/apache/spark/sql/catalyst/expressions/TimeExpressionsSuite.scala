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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
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

  test("HourExpressionBuilder") {
    // Empty expressions list
    checkError(
      exception = intercept[AnalysisException] {
        HourExpressionBuilder.build("hour", Seq.empty)
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`hour`",
        "expectedNum" -> "> 0",
        "actualNum" -> "0",
        "docroot" -> SPARK_DOC_ROOT)
    )

    // test TIME-typed child should build HoursOfTime
    val timeExpr = Literal(localTime(12, 58, 59), TimeType())
    val builtExprForTime = HourExpressionBuilder.build("hour", Seq(timeExpr))
    assert(builtExprForTime.isInstanceOf[HoursOfTime])
    assert(builtExprForTime.asInstanceOf[HoursOfTime].child eq timeExpr)

    // test non TIME-typed child should build hour
    val tsExpr = Literal("2007-09-03 10:45:23")
    val builtExprForTs = HourExpressionBuilder.build("hour", Seq(tsExpr))
    assert(builtExprForTs.isInstanceOf[Hour])
    assert(builtExprForTs.asInstanceOf[Hour].child eq tsExpr)
  }

  test("Hour with TIME type") {
    // A few test times in microseconds since midnight:
    //   time in microseconds -> expected hour
    val testTimes = Seq(
      localTime() -> 0,
      localTime(1) -> 1,
      localTime(0, 59) -> 0,
      localTime(14, 30) -> 14,
      localTime(12, 58, 59) -> 12,
      localTime(23, 0, 1) -> 23,
      localTime(23, 59, 59, 999999) -> 23
    )

    // Create a literal with TimeType() for each test microsecond value
    // evaluate HoursOfTime(...), and check that the result matches the expected hour.
    testTimes.foreach { case (micros, expectedHour) =>
      checkEvaluation(
        HoursOfTime(Literal(micros, TimeType())),
        expectedHour)
    }

    // Verify NULL handling
    checkEvaluation(
      HoursOfTime(Literal.create(null, TimeType(TimeType.MICROS_PRECISION))),
      null
    )

    checkConsistencyBetweenInterpretedAndCodegen(
      (child: Expression) => HoursOfTime(child).replacement, TimeType())
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

    checkConsistencyBetweenInterpretedAndCodegen(
      (child: Expression) => MinutesOfTime(child).replacement, TimeType())
  }

  test("SecondExpressionBuilder") {
    // Empty expressions list
    checkError(
      exception = intercept[AnalysisException] {
        SecondExpressionBuilder.build("second", Seq.empty)
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`second`",
        "expectedNum" -> "> 0",
        "actualNum" -> "0",
        "docroot" -> SPARK_DOC_ROOT)
    )

    // test TIME-typed child should build SecondsOfTime
    val timeExpr = Literal(localTime(12, 58, 59), TimeType())
    val builtExprForTime = SecondExpressionBuilder.build("second", Seq(timeExpr))
    assert(builtExprForTime.isInstanceOf[SecondsOfTime])
    assert(builtExprForTime.asInstanceOf[SecondsOfTime].child eq timeExpr)

    // test non TIME-typed child should build second
    val tsExpr = Literal("2007-09-03 10:45:23")
    val builtExprForTs = SecondExpressionBuilder.build("second", Seq(tsExpr))
    assert(builtExprForTs.isInstanceOf[Second])
    assert(builtExprForTs.asInstanceOf[Second].child eq tsExpr)
  }

  test("Second with TIME type") {
    // A few test times in microseconds since midnight:
    //   time in microseconds -> expected second
    val testTimes = Seq(
      localTime() -> 0,
      localTime(1) -> 0,
      localTime(0, 59) -> 0,
      localTime(14, 30) -> 0,
      localTime(12, 58, 59) -> 59,
      localTime(23, 0, 1) -> 1,
      localTime(23, 59, 59, 999999) -> 59
    )

    // Create a literal with TimeType() for each test microsecond value
    // evaluate SecondsOfTime(...), and check that the result matches the expected second.
    testTimes.foreach { case (micros, expectedSecond) =>
      checkEvaluation(
        SecondsOfTime(Literal(micros, TimeType())),
        expectedSecond)
    }

    // Verify NULL handling
    checkEvaluation(
      SecondsOfTime(Literal.create(null, TimeType(TimeType.MICROS_PRECISION))),
      null
    )

    checkConsistencyBetweenInterpretedAndCodegen(
      (child: Expression) => SecondsOfTime(child).replacement, TimeType())
  }

  test("CurrentTimeExpressionBuilder") {
    // Case 1: Zero arguments => should build CurrentTime(6) by default
    val builtExprNoArg = CurrentTimeExpressionBuilder.build("current_time", Seq.empty)
    assert(builtExprNoArg.isInstanceOf[CurrentTime])
    assert(builtExprNoArg.asInstanceOf[CurrentTime].precision == 6)

    // Case 2: One integer argument => e.g. current_time(3) => CurrentTime(3)
    val intArg = Literal(3)
    val builtExprOneArg = CurrentTimeExpressionBuilder.build("current_time", Seq(intArg))
    assert(builtExprOneArg.isInstanceOf[CurrentTime])
    assert(builtExprOneArg.asInstanceOf[CurrentTime].precision == 3)

    // Case 3: One non-integer literal => should fail
    checkError(
      exception = intercept[AnalysisException] {
        // e.g. current_time('string')
        val strArg = Literal("foo")
        CurrentTimeExpressionBuilder.build("current_time", Seq(strArg))
      },
      condition = "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT",
      parameters = Map(
        "sqlExpr" -> "\"foo\"",
        "inputName" -> "`precision`",
        "inputType" -> "\"INT\"",
        "inputExpr" -> "\"foo\""
      )
    )

    // Case 4: More than one argument => e.g. current_time(3, 4) => fail
    checkError(
      exception = intercept[AnalysisException] {
        val multiArgs = Seq(Literal(3), Literal(4))
        CurrentTimeExpressionBuilder.build("current_time", multiArgs)
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`current_time`",
        "expectedNum" -> "[0, 1]",
        "actualNum" -> "2",
        "docroot" -> SPARK_DOC_ROOT
        // Update keys/values to match your builder’s actual thrown error
      )
    )
  }

  test("CurrentTime") {
    // test valid precision
    var expr = CurrentTime(3)
    assert(expr.children.isEmpty)
    assert(expr.dataType == TimeType(3), "Should produce TIME(3) data type")
    assert(expr.checkInputDataTypes() == TypeCheckSuccess)

    // test default constructor => TIME(6)
    expr = CurrentTime()
    assert(expr.precision == 6, "Default precision should be 6")
    assert(expr.dataType == TimeType(6))
    assert(expr.checkInputDataTypes() == TypeCheckSuccess)

    // test out of range precision => checkInputDataTypes fails
    expr = CurrentTime(10)
    val result = expr.checkInputDataTypes()
    assert(result.isInstanceOf[TypeCheckFailure])
    val failure = result.asInstanceOf[TypeCheckFailure]
    assert(failure.message.contains("Invalid precision 10. Must be between 0 and 6."))
  }
}
