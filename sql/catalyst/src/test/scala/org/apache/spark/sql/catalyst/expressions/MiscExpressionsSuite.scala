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

import java.io.PrintStream
import java.math.{BigDecimal => JavaBigDecimal}
import java.sql.{Date, Timestamp}

import scala.math.BigDecimal
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval


class MiscExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("assert_true") {
    intercept[RuntimeException] {
      checkEvaluation(AssertTrue(Literal.create(false, BooleanType)), null)
    }
    intercept[RuntimeException] {
      checkEvaluation(AssertTrue(Cast(Literal(0), BooleanType)), null)
    }
    intercept[RuntimeException] {
      checkEvaluation(AssertTrue(Literal.create(null, NullType)), null)
    }
    intercept[RuntimeException] {
      checkEvaluation(AssertTrue(Literal.create(null, BooleanType)), null)
    }
    checkEvaluation(AssertTrue(Literal.create(true, BooleanType)), null)
    checkEvaluation(AssertTrue(Cast(Literal(1), BooleanType)), null)
  }

  test("data_type") {
    checkEvaluation(GetDataType(Literal.create(null, NullType)), null)
    checkEvaluation(GetDataType(Literal(false)), "boolean")
    checkEvaluation(GetDataType(Literal(0: Byte)), "tinyint")
    checkEvaluation(GetDataType(Literal(0: Short)), "smallint")
    checkEvaluation(GetDataType(Literal(0)), "int")
    checkEvaluation(GetDataType(Literal(0L)), "bigint")
    checkEvaluation(GetDataType(Literal(0.0f)), "float")
    checkEvaluation(GetDataType(Literal(0.0)), "double")
    checkEvaluation(GetDataType(Literal(new Decimal().set(0.0))), "decimal(2,1)")
    checkEvaluation(GetDataType(Literal(0.0: BigDecimal)), "decimal(1,1)")
    checkEvaluation(GetDataType(Literal(new JavaBigDecimal(0.0d.toString))), "decimal(1,1)")
    checkEvaluation(GetDataType(Literal(new Timestamp(0L))), "timestamp")
    checkEvaluation(GetDataType(Literal(new Date(0L))), "date")
    checkEvaluation(GetDataType(Literal(Array(0.0, 0.1))), "array<double>")
    checkEvaluation(GetDataType(Literal(Array(0.0, 0.1))), "array<double>")
    checkEvaluation(GetDataType(Literal("".getBytes)), "binary")
    checkEvaluation(GetDataType(Literal(new CalendarInterval(0, 0))), "calendarinterval")
    checkEvaluation(GetDataType(Literal("a")), "string")
  }

  test("uuid") {
    checkEvaluation(Length(Uuid(Some(0))), 36)
    val r = new Random()
    val seed1 = Some(r.nextLong())
    assert(evaluateWithoutCodegen(Uuid(seed1)) === evaluateWithoutCodegen(Uuid(seed1)))
    assert(evaluateWithGeneratedMutableProjection(Uuid(seed1)) ===
      evaluateWithGeneratedMutableProjection(Uuid(seed1)))
    assert(evaluateWithUnsafeProjection(Uuid(seed1)) ===
      evaluateWithUnsafeProjection(Uuid(seed1)))

    val seed2 = Some(r.nextLong())
    assert(evaluateWithoutCodegen(Uuid(seed1)) !== evaluateWithoutCodegen(Uuid(seed2)))
    assert(evaluateWithGeneratedMutableProjection(Uuid(seed1)) !==
      evaluateWithGeneratedMutableProjection(Uuid(seed2)))
    assert(evaluateWithUnsafeProjection(Uuid(seed1)) !==
      evaluateWithUnsafeProjection(Uuid(seed2)))

    val uuid = Uuid(seed1)
    assert(uuid.fastEquals(uuid))
    assert(!uuid.fastEquals(Uuid(seed1)))
    assert(!uuid.fastEquals(uuid.freshCopy()))
    assert(!uuid.fastEquals(Uuid(seed2)))
  }

  test("PrintToStderr") {
    val inputExpr = Literal(1)
    val systemErr = System.err

    val (outputEval, outputCodegen) = try {
      val errorStream = new java.io.ByteArrayOutputStream()
      System.setErr(new PrintStream(errorStream))
      // check without codegen
      checkEvaluationWithoutCodegen(PrintToStderr(inputExpr), 1)
      val outputEval = errorStream.toString
      errorStream.reset()
      // check with codegen
      checkEvaluationWithGeneratedMutableProjection(PrintToStderr(inputExpr), 1)
      val outputCodegen = errorStream.toString
      (outputEval, outputCodegen)
    } finally {
      System.setErr(systemErr)
    }

    assert(outputCodegen.contains(s"Result of $inputExpr is 1"))
    assert(outputEval.contains(s"Result of $inputExpr is 1"))
  }
}
