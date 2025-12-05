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

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.util.TypeUtils.ordinalNumber
import org.apache.spark.sql.types._

class MiscExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("RaiseError") {
    checkExceptionInExpression[RuntimeException](
      RaiseError(Literal("error message")),
      EmptyRow,
      "error message"
    )

    checkExceptionInExpression[RuntimeException](
      RaiseError(Literal.create(null, StringType)),
      EmptyRow,
      "[USER_RAISED_EXCEPTION] null"
    )

    // Expects a string
    assert(RaiseError(Literal(5)).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"MAP<STRING, STRING>\"",
          "inputSql" -> "\"map(errorMessage, 5)\"",
          "inputType" -> "\"MAP<STRING, INT>\""
        )
      )
    )
  }

  test("uuid") {
    checkEvaluation(Length(Uuid(Some(0))), 36)
    val r = new Random()
    val seed1 = Some(r.nextLong())
    assert(evaluateWithoutCodegen(Uuid(seed1)) === evaluateWithoutCodegen(Uuid(seed1)))
    assert(evaluateWithMutableProjection(Uuid(seed1)) ===
      evaluateWithMutableProjection(Uuid(seed1)))
    assert(evaluateWithUnsafeProjection(Uuid(seed1)) ===
      evaluateWithUnsafeProjection(Uuid(seed1)))

    val seed2 = Some(r.nextLong())
    assert(evaluateWithoutCodegen(Uuid(seed1)) !== evaluateWithoutCodegen(Uuid(seed2)))
    assert(evaluateWithMutableProjection(Uuid(seed1)) !==
      evaluateWithMutableProjection(Uuid(seed2)))
    assert(evaluateWithUnsafeProjection(Uuid(seed1)) !==
      evaluateWithUnsafeProjection(Uuid(seed2)))

    val seed3 = Literal.create(r.nextInt())
    assert(evaluateWithoutCodegen(new Uuid(seed3)) === evaluateWithoutCodegen(new Uuid(seed3)))
    assert(evaluateWithMutableProjection(new Uuid(seed3)) ===
      evaluateWithMutableProjection(new Uuid(seed3)))
    assert(evaluateWithUnsafeProjection(new Uuid(seed3)) ===
      evaluateWithUnsafeProjection(new Uuid(seed3)))
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
      checkEvaluationWithMutableProjection(PrintToStderr(inputExpr), 1)
      val outputCodegen = errorStream.toString
      (outputEval, outputCodegen)
    } finally {
      System.setErr(systemErr)
    }

    assert(outputCodegen.contains(s"Result of $inputExpr is 1"))
    assert(outputEval.contains(s"Result of $inputExpr is 1"))
  }
}
