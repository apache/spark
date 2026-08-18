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

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
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

  test("SPARK-55109: RaiseError.sql uses single-argument form only for known error classes") {
    assert(RaiseError(Literal("error!")).sql === "raise_error('error!')")

    // A custom errorClass should NOT produce the single-argument form
    val customError = RaiseError(
      Literal("CUSTOM_ERROR"),
      CreateMap(Seq(Literal("errorMessage"), Literal("error!"))))
    assert(customError.sql === "raise_error('CUSTOM_ERROR', map('errorMessage', 'error!'))")
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

  test("Hmac") {
    def bytes(hex: String): Array[Byte] =
      hex.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray

    val key = Literal("key".getBytes("UTF-8"))
    val message = Literal("message".getBytes("UTF-8"))

    // Default algorithm is SHA-256.
    checkEvaluation(
      new Hmac(key, message),
      bytes("6e9ef29b75fffc5b7abae527d58fdadb2fe42e7219011976917343065f58ed4a"))

    // Explicit algorithm.
    checkEvaluation(
      Hmac(key, message, Literal("SHA-1")),
      bytes("2088df74d5f2146b48146caf4965377e9d0be3a4"))

    // Null propagation.
    checkEvaluation(Hmac(Literal.create(null, BinaryType), message, Literal("SHA-256")), null)
    checkEvaluation(Hmac(key, Literal.create(null, BinaryType), Literal("SHA-256")), null)
    checkEvaluation(Hmac(key, message, Literal.create(null, StringType)), null)
  }

  test("Hmac unsupported algorithm") {
    checkErrorInExpression[SparkRuntimeException](
      Hmac(Literal("key".getBytes("UTF-8")), Literal("message".getBytes("UTF-8")),
        Literal("SHA-3")),
      "INVALID_PARAMETER_VALUE.HMAC_ALGORITHM",
      Map(
        "parameter" -> "`algorithm`",
        "functionName" -> "`hmac`",
        "algorithm" -> "'SHA-3'"))
  }
}
