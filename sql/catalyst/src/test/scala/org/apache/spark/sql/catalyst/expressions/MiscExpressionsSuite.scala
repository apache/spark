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

  private val zstdTestInput = ("Apache Spark " * 10).getBytes("UTF-8")

  test("ZStdCompress") {
    // Basic compression
    checkEvaluation(
      Base64(new ZstdCompress(Literal(zstdTestInput))),
      "KLUv/SCCpQAAaEFwYWNoZSBTcGFyayABABLS+QU=")

    // With level
    checkEvaluation(
      Base64(new ZstdCompress(Literal(zstdTestInput), Literal(5))),
      "KLUv/SCCpQAAaEFwYWNoZSBTcGFyayABABLS+QU=")

    // In streaming mode
    checkEvaluation(
      Base64(ZstdCompress(Literal(zstdTestInput), Literal(3), Literal(true))),
      "KLUv/QBYpAAAaEFwYWNoZSBTcGFyayABABLS+QUBAAA=")
  }

  test("ZStdDecompress") {
    // Basic decompression
    checkEvaluation(
      ZstdDecompress(new ZstdCompress(Literal(zstdTestInput))),
      zstdTestInput)

    // With different level
    checkEvaluation(
      ZstdDecompress(new ZstdCompress(Literal(zstdTestInput), Literal(20))),
      zstdTestInput)

    // In streaming mode
    checkEvaluation(
      ZstdDecompress(ZstdCompress(Literal(zstdTestInput), Literal(3), Literal(true))),
      zstdTestInput)

    // Invalid input
    checkErrorInExpression[SparkRuntimeException](
      ZstdDecompress(Literal("invalid input".getBytes("UTF-8"))),
      errorClass = "INVALID_PARAMETER_VALUE.ZSTD_DECOMPRESS_INPUT",
      parameters = Map("parameter" -> "`input`", "functionName" -> "`zstd_decompress`")
    )

    // Invalid input: empty byte array
    checkErrorInExpression[SparkRuntimeException](
      ZstdDecompress(Literal("".getBytes("UTF-8"))),
      errorClass = "INVALID_PARAMETER_VALUE.ZSTD_DECOMPRESS_INPUT",
      parameters = Map("parameter" -> "`input`", "functionName" -> "`zstd_decompress`")
    )
  }

  test("TryZStdDecompress") {
    // Valid input
    checkEvaluation(
      TryZstdDecompress(new ZstdCompress(Literal(zstdTestInput))),
      zstdTestInput)

    // Invalid input:
    checkEvaluation(
      TryZstdDecompress(Literal("invalid input".getBytes("UTF-8"))),
      null)

    // Invalid input: empty byte array
    checkEvaluation(
      TryZstdDecompress(Literal("".getBytes("UTF-8"))),
      null)
  }
}
