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

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult

class RedactionFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  /** Represents a function type under test. */
  object FunctionTypes extends Enumeration {
    type FunctionType = Value
    val MaskCcnFn, MaskPhoneFn, TryMaskCcnFn, TryMaskPhoneFn = Value

    /** Helper method to generate a function expression of the indicated type. */
    def getFn(fnType: Value, input: String, format: String): Expression =
      fnType match {
        case MaskCcnFn => MaskCcn(Literal(input), Literal(format))
        case MaskPhoneFn => MaskPhone(Literal(input), Literal(format))
        case TryMaskCcnFn => TryMaskCcn(Literal(input), Literal(format))
        case TryMaskPhoneFn => TryMaskPhone(Literal(input), Literal(format))
      }
  }

  test("[Try]MaskCcn and [Try]MaskPhone positive tests") {
    Seq(
      FunctionTypes.MaskCcnFn,
      FunctionTypes.MaskPhoneFn,
      FunctionTypes.TryMaskCcnFn,
      FunctionTypes.TryMaskPhoneFn).foreach { functionType =>
      compare(functionType, input = "123", format = "XXX", expected = "XXX")
      compare(functionType, input = "123", format = "XX9", expected = "XX3")
      compare(functionType, input = "1-2-3", format = "9-9-X", expected = "1-2-X")
      compare(functionType, input = "  1-2-3  ", format = "9-9-X", expected = "  1-2-X  ")
      compare(functionType, input = "  123  ", format = "  XXX  ", expected = "  XXX  ")
      compare(functionType, input = "1-2-3", format = "X-X-X", expected = "X-X-X")
      compare(functionType, input = " +(1234-5678-9876-5432) ",
        format = "+(XXXX - XXXX - XXXX - XXXX)",
        expected = " +(XXXX-XXXX-XXXX-XXXX) ")
    }
  }

  test("[Try]MaskCcn and [Try]MaskPhone invalid input string negative tests") {
    Seq(
      FunctionTypes.MaskCcnFn,
      FunctionTypes.MaskPhoneFn).foreach { functionType =>
      expectError(functionType, input = "12", format = "XXX")
      expectError(functionType, input = "1234", format = "XXX")
      expectError(functionType, input = "1-2-3", format = "XXX")
      expectError(functionType, input = "123", format = "X-X-X")
      expectError(functionType, input = "  123  ", format = "X-X-X")
      expectError(functionType, input = " 1-2-3 ", format = "(X-X-X)")
      expectError(functionType, input = "+(1-2-3)", format = "(X-X-X)")
    }
    Seq(
      FunctionTypes.TryMaskCcnFn,
      FunctionTypes.TryMaskPhoneFn).foreach { functionType =>
      expectNull(functionType, input = "12", format = "XXX")
      expectNull(functionType, input = "1234", format = "XXX")
      expectNull(functionType, input = "1-2-3", format = "XXX")
      expectNull(functionType, input = "123", format = "X-X-X")
      expectNull(functionType, input = "  123  ", format = "X-X-X")
      expectNull(functionType, input = " 1-2-3 ", format = "(X-X-X)")
      expectNull(functionType, input = "+(1-2-3)", format = "(X-X-X)")
    }
  }

  test("[Try]MaskCcn and [Try]MaskPhone invalid format string negative tests") {
    expectTypeCheckFailure(
      functionType = MaskCcn(Literal("123"), Literal("XYZ")),
      functionName = "MASK_CCN",
      defaultFormat = "XXXX-XXXX-XXXX-XXXX")
    expectTypeCheckFailure(
      functionType = MaskPhone(Literal("123"), Literal("XYZ")),
      functionName = "MASK_PHONE",
      defaultFormat = "(XXX) XXX-XXXX")
    expectTypeCheckFailure(
      functionType = TryMaskCcn(Literal("123"), Literal("XYZ")),
      functionName = "TRY_MASK_CCN",
      defaultFormat = "XXXX-XXXX-XXXX-XXXX")
    expectTypeCheckFailure(
      functionType = TryMaskPhone(Literal("123"), Literal("XYZ")),
      functionName = "TRY_MASK_PHONE",
      defaultFormat = "(XXX) XXX-XXXX")
  }

  private def compare(
      functionType: FunctionTypes.FunctionType,
      input: String,
      format: String,
      expected: String): Unit = {
    val fn = FunctionTypes.getFn(functionType, input, format)
    assert(fn.checkInputDataTypes() == TypeCheckResult.TypeCheckSuccess)
    checkEvaluation(fn, expected)
  }

  private def expectTypeCheckFailure(
      functionType: Expression,
      functionName: String,
      defaultFormat: String): Unit = {
    val exception = intercept[AnalysisException](functionType.checkInputDataTypes())
    checkError(exception,
      errorClass = "REDACTION_FUNCTION_ERROR",
      errorSubClass = Some("MASK_INVALID_FORMAT_ERROR"),
      parameters = Map(
        "functionName" -> functionName,
        "defaultFormat" -> defaultFormat))
  }

  private def expectError(
      functionType: FunctionTypes.FunctionType,
      input: String,
      format: String): Unit = {
    val fn = FunctionTypes.getFn(functionType, input, format)
    assert(fn.checkInputDataTypes() == TypeCheckResult.TypeCheckSuccess)
    checkExceptionInExpression[SparkRuntimeException](
      fn, "does not match the provided or default format")
  }

  private def expectNull(
      functionType: FunctionTypes.FunctionType,
      input: String,
      format: String): Unit = {
    val fn = FunctionTypes.getFn(functionType, input, format)
    assert(fn.checkInputDataTypes() == TypeCheckResult.TypeCheckSuccess)
    checkEvaluation(fn, null)
  }
}
