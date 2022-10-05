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
  test("MaskCcn") {
    // Positive tests
    compare(tryMask = false, input = "123", format = "XXX", expected = "XXX")
    compare(tryMask = false, input = "123", format = "XX9", expected = "XX3")
    compare(tryMask = false, input = "1-2-3", format = "9-9-X", expected = "1-2-X")
    compare(tryMask = false, input = "  1-2-3  ", format = "9-9-X", expected = "  1-2-X  ")
    compare(tryMask = false, input = "  123  ", format = "  XXX  ", expected = "  XXX  ")
    // Negative tests
    expectTypeCheckFailure(input = "123", format = "XYZ")
    expectTypeCheckFailure(input = "123", format = "")
    expectError(input = "12", format = "XXX")
    expectError(input = "1234", format = "XXX")
    expectError(input = "1-2-3", format = "XXX")
    expectError(input = "123", format = "X-X-X")
    expectError(input = "  123  ", format = "X-X-X")
  }

  test("TryMaskCcn") {
    // Positive tests
    compare(tryMask = true, input = "123", format = "XXX", expected = "XXX")
    compare(tryMask = true, input = "123", format = "XX9", expected = "XX3")
    compare(tryMask = true, input = "1-2-3", format = "9-9-X", expected = "1-2-X")
    compare(tryMask = true, input = "  1-2-3  ", format = "9-9-X", expected = "  1-2-X  ")
    compare(tryMask = true, input = "  123  ", format = "  XXX  ", expected = "  XXX  ")
    // Negative tests
    expectTypeCheckFailure(input = "123", format = "XYZ")
    expectTypeCheckFailure(input = "123", format = "")
    expectNull(input = "12", format = "XXX")
    expectNull(input = "1234", format = "XXX")
    expectNull(input = "1-2-3", format = "XXX")
    expectNull(input = "123", format = "X-X-X")
    expectNull(input = "  123  ", format = "X-X-X")
  }

  private def compare(
      tryMask: Boolean, input: String, format: String, expected: String): Unit = {
    val fn = if (tryMask) {
      TryMaskCcn(Literal(input), Literal(format))
    } else {
      MaskCcn(Literal(input), Literal(format))
    }
    assert(fn.checkInputDataTypes() == TypeCheckResult.TypeCheckSuccess)
    checkEvaluation(fn, expected)
  }

  private def expectTypeCheckFailure(input: String, format: String): Unit = {
    val fn = MaskCcn(Literal(input), Literal(format))
    val exception = intercept[AnalysisException](fn.checkInputDataTypes())
    checkError(exception,
      errorClass = "REDACTION_FUNCTION_ERROR",
      errorSubClass = Some("MASK_CCN_INVALID_FORMAT_ERROR"),
      parameters = Map("functionName" -> "mask_ccn"))
  }

  private def expectError(input: String, format: String): Unit = {
    val fn = MaskCcn(Literal(input), Literal(format))
    assert(fn.checkInputDataTypes() == TypeCheckResult.TypeCheckSuccess)
    checkExceptionInExpression[SparkRuntimeException](
      fn, "does not match the provided or default format")
  }

  private def expectNull(input: String, format: String): Unit = {
    val fn = TryMaskCcn(Literal(input), Literal(format))
    assert(fn.checkInputDataTypes() == TypeCheckResult.TypeCheckSuccess)
    checkEvaluation(fn, null)
  }
}
