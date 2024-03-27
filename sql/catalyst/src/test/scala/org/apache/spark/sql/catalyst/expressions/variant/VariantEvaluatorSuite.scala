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

package org.apache.spark.sql.catalyst.expressions.variant

import org.apache.spark.{SparkFunSuite, SparkThrowable}
import org.apache.spark.types.variant.VariantBuilder
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class VariantEvaluatorSuite extends SparkFunSuite {

  test("evaluate") {
    val json = """{"a": 1}"""
    val input: UTF8String = UTF8String.fromString(json)
    val actual = VariantEvaluator.evaluate(input)
    val v = VariantBuilder.parseJson(json)
    val expected = new VariantVal(v.getValue, v.getMetadata)
    assert(actual === expected)
  }

  test("evaluate - input is null") {
    val input: UTF8String = null
    val expected = VariantEvaluator.evaluate(input)
    assert(expected == null)
  }

  test("evaluate - MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION") {
    val input: UTF8String = UTF8String.fromString("[")
    checkError(
      exception = intercept[SparkThrowable] {
        VariantEvaluator.evaluate(input)
      },
      errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
      parameters = Map("badRecord" -> "[", "failFastMode" -> "FAILFAST")
    )
  }

  test("evaluate - VARIANT_SIZE_LIMIT") {
    val input: UTF8String = UTF8String.fromString("\"" + "a" * (16 * 1024 * 1024) + "\"")
    checkError(
      exception = intercept[SparkThrowable] {
        VariantEvaluator.evaluate(input)
      },
      errorClass = "VARIANT_SIZE_LIMIT",
      parameters = Map("sizeLimit" -> "16.0 MiB", "functionName" -> "`parse_json`")
    )
  }
}
