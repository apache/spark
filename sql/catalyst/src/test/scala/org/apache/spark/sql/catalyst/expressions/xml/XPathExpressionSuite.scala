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

package org.apache.spark.sql.catalyst.expressions.xml

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, Literal}
import org.apache.spark.sql.types.StringType

/**
 * Test suite for various xpath functions.
 */
class XPathExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  private def testBoolean[T](xml: String, path: String, expected: T): Unit = {
    checkEvaluation(
      XPathBoolean(Literal.create(xml, StringType), Literal.create(path, StringType)),
      expected)
  }

  test("xpath_boolean") {
    testBoolean("<a><b>b</b></a>", "a/b", true)
    testBoolean("<a><b>b</b></a>", "a/c", false)
    testBoolean("<a><b>b</b></a>", "a/b = \"b\"", true)
    testBoolean("<a><b>b</b></a>", "a/b = \"c\"", false)
    testBoolean("<a><b>10</b></a>", "a/b < 10", false)
    testBoolean("<a><b>10</b></a>", "a/b = 10", true)

    // null input
    testBoolean(null, null, null)
    testBoolean(null, "a", null)
    testBoolean("<a><b>10</b></a>", null, null)

    // exception handling for invalid input
    intercept[Exception] {
      testBoolean("<a>/a>", "a", null)
    }
  }

  test("xpath_boolean path cache invalidation") {
    // This is a test to ensure the expression is not reusing the path for different strings
    val expr = XPathBoolean(Literal("<a><b>b</b></a>"), 'path.string.at(0))
    checkEvaluation(expr, true, create_row("a/b"))
    checkEvaluation(expr, false, create_row("a/c"))
  }
}
