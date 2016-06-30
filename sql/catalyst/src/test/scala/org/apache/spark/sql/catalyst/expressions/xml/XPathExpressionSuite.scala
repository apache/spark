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

  test("xpath_boolean") {
    def testExpr[T](xml: String, path: String, expected: java.lang.Boolean): Unit = {
      checkEvaluation(
        XPathBoolean(Literal.create(xml, StringType), Literal.create(path, StringType)),
        expected)
    }

    testExpr("<a><b>b</b></a>", "a/b", true)
    testExpr("<a><b>b</b></a>", "a/c", false)
    testExpr("<a><b>b</b></a>", "a/b = \"b\"", true)
    testExpr("<a><b>b</b></a>", "a/b = \"c\"", false)
    testExpr("<a><b>10</b></a>", "a/b < 10", false)
    testExpr("<a><b>10</b></a>", "a/b = 10", true)
  }

  test("xpath_short") {
    def testExpr[T](xml: String, path: String, expected: java.lang.Short): Unit = {
      checkEvaluation(
        XPathShort(Literal.create(xml, StringType), Literal.create(path, StringType)),
        expected)
    }

    testExpr("<a>this is not a number</a>", "a", 0.toShort)
    testExpr("<a>try a boolean</a>", "a = 10", 0.toShort)
    testExpr("<a><b class=\"odd\">1</b><b class=\"even\">2</b><b class=\"odd\">4</b><c>8</c></a>",
      "sum(a/b[@class=\"odd\"])",
      5.toShort)
  }

  test("xpath_int") {
    def testExpr[T](xml: String, path: String, expected: java.lang.Integer): Unit = {
      checkEvaluation(
        XPathInt(Literal.create(xml, StringType), Literal.create(path, StringType)),
        expected)
    }

    testExpr("<a>this is not a number</a>", "a", 0)
    testExpr("<a>try a boolean</a>", "a = 10", 0)
    testExpr("<a><b class=\"odd\">1</b><b class=\"even\">2</b><b class=\"odd\">4</b><c>8</c></a>",
      "sum(a/b[@class=\"odd\"])",
      5)
  }

  test("xpath_long") {
    def testExpr[T](xml: String, path: String, expected: java.lang.Long): Unit = {
      checkEvaluation(
        XPathLong(Literal.create(xml, StringType), Literal.create(path, StringType)),
        expected)
    }

    testExpr("<a>this is not a number</a>", "a", 0L)
    testExpr("<a>try a boolean</a>", "a = 10", 0L)
    testExpr("<a><b class=\"odd\">1</b><b class=\"even\">2</b><b class=\"odd\">4</b><c>8</c></a>",
      "sum(a/b[@class=\"odd\"])",
      5L)
  }

  test("xpath_float") {
    def testExpr[T](xml: String, path: String, expected: java.lang.Float): Unit = {
      checkEvaluation(
        XPathFloat(Literal.create(xml, StringType), Literal.create(path, StringType)),
        expected)
    }

    testExpr("<a>this is not a number</a>", "a", Float.NaN)
    testExpr("<a>try a boolean</a>", "a = 10", 0.0F)
    testExpr("<a><b class=\"odd\">1</b><b class=\"even\">2</b><b class=\"odd\">4</b><c>8</c></a>",
      "sum(a/b[@class=\"odd\"])",
      5.0F)
  }

  test("xpath_double") {
    def testExpr[T](xml: String, path: String, expected: java.lang.Double): Unit = {
      checkEvaluation(
        XPathDouble(Literal.create(xml, StringType), Literal.create(path, StringType)),
        expected)
    }

    testExpr("<a>this is not a number</a>", "a", Double.NaN)
    testExpr("<a>try a boolean</a>", "a = 10", 0.0)
    testExpr("<a><b class=\"odd\">1</b><b class=\"even\">2</b><b class=\"odd\">4</b><c>8</c></a>",
      "sum(a/b[@class=\"odd\"])",
      5.0)
  }

  test("xpath_string") {
    def testExpr[T](xml: String, path: String, expected: String): Unit = {
      checkEvaluation(
        XPathString(Literal.create(xml, StringType), Literal.create(path, StringType)),
        expected)
    }

    testExpr("<a><b>bb</b><c>cc</c></a>", "a", "bbcc")
    testExpr("<a><b>bb</b><c>cc</c></a>", "a/b", "bb")
    testExpr("<a><b>bb</b><c>cc</c></a>", "a/c", "cc")
    testExpr("<a><b>bb</b><c>cc</c></a>", "a/d", "")
    testExpr("<a><b>b1</b><b>b2</b></a>", "//b", "b1")
    testExpr("<a><b>b1</b><b>b2</b></a>", "a/b[1]", "b1")
    testExpr("<a><b>b1</b><b id='b_2'>b2</b></a>", "a/b[@id='b_2']", "b2")
  }

  test("null handling") {
    // We only do this for one expression since they all share the same common base implementation
    checkEvaluation(
      XPathLong(Literal.create(null, StringType), Literal.create(null, StringType)), null)
    checkEvaluation(
      XPathLong(Literal.create("", StringType), Literal.create(null, StringType)), null)
    checkEvaluation(
      XPathLong(Literal.create(null, StringType), Literal.create("", StringType)), null)
  }

  test("invalid xml handling") {
    intercept[Exception] {
      checkEvaluation(
        XPathLong(Literal.create("<a>/a>", StringType), Literal.create("", StringType)), null)
    }
  }

  test("path cache invalidation") {
    // This is a test to ensure the expression is not reusing the path for different strings
    // We only do this for one expression since they all share the same common base implementation
    val expr = XPathBoolean(Literal("<a><b>b</b></a>"), 'path.string.at(0))
    checkEvaluation(expr, true, create_row("a/b"))
    checkEvaluation(expr, false, create_row("a/c"))
  }
}
