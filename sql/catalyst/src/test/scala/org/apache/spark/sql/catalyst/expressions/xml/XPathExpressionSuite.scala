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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StringType

/**
 * Test suite for various xpath functions.
 */
class XPathExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  /** A helper function that tests null and error behaviors for xpath expressions. */
  private def testNullAndErrorBehavior[T <: AnyRef](testExpr: (String, String, T) => Unit): Unit = {
    // null input should lead to null output
    testExpr("<a><b>b1</b><b id='b_2'>b2</b></a>", null, null.asInstanceOf[T])
    testExpr(null, "a", null.asInstanceOf[T])
    testExpr(null, null, null.asInstanceOf[T])

    // Empty input should also lead to null output
    testExpr("", "a", null.asInstanceOf[T])
    testExpr("<a></a>", "", null.asInstanceOf[T])
    testExpr("", "", null.asInstanceOf[T])

    // Test error message for invalid XML document
    val e1 = intercept[RuntimeException] { testExpr("<a>/a>", "a", null.asInstanceOf[T]) }
    assert(e1.getCause.getCause.getMessage.contains(
      "XML document structures must start and end within the same entity."))
    assert(e1.getMessage.contains("<a>/a>"))

    // Test error message for invalid xpath
    val e2 = intercept[RuntimeException] { testExpr("<a></a>", "!#$", null.asInstanceOf[T]) }
    assert(e2.getCause.getMessage.contains("Invalid XPath") &&
      e2.getCause.getMessage.contains("!#$"))
  }

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

    testNullAndErrorBehavior(testExpr)
  }

  test("xpath_short") {
    def testExpr[T](xml: String, path: String, expected: java.lang.Short): Unit = {
      checkEvaluation(
        XPathShort(Literal.create(xml, StringType), Literal.create(path, StringType)),
        expected)
    }

    testExpr("<a>this is not a number</a>", "a", 0.toShort)
    testExpr("<a>try a boolean</a>", "a = 10", 0.toShort)
    testExpr(
      "<a><b class=\"odd\">10000</b><b class=\"even\">2</b><b class=\"odd\">4</b><c>8</c></a>",
      "sum(a/b[@class=\"odd\"])",
      10004.toShort)

    testNullAndErrorBehavior(testExpr)
  }

  test("xpath_int") {
    def testExpr[T](xml: String, path: String, expected: java.lang.Integer): Unit = {
      checkEvaluation(
        XPathInt(Literal.create(xml, StringType), Literal.create(path, StringType)),
        expected)
    }

    testExpr("<a>this is not a number</a>", "a", 0)
    testExpr("<a>try a boolean</a>", "a = 10", 0)
    testExpr(
      "<a><b class=\"odd\">100000</b><b class=\"even\">2</b><b class=\"odd\">4</b><c>8</c></a>",
      "sum(a/b[@class=\"odd\"])",
      100004)

    testNullAndErrorBehavior(testExpr)
  }

  test("xpath_long") {
    def testExpr[T](xml: String, path: String, expected: java.lang.Long): Unit = {
      checkEvaluation(
        XPathLong(Literal.create(xml, StringType), Literal.create(path, StringType)),
        expected)
    }

    testExpr("<a>this is not a number</a>", "a", 0L)
    testExpr("<a>try a boolean</a>", "a = 10", 0L)
    testExpr(
      "<a><b class=\"odd\">9000000000</b><b class=\"even\">2</b><b class=\"odd\">4</b><c>8</c></a>",
      "sum(a/b[@class=\"odd\"])",
      9000000004L)

    testNullAndErrorBehavior(testExpr)
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

    testNullAndErrorBehavior(testExpr)
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

    testNullAndErrorBehavior(testExpr)
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

    testNullAndErrorBehavior(testExpr)
  }

  test("xpath") {
    def testExpr[T](xml: String, path: String, expected: Seq[String]): Unit = {
      checkEvaluation(
        XPathList(Literal.create(xml, StringType), Literal.create(path, StringType)),
        expected)
    }

    testExpr("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/text()", Seq.empty[String])
    testExpr("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/*/text()",
      Seq("b1", "b2", "b3", "c1", "c2"))
    testExpr("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b/text()",
      Seq("b1", "b2", "b3"))
    testExpr("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/c/text()", Seq("c1", "c2"))
    testExpr("<a><b class='bb'>b1</b><b>b2</b><b>b3</b><c class='bb'>c1</c><c>c2</c></a>",
      "a/*[@class='bb']/text()", Seq("b1", "c1"))

    checkEvaluation(
      Coalesce(Seq(
          GetArrayItem(XPathList(Literal("<a></a>"), Literal("a")), Literal(0)),
          Literal("nul"))), "nul")

    testNullAndErrorBehavior(testExpr)
  }

  test("accept only literal path") {
    def testExpr(exprCtor: (Expression, Expression) => Expression): Unit = {
      // Validate that literal (technically this is foldable) paths are supported
      val litPath = exprCtor(Literal("abcd"), Concat(Literal("/") :: Literal("/") :: Nil))
      assert(litPath.checkInputDataTypes().isSuccess)

      // Validate that non-foldable paths are not supported.
      val nonLitPath = exprCtor(Literal("abcd"), NonFoldableLiteral("/"))
      assert(nonLitPath.checkInputDataTypes() == DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "`path`",
          "inputType" -> "\"STRING\"",
          "inputExpr" -> "\"nonfoldableliteral()\"")
      ))
    }

    testExpr(XPathBoolean)
    testExpr(XPathShort)
    testExpr(XPathInt)
    testExpr(XPathLong)
    testExpr(XPathFloat)
    testExpr(XPathDouble)
    testExpr(XPathString)
  }
}
