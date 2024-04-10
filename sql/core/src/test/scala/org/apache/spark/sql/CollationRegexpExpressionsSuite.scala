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

package org.apache.spark.sql

import scala.collection.immutable.Seq

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class CollationRegexpExpressionsSuite
  extends QueryTest
  with SharedSparkSession
  with ExpressionEvalHelper {

  case class CollationTestCase[R](s1: String, s2: String, collation: String, expectedResult: R)
  case class CollationTestFail[R](s1: String, s2: String, collation: String)

  test("Support Like string expression with Collation") {
    def prepareLike(
        input: String,
        regExp: String,
        collation: String): Expression = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExpr = Literal.create(input, StringType(collationId))
      val regExpExpr = Literal.create(regExp, StringType(collationId))
      Like(inputExpr, regExpExpr, '\\')
    }
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABC", "%B%", "UTF8_BINARY", true)
    )
    checks.foreach(ct =>
      checkEvaluation(prepareLike(ct.s1, ct.s2, ct.collation), ct.expectedResult))
    // Unsupported collations
    val fails = Seq(
      CollationTestFail("ABC", "%b%", "UTF8_BINARY_LCASE"),
      CollationTestFail("ABC", "%B%", "UNICODE"),
      CollationTestFail("ABC", "%b%", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareLike(ct.s1, ct.s2, ct.collation)
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""'${ct.s1}' collate ${ct.collation}"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }

  test("Support ILike string expression with Collation") {
    def prepareILike(
        input: String,
        regExp: String,
        collation: String): Expression = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExpr = Literal.create(input, StringType(collationId))
      val regExpExpr = Literal.create(regExp, StringType(collationId))
      ILike(inputExpr, regExpExpr, '\\').replacement
    }

    // Supported collations
    val checks = Seq(
      CollationTestCase("ABC", "%b%", "UTF8_BINARY", true)
    )
    checks.foreach(ct =>
      checkEvaluation(prepareILike(ct.s1, ct.s2, ct.collation), ct.expectedResult)
    )
    // Unsupported collations
    val fails = Seq(
      CollationTestFail("ABC", "%b%", "UTF8_BINARY_LCASE"),
      CollationTestFail("ABC", "%b%", "UNICODE"),
      CollationTestFail("ABC", "%b%", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareILike(ct.s1, ct.s2, ct.collation)
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""lower('${ct.s1}' collate ${ct.collation})"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }

  test("Support RLike string expression with Collation") {
    def prepareRLike(
        input: String,
        regExp: String,
        collation: String): Expression = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExpr = Literal.create(input, StringType(collationId))
      val regExpExpr = Literal.create(regExp, StringType(collationId))
      RLike(inputExpr, regExpExpr)
    }
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABC", ".B.", "UTF8_BINARY", true)
    )
    checks.foreach(ct =>
      checkEvaluation(prepareRLike(ct.s1, ct.s2, ct.collation), ct.expectedResult)
    )
    // Unsupported collations
    val fails = Seq(
      CollationTestFail("ABC", ".b.", "UTF8_BINARY_LCASE"),
      CollationTestFail("ABC", ".B.", "UNICODE"),
      CollationTestFail("ABC", ".b.", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareRLike(ct.s1, ct.s2, ct.collation)
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""'${ct.s1}' collate ${ct.collation}"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }

  test("Support StringSplit string expression with Collation") {
    def prepareStringSplit(
        input: String,
        splitBy: String,
        collation: String): Expression = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExpr = Literal.create(input, StringType(collationId))
      val splitByExpr = Literal.create(splitBy, StringType(collationId))
      StringSplit(inputExpr, splitByExpr, Literal(-1))
    }

    // Supported collations
    val checks = Seq(
      CollationTestCase("ABC", "[B]", "UTF8_BINARY", Seq("A", "C"))
    )
    checks.foreach(ct =>
      checkEvaluation(prepareStringSplit(ct.s1, ct.s2, ct.collation), ct.expectedResult)
    )
    // Unsupported collations
    val fails = Seq(
      CollationTestFail("ABC", "[b]", "UTF8_BINARY_LCASE"),
      CollationTestFail("ABC", "[B]", "UNICODE"),
      CollationTestFail("ABC", "[b]", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareStringSplit(ct.s1, ct.s2, ct.collation)
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""'${ct.s1}' collate ${ct.collation}"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }

  test("Support RegExpReplace string expression with Collation") {
    def prepareRegExpReplace(
        input: String,
        regExp: String,
        collation: String): RegExpReplace = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExpr = Literal.create(input, StringType(collationId))
      val regExpExpr = Literal.create(regExp, StringType(collationId))
      RegExpReplace(inputExpr, regExpExpr, Literal.create("FFF", StringType(collationId)))
    }

    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UTF8_BINARY", "AFFFE")
    )
    checks.foreach(ct =>
      checkEvaluation(prepareRegExpReplace(ct.s1, ct.s2, ct.collation), ct.expectedResult)
    )
    // Unsupported collations
    val fails = Seq(
      CollationTestFail("ABCDE", ".c.", "UTF8_BINARY_LCASE"),
      CollationTestFail("ABCDE", ".C.", "UNICODE"),
      CollationTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareRegExpReplace(ct.s1, ct.s2, ct.collation)
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""'${ct.s1}' collate ${ct.collation}"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }

  test("Support RegExpExtract string expression with Collation") {
    def prepareRegExpExtract(
        input: String,
        regExp: String,
        collation: String): RegExpExtract = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExpr = Literal.create(input, StringType(collationId))
      val regExpExpr = Literal.create(regExp, StringType(collationId))
      RegExpExtract(inputExpr, regExpExpr, Literal(0))
    }

    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UTF8_BINARY", "BCD")
    )
    checks.foreach(ct =>
      checkEvaluation(prepareRegExpExtract(ct.s1, ct.s2, ct.collation), ct.expectedResult)
    )
    // Unsupported collations
    val fails = Seq(
      CollationTestFail("ABCDE", ".c.", "UTF8_BINARY_LCASE"),
      CollationTestFail("ABCDE", ".C.", "UNICODE"),
      CollationTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareRegExpExtract(ct.s1, ct.s2, ct.collation)
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""'${ct.s1}' collate ${ct.collation}"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }

  test("Support RegExpExtractAll string expression with Collation") {
    def prepareRegExpExtractAll(
        input: String,
        regExp: String,
        collation: String): RegExpExtractAll = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExpr = Literal.create(input, StringType(collationId))
      val regExpExpr = Literal.create(regExp, StringType(collationId))
      RegExpExtractAll(inputExpr, regExpExpr, Literal(0))
    }

    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UTF8_BINARY", Seq("BCD"))
    )
    checks.foreach(ct =>
      checkEvaluation(prepareRegExpExtractAll(ct.s1, ct.s2, ct.collation), ct.expectedResult)
    )
    // Unsupported collations
    val fails = Seq(
      CollationTestFail("ABCDE", ".c.", "UTF8_BINARY_LCASE"),
      CollationTestFail("ABCDE", ".C.", "UNICODE"),
      CollationTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareRegExpExtractAll(ct.s1, ct.s2, ct.collation)
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""'${ct.s1}' collate ${ct.collation}"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }

  test("Support RegExpCount string expression with Collation") {
    def prepareRegExpCount(
        input: String,
        regExp: String,
        collation: String): Expression = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExpr = Literal.create(input, StringType(collationId))
      val regExpExpr = Literal.create(regExp, StringType(collationId))
      RegExpCount(inputExpr, regExpExpr).replacement
    }

    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UTF8_BINARY", 1)
    )
    checks.foreach(ct =>
      checkEvaluation(prepareRegExpCount(ct.s1, ct.s2, ct.collation), ct.expectedResult)
    )
    // Unsupported collations
    val fails = Seq(
      CollationTestFail("ABCDE", ".c.", "UTF8_BINARY_LCASE"),
      CollationTestFail("ABCDE", ".C.", "UNICODE"),
      CollationTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareRegExpCount(ct.s1, ct.s2, ct.collation).asInstanceOf[Size].child
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""'${ct.s1}' collate ${ct.collation}"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }

  test("Support RegExpSubStr string expression with Collation") {
    def prepareRegExpSubStr(
        input: String,
        regExp: String,
        collation: String): Expression = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExpr = Literal.create(input, StringType(collationId))
      val regExpExpr = Literal.create(regExp, StringType(collationId))
      RegExpSubStr(inputExpr, regExpExpr).replacement.asInstanceOf[NullIf].left
    }

    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UTF8_BINARY", "BCD")
    )
    checks.foreach(ct =>
      checkEvaluation(prepareRegExpSubStr(ct.s1, ct.s2, ct.collation), ct.expectedResult)
    )
    // Unsupported collations
    val fails = Seq(
      CollationTestFail("ABCDE", ".c.", "UTF8_BINARY_LCASE"),
      CollationTestFail("ABCDE", ".C.", "UNICODE"),
      CollationTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareRegExpSubStr(ct.s1, ct.s2, ct.collation)
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""'${ct.s1}' collate ${ct.collation}"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }

  test("Support RegExpInStr string expression with Collation") {
    def prepareRegExpInStr(
        input: String,
        regExp: String,
        collation: String): RegExpInStr = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExpr = Literal.create(input, StringType(collationId))
      val regExpExpr = Literal.create(regExp, StringType(collationId))
      RegExpInStr(inputExpr, regExpExpr, Literal(0))
    }

    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UTF8_BINARY", 2)
    )
    checks.foreach(ct =>
      checkEvaluation(prepareRegExpInStr(ct.s1, ct.s2, ct.collation), ct.expectedResult)
    )
    // Unsupported collations
    val fails = Seq(
      CollationTestFail("ABCDE", ".c.", "UTF8_BINARY_LCASE"),
      CollationTestFail("ABCDE", ".C.", "UNICODE"),
      CollationTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareRegExpInStr(ct.s1, ct.s2, ct.collation)
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""'${ct.s1}' collate ${ct.collation}"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }
}

class CollationRegexpExpressionsANSISuite extends CollationRegexpExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
