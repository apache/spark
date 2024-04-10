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
import org.apache.spark.sql.catalyst.expressions.{Collation, ConcatWs, ExpressionEvalHelper, Literal, StringRepeat}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class CollationStringExpressionsSuite
  extends QueryTest
  with SharedSparkSession
  with ExpressionEvalHelper {

  test("Support ConcatWs string expression with Collation") {
    case class ConcatWsTestCase[R](s1: String, s2: String, collation: String, expectedResult: R)
    case class ConcatWsTestFail[R](s1: String, s2: String, collation: String)
    def prepareConcatWs(
        sep: String,
        collation: String,
        inputs: Any*): ConcatWs = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExprs = inputs.map(s => Literal.create(s, StringType(collationId)))
      val sepExpr = Literal.create(sep, StringType(collationId))
      ConcatWs(sepExpr +: inputExprs)
    }
    // Supported Collations
    val checks = Seq(
      ConcatWsTestCase("Spark", "SQL", "UTF8_BINARY", "Spark SQL")
    )
    checks.foreach(ct =>
      checkEvaluation(prepareConcatWs(" ", ct.collation, ct.s1, ct.s2), ct.expectedResult)
    )

    // Unsupported Collations
    val fails = Seq(
      ConcatWsTestFail("ABC", "%b%", "UTF8_BINARY_LCASE"),
      ConcatWsTestFail("ABC", "%B%", "UNICODE"),
      ConcatWsTestFail("ABC", "%b%", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareConcatWs(" ", ct.collation, ct.s1, ct.s2)
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""' ' collate ${ct.collation}"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }

  test("Support contains string expression with Collation") {
    case class ContainsTestCase[R](l: String, r: String, c: String, e: R)
    // Supported collations
    val testCases = Seq(
      ContainsTestCase("", "", "UTF8_BINARY", true),
      ContainsTestCase("c", "", "UTF8_BINARY", true),
      ContainsTestCase("", "c", "UTF8_BINARY", false),
      ContainsTestCase("abcde", "c", "UTF8_BINARY", true),
      ContainsTestCase("abcde", "C", "UTF8_BINARY", false),
      ContainsTestCase("abcde", "bcd", "UTF8_BINARY", true),
      ContainsTestCase("abcde", "BCD", "UTF8_BINARY", false),
      ContainsTestCase("abcde", "fgh", "UTF8_BINARY", false),
      ContainsTestCase("abcde", "FGH", "UTF8_BINARY", false),
      ContainsTestCase("", "", "UNICODE", true),
      ContainsTestCase("c", "", "UNICODE", true),
      ContainsTestCase("", "c", "UNICODE", false),
      ContainsTestCase("abcde", "c", "UNICODE", true),
      ContainsTestCase("abcde", "C", "UNICODE", false),
      ContainsTestCase("abcde", "bcd", "UNICODE", true),
      ContainsTestCase("abcde", "BCD", "UNICODE", false),
      ContainsTestCase("abcde", "fgh", "UNICODE", false),
      ContainsTestCase("abcde", "FGH", "UNICODE", false),
      ContainsTestCase("", "", "UTF8_BINARY_LCASE", true),
      ContainsTestCase("c", "", "UTF8_BINARY_LCASE", true),
      ContainsTestCase("", "c", "UTF8_BINARY_LCASE", false),
      ContainsTestCase("abcde", "c", "UTF8_BINARY_LCASE", true),
      ContainsTestCase("abcde", "C", "UTF8_BINARY_LCASE", true),
      ContainsTestCase("abcde", "bcd", "UTF8_BINARY_LCASE", true),
      ContainsTestCase("abcde", "BCD", "UTF8_BINARY_LCASE", true),
      ContainsTestCase("abcde", "fgh", "UTF8_BINARY_LCASE", false),
      ContainsTestCase("abcde", "FGH", "UTF8_BINARY_LCASE", false),
      ContainsTestCase("", "", "UNICODE_CI", true),
      ContainsTestCase("c", "", "UNICODE_CI", true),
      ContainsTestCase("", "c", "UNICODE_CI", false),
      ContainsTestCase("abcde", "c", "UNICODE_CI", true),
      ContainsTestCase("abcde", "C", "UNICODE_CI", true),
      ContainsTestCase("abcde", "bcd", "UNICODE_CI", true),
      ContainsTestCase("abcde", "BCD", "UNICODE_CI", true),
      ContainsTestCase("abcde", "fgh", "UNICODE_CI", false),
      ContainsTestCase("abcde", "FGH", "UNICODE_CI", false)
    )
    testCases.foreach(t => { checkAnswer(sql(
      s"SELECT contains(collate('${t.l}', '${t.c}'),collate('${t.r}', '${t.c}'))"
      ), Row(t.e))
    })
    // Casting
    val testCasting = Seq(
      ContainsTestCase("", "", "UTF8_BINARY", true),
      ContainsTestCase("x", "", "UTF8_BINARY", true),
      ContainsTestCase("", "x", "UTF8_BINARY", false),
      ContainsTestCase("abcde", "C", "UTF8_BINARY", false),
      ContainsTestCase("abcde", "C", "UTF8_BINARY_LCASE", true),
      ContainsTestCase("abcde", "C", "UNICODE", false),
      ContainsTestCase("abcde", "C", "UNICODE_CI", true)
    )
    testCasting.foreach(t => {
      checkAnswer(sql(s"SELECT contains(collate('${t.l}', '${t.c}'),'${t.r}')"), Row(t.e))
      checkAnswer(sql(s"SELECT contains('${t.l}', collate('${t.r}', '${t.c}'))"), Row(t.e))
    })
    val collationMismatch = intercept[AnalysisException] {
      sql(s"SELECT contains(collate('abcde', 'UTF8_BINARY_LCASE'),collate('C', 'UNICODE_CI'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("Support startsWith string expression with Collation") {
    case class StartsWithTestCase[R](l: String, r: String, c: String, e: R)
    // Supported collations
    val testCases = Seq(
      StartsWithTestCase("abcde", "abc", "UTF8_BINARY", true),
      StartsWithTestCase("abcde", "ABC", "UTF8_BINARY", false),
      StartsWithTestCase("abcde", "abc", "UNICODE", true),
      StartsWithTestCase("abcde", "ABC", "UNICODE", false),
      StartsWithTestCase("abcde", "ABC", "UTF8_BINARY_LCASE", true),
      StartsWithTestCase("abcde", "bcd", "UTF8_BINARY_LCASE", false),
      StartsWithTestCase("abcde", "ABC", "UNICODE_CI", true),
      StartsWithTestCase("abcde", "bcd", "UNICODE_CI", false)
    )
    testCases.foreach(t => {
      checkAnswer(sql(
        s"SELECT startswith(collate('${t.l}', '${t.c}'),collate('${t.r}', '${t.c}'))"
      ), Row(t.e))})
    // Casting
    val testCasting = Seq(
      StartsWithTestCase("", "", "UTF8_BINARY", true),
      StartsWithTestCase("x", "", "UTF8_BINARY", true),
      StartsWithTestCase("", "x", "UTF8_BINARY", false),
      StartsWithTestCase("abcde", "A", "UTF8_BINARY", false),
      StartsWithTestCase("abcde", "A", "UTF8_BINARY_LCASE", true),
      StartsWithTestCase("abcde", "A", "UNICODE", false),
      StartsWithTestCase("abcde", "A", "UNICODE_CI", true)
    )
    testCasting.foreach(t => {
      checkAnswer(sql(s"SELECT startswith(collate('${t.l}', '${t.c}'),'${t.r}')"), Row(t.e))
      checkAnswer(sql(s"SELECT startswith('${t.l}', collate('${t.r}', '${t.c}'))"), Row(t.e))
    })
    val collationMismatch = intercept[AnalysisException] {
      sql(s"SELECT startswith(collate('abcde', 'UTF8_BINARY_LCASE'),collate('C', 'UNICODE_CI'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("Support endsWith string expression with Collation") {
    case class EndsWithTestCase[R](l: String, r: String, c: String, e: R)
    // Supported collations
    val testCases = Seq(
      EndsWithTestCase("abcde", "cde", "UTF8_BINARY", true),
      EndsWithTestCase("abcde", "CDE", "UTF8_BINARY", false),
      EndsWithTestCase("abcde", "cde", "UNICODE", true),
      EndsWithTestCase("abcde", "CDE", "UNICODE", false),
      EndsWithTestCase("abcde", "CDE", "UTF8_BINARY_LCASE", true),
      EndsWithTestCase("abcde", "bcd", "UTF8_BINARY_LCASE", false),
      EndsWithTestCase("abcde", "CDE", "UNICODE_CI", true),
      EndsWithTestCase("abcde", "bcd", "UNICODE_CI", false)
    )
    testCases.foreach(t => {
      checkAnswer(sql(
        s"SELECT endswith(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      ), Row(t.e))
    })
    // Casting
    val testCasting = Seq(
      EndsWithTestCase("", "", "UTF8_BINARY", true),
      EndsWithTestCase("x", "", "UTF8_BINARY", true),
      EndsWithTestCase("", "x", "UTF8_BINARY", false),
      EndsWithTestCase("abcde", "E", "UTF8_BINARY", false),
      EndsWithTestCase("abcde", "E", "UTF8_BINARY_LCASE", true),
      EndsWithTestCase("abcde", "E", "UNICODE", false),
      EndsWithTestCase("abcde", "E", "UNICODE_CI", true)
    )
    testCasting.foreach(t => {
      checkAnswer(sql(s"SELECT endswith(collate('${t.l}', '${t.c}'),'${t.r}')"), Row(t.e))
      checkAnswer(sql(s"SELECT endswith('${t.l}', collate('${t.r}', '${t.c}'))"), Row(t.e))
    })
    val collationMismatch = intercept[AnalysisException] {
      sql(s"SELECT endswith(collate('abcde', 'UTF8_BINARY_LCASE'),collate('C', 'UNICODE_CI'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("REPEAT check output type on explicitly collated string") {
    def testRepeat(expected: String, collationId: Int, input: String, n: Int): Unit = {
      val s = Literal.create(input, StringType(collationId))

      checkEvaluation(Collation(StringRepeat(s, Literal.create(n))).replacement, expected)
    }

    testRepeat("UTF8_BINARY", 0, "abc", 2)
    testRepeat("UTF8_BINARY_LCASE", 1, "abc", 2)
    testRepeat("UNICODE", 2, "abc", 2)
    testRepeat("UNICODE_CI", 3, "abc", 2)
  }

  // TODO: Add more tests for other string expressions

}

class CollationStringExpressionsANSISuite extends CollationRegexpExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)

  // TODO: Add more tests for other string expressions (with ANSI mode enabled)

}
