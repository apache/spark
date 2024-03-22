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
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{Collate, ExpressionEvalHelper, Literal, StringInstr}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class CollationStringExpressionsSuite extends QueryTest
  with SharedSparkSession with ExpressionEvalHelper {

  case class CollationTestCase[R](s1: String, s2: String, collation: String, expectedResult: R)
  case class CollationTestFail[R](s1: String, s2: String, collation: String)

  test("Support ConcatWs string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("Spark", "SQL", "UTF8_BINARY", "Spark SQL")
    )
    checks.foreach(ct => {
      checkAnswer(sql(s"SELECT concat_ws(collate(' ', '${ct.collation}'), " +
        s"collate('${ct.s1}', '${ct.collation}'), collate('${ct.s2}', '${ct.collation}'))"),
        Row(ct.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABC", "%b%", "UTF8_BINARY_LCASE", false),
      CollationTestCase("ABC", "%B%", "UNICODE", true),
      CollationTestCase("ABC", "%b%", "UNICODE_CI", false)
    )
    fails.foreach(ct => {
      val expr = s"concat_ws(collate(' ', '${ct.collation}'), " +
        s"collate('${ct.s1}', '${ct.collation}'), collate('${ct.s2}', '${ct.collation}'))"
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT $expr")
        },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"concat_ws(collate( ), collate(${ct.s1}), collate(${ct.s2}))\"",
          "paramIndex" -> "first",
          "inputSql" -> s"\"collate( )\"",
          "inputType" -> s"\"STRING COLLATE ${ct.collation}\"",
          "requiredType" -> "\"STRING\""
        ),
        context = ExpectedContext(
          fragment = s"$expr",
          start = 7,
          stop = 73 + 3 * ct.collation.length
        )
      )
    })
  }

  test("INSTR check result on non-explicit default collation") {
    checkEvaluation(StringInstr(Literal("aAads"), Literal("Aa")), 2)
  }

  test("INSTR check result on explicitly collated strings") {
    // UTF8_BINARY_LCASE
    checkEvaluation(StringInstr(Literal.create("aaads", StringType(1)),
      Literal.create("Aa", StringType(1))), 1)
    checkEvaluation(StringInstr(Collate(Literal("aaads"), "UTF8_BINARY_LCASE"),
      Collate(Literal("Aa"), "UTF8_BINARY_LCASE")), 1)
    // UNICODE
    checkEvaluation(StringInstr(Literal.create("aaads", StringType(2)),
      Literal.create("Aa", StringType(2))), 0)
    checkEvaluation(StringInstr(Collate(Literal("aaads"), "UNICODE"),
      Collate(Literal("Aa"), "UNICODE")), 0)
    // UNICODE_CI
    checkEvaluation(StringInstr(Literal.create("aaads", StringType(3)),
      Literal.create("de", StringType(3))), 0)
    checkEvaluation(StringInstr(Collate(Literal("aaads"), "UNICODE_CI"),
      Collate(Literal("Aa"), "UNICODE_CI")), 0)
  }

  test("INSTR fail mismatched collation types") {
    // UNICODE and UNICODE_CI
    val expr1 = StringInstr(Collate(Literal("aaads"), "UNICODE"),
      Collate(Literal("Aa"), "UNICODE_CI"))
    assert(expr1.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "COLLATION_MISMATCH",
        messageParameters = Map(
          "collationNameLeft" -> "UNICODE",
          "collationNameRight" -> "UNICODE_CI"
        )
      )
    )
    // DEFAULT(UTF8_BINARY) and UTF8_BINARY_LCASE
    val expr2 = StringInstr(Literal("aaads"),
      Collate(Literal("Aa"), "UTF8_BINARY_LCASE"))
    assert(expr2.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "COLLATION_MISMATCH",
        messageParameters = Map(
          "collationNameLeft" -> "UTF8_BINARY",
          "collationNameRight" -> "UTF8_BINARY_LCASE"
        )
      )
    )
    // UTF8_BINARY_LCASE and UNICODE_CI
    val expr3 = StringInstr(Collate(Literal("aaads"), "UTF8_BINARY_LCASE"),
      Collate(Literal("Aa"), "UNICODE_CI"))
    assert(expr3.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "COLLATION_MISMATCH",
        messageParameters = Map(
          "collationNameLeft" -> "UTF8_BINARY_LCASE",
          "collationNameRight" -> "UNICODE_CI"
        )
      )
    )
  }

  case class SubstringIndexTestFail[R](s1: String, s2: String, c1: String, c2: String)

  test("Support SubstringIndex with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("The quick brown fox jumps over the dog.", "fox", "UTF8_BINARY", 17),
      CollationTestCase("The quick brown fox jumps over the dog.", "FOX", "UTF8_BINARY", 0),
      CollationTestCase("The quick brown fox jumps over the dog.", "FOX", "UTF8_BINARY_LCASE", 17),
      CollationTestCase("The quick brown fox jumps over the dog.", "fox", "UNICODE", 17),
      CollationTestCase("The quick brown fox jumps over the dog.", "FOX", "UNICODE", 0),
      CollationTestCase("The quick brown fox jumps over the dog.", "FOX", "UNICODE_CI", 17)
    )
    checks.foreach(ct => {
      checkAnswer(sql(s"SELECT instr(collate('${ct.s1}', '${ct.collation}'), " +
        s"collate('${ct.s2}', '${ct.collation}'))"),
        Row(ct.expectedResult))
    })
    // Unsupported collation pairs
    val fails = Seq(
      SubstringIndexTestFail("The quick brown fox jumps over the dog.",
        "Fox", "UTF8_BINARY_LCASE", "UTF8_BINARY"),
      SubstringIndexTestFail("The quick brown fox jumps over the dog.",
        "FOX", "UNICODE_CI", "UNICODE")
    )
    fails.foreach(ct => {
      val expr = s"instr(collate('${ct.s1}', '${ct.c1}'), collate('${ct.s2}', '${ct.c2}'))"
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT $expr")
        },
        errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"instr(collate(${ct.s1}), collate(${ct.s2}))\"",
          "collationNameLeft" -> s"${ct.c1}",
          "collationNameRight" -> s"${ct.c2}"
        ),
        context = ExpectedContext(
          fragment = s"$expr",
          start = 7,
          stop = 45 + ct.s1.length + ct.c1.length + ct.s2.length + ct.c2.length
        )
      )
    })
  }

  test("Support FindInSet with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("a", "abc,b,ab,c,def", "UTF8_BINARY", 0),
      CollationTestCase("c", "abc,b,ab,c,def", "UTF8_BINARY", 4),
      CollationTestCase("abc", "abc,b,ab,c,def", "UTF8_BINARY", 1),
      CollationTestCase("ab", "abc,b,ab,c,def", "UTF8_BINARY", 3),
      CollationTestCase("AB", "abc,b,ab,c,def", "UTF8_BINARY", 0),
      CollationTestCase("Ab", "abc,b,ab,c,def", "UTF8_BINARY_LCASE", 3),
      CollationTestCase("ab", "abc,b,ab,c,def", "UNICODE", 3),
      CollationTestCase("aB", "abc,b,ab,c,def", "UNICODE", 0),
      CollationTestCase("AB", "abc,b,ab,c,def", "UNICODE_CI", 3)
    )
    checks.foreach(ct => {
      checkAnswer(sql(s"SELECT find_in_set(collate('${ct.s1}', '${ct.collation}'), " +
        s"collate('${ct.s2}', '${ct.collation}'))"),
        Row(ct.expectedResult))
    })
    // Unsupported collation pairs
    val fails = Seq(
      SubstringIndexTestFail("a", "abc,b,ab,c,def", "UTF8_BINARY_LCASE", "UTF8_BINARY"),
      SubstringIndexTestFail("a", "abc,b,ab,c,def", "UNICODE_CI", "UNICODE")
    )
    fails.foreach(ct => {
      val expr = s"find_in_set(collate('${ct.s1}', '${ct.c1}'), collate('${ct.s2}', '${ct.c2}'))"
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT $expr")
        },
        errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"find_in_set(collate(${ct.s1}), collate(${ct.s2}))\"",
          "collationNameLeft" -> s"${ct.c1}",
          "collationNameRight" -> s"${ct.c2}"
        ),
        context = ExpectedContext(
          fragment = s"$expr",
          start = 7,
          stop = 51 + ct.s1.length + ct.c1.length + ct.s2.length + ct.c2.length
        )
      )
    })
  }

  // TODO: Add more tests for other string expressions

}

class CollationStringExpressionsANSISuite extends CollationRegexpExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
