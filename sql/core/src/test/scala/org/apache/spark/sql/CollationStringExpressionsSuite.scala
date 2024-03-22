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
import org.apache.spark.sql.catalyst.expressions.{Collate, ExpressionEvalHelper, FindInSet, Literal, StringInstr}
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

  test("FIND_IN_SET check result on non-explicit default collation") {
    checkEvaluation(FindInSet(Literal("def"), Literal("abc,b,ab,c,def")), 5)
    checkEvaluation(FindInSet(Literal("defg"), Literal("abc,b,ab,c,def")), 0)
  }

  test("FIND_IN_SET check result on explicitly collated strings") {
    // UTF8_BINARY
    checkEvaluation(FindInSet(Collate(Literal("a"), "UTF8_BINARY"),
      Collate(Literal("abc,b,ab,c,def"), "UTF8_BINARY")), 0)
    checkEvaluation(FindInSet(Collate(Literal("c"), "UTF8_BINARY"),
      Collate(Literal("abc,b,ab,c,def"), "UTF8_BINARY")), 4)
    checkEvaluation(FindInSet(Collate(Literal("AB"), "UTF8_BINARY"),
      Collate(Literal("abc,b,ab,c,def"), "UTF8_BINARY")), 0)
    checkEvaluation(FindInSet(Collate(Literal("abcd"), "UTF8_BINARY"),
      Collate(Literal("abc,b,ab,c,def"), "UTF8_BINARY")), 0)
    // UTF8_BINARY_LCASE
    checkEvaluation(FindInSet(Collate(Literal("aB"), "UTF8_BINARY_LCASE"),
      Collate(Literal("abc,b,ab,c,def"), "UTF8_BINARY_LCASE")), 3)
    checkEvaluation(FindInSet(Collate(Literal("a"), "UTF8_BINARY_LCASE"),
      Collate(Literal("abc,b,ab,c,def"), "UTF8_BINARY_LCASE")), 0)
    checkEvaluation(FindInSet(Collate(Literal("abc"), "UTF8_BINARY_LCASE"),
      Collate(Literal("aBc,b,ab,c,def"), "UTF8_BINARY_LCASE")), 1)
    checkEvaluation(FindInSet(Collate(Literal("abcd"), "UTF8_BINARY_LCASE"),
      Collate(Literal("aBc,b,ab,c,def"), "UTF8_BINARY_LCASE")), 0)
    // UNICODE
    checkEvaluation(FindInSet(Collate(Literal("a"), "UNICODE"),
      Collate(Literal("abc,b,ab,c,def"), "UNICODE")), 0)
    checkEvaluation(FindInSet(Collate(Literal("ab"), "UNICODE"),
      Collate(Literal("abc,b,ab,c,def"), "UNICODE")), 3)
    checkEvaluation(FindInSet(Collate(Literal("Ab"), "UNICODE"),
      Collate(Literal("abc,b,ab,c,def"), "UNICODE")), 0)
    // UNICODE_CI
    checkEvaluation(FindInSet(Collate(Literal("a"), "UNICODE_CI"),
      Collate(Literal("abc,b,ab,c,def"), "UNICODE_CI")), 0)
    checkEvaluation(FindInSet(Collate(Literal("C"), "UNICODE_CI"),
      Collate(Literal("abc,b,ab,c,def"), "UNICODE_CI")), 4)
    checkEvaluation(FindInSet(Collate(Literal("DeF"), "UNICODE_CI"),
      Collate(Literal("abc,b,ab,c,dEf"), "UNICODE_CI")), 5)
    checkEvaluation(FindInSet(Collate(Literal("DEFG"), "UNICODE_CI"),
      Collate(Literal("abc,b,ab,c,def"), "UNICODE_CI")), 0)
  }

  test("FIND_IN_SET fail mismatched collation types") {
    // UNICODE and UNICODE_CI
    val expr1 = FindInSet(Collate(Literal("a"), "UNICODE"),
      Collate(Literal("abc,b,ab,c,def"), "UNICODE_CI"))
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
    val expr2 = FindInSet(Collate(Literal("a"), "UTF8_BINARY"),
      Collate(Literal("abc,b,ab,c,def"), "UTF8_BINARY_LCASE"))
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
    val expr3 = FindInSet(Collate(Literal("a"), "UTF8_BINARY_LCASE"),
      Collate(Literal("abc,b,ab,c,def"), "UNICODE_CI"))
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

  // TODO: Add more tests for other string expressions

}

class CollationStringExpressionsANSISuite extends CollationRegexpExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
