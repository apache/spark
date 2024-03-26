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
import org.apache.spark.sql.catalyst.expressions.{Collation, ExpressionEvalHelper, FindInSet, Literal, StringInstr, StringRepeat}
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

  test("INSTR check result on explicitly collated strings") {
    def testInStr(str: String, substr: String, collationId: Integer, expected: Integer): Unit = {
      val string = Literal.create(str, StringType(collationId))
      val substring = Literal.create(substr, StringType(collationId))

      checkEvaluation(StringInstr(string, substring), expected)
    }

    // UTF8_BINARY_LCASE
    testInStr("aaads", "Aa", 1, 1)
    testInStr("aaaDs", "de", 1, 0)
    // UNICODE
    testInStr("aaads", "Aa", 2, 0)
    testInStr("aaads", "de", 2, 0)
    // UNICODE_CI
    testInStr("aaads", "AD", 3, 3)
    testInStr("aaads", "dS", 3, 4)
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

  test("FIND_IN_SET check result on explicitly collated strings") {
    def testFindInSet(expected: Integer, stringType: Integer, word: String, set: String): Unit = {
      val w = Literal.create(word, StringType(stringType))
      val s = Literal.create(set, StringType(stringType))

      checkEvaluation(FindInSet(w, s), expected)
    }

    // UTF8_BINARY
    testFindInSet(0, 0, "AB", "abc,b,ab,c,def")
    // UTF8_BINARY_LCASE
    testFindInSet(0, 1, "a", "abc,b,ab,c,def")
    testFindInSet(4, 1, "c", "abc,b,ab,c,def")
    testFindInSet(3, 1, "AB", "abc,b,ab,c,def")
    testFindInSet(1, 1, "AbC", "abc,b,ab,c,def")
    testFindInSet(0, 1, "abcd", "abc,b,ab,c,def")
    // UNICODE
    testFindInSet(0, 2, "a", "abc,b,ab,c,def")
    testFindInSet(3, 2, "ab", "abc,b,ab,c,def")
    testFindInSet(0, 2, "Ab", "abc,b,ab,c,def")
    // UNICODE_CI
    testFindInSet(0, 3, "a", "abc,b,ab,c,def")
    testFindInSet(4, 3, "C", "abc,b,ab,c,def")
    testFindInSet(5, 3, "DeF", "abc,b,ab,c,dEf")
    testFindInSet(0, 3, "DEFG", "abc,b,ab,c,def")
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
}
