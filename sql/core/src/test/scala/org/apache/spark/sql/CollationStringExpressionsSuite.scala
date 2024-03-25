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
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, Literal, StringReplace}
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

  test("REPLACE check result on explicitly collated strings") {
    def testReplace(expected: String, collationId: Integer,
                    source: String, search: String, replace: String): Unit = {
      val sourceLiteral = Literal.create(source, StringType(collationId))
      val searchLiteral = Literal.create(search, StringType(collationId))
      val replaceLiteral = Literal.create(replace, StringType(collationId))

      checkEvaluation(StringReplace(sourceLiteral, searchLiteral, replaceLiteral), expected)
    }

    // scalastyle:off
    // UTF8_BINARY
    testReplace("r世e123ace", 0, "r世eplace", "pl", "123")
    testReplace("reace", 0, "replace", "pl", "")
    testReplace("repl世ace", 0, "repl世ace", "Pl", "")
    testReplace("replace", 0, "replace", "", "123")
    testReplace("a12ca12c", 0, "abcabc", "b", "12")
    testReplace("adad", 0, "abcdabcd", "bc", "")
    // UTF8_BINARY_LCASE
    testReplace("r世exxace", 1, "r世eplace", "pl", "xx")
    testReplace("reAB世ace", 1, "repl世ace", "PL", "AB")
    testReplace("Replace", 1, "Replace", "", "123")
    testReplace("rexplace", 1, "re世place", "世", "x")
    testReplace("a12ca12c", 1, "abcaBc", "B", "12")
    testReplace("Adad", 1, "AbcdabCd", "Bc", "")
    // UNICODE
    testReplace("re世place", 2, "re世place", "plx", "123")
    testReplace("世Replace", 2, "世Replace", "re", "")
    testReplace("replace世", 2, "replace世", "", "123")
    testReplace("aBc世a12c", 2, "aBc世abc", "b", "12")
    testReplace("adad", 2, "abcdabcd", "bc", "")
    // UNICODE_CI
    testReplace("replace", 3, "replace", "plx", "123")
    testReplace("place", 3, "Replace", "re", "")
    testReplace("replace", 3, "replace", "", "123")
    testReplace("a12c世a12c", 3, "aBc世abc", "b", "12")
    testReplace("a世dad", 3, "a世Bcdabcd", "bC", "")
    // scalastyle:on
  }

  // TODO: Add more tests for other string expressions

}

class CollationStringExpressionsANSISuite extends CollationRegexpExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
