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
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions.{Collation, ExpressionEvalHelper, Literal, StringRepeat}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}

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
/*
Enable collation support for the Substring built-in string function in Spark (including Right and Left functions). First confirm what is the expected behaviour for these functions when given collated strings, then move on to the implementation that would enable handling strings of all collation types. Implement the corresponding unit tests (CollationStringExpressionsSuite) and E2E tests (CollationSuite) to reflect how this function should be used with collation in SparkSQL, and feel free to use your chosen Spark SQL Editor to experiment with the existing functions to learn more about how they work. In addition, look into the possible use-cases and implementation of similar functions within other other open-source DBMS, such as PostgreSQL.

The goal for this Jira ticket is to implement the Substring, Right, and Left functions so that they support all collation types currently supported in Spark. To understand what changes were introduced in order to enable full collation support for other existing functions in Spark, take a look at the Spark PRs and Jira tickets for completed tasks in this parent (for example: Contains, StartsWith, EndsWith).
todo ===== POSSIBLE THREAD LEAK IN SUITE o.a.s.sql.CollationStringExpressionsSuite, threads: rpc-boss-3-1 (daemon=true), shuffle-boss-6-1 (daemon=true) =====

Read more about ICU Collation Concepts and Collator class. Also, refer to the Unicode Technical Standard for collation.
 */
  test("substring check output type on explicitly collated string") {
    val checks = Seq(
      CollationTestCase("Spark", "2", "UTF8_BINARY", "park")
    )
    checks.foreach(ct => {
      checkAnswer(sql(s"SELECT substr(collate('${ct.s1}', '${ct.collation}'), 2)"),
        Row(ct.expectedResult))
    })
    def testSubstring(
        expected: String,
        collationId: Int,
        input: String,
        pos: Int,
        len: Int): Unit = {
      val s = Literal.create(input, StringType(collationId))
      val l = Literal.create(pos, IntegerType)

      checkEvaluation(Collation(s.substring(s, l)).replacement, expected)
    }

    testSubstring("UTF8_BINARY", 0, "abc", 1, 2)

    def testSubstringValue(
        expected: String,
        collationId: Int,
        input: String,
        pos: Int,
        len: Int): Unit = {
      val s = Literal.create(input, StringType(collationId))
      val l = Literal.create(pos, IntegerType)
//      checkResult(
//        s.substring(s, l).,
//        expected)

    }
    testSubstringValue("bc", 0, "abc", 2, 2)
  }
  // TODO: Add more tests for other string expressions

}

class CollationStringExpressionsANSISuite extends CollationRegexpExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
