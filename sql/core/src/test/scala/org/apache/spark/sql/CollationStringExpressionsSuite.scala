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
import org.apache.spark.sql.catalyst.expressions.{Collation, ExpressionEvalHelper, Literal, StringLocate, StringRepeat}
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

  test("LOCATE check result on explicitly collated string") {
    def testStringLocate(substring: String, string: String, start: Integer,
                         collationId: Integer, expected: Integer): Unit = {
      val substr = Literal.create(substring, StringType(collationId))
      val str = Literal.create(string, StringType(collationId))
      val startFrom = Literal.create(start)

      checkEvaluation(StringLocate(substr, str, startFrom), expected)
    }

    // UTF8_BINARY
//    testStringLocate("aa", "aaads", 0, 0, 0)
//    testStringLocate("aa", "aaads", 1, 0, 1)
//    testStringLocate("aa", "aaads", 2, 0, 2)
//    testStringLocate("aa", "aaads", 3, 0, 0)
//    testStringLocate("Aa", "aaads", 1, 0, 0)
//    testStringLocate("Aa", "aAads", 1, 0, 2)
//    // scalastyle:off
//    testStringLocate("界x", "test大千世界X大千世界", 1, 0, 0)
//    testStringLocate("界X", "test大千世界X大千世界", 1, 0, 8)
//    testStringLocate("界", "test大千世界X大千世界", 13, 0, 13)
//    // scalastyle:on
//    // UTF8_BINARY_LCASE
//    testStringLocate("aa", "Aaads", 0, 1, 0)
//    testStringLocate("AA", "aaads", 1, 1, 1)
//    testStringLocate("aa", "aAads", 2, 1, 2)
//    testStringLocate("aa", "aaAds", 3, 1, 0)
//    testStringLocate("abC", "abcabc", 1, 1, 1)
//    testStringLocate("abC", "abCabc", 2, 1, 4)
//    testStringLocate("abc", "abcabc", 4, 1, 4)
//    // scalastyle:off
//    testStringLocate("界x", "test大千世界X大千世界", 1, 1, 8)
//    testStringLocate("界X", "test大千世界Xtest大千世界", 1, 1, 8)
//    testStringLocate("界", "test大千世界X大千世界", 13, 1, 13)
//    testStringLocate("大千", "test大千世界大千世界", 1, 1, 5)
//    testStringLocate("大千", "test大千世界大千世界", 9, 1, 9)
//    testStringLocate("大千", "大千世界大千世界", 1, 1, 1)
//    // scalastyle:on
//    // UNICODE
//    testStringLocate("aa", "Aaads", 0, 2, 0)
//    testStringLocate("aa", "Aaads", 1, 2, 2)
//    testStringLocate("AA", "aaads", 1, 2, 0)
//    testStringLocate("aa", "aAads", 2, 2, 0)
//    testStringLocate("aa", "aaAds", 3, 2, 0)
//    testStringLocate("abC", "abcabc", 1, 2, 0)
//    testStringLocate("abC", "abCabc", 2, 2, 0)
//    testStringLocate("abC", "abCabC", 2, 2, 4)
//    testStringLocate("abc", "abcabc", 1, 2, 1)
//    testStringLocate("abc", "abcabc", 3, 2, 4)
//    // scalastyle:off
//    testStringLocate("界x", "test大千世界X大千世界", 1, 2, 0)
//    testStringLocate("界X", "test大千世界X大千世界", 1, 2, 8)
//    testStringLocate("界", "test大千世界X大千世界", 13, 2, 13)
//    // scalastyle:on
//    // UNICODE_CI
//    testStringLocate("aa", "Aaads", 0, 3, 0)
//    testStringLocate("AA", "aaads", 1, 3, 1)
    testStringLocate("aa", "aAads", 2, 3, 2)
    testStringLocate("aa", "aaAds", 3, 3, 0)
    testStringLocate("abC", "abcabc", 1, 3, 1)
    testStringLocate("abC", "abCabc", 2, 3, 4)
    testStringLocate("abc", "abcabc", 4, 3, 4)
    // scalastyle:off
    testStringLocate("界x", "test大千世界X大千世界", 1, 3, 8)
    testStringLocate("界", "test大千世界X大千世界", 13, 3, 13)
    testStringLocate("大千", "test大千世界大千世界", 1, 3, 5)
    testStringLocate("大千", "test大千世界大千世界", 9, 3, 9)
    testStringLocate("大千", "大千世界大千世界", 1, 3, 1)
    // scalastyle:on
  }

  // TODO: Add more tests for other string expressions

}

class CollationStringExpressionsANSISuite extends CollationRegexpExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
