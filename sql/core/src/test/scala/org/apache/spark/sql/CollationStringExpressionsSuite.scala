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
import org.apache.spark.sql.catalyst.expressions.{Collation, ExpressionEvalHelper, Literal, StringRepeat, SubstringIndex}
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

  test("SUBSTRING_INDEX check result on explicitly collated strings") {
    def testSubstringIndex(str: String,
                  delim: String,
                  cnt: Integer,
                  stringType: Integer,
                  expected: String,
                  ): Unit = {
      val string = Literal.create(str, StringType(stringType))
      val delimiter = Literal.create(delim, StringType(stringType))
      val count = Literal(cnt)

      checkEvaluation(SubstringIndex(string, delimiter, count), expected)
    }

    testSubstringIndex("wwwgapachegorg", "g", -3, 0, "apachegorg")
    testSubstringIndex("www||apache||org", "||", 2, 0, "www||apache")
    // UTF8_BINARY_LCASE
    testSubstringIndex("AaAaAaAaAa", "aa", 2, 1, "A")
    testSubstringIndex("www.apache.org", ".", 3, 1, "www.apache.org")
    testSubstringIndex("wwwXapachexorg", "x", 2, 1, "wwwXapache")
    testSubstringIndex("wwwxapacheXorg", "X", 1, 1, "www")
    testSubstringIndex("www.apache.org", ".", 0, 1, "")
    testSubstringIndex("www.apache.ORG", ".", -3, 1, "www.apache.ORG")
    testSubstringIndex("wwwGapacheGorg", "g", 1, 1, "www")
    testSubstringIndex("wwwGapacheGorg", "g", 3, 1, "wwwGapacheGor")
    testSubstringIndex("gwwwGapacheGorg", "g", 3, 1, "gwwwGapache")
    testSubstringIndex("wwwGapacheGorg", "g", -3, 1, "apacheGorg")
    testSubstringIndex("wwwmapacheMorg", "M", -2, 1, "apacheMorg")
    testSubstringIndex("www.apache.org", ".", -1, 1, "org")
    testSubstringIndex("", ".", -2, 1, "")
    // scalastyle:off
    testSubstringIndex("test大千世界X大千世界", "x", -1, 1, "大千世界")
    testSubstringIndex("test大千世界X大千世界", "X", 1, 1, "test大千世界")
    testSubstringIndex("test大千世界大千世界", "千", 2, 1, "test大千世界大")
    // scalastyle:on
    testSubstringIndex("www||APACHE||org", "||", 2, 1, "www||APACHE")
    testSubstringIndex("www||APACHE||org", "||", -1, 1, "org")
    // UNICODE
    testSubstringIndex("AaAaAaAaAa", "Aa", 2, 2, "Aa")
    testSubstringIndex("wwwYapacheyorg", "y", 3, 2, "wwwYapacheyorg")
    testSubstringIndex("www.apache.org", ".", 2, 2, "www.apache")
    testSubstringIndex("wwwYapacheYorg", "Y", 1, 2, "www")
    testSubstringIndex("wwwYapacheYorg", "y", 1, 2, "wwwYapacheYorg")
    testSubstringIndex("wwwGapacheGorg", "g", 1, 2, "wwwGapacheGor")
    testSubstringIndex("GwwwGapacheGorG", "G", 3, 2, "GwwwGapache")
    testSubstringIndex("wwwGapacheGorG", "G", -3, 2, "apacheGorG")
    testSubstringIndex("www.apache.org", ".", 0, 2, "")
    testSubstringIndex("www.apache.org", ".", -3, 2, "www.apache.org")
    testSubstringIndex("www.apache.org", ".", -2, 2, "apache.org")
    testSubstringIndex("www.apache.org", ".", -1, 2, "org")
    testSubstringIndex("", ".", -2, 2, "")
    // scalastyle:off
    testSubstringIndex("test大千世界X大千世界", "X", -1, 2, "大千世界")
    testSubstringIndex("test大千世界X大千世界", "X", 1, 2, "test大千世界")
    testSubstringIndex("大x千世界大千世x界", "x", 1, 2, "大")
    testSubstringIndex("大x千世界大千世x界", "x", -1, 2, "界")
    testSubstringIndex("大x千世界大千世x界", "x", -2, 2, "千世界大千世x界")
    testSubstringIndex("大千世界大千世界", "千", 2, 2, "大千世界大")
    // scalastyle:on
    testSubstringIndex("www||apache||org", "||", 2, 2, "www||apache")
    // UNICODE_CI
    testSubstringIndex("AaAaAaAaAa", "aa", 2, 3, "A")
    testSubstringIndex("www.apache.org", ".", 3, 3, "www.apache.org")
    testSubstringIndex("wwwXapachexorg", "x", 2, 3, "wwwXapache")
    testSubstringIndex("wwwxapacheXorg", "X", 1, 3, "www")
    testSubstringIndex("www.apache.org", ".", 0, 3, "")
    testSubstringIndex("wwwGapacheGorg", "g", 1, 3, "www")
    testSubstringIndex("wwwGapacheGorg", "g", 3, 3, "wwwGapacheGor")
    testSubstringIndex("gwwwGapacheGorg", "g", 3, 3, "gwwwGapache")
    testSubstringIndex("wwwGapacheGorg", "g", -3, 3, "apacheGorg")
    testSubstringIndex("www.apache.ORG", ".", -3, 3, "www.apache.ORG")
    testSubstringIndex("wwwmapacheMorg", "M", -2, 3, "apacheMorg")
    testSubstringIndex("www.apache.org", ".", -1, 3, "org")
    testSubstringIndex("", ".", -2, 3, "")
    // scalastyle:off
    testSubstringIndex("test大千世界X大千世界", "X", -1, 3, "大千世界")
    testSubstringIndex("test大千世界X大千世界", "X", 1, 3, "test大千世界")
    testSubstringIndex("test大千世界大千世界", "千", 2, 3, "test大千世界大")
    // scalastyle:on
    testSubstringIndex("www||APACHE||org", "||", 2, 3, "www||APACHE")
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
