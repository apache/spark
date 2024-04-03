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
import org.apache.spark.sql.catalyst.expressions.{Collation, ExpressionEvalHelper, Literal, StringRepeat, StringTranslate}
import org.apache.spark.sql.catalyst.util.CollationFactory
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

  test("TRANSLATE check result on explicitly collated string") {
    def testTranslate(input: String,
        matchExpression: String,
        replaceExpression: String,
        collationId: Int,
        expected: String): Unit = {
      val srcExpr = Literal.create(input, StringType(collationId))
      val matchExpr = Literal.create(matchExpression, StringType(collationId))
      val replaceExpr = Literal.create(replaceExpression, StringType(collationId))

      checkEvaluation(StringTranslate(srcExpr, matchExpr, replaceExpr), expected)
    }

    var collationId = CollationFactory.collationNameToId("UTF8_BINARY_LCASE")
    testTranslate("Translate", "Rnlt", "1234", collationId, "41a2s3a4e")
    testTranslate("TRanslate", "rnlt", "XxXx", collationId, "xXaxsXaxe")
    testTranslate("TRanslater", "Rrnlt", "xXxXx", collationId, "xxaxsXaxex")
    testTranslate("TRanslater", "Rrnlt", "XxxXx", collationId, "xXaxsXaxeX")
    // scalastyle:off
    testTranslate("test大千世界X大千世界", "界x", "AB", collationId, "test大千世AB大千世A")
    testTranslate("大千世界test大千世界", "TEST", "abcd", collationId, "大千世界abca大千世界")
    testTranslate("Test大千世界大千世界", "tT", "oO", collationId, "oeso大千世界大千世界")
    testTranslate("大千世界大千世界tesT", "Tt", "Oo", collationId, "大千世界大千世界OesO")
    testTranslate("大千世界大千世界tesT", "大千", "世世", collationId, "世世世界世世世界tesT")
    // scalastyle:on

    collationId = CollationFactory.collationNameToId("UNICODE")
    testTranslate("Translate", "Rnlt", "1234", collationId, "Tra2s3a4e")
    testTranslate("TRanslate", "rnlt", "XxXx", collationId, "TRaxsXaxe")
    testTranslate("TRanslater", "Rrnlt", "xXxXx", collationId, "TxaxsXaxeX")
    testTranslate("TRanslater", "Rrnlt", "XxxXx", collationId, "TXaxsXaxex")
    // scalastyle:off
    testTranslate("test大千世界X大千世界", "界x", "AB", collationId, "test大千世AX大千世A")
    testTranslate("Test大千世界大千世界", "tT", "oO", collationId, "Oeso大千世界大千世界")
    testTranslate("大千世界大千世界tesT", "Tt", "Oo", collationId, "大千世界大千世界oesO")
    // scalastyle:on

    collationId = CollationFactory.collationNameToId("UNICODE_CI")
    testTranslate("Translate", "Rnlt", "1234", collationId, "41a2s3a4e")
    testTranslate("TRanslate", "rnlt", "XxXx", collationId, "xXaxsXaxe")
    testTranslate("TRanslater", "Rrnlt", "xXxXx", collationId, "xxaxsXaxex")
    testTranslate("TRanslater", "Rrnlt", "XxxXx", collationId, "xXaxsXaxeX")
    // scalastyle:off
    testTranslate("test大千世界X大千世界", "界x", "AB", collationId, "test大千世AB大千世A")
    testTranslate("大千世界test大千世界", "TEST", "abcd", collationId, "大千世界abca大千世界")
    testTranslate("Test大千世界大千世界", "tT", "oO", collationId, "oeso大千世界大千世界")
    testTranslate("大千世界大千世界tesT", "Tt", "Oo", collationId, "大千世界大千世界OesO")
    testTranslate("大千世界大千世界tesT", "大千", "世世", collationId, "世世世界世世世界tesT")
    // scalastyle:on
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
