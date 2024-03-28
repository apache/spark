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
import org.apache.spark.sql.catalyst.expressions.{Collation, ExpressionEvalHelper, Literal, StringRepeat}
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

  case class StringTrimTestCase(
    collation: String,
    trimFunc: String,
    sourceString: String,
    trimString: String,
    expectedResultString: String)

  test("string trim functions with collation - success") {
    // scalastyle:off

    val testCases = Seq(
      // Basic test cases
      StringTrimTestCase("UTF8_BINARY", "TRIM", "asd", null, "asd"),
      StringTrimTestCase("UTF8_BINARY", "TRIM", "  asd  ", null, "asd"),
      StringTrimTestCase("UTF8_BINARY", "TRIM", " a世a ", null, "a世a"),
      StringTrimTestCase("UTF8_BINARY", "TRIM", "asd", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY", "TRIM", "xxasdxx", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY", "TRIM", "xa世ax", "x", "a世a"),

      StringTrimTestCase("UTF8_BINARY", "BTRIM", "asd", null, "asd"),
      StringTrimTestCase("UTF8_BINARY", "BTRIM", "  asd  ", null, "asd"),
      StringTrimTestCase("UTF8_BINARY", "BTRIM", " a世a ", null, "a世a"),
      StringTrimTestCase("UTF8_BINARY", "BTRIM", "asd", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY", "BTRIM", "xxasdxx", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY", "BTRIM", "xa世ax", "x", "a世a"),

      StringTrimTestCase("UTF8_BINARY", "LTRIM", "asd", null, "asd"),
      StringTrimTestCase("UTF8_BINARY", "LTRIM", "  asd  ", null, "asd  "),
      StringTrimTestCase("UTF8_BINARY", "LTRIM", " a世a ", null, "a世a "),
      StringTrimTestCase("UTF8_BINARY", "LTRIM", "asd", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY", "LTRIM", "xxasdxx", "x", "asdxx"),
      StringTrimTestCase("UTF8_BINARY", "LTRIM", "xa世ax", "x", "a世ax"),

      StringTrimTestCase("UTF8_BINARY", "RTRIM", "asd", null, "asd"),
      StringTrimTestCase("UTF8_BINARY", "RTRIM", "  asd  ", null, "  asd"),
      StringTrimTestCase("UTF8_BINARY", "RTRIM", " a世a ", null, " a世a"),
      StringTrimTestCase("UTF8_BINARY", "RTRIM", "asd", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY", "RTRIM", "xxasdxx", "x", "xxasd"),
      StringTrimTestCase("UTF8_BINARY", "RTRIM", "xa世ax", "x", "xa世a"),

      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "asd", null, "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "  asd  ", null, "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", " a世a ", null, "a世a"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "asd", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "xxasdxx", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "xa世ax", "x", "a世a"),

      StringTrimTestCase("UTF8_BINARY_LCASE", "BTRIM", "asd", null, "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "BTRIM", "  asd  ", null, "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "BTRIM", " a世a ", null, "a世a"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "BTRIM", "asd", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "BTRIM", "xxasdxx", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "BTRIM", "xa世ax", "x", "a世a"),

      StringTrimTestCase("UTF8_BINARY_LCASE", "LTRIM", "asd", null, "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "LTRIM", "  asd  ", null, "asd  "),
      StringTrimTestCase("UTF8_BINARY_LCASE", "LTRIM", " a世a ", null, "a世a "),
      StringTrimTestCase("UTF8_BINARY_LCASE", "LTRIM", "asd", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "LTRIM", "xxasdxx", "x", "asdxx"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "LTRIM", "xa世ax", "x", "a世ax"),

      StringTrimTestCase("UTF8_BINARY_LCASE", "RTRIM", "asd", null, "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "RTRIM", "  asd  ", null, "  asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "RTRIM", " a世a ", null, " a世a"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "RTRIM", "asd", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "RTRIM", "xxasdxx", "x", "xxasd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "RTRIM", "xa世ax", "x", "xa世a"),

      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "asd", null, "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "  asd  ", null, "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", " a世a ", null, "a世a"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "asd", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "xxasdxx", "x", "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "xa世ax", "x", "a世a"),

      StringTrimTestCase("UNICODE", "BTRIM", "asd", null, "asd"),
      StringTrimTestCase("UNICODE", "BTRIM", "  asd  ", null, "asd"),
      StringTrimTestCase("UNICODE", "BTRIM", " a世a ", null, "a世a"),
      StringTrimTestCase("UNICODE", "BTRIM", "asd", "x", "asd"),
      StringTrimTestCase("UNICODE", "BTRIM", "xxasdxx", "x", "asd"),
      StringTrimTestCase("UNICODE", "BTRIM", "xa世ax", "x", "a世a"),

      StringTrimTestCase("UNICODE", "LTRIM", "asd", null, "asd"),
      StringTrimTestCase("UNICODE", "LTRIM", "  asd  ", null, "asd  "),
      StringTrimTestCase("UNICODE", "LTRIM", " a世a ", null, "a世a "),
      StringTrimTestCase("UNICODE", "LTRIM", "asd", "x", "asd"),
      StringTrimTestCase("UNICODE", "LTRIM", "xxasdxx", "x", "asdxx"),
      StringTrimTestCase("UNICODE", "LTRIM", "xa世ax", "x", "a世ax"),

      StringTrimTestCase("UNICODE", "RTRIM", "asd", null, "asd"),
      StringTrimTestCase("UNICODE", "RTRIM", "  asd  ", null, "  asd"),
      StringTrimTestCase("UNICODE", "RTRIM", " a世a ", null, " a世a"),
      StringTrimTestCase("UNICODE", "RTRIM", "asd", "x", "asd"),
      StringTrimTestCase("UNICODE", "RTRIM", "xxasdxx", "x", "xxasd"),
      StringTrimTestCase("UNICODE", "RTRIM", "xa世ax", "x", "xa世a"),

      StringTrimTestCase("UNICODE_CI", "TRIM", "asd", null, "asd"),
      StringTrimTestCase("UNICODE_CI", "TRIM", "  asd  ", null, "asd"),
      StringTrimTestCase("UNICODE_CI", "TRIM", " a世a ", null, "a世a"),
      StringTrimTestCase("UNICODE_CI", "TRIM", "asd", "x", "asd"),
      StringTrimTestCase("UNICODE_CI", "TRIM", "xxasdxx", "x", "asd"),
      StringTrimTestCase("UNICODE_CI", "TRIM", "xa世ax", "x", "a世a"),

      StringTrimTestCase("UNICODE_CI", "BTRIM", "asd", null, "asd"),
      StringTrimTestCase("UNICODE_CI", "BTRIM", "  asd  ", null, "asd"),
      StringTrimTestCase("UNICODE_CI", "BTRIM", " a世a ", null, "a世a"),
      StringTrimTestCase("UNICODE_CI", "BTRIM", "asd", "x", "asd"),
      StringTrimTestCase("UNICODE_CI", "BTRIM", "xxasdxx", "x", "asd"),
      StringTrimTestCase("UNICODE_CI", "BTRIM", "xa世ax", "x", "a世a"),

      StringTrimTestCase("UNICODE_CI", "LTRIM", "asd", null, "asd"),
      StringTrimTestCase("UNICODE_CI", "LTRIM", "  asd  ", null, "asd  "),
      StringTrimTestCase("UNICODE_CI", "LTRIM", " a世a ", null, "a世a "),
      StringTrimTestCase("UNICODE_CI", "LTRIM", "asd", "x", "asd"),
      StringTrimTestCase("UNICODE_CI", "LTRIM", "xxasdxx", "x", "asdxx"),
      StringTrimTestCase("UNICODE_CI", "LTRIM", "xa世ax", "x", "a世ax"),

      StringTrimTestCase("UNICODE_CI", "RTRIM", "asd", null, "asd"),
      StringTrimTestCase("UNICODE_CI", "RTRIM", "  asd  ", null, "  asd"),
      StringTrimTestCase("UNICODE_CI", "RTRIM", " a世a ", null, " a世a"),
      StringTrimTestCase("UNICODE_CI", "RTRIM", "asd", "x", "asd"),
      StringTrimTestCase("UNICODE_CI", "RTRIM", "xxasdxx", "x", "xxasd"),
      StringTrimTestCase("UNICODE_CI", "RTRIM", "xa世ax", "x", "xa世a"),

      // Test cases where trimString has more than one character
      StringTrimTestCase("UTF8_BINARY", "TRIM", "ddsXXXaa", "asd", "XXX"),
      StringTrimTestCase("UTF8_BINARY", "BTRIM", "ddsXXXaa", "asd", "XXX"),
      StringTrimTestCase("UTF8_BINARY", "LTRIM", "ddsXXXaa", "asd", "XXXaa"),
      StringTrimTestCase("UTF8_BINARY", "RTRIM", "ddsXXXaa", "asd", "ddsXXX"),

      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "ddsXXXaa", "asd", "XXX"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "BTRIM", "ddsXXXaa", "asd", "XXX"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "LTRIM", "ddsXXXaa", "asd", "XXXaa"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "RTRIM", "ddsXXXaa", "asd", "ddsXXX"),

      StringTrimTestCase("UNICODE", "TRIM", "ddsXXXaa", "asd", "XXX"),
      StringTrimTestCase("UNICODE", "BTRIM", "ddsXXXaa", "asd", "XXX"),
      StringTrimTestCase("UNICODE", "LTRIM", "ddsXXXaa", "asd", "XXXaa"),
      StringTrimTestCase("UNICODE", "RTRIM", "ddsXXXaa", "asd", "ddsXXX"),

      StringTrimTestCase("UNICODE_CI", "TRIM", "ddsXXXaa", "asd", "XXX"),
      StringTrimTestCase("UNICODE_CI", "BTRIM", "ddsXXXaa", "asd", "XXX"),
      StringTrimTestCase("UNICODE_CI", "LTRIM", "ddsXXXaa", "asd", "XXXaa"),
      StringTrimTestCase("UNICODE_CI", "RTRIM", "ddsXXXaa", "asd", "ddsXXX"),

      // Test cases specific to collation type
      // uppercase trim, lowercase src
      StringTrimTestCase("UTF8_BINARY", "TRIM", "asd", "A", "asd"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "asd", "A", "sd"),
      StringTrimTestCase("UNICODE", "TRIM", "asd", "A", "asd"),
      StringTrimTestCase("UNICODE_CI", "TRIM", "asd", "A", "sd"),

      // lowercase trim, uppercase src
      StringTrimTestCase("UTF8_BINARY", "TRIM", "ASD", "a", "ASD"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "ASD", "a", "SD"),
      StringTrimTestCase("UNICODE", "TRIM", "ASD", "a", "ASD"),
      StringTrimTestCase("UNICODE_CI", "TRIM", "ASD", "a", "SD"),

      // uppercase and lowercase chars of different byte-length (utf8)
      StringTrimTestCase("UTF8_BINARY", "TRIM", "ẞaaaẞ", "ß", "ẞaaaẞ"),
      StringTrimTestCase("UTF8_BINARY", "BTRIM", "ẞaaaẞ", "ß", "ẞaaaẞ"),
      StringTrimTestCase("UTF8_BINARY", "LTRIM", "ẞaaaẞ", "ß", "ẞaaaẞ"),
      StringTrimTestCase("UTF8_BINARY", "RTRIM", "ẞaaaẞ", "ß", "ẞaaaẞ"),

      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "ẞaaaẞ", "ß", "aaa"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "BTRIM", "ẞaaaẞ", "ß", "aaa"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "LTRIM", "ẞaaaẞ", "ß", "aaaẞ"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "RTRIM", "ẞaaaẞ", "ß", "ẞaaa"),

      StringTrimTestCase("UNICODE", "TRIM", "ẞaaaẞ", "ß", "ẞaaaẞ"),
      StringTrimTestCase("UNICODE", "BTRIM", "ẞaaaẞ", "ß", "ẞaaaẞ"),
      StringTrimTestCase("UNICODE", "LTRIM", "ẞaaaẞ", "ß", "ẞaaaẞ"),
      StringTrimTestCase("UNICODE", "RTRIM", "ẞaaaẞ", "ß", "ẞaaaẞ"),

      StringTrimTestCase("UNICODE_CI", "TRIM", "ẞaaaẞ", "ß", "aaa"),
      StringTrimTestCase("UNICODE_CI", "BTRIM", "ẞaaaẞ", "ß", "aaa"),
      StringTrimTestCase("UNICODE_CI", "LTRIM", "ẞaaaẞ", "ß", "aaaẞ"),
      StringTrimTestCase("UNICODE_CI", "RTRIM", "ẞaaaẞ", "ß", "ẞaaa"),

      StringTrimTestCase("UTF8_BINARY", "TRIM", "ßaaaß", "ẞ", "ßaaaß"),
      StringTrimTestCase("UTF8_BINARY", "BTRIM", "ßaaaß", "ẞ", "ßaaaß"),
      StringTrimTestCase("UTF8_BINARY", "LTRIM", "ßaaaß", "ẞ", "ßaaaß"),
      StringTrimTestCase("UTF8_BINARY", "RTRIM", "ßaaaß", "ẞ", "ßaaaß"),

      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "ßaaaß", "ẞ", "aaa"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "BTRIM", "ßaaaß", "ẞ", "aaa"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "LTRIM", "ßaaaß", "ẞ", "aaaß"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "RTRIM", "ßaaaß", "ẞ", "ßaaa"),

      StringTrimTestCase("UNICODE", "TRIM", "ßaaaß", "ẞ", "ßaaaß"),
      StringTrimTestCase("UNICODE", "BTRIM", "ßaaaß", "ẞ", "ßaaaß"),
      StringTrimTestCase("UNICODE", "LTRIM", "ßaaaß", "ẞ", "ßaaaß"),
      StringTrimTestCase("UNICODE", "RTRIM", "ßaaaß", "ẞ", "ßaaaß"),

      StringTrimTestCase("UNICODE_CI", "TRIM", "ßaaaß", "ẞ", "aaa"),
      StringTrimTestCase("UNICODE_CI", "BTRIM", "ßaaaß", "ẞ", "aaa"),
      StringTrimTestCase("UNICODE_CI", "LTRIM", "ßaaaß", "ẞ", "aaaß"),
      StringTrimTestCase("UNICODE_CI", "RTRIM", "ßaaaß", "ẞ", "ßaaa"),

      // different byte-length (utf8) chars trimmed
      StringTrimTestCase("UTF8_BINARY", "TRIM", "Ëaaaẞ", "Ëẞ", "aaa"),
      StringTrimTestCase("UTF8_BINARY", "BTRIM", "Ëaaaẞ", "Ëẞ", "aaa"),
      StringTrimTestCase("UTF8_BINARY", "LTRIM", "Ëaaaẞ", "Ëẞ", "aaaẞ"),
      StringTrimTestCase("UTF8_BINARY", "RTRIM", "Ëaaaẞ", "Ëẞ", "Ëaaa"),

      StringTrimTestCase("UTF8_BINARY_LCASE", "TRIM", "Ëaaaẞ", "Ëẞ", "aaa"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "BTRIM", "Ëaaaẞ", "Ëẞ", "aaa"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "LTRIM", "Ëaaaẞ", "Ëẞ", "aaaẞ"),
      StringTrimTestCase("UTF8_BINARY_LCASE", "RTRIM", "Ëaaaẞ", "Ëẞ", "Ëaaa"),

      StringTrimTestCase("UNICODE", "TRIM", "Ëaaaẞ", "Ëẞ", "aaa"),
      StringTrimTestCase("UNICODE", "BTRIM", "Ëaaaẞ", "Ëẞ", "aaa"),
      StringTrimTestCase("UNICODE", "LTRIM", "Ëaaaẞ", "Ëẞ", "aaaẞ"),
      StringTrimTestCase("UNICODE", "RTRIM", "Ëaaaẞ", "Ëẞ", "Ëaaa"),

      StringTrimTestCase("UNICODE_CI", "TRIM", "Ëaaaẞ", "Ëẞ", "aaa"),
      StringTrimTestCase("UNICODE_CI", "BTRIM", "Ëaaaẞ", "Ëẞ", "aaa"),
      StringTrimTestCase("UNICODE_CI", "LTRIM", "Ëaaaẞ", "Ëẞ", "aaaẞ"),
      StringTrimTestCase("UNICODE_CI", "RTRIM", "Ëaaaẞ", "Ëẞ", "Ëaaa")
    )

    testCases.foreach(testCase => {
      var df: DataFrame = null

      if (testCase.trimFunc.equalsIgnoreCase("BTRIM")) {
        // BTRIM has arguments in (srcStr, trimStr) order
        df = sql(s"SELECT ${testCase.trimFunc}(" +
          s"COLLATE('${testCase.sourceString}', '${testCase.collation}')" +
          (if (testCase.trimString == null) "" else s", COLLATE('${testCase.trimString}', '${testCase.collation}')") +
          ")")
      }
      else {
        // While other functions have arguments in (trimStr, srcStr) order
        df = sql(s"SELECT ${testCase.trimFunc}(" +
          (if (testCase.trimString == null) "" else s"COLLATE('${testCase.trimString}', '${testCase.collation}'), ") +
          s"COLLATE('${testCase.sourceString}', '${testCase.collation}')" +
          ")")
      }

      checkAnswer(df = df, expectedAnswer = Row(testCase.expectedResultString))
    })

    // scalastyle:on
  }

  test("string trim functions with collation - exceptions") {
    // scalastyle:off

    // TRIM
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT TRIM(COLLATE('trimStr', 'UTF8_BINARY_LCASE'), COLLATE('sourceStr', 'UNICODE'))")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> "UNICODE",
        "collationNameRight" -> "UTF8_BINARY_LCASE",
        "sqlExpr" -> "\"TRIM(BOTH collate(trimStr) FROM collate(sourceStr))\""
      ),
      context = ExpectedContext(fragment =
        "TRIM(COLLATE('trimStr', 'UTF8_BINARY_LCASE'), COLLATE('sourceStr', 'UNICODE'))",
        start = 7, stop = 84)
    )

    // BTRIM
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT BTRIM(COLLATE('sourceStr', 'UTF8_BINARY_LCASE'), COLLATE('trimStr', 'UNICODE'))")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> "UTF8_BINARY_LCASE",
        "collationNameRight" -> "UNICODE",
        "sqlExpr" -> "\"TRIM(BOTH collate(trimStr) FROM collate(sourceStr))\""
      ),
      context = ExpectedContext(fragment =
        "BTRIM(COLLATE('sourceStr', 'UTF8_BINARY_LCASE'), COLLATE('trimStr', 'UNICODE'))",
        start = 7, stop = 85)
    )

    // LTRIM
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT LTRIM(COLLATE('trimStr', 'UTF8_BINARY_LCASE'), COLLATE('sourceStr', 'UNICODE'))")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> "UNICODE",
        "collationNameRight" -> "UTF8_BINARY_LCASE",
        "sqlExpr" -> "\"TRIM(LEADING collate(trimStr) FROM collate(sourceStr))\""
      ),
      context = ExpectedContext(fragment =
        "LTRIM(COLLATE('trimStr', 'UTF8_BINARY_LCASE'), COLLATE('sourceStr', 'UNICODE'))",
        start = 7, stop = 85)
    )

    // RTRIM
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT RTRIM(COLLATE('trimStr', 'UTF8_BINARY_LCASE'), COLLATE('sourceStr', 'UNICODE'))")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> "UNICODE",
        "collationNameRight" -> "UTF8_BINARY_LCASE",
        "sqlExpr" -> "\"TRIM(TRAILING collate(trimStr) FROM collate(sourceStr))\""
      ),
      context = ExpectedContext(fragment =
        "RTRIM(COLLATE('trimStr', 'UTF8_BINARY_LCASE'), COLLATE('sourceStr', 'UNICODE'))",
        start = 7, stop = 85)
    )

    // scalastyle:on
  }

  // TODO: Add more tests for other string expressions
}

class CollationStringExpressionsANSISuite extends CollationRegexpExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
