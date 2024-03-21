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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class CollationRegexpExpressionsSuite extends QueryTest with SharedSparkSession {

  case class CollationTestCase[R](s1: String, s2: String, collation: String, expectedResult: R)
  case class CollationTestFail[R](s1: String, s2: String, collation: String)

  test("Support Like string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABC", "%B%", "UTF8_BINARY", true)
    )
    checks.foreach(ct => {
      checkAnswer(sql(s"SELECT collate('${ct.s1}', '${ct.collation}') like " +
        s"collate('${ct.s2}', '${ct.collation}')"), Row(ct.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABC", "%b%", "UTF8_BINARY_LCASE", false),
      CollationTestCase("ABC", "%B%", "UNICODE", true),
      CollationTestCase("ABC", "%b%", "UNICODE_CI", false)
    )
    fails.foreach(ct => {
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT collate('${ct.s1}', '${ct.collation}') like " +
            s"collate('${ct.s2}', '${ct.collation}')")
        },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"collate(${ct.s1}) LIKE collate(${ct.s2})\"",
          "paramIndex" -> "first",
          "inputSql" -> s"\"collate(${ct.s1})\"",
          "inputType" -> s"\"STRING COLLATE ${ct.collation}\"",
          "requiredType" -> "\"STRING\""
        ),
        context = ExpectedContext(
          fragment = s"like collate('${ct.s2}', '${ct.collation}')",
          start = 26 + ct.collation.length,
          stop = 48 + 2 * ct.collation.length
        )
      )
    })
  }

  test("Support ILike string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABC", "%b%", "UTF8_BINARY", true)
    )
    checks.foreach(ct => {
      checkAnswer(sql(s"SELECT collate('${ct.s1}', '${ct.collation}') ilike " +
        s"collate('${ct.s2}', '${ct.collation}')"), Row(ct.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABC", "%b%", "UTF8_BINARY_LCASE", false),
      CollationTestCase("ABC", "%b%", "UNICODE", true),
      CollationTestCase("ABC", "%b%", "UNICODE_CI", false)
    )
    fails.foreach(ct => {
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT collate('${ct.s1}', '${ct.collation}') ilike " +
            s"collate('${ct.s2}', '${ct.collation}')")
        },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"ilike(collate(${ct.s1}), collate(${ct.s2}))\"",
          "paramIndex" -> "first",
          "inputSql" -> s"\"collate(${ct.s1})\"",
          "inputType" -> s"\"STRING COLLATE ${ct.collation}\"",
          "requiredType" -> "\"STRING\""
        ),
        context = ExpectedContext(
          fragment = s"ilike collate('${ct.s2}', '${ct.collation}')",
          start = 26 + ct.collation.length,
          stop = 49 + 2 * ct.collation.length
        )
      )
    })
  }

  test("Support RLike string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABC", ".B.", "UTF8_BINARY", true)
    )
    checks.foreach(ct => {
      checkAnswer(sql(s"SELECT collate('${ct.s1}', '${ct.collation}') rlike " +
        s"collate('${ct.s2}', '${ct.collation}')"), Row(ct.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABC", ".b.", "UTF8_BINARY_LCASE", false),
      CollationTestCase("ABC", ".B.", "UNICODE", true),
      CollationTestCase("ABC", ".b.", "UNICODE_CI", false)
    )
    fails.foreach(ct => {
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT collate('${ct.s1}', '${ct.collation}') rlike " +
            s"collate('${ct.s2}', '${ct.collation}')")
        },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"RLIKE(collate(${ct.s1}), collate(${ct.s2}))\"",
          "paramIndex" -> "first",
          "inputSql" -> s"\"collate(${ct.s1})\"",
          "inputType" -> s"\"STRING COLLATE ${ct.collation}\"",
          "requiredType" -> "\"STRING\""
        ),
        context = ExpectedContext(
          fragment = s"rlike collate('${ct.s2}', '${ct.collation}')",
          start = 26 + ct.collation.length,
          stop = 49 + 2 * ct.collation.length
        )
      )
    })
  }

  test("Support StringSplit string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABC", "[B]", "UTF8_BINARY", 2)
    )
    checks.foreach(ct => {
      checkAnswer(sql(s"SELECT size(split(collate('${ct.s1}', '${ct.collation}')" +
        s",collate('${ct.s2}', '${ct.collation}')))"), Row(ct.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABC", "[b]", "UTF8_BINARY_LCASE", 0),
      CollationTestCase("ABC", "[B]", "UNICODE", 2),
      CollationTestCase("ABC", "[b]", "UNICODE_CI", 0)
    )
    fails.foreach(ct => {
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT size(split(collate('${ct.s1}', '${ct.collation}')" +
            s",collate('${ct.s2}', '${ct.collation}')))")
        },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"split(collate(${ct.s1}), collate(${ct.s2}), -1)\"",
          "paramIndex" -> "first",
          "inputSql" -> s"\"collate(${ct.s1})\"",
          "inputType" -> s"\"STRING COLLATE ${ct.collation}\"",
          "requiredType" -> "\"STRING\""
        ),
        context = ExpectedContext(
          fragment = s"split(collate('${ct.s1}', '${ct.collation}')," +
            s"collate('${ct.s2}', '${ct.collation}'))",
          start = 12,
          stop = 55 + 2 * ct.collation.length
        )
      )
    })
  }

  test("Support RegExpReplace string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UTF8_BINARY", "AFFFE")
    )
    checks.foreach(ct => {
      checkAnswer(
        sql(s"SELECT regexp_replace(collate('${ct.s1}', '${ct.collation}')" +
          s",collate('${ct.s2}', '${ct.collation}')" +
          s",collate('FFF', '${ct.collation}'))"),
        Row(ct.expectedResult)
      )
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABCDE", ".c.", "UTF8_BINARY_LCASE", ""),
      CollationTestCase("ABCDE", ".C.", "UNICODE", "AFFFE"),
      CollationTestCase("ABCDE", ".c.", "UNICODE_CI", "")
    )
    fails.foreach(ct => {
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT regexp_replace(collate('${ct.s1}', '${ct.collation}')" +
            s",collate('${ct.s2}', '${ct.collation}')" +
            s",collate('FFF', '${ct.collation}'))")
        },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"regexp_replace(collate(${ct.s1}), collate(${ct.s2}), collate(FFF), 1)\"",
          "paramIndex" -> "first",
          "inputSql" -> s"\"collate(${ct.s1})\"",
          "inputType" -> s"\"STRING COLLATE ${ct.collation}\"",
          "requiredType" -> "\"STRING\""
        ),
        context = ExpectedContext(
          fragment = s"regexp_replace(collate('${ct.s1}', '${ct.collation}'),collate('${ct.s2}'," +
            s" '${ct.collation}'),collate('FFF', '${ct.collation}'))",
          start = 7,
          stop = 80 + 3 * ct.collation.length
        )
      )
    })
  }

  test("Support RegExpExtract string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UTF8_BINARY", "BCD")
    )
    checks.foreach(ct => {
      checkAnswer(
        sql(s"SELECT regexp_extract(collate('${ct.s1}', '${ct.collation}')" +
          s",collate('${ct.s2}', '${ct.collation}'),0)"),
        Row(ct.expectedResult)
      )
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABCDE", ".c.", "UTF8_BINARY_LCASE", ""),
      CollationTestCase("ABCDE", ".C.", "UNICODE", "BCD"),
      CollationTestCase("ABCDE", ".c.", "UNICODE_CI", "")
    )
    fails.foreach(ct => {
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT regexp_extract(collate('${ct.s1}', '${ct.collation}')" +
              s",collate('${ct.s2}', '${ct.collation}'),0)")
        },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"regexp_extract(collate(${ct.s1}), collate(${ct.s2}), 0)\"",
          "paramIndex" -> "first",
          "inputSql" -> s"\"collate(${ct.s1})\"",
          "inputType" -> s"\"STRING COLLATE ${ct.collation}\"",
          "requiredType" -> "\"STRING\""
        ),
        context = ExpectedContext(
          fragment = s"regexp_extract(collate('${ct.s1}', '${ct.collation}')," +
            s"collate('${ct.s2}', '${ct.collation}'),0)",
          start = 7,
          stop = 63 + 2 * ct.collation.length
        )
      )
    })
  }

  test("Support RegExpExtractAll string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UTF8_BINARY", 1)
    )
    checks.foreach(ct => {
      checkAnswer(
        sql(s"SELECT size(regexp_extract_all(collate('${ct.s1}', '${ct.collation}')" +
          s",collate('${ct.s2}', '${ct.collation}'),0))"),
        Row(ct.expectedResult)
      )
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABCDE", ".c.", "UTF8_BINARY_LCASE", 0),
      CollationTestCase("ABCDE", ".C.", "UNICODE", 1),
      CollationTestCase("ABCDE", ".c.", "UNICODE_CI", 0)
    )
    fails.foreach(ct => {
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT size(regexp_extract_all(collate('${ct.s1}', " +
            s"'${ct.collation}'),collate('${ct.s2}', '${ct.collation}'),0))")
        },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"regexp_extract_all(collate(${ct.s1}), collate(${ct.s2}), 0)\"",
          "paramIndex" -> "first",
          "inputSql" -> s"\"collate(${ct.s1})\"",
          "inputType" -> s"\"STRING COLLATE ${ct.collation}\"",
          "requiredType" -> "\"STRING\""
        ),
        context = ExpectedContext(
          fragment = s"regexp_extract_all(collate('${ct.s1}', '${ct.collation}')," +
            s"collate('${ct.s2}', '${ct.collation}'),0)",
          start = 12,
          stop = 72 + 2 * ct.collation.length
        )
      )
    })
  }

  test("Support RegExpCount string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UTF8_BINARY", 1)
    )
    checks.foreach(ct => {
      checkAnswer(sql(s"SELECT regexp_count(collate('${ct.s1}', '${ct.collation}')" +
        s",collate('${ct.s2}', '${ct.collation}'))"), Row(ct.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABCDE", ".c.", "UTF8_BINARY_LCASE", 0),
      CollationTestCase("ABCDE", ".C.", "UNICODE", 1),
      CollationTestCase("ABCDE", ".c.", "UNICODE_CI", 0)
    )
    fails.foreach(ct => {
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT regexp_count(collate('${ct.s1}', '${ct.collation}')" +
            s",collate('${ct.s2}', '${ct.collation}'))")
        },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"regexp_count(collate(${ct.s1}), collate(${ct.s2}))\"",
          "paramIndex" -> "first",
          "inputSql" -> s"\"collate(${ct.s1})\"",
          "inputType" -> s"\"STRING COLLATE ${ct.collation}\"",
          "requiredType" -> "\"STRING\""
        ),
        context = ExpectedContext(
          fragment = s"regexp_count(collate('${ct.s1}', '${ct.collation}')," +
            s"collate('${ct.s2}', '${ct.collation}'))",
          start = 7,
          stop = 59 + 2 * ct.collation.length
        )
      )
    })
  }

  test("Support RegExpSubStr string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UTF8_BINARY", "BCD")
    )
    checks.foreach(ct => {
      checkAnswer(sql(s"SELECT regexp_substr(collate('${ct.s1}', '${ct.collation}')" +
        s",collate('${ct.s2}', '${ct.collation}'))"), Row(ct.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABCDE", ".c.", "UTF8_BINARY_LCASE", ""),
      CollationTestCase("ABCDE", ".C.", "UNICODE", "BCD"),
      CollationTestCase("ABCDE", ".c.", "UNICODE_CI", "")
    )
    fails.foreach(ct => {
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT regexp_substr(collate('${ct.s1}', '${ct.collation}')" +
            s",collate('${ct.s2}', '${ct.collation}'))")
        },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"regexp_substr(collate(${ct.s1}), collate(${ct.s2}))\"",
          "paramIndex" -> "first",
          "inputSql" -> s"\"collate(${ct.s1})\"",
          "inputType" -> s"\"STRING COLLATE ${ct.collation}\"",
          "requiredType" -> "\"STRING\""
        ),
        context = ExpectedContext(
          fragment = s"regexp_substr(collate('${ct.s1}', '${ct.collation}')," +
            s"collate('${ct.s2}', '${ct.collation}'))",
          start = 7,
          stop = 60 + 2 * ct.collation.length
        )
      )
    })
  }

  test("Support RegExpInStr string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UTF8_BINARY", 2)
    )
    checks.foreach(ct => {
      checkAnswer(sql(s"SELECT regexp_instr(collate('${ct.s1}', '${ct.collation}')" +
        s",collate('${ct.s2}', '${ct.collation}'))"), Row(ct.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABCDE", ".c.", "UTF8_BINARY_LCASE", 0),
      CollationTestCase("ABCDE", ".C.", "UNICODE", 2),
      CollationTestCase("ABCDE", ".c.", "UNICODE_CI", 0)
    )
    fails.foreach(ct => {
      checkError(
        exception = intercept[ExtendedAnalysisException] {
          sql(s"SELECT regexp_instr(collate('${ct.s1}', '${ct.collation}')" +
            s",collate('${ct.s2}', '${ct.collation}'))")
        },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"regexp_instr(collate(${ct.s1}), collate(${ct.s2}), 0)\"",
          "paramIndex" -> "first",
          "inputSql" -> s"\"collate(${ct.s1})\"",
          "inputType" -> s"\"STRING COLLATE ${ct.collation}\"",
          "requiredType" -> "\"STRING\""
        ),
        context = ExpectedContext(
          fragment = s"regexp_instr(collate('${ct.s1}', '${ct.collation}')," +
            s"collate('${ct.s2}', '${ct.collation}'))",
          start = 7,
          stop = 59 + 2 * ct.collation.length
        )
      )
    })
  }
}

class CollationRegexpExpressionsANSISuite extends CollationRegexpExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
