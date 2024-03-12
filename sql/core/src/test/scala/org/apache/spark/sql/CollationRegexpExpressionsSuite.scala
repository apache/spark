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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.test.SharedSparkSession

class CollationRegexpExpressionsSuite extends QueryTest with SharedSparkSession {

  test("Check collation compatibility for regexp functions") {
    // like
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT 'ABC' like collate('%b%', 'UNICODE_CI')")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"UCS_BASIC",
        "collationNameRight" -> s"UNICODE_CI",
        "sqlExpr" -> "\"ABC LIKE collate(%b%)\""
      ),
      context = ExpectedContext(fragment =
        s"like collate('%b%', 'UNICODE_CI')",
        start = 13, stop = 45)
    )
    // ilike
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT 'ABC' ilike collate('%b%', 'UNICODE_CI')")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"UCS_BASIC",
        "collationNameRight" -> s"UNICODE_CI",
        "sqlExpr" -> "\"ilike(ABC, collate(%b%))\""
      ),
      context = ExpectedContext(fragment =
        s"ilike collate('%b%', 'UNICODE_CI')",
        start = 13, stop = 46)
    )
    // rlike
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT 'ABC' rlike collate('%b%', 'UNICODE_CI')")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"UCS_BASIC",
        "collationNameRight" -> s"UNICODE_CI",
        "sqlExpr" -> "\"RLIKE(ABC, collate(%b%))\""
      ),
      context = ExpectedContext(fragment =
        s"rlike collate('%b%', 'UNICODE_CI')",
        start = 13, stop = 46)
    )
    // split
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT split('ABC', collate('[B]', 'UNICODE_CI'))")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"UCS_BASIC",
        "collationNameRight" -> s"UNICODE_CI",
        "sqlExpr" -> "\"split(ABC, collate([B]), -1)\""
      ),
      context = ExpectedContext(fragment =
        s"split('ABC', collate('[B]', 'UNICODE_CI'))",
        start = 7, stop = 48)
    )
    // regexp_replace
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT regexp_replace('ABCDE', collate('.c.', 'UNICODE_CI'), 'F')")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"UCS_BASIC",
        "collationNameRight" -> s"UNICODE_CI",
        "sqlExpr" -> "\"regexp_replace(ABCDE, collate(.c.), F, 1)\""
      ),
      context = ExpectedContext(fragment =
        s"regexp_replace('ABCDE', collate('.c.', 'UNICODE_CI'), 'F')",
        start = 7, stop = 64)
    )
    // regexp_extract
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT regexp_extract('ABCDE', collate('.c.', 'UNICODE_CI'))")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"UCS_BASIC",
        "collationNameRight" -> s"UNICODE_CI",
        "sqlExpr" -> "\"regexp_extract(ABCDE, collate(.c.), 1)\""
      ),
      context = ExpectedContext(fragment =
        s"regexp_extract('ABCDE', collate('.c.', 'UNICODE_CI'))",
        start = 7, stop = 59)
    )
    // regexp_extract_all
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT regexp_extract_all('ABCDE', collate('.c.', 'UNICODE_CI'))")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"UCS_BASIC",
        "collationNameRight" -> s"UNICODE_CI",
        "sqlExpr" -> "\"regexp_extract_all(ABCDE, collate(.c.), 1)\""
      ),
      context = ExpectedContext(fragment =
        s"regexp_extract_all('ABCDE', collate('.c.', 'UNICODE_CI'))",
        start = 7, stop = 63)
    )
    // regexp_count
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT regexp_count('ABCDE', collate('.c.', 'UNICODE_CI'))")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"UCS_BASIC",
        "collationNameRight" -> s"UNICODE_CI",
        "sqlExpr" -> "\"regexp_count(ABCDE, collate(.c.))\""
      ),
      context = ExpectedContext(fragment =
        s"regexp_count('ABCDE', collate('.c.', 'UNICODE_CI'))",
        start = 7, stop = 57)
    )
    // regexp_substr
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT regexp_substr('ABCDE', collate('.c.', 'UNICODE_CI'))")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"UCS_BASIC",
        "collationNameRight" -> s"UNICODE_CI",
        "sqlExpr" -> "\"regexp_substr(ABCDE, collate(.c.))\""
      ),
      context = ExpectedContext(fragment =
        s"regexp_substr('ABCDE', collate('.c.', 'UNICODE_CI'))",
        start = 7, stop = 58)
    )
    // regexp_instr
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        spark.sql(s"SELECT regexp_instr('ABCDE', collate('.c.', 'UNICODE_CI'))")
      },
      errorClass = "DATATYPE_MISMATCH.COLLATION_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "collationNameLeft" -> s"UCS_BASIC",
        "collationNameRight" -> s"UNICODE_CI",
        "sqlExpr" -> "\"regexp_instr(ABCDE, collate(.c.), 0)\""
      ),
      context = ExpectedContext(fragment =
        s"regexp_instr('ABCDE', collate('.c.', 'UNICODE_CI'))",
        start = 7, stop = 57)
    )
  }

  case class CollationTestCase[R](left: String, right: String, collation: String, expectedResult: R)
  case class CollationTestFail[R](left: String, right: String, collation: String)

  test("Support Like string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABC", "%B%", "UCS_BASIC", true),
      CollationTestCase("ABC", "%B%", "UNICODE", true)
    )
    checks.foreach(testCase => {
      checkAnswer(sql(s"SELECT collate('${testCase.left}', '${testCase.collation}') like " +
        s"collate('${testCase.right}', '${testCase.collation}')"), Row(testCase.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABC", "%b%", "UCS_BASIC_LCASE", false),
      CollationTestCase("ABC", "%b%", "UNICODE_CI", false)
    )
    fails.foreach(testCase => {
      checkError(
        exception = intercept[SparkException] {
          sql(s"SELECT collate('${testCase.left}', '${testCase.collation}') like " +
            s"collate('${testCase.right}', '${testCase.collation}')")
        },
        errorClass = "UNSUPPORTED_COLLATION.FOR_FUNCTION",
        sqlState = "0A000",
        parameters = Map(
          "functionName" -> "`like`",
          "collationName" -> s"${testCase.collation}"
        )
      )
    })
  }

  test("Support ILike string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABC", "%b%", "UCS_BASIC", true),
      CollationTestCase("ABC", "%b%", "UNICODE", true)
    )
    checks.foreach(testCase => {
      checkAnswer(sql(s"SELECT collate('${testCase.left}', '${testCase.collation}') ilike " +
        s"collate('${testCase.right}', '${testCase.collation}')"), Row(testCase.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABC", "%b%", "UCS_BASIC_LCASE", false),
      CollationTestCase("ABC", "%b%", "UNICODE_CI", false)
    )
    fails.foreach(testCase => {
      checkError(
        exception = intercept[SparkException] {
          sql(s"SELECT collate('${testCase.left}', '${testCase.collation}') ilike " +
            s"collate('${testCase.right}', '${testCase.collation}')")
        },
        errorClass = "UNSUPPORTED_COLLATION.FOR_FUNCTION",
        sqlState = "0A000",
        parameters = Map(
          "functionName" -> "`ilike`",
          "collationName" -> s"${testCase.collation}"
        )
      )
    })
  }

  test("Support RLike string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABC", ".B.", "UCS_BASIC", true),
      CollationTestCase("ABC", ".B.", "UNICODE", true)
    )
    checks.foreach(testCase => {
      checkAnswer(sql(s"SELECT collate('${testCase.left}', '${testCase.collation}') rlike " +
        s"collate('${testCase.right}', '${testCase.collation}')"), Row(testCase.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABC", ".b.", "UCS_BASIC_LCASE", false),
      CollationTestCase("ABC", ".b.", "UNICODE_CI", false)
    )
    fails.foreach(testCase => {
      checkError(
        exception = intercept[SparkException] {
          sql(s"SELECT collate('${testCase.left}', '${testCase.collation}') rlike " +
            s"collate('${testCase.right}', '${testCase.collation}')")
        },
        errorClass = "UNSUPPORTED_COLLATION.FOR_FUNCTION",
        sqlState = "0A000",
        parameters = Map(
          "functionName" -> "`rlike`",
          "collationName" -> s"${testCase.collation}"
        )
      )
    })
  }

  test("Support StringSplit string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABC", "[B]", "UCS_BASIC", 2),
      CollationTestCase("ABC", "[B]", "UNICODE", 2)
    )
    checks.foreach(testCase => {
      checkAnswer(sql(s"SELECT size(split(collate('${testCase.left}', '${testCase.collation}')" +
        s",collate('${testCase.right}', '${testCase.collation}')))"), Row(testCase.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABC", "b", "UCS_BASIC_LCASE", 0),
      CollationTestCase("ABC", "b", "UNICODE_CI", 0)
    )
    fails.foreach(testCase => {
      checkError(
        exception = intercept[SparkException] {
          sql(s"SELECT size(split(collate('${testCase.left}', '${testCase.collation}')" +
            s",collate('${testCase.right}', '${testCase.collation}')))")
        },
        errorClass = "UNSUPPORTED_COLLATION.FOR_FUNCTION",
        sqlState = "0A000",
        parameters = Map(
          "functionName" -> "`split`",
          "collationName" -> s"${testCase.collation}"
        )
      )
    })
  }

  test("Support RegExpReplace string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UCS_BASIC", "AFFFE"),
      CollationTestCase("ABCDE", ".C.", "UNICODE", "AFFFE")
    )
    checks.foreach(testCase => {
      checkAnswer(
        sql(s"SELECT regexp_replace(collate('${testCase.left}', '${testCase.collation}')" +
          s",collate('${testCase.right}', '${testCase.collation}')" +
          s",collate('FFF', '${testCase.collation}'))"),
        Row(testCase.expectedResult)
      )
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABCDE", ".c.", "UCS_BASIC_LCASE", ""),
      CollationTestCase("ABCDE", ".c.", "UNICODE_CI", "")
    )
    fails.foreach(testCase => {
      checkError(
        exception = intercept[SparkException] {
          sql(s"SELECT regexp_replace(collate('${testCase.left}', '${testCase.collation}')" +
            s",collate('${testCase.right}', '${testCase.collation}')" +
            s",collate('FFF', '${testCase.collation}'))")
        },
        errorClass = "UNSUPPORTED_COLLATION.FOR_FUNCTION",
        sqlState = "0A000",
        parameters = Map(
          "functionName" -> "`regexp_replace`",
          "collationName" -> s"${testCase.collation}"
        )
      )
    })
  }

  test("Support RegExpExtract string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UCS_BASIC", "BCD"),
      CollationTestCase("ABCDE", ".C.", "UNICODE", "BCD")
    )
    checks.foreach(testCase => {
      checkAnswer(
        sql(s"SELECT regexp_extract(collate('${testCase.left}', '${testCase.collation}')" +
          s",collate('${testCase.right}', '${testCase.collation}'),0)"),
        Row(testCase.expectedResult)
      )
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABCDE", ".c.", "UCS_BASIC_LCASE", ""),
      CollationTestCase("ABCDE", ".c.", "UNICODE_CI", "")
    )
    fails.foreach(testCase => {
      checkError(
        exception = intercept[SparkException] {
          sql(
            s"SELECT regexp_extract(collate('${testCase.left}', '${testCase.collation}')" +
              s",collate('${testCase.right}', '${testCase.collation}'),0)")
        },
        errorClass = "UNSUPPORTED_COLLATION.FOR_FUNCTION",
        sqlState = "0A000",
        parameters = Map(
          "functionName" -> "`regexp_extract`",
          "collationName" -> s"${testCase.collation}"
        )
      )
    })
  }

  test("Support RegExpExtractAll string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UCS_BASIC", 1),
      CollationTestCase("ABCDE", ".C.", "UNICODE", 1)
    )
    checks.foreach(testCase => {
      checkAnswer(
        sql(s"SELECT size(regexp_extract_all(collate('${testCase.left}', '${testCase.collation}')" +
          s",collate('${testCase.right}', '${testCase.collation}'),0))"),
        Row(testCase.expectedResult)
      )
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABCDE", ".c.", "UCS_BASIC_LCASE", 0),
      CollationTestCase("ABCDE", ".c.", "UNICODE_CI", 0)
    )
    fails.foreach(testCase => {
      checkError(
        exception = intercept[SparkException] {
          sql(
            s"SELECT size(regexp_extract_all(collate('${testCase.left}', '${testCase.collation}')" +
              s",collate('${testCase.right}', '${testCase.collation}'),0))")
        },
        errorClass = "UNSUPPORTED_COLLATION.FOR_FUNCTION",
        sqlState = "0A000",
        parameters = Map(
          "functionName" -> "`regexp_extract_all`",
          "collationName" -> s"${testCase.collation}"
        )
      )
    })
  }

  test("Support RegExpCount string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UCS_BASIC", 1),
      CollationTestCase("ABCDE", ".C.", "UNICODE", 1)
    )
    checks.foreach(testCase => {
      checkAnswer(sql(s"SELECT regexp_count(collate('${testCase.left}', '${testCase.collation}')" +
        s",collate('${testCase.right}', '${testCase.collation}'))"), Row(testCase.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABCDE", ".c.", "UCS_BASIC_LCASE", 0),
      CollationTestCase("ABCDE", ".c.", "UNICODE_CI", 0)
    )
    fails.foreach(testCase => {
      checkError(
        exception = intercept[SparkException] {
          sql(s"SELECT regexp_count(collate('${testCase.left}', '${testCase.collation}')" +
            s",collate('${testCase.right}', '${testCase.collation}'))")
        },
        errorClass = "UNSUPPORTED_COLLATION.FOR_FUNCTION",
        sqlState = "0A000",
        parameters = Map(
          "functionName" -> "`regexp_count`",
          "collationName" -> s"${testCase.collation}"
        )
      )
    })
  }

  test("Support RegExpSubStr string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UCS_BASIC", "BCD"),
      CollationTestCase("ABCDE", ".C.", "UNICODE", "BCD")
    )
    checks.foreach(testCase => {
      checkAnswer(sql(s"SELECT regexp_substr(collate('${testCase.left}', '${testCase.collation}')" +
        s",collate('${testCase.right}', '${testCase.collation}'))"), Row(testCase.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABCDE", ".c.", "UCS_BASIC_LCASE", ""),
      CollationTestCase("ABCDE", ".c.", "UNICODE_CI", "")
    )
    fails.foreach(testCase => {
      checkError(
        exception = intercept[SparkException] {
          sql(s"SELECT regexp_substr(collate('${testCase.left}', '${testCase.collation}')" +
            s",collate('${testCase.right}', '${testCase.collation}'))")
        },
        errorClass = "UNSUPPORTED_COLLATION.FOR_FUNCTION",
        sqlState = "0A000",
        parameters = Map(
          "functionName" -> "`regexp_substr`",
          "collationName" -> s"${testCase.collation}"
        )
      )
    })
  }

  test("Support RegExpInStr string expression with Collation") {
    // Supported collations
    val checks = Seq(
      CollationTestCase("ABCDE", ".C.", "UCS_BASIC", 2),
      CollationTestCase("ABCDE", ".C.", "UNICODE", 2)
    )
    checks.foreach(testCase => {
      checkAnswer(sql(s"SELECT regexp_instr(collate('${testCase.left}', '${testCase.collation}')" +
        s",collate('${testCase.right}', '${testCase.collation}'))"), Row(testCase.expectedResult))
    })
    // Unsupported collations
    val fails = Seq(
      CollationTestCase("ABCDE", ".c.", "UCS_BASIC_LCASE", 0),
      CollationTestCase("ABCDE", ".c.", "UNICODE_CI", 0)
    )
    fails.foreach(testCase => {
      checkError(
        exception = intercept[SparkException] {
          sql(s"SELECT regexp_instr(collate('${testCase.left}', '${testCase.collation}')" +
            s",collate('${testCase.right}', '${testCase.collation}'))")
        },
        errorClass = "UNSUPPORTED_COLLATION.FOR_FUNCTION",
        sqlState = "0A000",
        parameters = Map(
          "functionName" -> "`regexp_instr`",
          "collationName" -> s"${testCase.collation}"
        )
      )
    })
  }

}
