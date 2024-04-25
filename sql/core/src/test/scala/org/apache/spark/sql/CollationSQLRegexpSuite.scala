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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, StringType}

// scalastyle:off nonascii
class CollationSQLRegexpSuite
  extends QueryTest
  with SharedSparkSession
  with ExpressionEvalHelper {

  test("Support Like string expression with collation") {
    // Supported collations
    case class LikeTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      LikeTestCase("ABC", "%B%", "UTF8_BINARY", true),
      LikeTestCase("AḂC", "%ḃ%", "UTF8_BINARY_LCASE", true),
      LikeTestCase("ABC", "%b%", "UNICODE", false)
    )
    testCases.foreach(t => {
      val query = s"SELECT like(collate('${t.l}', '${t.c}'), '${t.r}')"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
    })
    // Unsupported collations
    case class LikeTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      LikeTestFail("ABC", "%b%", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT like(collate('${t.l}', '${t.c}'), '${t.r}')"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support ILike string expression with collation") {
    // Supported collations
    case class ILikeTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      ILikeTestCase("ABC", "%b%", "UTF8_BINARY", true),
      ILikeTestCase("AḂC", "%ḃ%", "UTF8_BINARY_LCASE", true),
      ILikeTestCase("ABC", "%b%", "UNICODE", true)
    )
    testCases.foreach(t => {
      val query = s"SELECT ilike(collate('${t.l}', '${t.c}'), '${t.r}')"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
    })
    // Unsupported collations
    case class ILikeTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      ILikeTestFail("ABC", "%b%", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT ilike(collate('${t.l}', '${t.c}'), '${t.r}')"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support LikeAll string expression with collation") {
    // Supported collations
    case class LikeAllTestCase[R](s: String, p: Seq[String], c: String, result: R)
    val testCases = Seq(
      LikeAllTestCase("foo", Seq("%foo%", "%oo"), "UTF8_BINARY", true),
      LikeAllTestCase("Foo", Seq("%foo%", "%oo"), "UTF8_BINARY_LCASE", true),
      LikeAllTestCase("foo", Seq("%foo%", "%bar%"), "UNICODE", false)
    )
    testCases.foreach(t => {
      val query = s"SELECT collate('${t.s}', '${t.c}') LIKE ALL ('${t.p.mkString("','")}')"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
    })
    // Unsupported collations
    case class LikeAllTestFail(s: String, p: Seq[String], c: String)
    val failCases = Seq(
      LikeAllTestFail("Foo", Seq("%foo%", "%oo"), "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT collate('${t.s}', '${t.c}') LIKE ALL ('${t.p.mkString("','")}')"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support NotLikeAll string expression with collation") {
    // Supported collations
    case class NotLikeAllTestCase[R](s: String, p: Seq[String], c: String, result: R)
    val testCases = Seq(
      NotLikeAllTestCase("foo", Seq("%foo%", "%oo"), "UTF8_BINARY", false),
      NotLikeAllTestCase("Foo", Seq("%foo%", "%oo"), "UTF8_BINARY_LCASE", false),
      NotLikeAllTestCase("foo", Seq("%goo%", "%bar%"), "UNICODE", true)
    )
    testCases.foreach(t => {
      val query = s"SELECT collate('${t.s}', '${t.c}') NOT LIKE ALL ('${t.p.mkString("','")}')"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
    })
    // Unsupported collations
    case class NotLikeAllTestFail(s: String, p: Seq[String], c: String)
    val failCases = Seq(
      NotLikeAllTestFail("Foo", Seq("%foo%", "%oo"), "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT collate('${t.s}', '${t.c}') NOT LIKE ALL ('${t.p.mkString("','")}')"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support LikeAny string expression with collation") {
    // Supported collations
    case class LikeAnyTestCase[R](s: String, p: Seq[String], c: String, result: R)
    val testCases = Seq(
      LikeAnyTestCase("foo", Seq("%foo%", "%bar"), "UTF8_BINARY", true),
      LikeAnyTestCase("Foo", Seq("%foo%", "%bar"), "UTF8_BINARY_LCASE", true),
      LikeAnyTestCase("foo", Seq("%goo%", "%hoo%"), "UNICODE", false)
    )
    testCases.foreach(t => {
      val query = s"SELECT collate('${t.s}', '${t.c}') LIKE ANY ('${t.p.mkString("','")}')"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
    })
    // Unsupported collations
    case class LikeAnyTestFail(s: String, p: Seq[String], c: String)
    val failCases = Seq(
      LikeAnyTestFail("Foo", Seq("%foo%", "%oo"), "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT collate('${t.s}', '${t.c}') LIKE ANY ('${t.p.mkString("','")}')"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support NotLikeAny string expression with collation") {
    // Supported collations
    case class NotLikeAnyTestCase[R](s: String, p: Seq[String], c: String, result: R)
    val testCases = Seq(
      NotLikeAnyTestCase("foo", Seq("%foo%", "%hoo"), "UTF8_BINARY", true),
      NotLikeAnyTestCase("Foo", Seq("%foo%", "%hoo"), "UTF8_BINARY_LCASE", true),
      NotLikeAnyTestCase("foo", Seq("%foo%", "%oo%"), "UNICODE", false)
    )
    testCases.foreach(t => {
      val query = s"SELECT collate('${t.s}', '${t.c}') NOT LIKE ANY ('${t.p.mkString("','")}')"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
    })
    // Unsupported collations
    case class NotLikeAnyTestFail(s: String, p: Seq[String], c: String)
    val failCases = Seq(
      NotLikeAnyTestFail("Foo", Seq("%foo%", "%oo"), "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT collate('${t.s}', '${t.c}') NOT LIKE ANY ('${t.p.mkString("','")}')"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support RLike string expression with collation") {
    // Supported collations
    case class RLikeTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RLikeTestCase("ABC", ".B.", "UTF8_BINARY", true),
      RLikeTestCase("AḂC", ".ḃ.", "UTF8_BINARY_LCASE", true),
      RLikeTestCase("ABC", ".b.", "UNICODE", false)
    )
    testCases.foreach(t => {
      val query = s"SELECT rlike(collate('${t.l}', '${t.c}'), '${t.r}')"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
    })
    // Unsupported collations
    case class RLikeTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RLikeTestFail("ABC", ".b.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT rlike(collate('${t.l}', '${t.c}'), '${t.r}')"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support StringSplit string expression with collation") {
    // Supported collations
    case class StringSplitTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      StringSplitTestCase("ABC", "[B]", "UTF8_BINARY", Seq("A", "C")),
      StringSplitTestCase("AḂC", "[ḃ]", "UTF8_BINARY_LCASE", Seq("A", "C")),
      StringSplitTestCase("ABC", "[B]", "UNICODE", Seq("A", "C"))
    )
    testCases.foreach(t => {
      val query = s"SELECT split(collate('${t.l}', '${t.c}'), '${t.r}')"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(ArrayType(StringType(t.c))))
    })
    // Unsupported collations
    case class StringSplitTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      StringSplitTestFail("ABC", "[b]", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT split(collate('${t.l}', '${t.c}'), '${t.r}')"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support RegExpReplace string expression with collation") {
    // Supported collations
    case class RegExpReplaceTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpReplaceTestCase("ABCDE", ".C.", "UTF8_BINARY", "AFFFE"),
      RegExpReplaceTestCase("ABĆDE", ".ć.", "UTF8_BINARY_LCASE", "AFFFE"),
      RegExpReplaceTestCase("ABCDE", ".c.", "UNICODE", "ABCDE")
    )
    testCases.foreach(t => {
      val query =
        s"SELECT regexp_replace(collate('${t.l}', '${t.c}'), '${t.r}', collate('FFF', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      // Implicit casting
      checkAnswer(sql(s"SELECT regexp_replace(collate('${t.l}', '${t.c}'), '${t.r}', 'FFF')"),
        Row(t.result))
      checkAnswer(sql(s"SELECT regexp_replace('${t.l}', '${t.r}', collate('FFF', '${t.c}'))"),
        Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT regexp_replace(collate('ABCDE','UTF8_BINARY'), '.c.', collate('FFF','UNICODE'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
    // Unsupported collations
    case class RegExpReplaceTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpReplaceTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query =
        s"SELECT regexp_replace(collate('${t.l}', '${t.c}'), '${t.r}', 'FFF')"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support RegExpExtract string expression with collation") {
    // Supported collations
    case class RegExpExtractTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpExtractTestCase("ABCDE", ".C.", "UTF8_BINARY", "BCD"),
      RegExpExtractTestCase("ABĆDE", ".ć.", "UTF8_BINARY_LCASE", "BĆD"),
      RegExpExtractTestCase("ABCDE", ".c.", "UNICODE", "")
    )
    testCases.foreach(t => {
      val query =
        s"SELECT regexp_extract(collate('${t.l}', '${t.c}'), '${t.r}', 0)"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
    })
    // Unsupported collations
    case class RegExpExtractTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpExtractTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query =
        s"SELECT regexp_extract(collate('${t.l}', '${t.c}'), '${t.r}', 0)"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support RegExpExtractAll string expression with collation") {
    // Supported collations
    case class RegExpExtractAllTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpExtractAllTestCase("ABCDE", ".C.", "UTF8_BINARY", Seq("BCD")),
      RegExpExtractAllTestCase("ABĆDE", ".ć.", "UTF8_BINARY_LCASE", Seq("BĆD")),
      RegExpExtractAllTestCase("ABCDE", ".c.", "UNICODE", Seq())
    )
    testCases.foreach(t => {
      val query =
        s"SELECT regexp_extract_all(collate('${t.l}', '${t.c}'), '${t.r}', 0)"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(ArrayType(StringType(t.c))))
    })
    // Unsupported collations
    case class RegExpExtractAllTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpExtractAllTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query =
        s"SELECT regexp_extract_all(collate('${t.l}', '${t.c}'), '${t.r}', 0)"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support RegExpCount string expression with collation") {
    // Supported collations
    case class RegExpCountTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpCountTestCase("ABCDE", ".C.", "UTF8_BINARY", 1),
      RegExpCountTestCase("ABĆDE", ".ć.", "UTF8_BINARY_LCASE", 1),
      RegExpCountTestCase("ABCDE", ".c.", "UNICODE", 0)
    )
    testCases.foreach(t => {
      val query = s"SELECT regexp_count(collate('${t.l}', '${t.c}'), '${t.r}')"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
    })
    // Unsupported collations
    case class RegExpCountTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpCountTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT regexp_count(collate('${t.l}', '${t.c}'), '${t.r}')"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support RegExpSubStr string expression with collation") {
    // Supported collations
    case class RegExpSubStrTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpSubStrTestCase("ABCDE", ".C.", "UTF8_BINARY", "BCD"),
      RegExpSubStrTestCase("ABĆDE", ".ć.", "UTF8_BINARY_LCASE", "BĆD"),
      RegExpSubStrTestCase("ABCDE", ".c.", "UNICODE", null)
    )
    testCases.foreach(t => {
      val query = s"SELECT regexp_substr(collate('${t.l}', '${t.c}'), '${t.r}')"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
    })
    // Unsupported collations
    case class RegExpSubStrTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpSubStrTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT regexp_substr(collate('${t.l}', '${t.c}'), '${t.r}')"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

  test("Support RegExpInStr string expression with collation") {
    // Supported collations
    case class RegExpInStrTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpInStrTestCase("ABCDE", ".C.", "UTF8_BINARY", 2),
      RegExpInStrTestCase("ABĆDE", ".ć.", "UTF8_BINARY_LCASE", 2),
      RegExpInStrTestCase("ABCDE", ".c.", "UNICODE", 0)
    )
    testCases.foreach(t => {
      val query = s"SELECT regexp_instr(collate('${t.l}', '${t.c}'), '${t.r}')"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
    })
    // Unsupported collations
    case class RegExpInStrTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpInStrTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT regexp_instr(collate('${t.l}', '${t.c}'), '${t.r}')"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
  }

}
// scalastyle:on nonascii
