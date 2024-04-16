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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, StringType}

class CollationRegexpExpressionsSuite
  extends QueryTest
  with SharedSparkSession
  with ExpressionEvalHelper {

  test("Support Like string expression with collation") {
    // Supported collations
    case class LikeTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      LikeTestCase("ABC", "%B%", "UTF8_BINARY", true)
    )
    testCases.foreach(t => {
      val query = s"SELECT like(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      // TODO: Implicit casting (not currently supported)
    })
    // Unsupported collations
    case class LikeTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      LikeTestFail("ABC", "%b%", "UTF8_BINARY_LCASE"),
      LikeTestFail("ABC", "%B%", "UNICODE"),
      LikeTestFail("ABC", "%b%", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT like(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      val unsupportedCollation = intercept[AnalysisException] { sql(query) }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
    // TODO: Collation mismatch (not currently supported)
  }

  test("Support ILike string expression with collation") {
    // Supported collations
    case class ILikeTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      ILikeTestCase("ABC", "%b%", "UTF8_BINARY", true)
    )
    testCases.foreach(t => {
      val query = s"SELECT ilike(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      // TODO: Implicit casting (not currently supported)
    })
    // Unsupported collations
    case class ILikeTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      ILikeTestFail("ABC", "%b%", "UTF8_BINARY_LCASE"),
      ILikeTestFail("ABC", "%b%", "UNICODE"),
      ILikeTestFail("ABC", "%b%", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT ilike(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      val unsupportedCollation = intercept[AnalysisException] { sql(query) }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
    // TODO: Collation mismatch (not currently supported)
  }

  test("Support RLike string expression with collation") {
    // Supported collations
    case class RLikeTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RLikeTestCase("ABC", ".B.", "UTF8_BINARY", true)
    )
    testCases.foreach(t => {
      val query = s"SELECT rlike(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      // TODO: Implicit casting (not currently supported)
    })
    // Unsupported collations
    case class RLikeTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RLikeTestFail("ABC", ".b.", "UTF8_BINARY_LCASE"),
      RLikeTestFail("ABC", ".B.", "UNICODE"),
      RLikeTestFail("ABC", ".b.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT rlike(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      val unsupportedCollation = intercept[AnalysisException] { sql(query) }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
    // TODO: Collation mismatch (not currently supported)
  }

  test("Support StringSplit string expression with collation") {
    // Supported collations
    case class StringSplitTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      StringSplitTestCase("ABC", "[B]", "UTF8_BINARY", Seq("A", "C"))
    )
    testCases.foreach(t => {
      val query = s"SELECT split(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(ArrayType(StringType(t.c))))
      // TODO: Implicit casting (not currently supported)
    })
    // Unsupported collations
    case class StringSplitTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      StringSplitTestFail("ABC", "[b]", "UTF8_BINARY_LCASE"),
      StringSplitTestFail("ABC", "[B]", "UNICODE"),
      StringSplitTestFail("ABC", "[b]", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT split(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      val unsupportedCollation = intercept[AnalysisException] { sql(query) }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
    // TODO: Collation mismatch (not currently supported)
  }

  test("Support RegExpReplace string expression with collation") {
    // Supported collations
    case class RegExpReplaceTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpReplaceTestCase("ABCDE", ".C.", "UTF8_BINARY", "AFFFE")
    )
    testCases.foreach(t => {
      val query =
        s"SELECT regexp_replace(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'), 'FFF')"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      // TODO: Implicit casting (not currently supported)
    })
    // Unsupported collations
    case class RegExpReplaceTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpReplaceTestFail("ABCDE", ".c.", "UTF8_BINARY_LCASE"),
      RegExpReplaceTestFail("ABCDE", ".C.", "UNICODE"),
      RegExpReplaceTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query =
        s"SELECT regexp_replace(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'), 'FFF')"
      val unsupportedCollation = intercept[AnalysisException] { sql(query) }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
    // TODO: Collation mismatch (not currently supported)
  }

  test("Support RegExpExtract string expression with collation") {
    // Supported collations
    case class RegExpExtractTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpExtractTestCase("ABCDE", ".C.", "UTF8_BINARY", "BCD")
    )
    testCases.foreach(t => {
      val query =
        s"SELECT regexp_extract(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'), 0)"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      // TODO: Implicit casting (not currently supported)
    })
    // Unsupported collations
    case class RegExpExtractTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpExtractTestFail("ABCDE", ".c.", "UTF8_BINARY_LCASE"),
      RegExpExtractTestFail("ABCDE", ".C.", "UNICODE"),
      RegExpExtractTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query =
        s"SELECT regexp_extract(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'), 0)"
      val unsupportedCollation = intercept[AnalysisException] { sql(query) }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
    // TODO: Collation mismatch (not currently supported)
  }

  test("Support RegExpExtractAll string expression with collation") {
    // Supported collations
    case class RegExpExtractAllTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpExtractAllTestCase("ABCDE", ".C.", "UTF8_BINARY", Seq("BCD"))
    )
    testCases.foreach(t => {
      val query =
        s"SELECT regexp_extract_all(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'), 0)"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(ArrayType(StringType(t.c))))
      // TODO: Implicit casting (not currently supported)
    })
    // Unsupported collations
    case class RegExpExtractAllTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpExtractAllTestFail("ABCDE", ".c.", "UTF8_BINARY_LCASE"),
      RegExpExtractAllTestFail("ABCDE", ".C.", "UNICODE"),
      RegExpExtractAllTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query =
        s"SELECT regexp_extract_all(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'), 0)"
      val unsupportedCollation = intercept[AnalysisException] { sql(query) }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
    // TODO: Collation mismatch (not currently supported)
  }

  test("Support RegExpCount string expression with collation") {
    // Supported collations
    case class RegExpCountTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpCountTestCase("ABCDE", ".C.", "UTF8_BINARY", 1)
    )
    testCases.foreach(t => {
      val query = s"SELECT regexp_count(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      // TODO: Implicit casting (not currently supported)
    })
    // Unsupported collations
    case class RegExpCountTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpCountTestFail("ABCDE", ".c.", "UTF8_BINARY_LCASE"),
      RegExpCountTestFail("ABCDE", ".C.", "UNICODE"),
      RegExpCountTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT regexp_count(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      val unsupportedCollation = intercept[AnalysisException] { sql(query) }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
    // TODO: Collation mismatch (not currently supported)
  }

  test("Support RegExpSubStr string expression with collation") {
    // Supported collations
    case class RegExpSubStrTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpSubStrTestCase("ABCDE", ".C.", "UTF8_BINARY", "BCD")
    )
    testCases.foreach(t => {
      val query = s"SELECT regexp_substr(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      // TODO: Implicit casting (not currently supported)
    })
    // Unsupported collations
    case class RegExpSubStrTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpSubStrTestFail("ABCDE", ".c.", "UTF8_BINARY_LCASE"),
      RegExpSubStrTestFail("ABCDE", ".C.", "UNICODE"),
      RegExpSubStrTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT regexp_substr(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      val unsupportedCollation = intercept[AnalysisException] { sql(query) }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
    // TODO: Collation mismatch (not currently supported)
  }

  test("Support RegExpInStr string expression with collation") {
    // Supported collations
    case class RegExpInStrTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpInStrTestCase("ABCDE", ".C.", "UTF8_BINARY", 2)
    )
    testCases.foreach(t => {
      val query = s"SELECT regexp_instr(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      // TODO: Implicit casting (not currently supported)
    })
    // Unsupported collations
    case class RegExpInStrTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpInStrTestFail("ABCDE", ".c.", "UTF8_BINARY_LCASE"),
      RegExpInStrTestFail("ABCDE", ".C.", "UNICODE"),
      RegExpInStrTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query = s"SELECT regexp_instr(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      val unsupportedCollation = intercept[AnalysisException] {
        sql(query)
      }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
    // TODO: Collation mismatch (not currently supported)
  }

}

class CollationRegexpExpressionsANSISuite extends CollationRegexpExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)

  // TODO: If needed, add more tests for other regexp expressions (with ANSI mode enabled)

}
