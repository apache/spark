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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, Literal, StringReplace}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, StringType}

class CollationStringExpressionsSuite
  extends QueryTest
  with SharedSparkSession
  with ExpressionEvalHelper {

  test("Support ConcatWs string expression with collation") {
    // Supported collations
    case class ConcatWsTestCase[R](s: String, a: Array[String], c: String, result: R)
    val testCases = Seq(
      ConcatWsTestCase(" ", Array("Spark", "SQL"), "UTF8_BINARY", "Spark SQL")
    )
    testCases.foreach(t => {
      val arrCollated = t.a.map(s => s"collate('$s', '${t.c}')").mkString(", ")
      var query = s"SELECT concat_ws(collate('${t.s}', '${t.c}'), $arrCollated)"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      // Implicit casting
      val arr = t.a.map(s => s"'$s'").mkString(", ")
      query = s"SELECT concat_ws(collate('${t.s}', '${t.c}'), $arr)"
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      query = s"SELECT concat_ws('${t.s}', $arrCollated)"
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
    })
    // Unsupported collations
    case class ConcatWsTestFail(s: String, a: Array[String], c: String)
    val failCases = Seq(
      ConcatWsTestFail(" ", Array("ABC", "%b%"), "UTF8_BINARY_LCASE"),
      ConcatWsTestFail(" ", Array("ABC", "%B%"), "UNICODE"),
      ConcatWsTestFail(" ", Array("ABC", "%b%"), "UNICODE_CI")
    )
    failCases.foreach(t => {
      val arrCollated = t.a.map(s => s"collate('$s', '${t.c}')").mkString(", ")
      val query = s"SELECT concat_ws(collate('${t.s}', '${t.c}'), $arrCollated)"
      val unsupportedCollation = intercept[AnalysisException] { sql(query) }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT concat_ws(' ',collate('Spark', 'UTF8_BINARY_LCASE'),collate('SQL', 'UNICODE'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("Support Contains string expression with collation") {
    // Supported collations
    case class ContainsTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      ContainsTestCase("", "", "UTF8_BINARY", true),
      ContainsTestCase("abcde", "C", "UNICODE", false),
      ContainsTestCase("abcde", "FGH", "UTF8_BINARY_LCASE", false),
      ContainsTestCase("abcde", "BCD", "UNICODE_CI", true)
    )
    testCases.foreach(t => {
      val query = s"SELECT contains(collate('${t.l}','${t.c}'),collate('${t.r}','${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      // Implicit casting
      checkAnswer(sql(s"SELECT contains(collate('${t.l}','${t.c}'),'${t.r}')"), Row(t.result))
      checkAnswer(sql(s"SELECT contains('${t.l}',collate('${t.r}','${t.c}'))"), Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT contains(collate('abcde','UTF8_BINARY_LCASE'),collate('C','UNICODE_CI'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("Support StartsWith string expression with collation") {
    // Supported collations
    case class StartsWithTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      StartsWithTestCase("", "", "UTF8_BINARY", true),
      StartsWithTestCase("abcde", "A", "UNICODE", false),
      StartsWithTestCase("abcde", "FGH", "UTF8_BINARY_LCASE", false),
      StartsWithTestCase("abcde", "ABC", "UNICODE_CI", true)
    )
    testCases.foreach(t => {
      val query = s"SELECT startswith(collate('${t.l}','${t.c}'),collate('${t.r}','${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      // Implicit casting
      checkAnswer(sql(s"SELECT startswith(collate('${t.l}', '${t.c}'),'${t.r}')"), Row(t.result))
      checkAnswer(sql(s"SELECT startswith('${t.l}', collate('${t.r}', '${t.c}'))"), Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT startswith(collate('abcde', 'UTF8_BINARY_LCASE'),collate('C', 'UNICODE_CI'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("REPLACE check result on explicitly collated strings") {
    def testReplace(source: String, search: String, replace: String,
                    collationId: Integer, expected: String): Unit = {
      val sourceLiteral = Literal.create(source, StringType(collationId))
      val searchLiteral = Literal.create(search, StringType(collationId))
      val replaceLiteral = Literal.create(replace, StringType(collationId))

      checkEvaluation(StringReplace(sourceLiteral, searchLiteral, replaceLiteral), expected)
    }

    // scalastyle:off
    var collationId = CollationFactory.collationNameToId("UTF8_BINARY")
    testReplace("r世eplace", "pl", "123", collationId, "r世e123ace")
    testReplace("replace", "pl", "", collationId, "reace")
    testReplace("repl世ace", "Pl", "", collationId, "repl世ace")
    testReplace("replace", "", "123", collationId, "replace")
    testReplace("abcabc", "b", "12", collationId, "a12ca12c")
    testReplace("abcdabcd", "bc", "", collationId, "adad")

    collationId = CollationFactory.collationNameToId("UTF8_BINARY_LCASE")
    testReplace("r世eplace", "pl", "xx", collationId, "r世exxace")
    testReplace("repl世ace", "PL", "AB", collationId, "reAB世ace")
    testReplace("Replace", "", "123", collationId, "Replace")
    testReplace("re世place", "世", "x", collationId, "rexplace")
    testReplace("abcaBc", "B", "12", collationId, "a12ca12c")
    testReplace("AbcdabCd", "Bc", "", collationId, "Adad")

    collationId = CollationFactory.collationNameToId("UNICODE")
    testReplace("re世place", "plx", "123", collationId, "re世place")
    testReplace("世Replace", "re", "", collationId, "世Replace")
    testReplace("replace世", "", "123", collationId, "replace世")
    testReplace("aBc世abc", "b", "12", collationId, "aBc世a12c")
    testReplace("abcdabcd", "bc", "", collationId, "adad")

    collationId = CollationFactory.collationNameToId("UNICODE_CI")
    testReplace("replace", "plx", "123", collationId, "replace")
    testReplace("Replace", "re", "", collationId, "place")
    testReplace("replace", "", "123", collationId, "replace")
    testReplace("aBc世abc", "b", "12", collationId, "a12c世a12c")
    testReplace("a世Bcdabcd", "bC", "", collationId, "a世dad")
    // scalastyle:on
  }

  test("Support EndsWith string expression with collation") {
    // Supported collations
    case class EndsWithTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      EndsWithTestCase("", "", "UTF8_BINARY", true),
      EndsWithTestCase("abcde", "E", "UNICODE", false),
      EndsWithTestCase("abcde", "FGH", "UTF8_BINARY_LCASE", false),
      EndsWithTestCase("abcde", "CDE", "UNICODE_CI", true)
    )
    testCases.foreach(t => {
      val query = s"SELECT endswith(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      // Implicit casting
      checkAnswer(sql(s"SELECT endswith(collate('${t.l}', '${t.c}'),'${t.r}')"), Row(t.result))
      checkAnswer(sql(s"SELECT endswith('${t.l}', collate('${t.r}', '${t.c}'))"), Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT endswith(collate('abcde', 'UTF8_BINARY_LCASE'),collate('C', 'UNICODE_CI'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("Support StringRepeat string expression with collation") {
    // Supported collations
    case class StringRepeatTestCase[R](s: String, n: Int, c: String, result: R)
    val testCases = Seq(
      StringRepeatTestCase("", 1, "UTF8_BINARY", ""),
      StringRepeatTestCase("a", 0, "UNICODE", ""),
      StringRepeatTestCase("XY", 3, "UTF8_BINARY_LCASE", "XYXYXY"),
      StringRepeatTestCase("123", 2, "UNICODE_CI", "123123")
    )
    testCases.foreach(t => {
      val query = s"SELECT repeat(collate('${t.s}', '${t.c}'), ${t.n})"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
    })
  }

  // TODO: Add more tests for other string expressions

}

class CollationStringExpressionsANSISuite extends CollationStringExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)

  // TODO: If needed, add more tests for other string expressions (with ANSI mode enabled)

}
