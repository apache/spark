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
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, Literal, SubstringIndex}
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

  test("SUBSTRING_INDEX check result on explicitly collated strings") {
    def testSubstringIndex(str: String, delim: String, cnt: Integer,
                           collationId: Integer, expected: String): Unit = {
      val string = Literal.create(str, StringType(collationId))
      val delimiter = Literal.create(delim, StringType(collationId))
      val count = Literal(cnt)

      checkEvaluation(SubstringIndex(string, delimiter, count), expected)
    }

    var collationId = CollationFactory.collationNameToId("UTF8_BINARY")
    testSubstringIndex("wwwgapachegorg", "g", -3, collationId, "apachegorg")
    testSubstringIndex("www||apache||org", "||", 2, collationId, "www||apache")

    collationId = CollationFactory.collationNameToId("UTF8_BINARY_LCASE")
    testSubstringIndex("AaAaAaAaAa", "aa", 2, collationId, "A")
    testSubstringIndex("www.apache.org", ".", 3, collationId, "www.apache.org")
    testSubstringIndex("wwwXapacheXorg", "x", 2, collationId, "wwwXapache")
    testSubstringIndex("wwwxapachexorg", "X", 1, collationId, "www")
    testSubstringIndex("www.apache.org", ".", 0, collationId, "")
    testSubstringIndex("www.apache.ORG", ".", -3, collationId, "www.apache.ORG")
    testSubstringIndex("wwwGapacheGorg", "g", 1, collationId, "www")
    testSubstringIndex("wwwGapacheGorg", "g", 3, collationId, "wwwGapacheGor")
    testSubstringIndex("gwwwGapacheGorg", "g", 3, collationId, "gwwwGapache")
    testSubstringIndex("wwwGapacheGorg", "g", -3, collationId, "apacheGorg")
    testSubstringIndex("wwwmapacheMorg", "M", -2, collationId, "apacheMorg")
    testSubstringIndex("www.apache.org", ".", -1, collationId, "org")
    testSubstringIndex("www.apache.org.", ".", -1, collationId, "")
    testSubstringIndex("", ".", -2, collationId, "")
    // scalastyle:off
    testSubstringIndex("test大千世界X大千世界", "x", -1, collationId, "大千世界")
    testSubstringIndex("test大千世界X大千世界", "X", 1, collationId, "test大千世界")
    testSubstringIndex("test大千世界大千世界", "千", 2, collationId, "test大千世界大")
    // scalastyle:on
    testSubstringIndex("www||APACHE||org", "||", 2, collationId, "www||APACHE")
    testSubstringIndex("www||APACHE||org", "||", -1, collationId, "org")

    collationId = CollationFactory.collationNameToId("UNICODE")
    testSubstringIndex("AaAaAaAaAa", "Aa", 2, collationId, "Aa")
    testSubstringIndex("wwwYapacheyorg", "y", 3, collationId, "wwwYapacheyorg")
    testSubstringIndex("www.apache.org", ".", 2, collationId, "www.apache")
    testSubstringIndex("wwwYapacheYorg", "Y", 1, collationId, "www")
    testSubstringIndex("wwwYapacheYorg", "y", 1, collationId, "wwwYapacheYorg")
    testSubstringIndex("wwwGapacheGorg", "g", 1, collationId, "wwwGapacheGor")
    testSubstringIndex("GwwwGapacheGorG", "G", 3, collationId, "GwwwGapache")
    testSubstringIndex("wwwGapacheGorG", "G", -3, collationId, "apacheGorG")
    testSubstringIndex("www.apache.org", ".", 0, collationId, "")
    testSubstringIndex("www.apache.org", ".", -3, collationId, "www.apache.org")
    testSubstringIndex("www.apache.org", ".", -2, collationId, "apache.org")
    testSubstringIndex("www.apache.org", ".", -1, collationId, "org")
    testSubstringIndex("", ".", -2, collationId, "")
    // scalastyle:off
    testSubstringIndex("test大千世界X大千世界", "X", -1, collationId, "大千世界")
    testSubstringIndex("test大千世界X大千世界", "X", 1, collationId, "test大千世界")
    testSubstringIndex("大x千世界大千世x界", "x", 1, collationId, "大")
    testSubstringIndex("大x千世界大千世x界", "x", -1, collationId, "界")
    testSubstringIndex("大x千世界大千世x界", "x", -2, collationId, "千世界大千世x界")
    testSubstringIndex("大千世界大千世界", "千", 2, collationId, "大千世界大")
    // scalastyle:on
    testSubstringIndex("www||apache||org", "||", 2, collationId, "www||apache")

    collationId = CollationFactory.collationNameToId("UNICODE_CI")
    testSubstringIndex("AaAaAaAaAa", "aa", 2, collationId, "A")
    testSubstringIndex("www.apache.org", ".", 3, collationId, "www.apache.org")
    testSubstringIndex("wwwXapacheXorg", "x", 2, collationId, "wwwXapache")
    testSubstringIndex("wwwxapacheXorg", "X", 1, collationId, "www")
    testSubstringIndex("www.apache.org", ".", 0, collationId, "")
    testSubstringIndex("wwwGapacheGorg", "G", 3, collationId, "wwwGapacheGor")
    testSubstringIndex("gwwwGapacheGorg", "g", 3, collationId, "gwwwGapache")
    testSubstringIndex("gwwwGapacheGorg", "g", -3, collationId, "apacheGorg")
    testSubstringIndex("www.apache.ORG", ".", -3, collationId, "www.apache.ORG")
    testSubstringIndex("wwwmapacheMorg", "M", -2, collationId, "apacheMorg")
    testSubstringIndex("www.apache.org", ".", -1, collationId, "org")
    testSubstringIndex("", ".", -2, collationId, "")
    // scalastyle:off
    testSubstringIndex("test大千世界X大千世界", "X", -1, collationId, "大千世界")
    testSubstringIndex("test大千世界X大千世界", "X", 1, collationId, "test大千世界")
    testSubstringIndex("test大千世界大千世界", "千", 2, collationId, "test大千世界大")
    // scalastyle:on
    testSubstringIndex("www||APACHE||org", "||", 2, collationId, "www||APACHE")
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
