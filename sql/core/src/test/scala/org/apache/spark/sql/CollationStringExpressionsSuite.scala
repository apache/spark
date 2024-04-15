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
import org.apache.spark.sql.catalyst.expressions.ExpressionEvalHelper
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
  test("TRANSLATE check result on explicitly collated string") {
    // Supported collations
    case class TranslateTestCase[R](input: String, matchExpression: String,
        replaceExpression: String, collation: String, result: R)
    val testCases = Seq(
      TranslateTestCase("Translate", "Rnlt", "1234", "UTF8_BINARY_LCASE", "41a2s3a4e"),
      TranslateTestCase("Translate", "Rnlt", "1234", "UTF8_BINARY_LCASE", "41a2s3a4e"),
      TranslateTestCase("TRanslate", "rnlt", "XxXx", "UTF8_BINARY_LCASE", "xXaxsXaxe"),
      TranslateTestCase("TRanslater", "Rrnlt", "xXxXx", "UTF8_BINARY_LCASE", "xxaxsXaxex"),
      TranslateTestCase("TRanslater", "Rrnlt", "XxxXx", "UTF8_BINARY_LCASE", "xXaxsXaxeX"),
      // scalastyle:off
      TranslateTestCase("test大千世界X大千世界", "界x", "AB", "UTF8_BINARY_LCASE", "test大千世AB大千世A"),
      TranslateTestCase("大千世界test大千世界", "TEST", "abcd", "UTF8_BINARY_LCASE", "大千世界abca大千世界"),
      TranslateTestCase("Test大千世界大千世界", "tT", "oO", "UTF8_BINARY_LCASE", "oeso大千世界大千世界"),
      TranslateTestCase("大千世界大千世界tesT", "Tt", "Oo", "UTF8_BINARY_LCASE", "大千世界大千世界OesO"),
      TranslateTestCase("大千世界大千世界tesT", "大千", "世世", "UTF8_BINARY_LCASE", "世世世界世世世界tesT"),
      // scalastyle:on
      TranslateTestCase("Translate", "Rnlt", "1234", "UNICODE", "Tra2s3a4e"),
      TranslateTestCase("TRanslate", "rnlt", "XxXx", "UNICODE", "TRaxsXaxe"),
      TranslateTestCase("TRanslater", "Rrnlt", "xXxXx", "UNICODE", "TxaxsXaxeX"),
      TranslateTestCase("TRanslater", "Rrnlt", "XxxXx", "UNICODE", "TXaxsXaxex"),
      // scalastyle:off
      TranslateTestCase("test大千世界X大千世界", "界x", "AB", "UNICODE", "test大千世AX大千世A"),
      TranslateTestCase("Test大千世界大千世界", "tT", "oO", "UNICODE", "Oeso大千世界大千世界"),
      TranslateTestCase("大千世界大千世界tesT", "Tt", "Oo", "UNICODE", "大千世界大千世界oesO"),
      // scalastyle:on
      TranslateTestCase("Translate", "Rnlt", "1234", "UNICODE_CI", "41a2s3a4e"),
      TranslateTestCase("TRanslate", "rnlt", "XxXx", "UNICODE_CI", "xXaxsXaxe"),
      TranslateTestCase("TRanslater", "Rrnlt", "xXxXx", "UNICODE_CI", "xxaxsXaxex"),
      TranslateTestCase("TRanslater", "Rrnlt", "XxxXx", "UNICODE_CI", "xXaxsXaxeX"),
      // scalastyle:off
      TranslateTestCase("test大千世界X大千世界", "界x", "AB", "UNICODE_CI", "test大千世AB大千世A"),
      TranslateTestCase("大千世界test大千世界", "TEST", "abcd", "UNICODE_CI", "大千世界abca大千世界"),
      TranslateTestCase("Test大千世界大千世界", "tT", "oO", "UNICODE_CI", "oeso大千世界大千世界"),
      TranslateTestCase("大千世界大千世界tesT", "Tt", "Oo", "UNICODE_CI", "大千世界大千世界OesO"),
      TranslateTestCase("大千世界大千世界tesT", "大千", "世世", "UNICODE_CI", "世世世界世世世界tesT")
      // scalastyle:on
    )

    testCases.foreach(t => {
      val query = s"SELECT translate(collate('${t.input}', '${t.collation}')," +
        s"collate('${t.matchExpression}', '${t.collation}')," +
        s"collate('${t.replaceExpression}', '${t.collation}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(
        StringType(CollationFactory.collationNameToId(t.collation))))
      // Implicit casting
      checkAnswer(sql(s"SELECT translate(collate('${t.input}', '${t.collation}')," +
        s"'${t.matchExpression}', '${t.replaceExpression}')"), Row(t.result))
      checkAnswer(sql(s"SELECT translate('${t.input}', collate('${t.matchExpression}'," +
        s"'${t.collation}'), '${t.replaceExpression}')"), Row(t.result))
      checkAnswer(sql(s"SELECT translate('${t.input}', '${t.matchExpression}'," +
        s"collate('${t.replaceExpression}', '${t.collation}'))"), Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql(s"SELECT translate(collate('Translate', 'UTF8_BINARY_LCASE')," +
        s"collate('Rnlt', 'UNICODE'), '1234')")
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
