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
import org.apache.spark.sql.catalyst.expressions.ExpressionEvalHelper
import org.apache.spark.sql.internal.{SQLConf, SqlApiConf}
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

  test("substring check output type on explicitly collated string") {
    case class SubstringTestCase[R](args: Seq[String], collation: String, result: R)
    val checks = Seq(
      SubstringTestCase(Seq("Spark", "2"), "UTF8_BINARY", "park"),
      SubstringTestCase(Seq("Spark", "2"), "UTF8_BINARY_LCASE", "park")
    )
    checks.foreach(ct => {
      val query = s"SELECT substr(collate('${ct.args.head}', '${ct.collation}')," +
        s" ${ct.args.tail.head})"
      // Result & data type
      checkAnswer(sql(query), Row(ct.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(ct.collation)))
    })
  }

  test("left/right/substr on collated proper string returns proper value") { // scalastyle:ignore
    case class QTestCase(query: String, collation: String, result: Row)
    val checks = Seq("utf8_binary_lcase", "utf8_binary", "unicode", "unicode_ci").flatMap(
      c => Seq(
        QTestCase("select left('abc' collate " + c + ", 1)", c, Row("a")),
        QTestCase("select right('def' collate " + c + ", 1)", c, Row("f")),
        QTestCase("select substr('abc' collate " + c + ", 2)", c, Row("bc")),
        QTestCase("select substr('example' collate " + c + ", 0, 2)", c, Row("ex")),
        QTestCase("select substr('example' collate " + c + ", 1, 2)", c, Row("ex")),
        QTestCase("select substr('example' collate " + c + ", 0, 7)", c, Row("example")),
        QTestCase("select substr('example' collate " + c + ", 1, 7)", c, Row("example")),
        QTestCase("select substr('example' collate " + c + ", 0, 100)", c, Row("example")),
        QTestCase("select substr('example' collate " + c + ", 1, 100)", c, Row("example")),
        QTestCase("select substr('example' collate " + c + ", 2, 2)", c, Row("xa")),
        QTestCase("select substr('example' collate " + c + ", 1, 6)", c, Row("exampl")),
        QTestCase("select substr('example' collate " + c + ", 2, 100)", c, Row("xample")),
        QTestCase("select substr('example' collate " + c + ", 0, 0)", c, Row("")),
        QTestCase("select substr('example' collate " + c + ", 100, 4)", c, Row("")),
        QTestCase("select substr('example' collate " + c + ", 0, 100)", c, Row("example")),
        QTestCase("select substr('example' collate " + c + ", 1, 100)", c, Row("example")),
        QTestCase("select substr('example' collate " + c + ", 2, 100)", c, Row("xample")),
        QTestCase("select substr('example' collate " + c + ", -3, 2)", c, Row("pl")),
        QTestCase("select substr('example' collate " + c + ", -100, 4)", c, Row("")),
        QTestCase("select substr('example' collate " + c + ", -2147483648, 6)", c, Row("")),
        QTestCase("select substr(' a世a ' collate " + c + ", 2, 3)", c, Row("a世a")), // scalastyle:ignore
        QTestCase("select left(' a世a ' collate " + c + ", 3)", c, Row(" a世")), // scalastyle:ignore
        QTestCase("select right(' a世a ' collate " + c + ", 3)", c, Row("世a ")), // scalastyle:ignore
        QTestCase("select substr('AaAaAaAa000000' collate " + c + ", 2, 3)", c, Row("aAa")),
        QTestCase("select left('AaAaAaAa000000' collate " + c + ", 3)", c, Row("AaA")),
        QTestCase("select right('AaAaAaAa000000' collate " + c + ", 3)", c, Row("000")),
        QTestCase("select substr('' collate " + c + ", 1, 1)", c, Row("")),
        QTestCase("select left('' collate " + c + ", 1)", c, Row("")),
        QTestCase("select right('' collate " + c + ", 1)", c, Row("")),
        QTestCase("select left('ghi' collate " + c + ", 1)", c, Row("g"))
      )
    )

    checks.foreach { check =>
      // Result & data type
      checkAnswer(sql(check.query), Row(check.result))
      assert(sql(check.query).schema.fields.head.dataType.sameType(StringType(check.collation)))
    }
  }

  test("left/right/substr on collated improper string returns proper value") {
    case class QTestCase(query: String, collation: String, result: Row)
    val checks = Seq("utf8_binary_lcase", "utf8_binary", "unicode", "unicode_ci").flatMap(
      c => Seq(
        QTestCase("select left(null collate " + c + ", 1)", c, Row(null)),
        QTestCase("select right(null collate " + c + ", 1)", c, Row(null)),
        QTestCase("select substr(null collate " + c + ", 1)", c, Row(null)),
        QTestCase("select substr(null collate " + c + ", 1, 1)", c, Row(null)),
        QTestCase("select left('' collate " + c + ", null)", c, Row(null)),
        QTestCase("select right('' collate " + c + ", null)", c, Row(null)),
        QTestCase("select substr('' collate " + c + ", null)", c, Row(null)),
        QTestCase("select substr('' collate " + c + ", null, null)", c, Row(null))
      )
    )
    checks.foreach(check => {
      // Result & data type
      checkAnswer(sql(check.query), Row(check.result))
      assert(sql(check.query).schema.fields.head.dataType.sameType(StringType(check.collation)))
    })
  }

  test("left/right/substr on collated improper length and position returns proper value") {
    case class QTestCase(query: String, collation: String, result: Row)
    val checks = Seq("utf8_binary_lcase", "utf8_binary", "unicode", "unicode_ci").flatMap(
      c => Seq(
        QTestCase("select left(' a世a ' collate " + c + ", '3')", c, Row(" a世")), // scalastyle:ignore
        QTestCase("select right(' a世a ' collate " + c + ", '3')", c, Row("世a ")), // scalastyle:ignore
        QTestCase("select right('' collate " + c + ", null)", c, Row(null)),
        QTestCase("select substr('' collate " + c + ", null)", c, Row(null)),
        QTestCase("select substr('' collate " + c + ", null, null)", c, Row(null)),
        QTestCase("select left('' collate " + c + ", null)", c, Row(null))
      )
    )
    checks.foreach(check => {
      // Result & data type
      checkAnswer(sql(check.query), Row(check.result))
      assert(sql(check.query).schema.fields.head.dataType.sameType(StringType(check.collation)))
    })
  }

  test("left/right/substr on session-collated string returns proper type") {
    Seq("utf8_binary_lcase", "utf8_binary", "unicode", "unicode_ci").foreach { collationName =>
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        assert(sql("select left('abc', 1)")
          .schema.fields.head.dataType.sameType(StringType(collationName)))
        assert(sql("select right('def', 1)")
          .schema.fields.head.dataType.sameType(StringType(collationName)))
        assert(sql("select substr('ghi', 1)")
          .schema.fields.head.dataType.sameType(StringType(collationName)))
      }
    }
  }

  test("left/right/substr on collated improper string returns proper type") {
    Seq("utf8_binary_lcase", "utf8_binary", "unicode", "unicode_ci").foreach { collationName =>
      assert(sql(s"select left(null collate $collationName, 1)")
        .schema.fields.head.dataType.sameType(StringType(collationName)))
      assert(sql(s"select right(null collate $collationName, 1)")
        .schema.fields.head.dataType.sameType(StringType(collationName)))
      assert(sql(s"select substr(null collate $collationName, 1)")
        .schema.fields.head.dataType.sameType(StringType(collationName)))
    }
  }
  test("left/right/substr on collated proper string returns proper type") {
    Seq("utf8_binary_lcase", "utf8_binary", "unicode", "unicode_ci").foreach { collationName =>
      assert(sql(s"select left('hij' collate $collationName, 1)")
        .schema.fields.head.dataType.sameType(StringType(collationName)))
      assert(sql(s"select right('klm' collate $collationName, 1)")
        .schema.fields.head.dataType.sameType(StringType(collationName)))
      assert(sql(s"select substr('nop' collate $collationName, 1)")
        .schema.fields.head.dataType.sameType(StringType(collationName)))
    }
  }

  // TODO: Add more tests for other string expressions

}

class CollationStringExpressionsANSISuite extends CollationStringExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)

  // TODO: If needed, add more tests for other string expressions (with ANSI mode enabled)

}
