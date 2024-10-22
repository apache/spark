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
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.internal.SqlApiConf
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
      LikeTestCase("AḂC", "%ḃ%", "UTF8_LCASE", true),
      LikeTestCase("ABC", "%b%", "UTF8_BINARY", false)
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"collate(ABC, UNICODE_CI) LIKE %b%\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(ABC, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"like(collate('${t.l}', '${t.c}'), '${t.r}')",
          start = 7,
          stop = 47)
      )
    })
  }

  test("Like simplification should work with collated strings") {
    case class SimplifyLikeTestCase[R](collation: String, str: String, cls: Class[_], result: R)
    val testCases = Seq(
      SimplifyLikeTestCase("UTF8_BINARY", "ab%", classOf[StartsWith], false),
      SimplifyLikeTestCase("UTF8_BINARY", "%bc", classOf[EndsWith], false),
      SimplifyLikeTestCase("UTF8_BINARY", "a%c", classOf[And], false),
      SimplifyLikeTestCase("UTF8_BINARY", "%b%", classOf[Contains], false),
      SimplifyLikeTestCase("UTF8_BINARY", "abc", classOf[EqualTo], false),
      SimplifyLikeTestCase("UTF8_LCASE", "ab%", classOf[StartsWith], true),
      SimplifyLikeTestCase("UTF8_LCASE", "%bc", classOf[EndsWith], true),
      SimplifyLikeTestCase("UTF8_LCASE", "a%c", classOf[And], true),
      SimplifyLikeTestCase("UTF8_LCASE", "%b%", classOf[Contains], true),
      SimplifyLikeTestCase("UTF8_LCASE", "abc", classOf[EqualTo], true)
    )
    val tableName = "T"
    withTable(tableName) {
      sql(s"CREATE TABLE IF NOT EXISTS $tableName(c STRING) using PARQUET")
      sql(s"INSERT INTO $tableName(c) VALUES('ABC')")
      testCases.foreach { t =>
        val query = sql(s"select c collate ${t.collation} like '${t.str}' FROM t")
        checkAnswer(query, Row(t.result))
        val optimizedPlan = query.queryExecution.optimizedPlan.asInstanceOf[Project]
        assert(optimizedPlan.projectList.head.asInstanceOf[Alias].child.getClass == t.cls)
      }
    }
  }

  test("Like simplification should work with collated strings (for default collation)") {
    val tableNameBinary = "T_BINARY"
    withTable(tableNameBinary) {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UTF8_BINARY") {
        sql(s"CREATE TABLE IF NOT EXISTS $tableNameBinary(c STRING) using PARQUET")
        sql(s"INSERT INTO $tableNameBinary(c) VALUES('ABC')")
        checkAnswer(sql(s"select c like 'ab%' FROM $tableNameBinary"), Row(false))
        checkAnswer(sql(s"select c like '%bc' FROM $tableNameBinary"), Row(false))
        checkAnswer(sql(s"select c like 'a%c' FROM $tableNameBinary"), Row(false))
        checkAnswer(sql(s"select c like '%b%' FROM $tableNameBinary"), Row(false))
        checkAnswer(sql(s"select c like 'abc' FROM $tableNameBinary"), Row(false))
      }
    }
    val tableNameLcase = "T_LCASE"
    withTable(tableNameLcase) {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UTF8_LCASE") {
        sql(s"""
             |CREATE TABLE IF NOT EXISTS $tableNameLcase(
             |  c STRING COLLATE UTF8_LCASE
             |) using PARQUET
             |""".stripMargin)
        sql(s"INSERT INTO $tableNameLcase(c) VALUES('ABC')")
        checkAnswer(sql(s"select c like 'ab%' FROM $tableNameLcase"), Row(true))
        checkAnswer(sql(s"select c like '%bc' FROM $tableNameLcase"), Row(true))
        checkAnswer(sql(s"select c like 'a%c' FROM $tableNameLcase"), Row(true))
        checkAnswer(sql(s"select c like '%b%' FROM $tableNameLcase"), Row(true))
        checkAnswer(sql(s"select c like 'abc' FROM $tableNameLcase"), Row(true))
      }
    }
  }

  test("Support ILike string expression with collation") {
    // Supported collations
    case class ILikeTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      ILikeTestCase("ABC", "%b%", "UTF8_BINARY", true),
      ILikeTestCase("AḂC", "%ḃ%", "UTF8_LCASE", true),
      ILikeTestCase("ABC", "%b%", "UTF8_BINARY", true)
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"ilike(collate(ABC, UNICODE_CI), %b%)\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(ABC, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"ilike(collate('${t.l}', '${t.c}'), '${t.r}')",
          start = 7,
          stop = 48)
      )
    })
  }

  test("Support LikeAll string expression with collation") {
    // Supported collations
    case class LikeAllTestCase[R](s: String, p: Seq[String], c: String, result: R)
    val testCases = Seq(
      LikeAllTestCase("foo", Seq("%foo%", "%oo"), "UTF8_BINARY", true),
      LikeAllTestCase("Foo", Seq("%foo%", "%oo"), "UTF8_LCASE", true),
      LikeAllTestCase("foo", Seq("%foo%", "%bar%"), "UTF8_BINARY", false)
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"likeall(collate(Foo, UNICODE_CI))\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(Foo, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"LIKE ALL ('${t.p.mkString("','")}')",
          start = 36,
          stop = 59)
      )
    })
  }

  test("Support NotLikeAll string expression with collation") {
    // Supported collations
    case class NotLikeAllTestCase[R](s: String, p: Seq[String], c: String, result: R)
    val testCases = Seq(
      NotLikeAllTestCase("foo", Seq("%foo%", "%oo"), "UTF8_BINARY", false),
      NotLikeAllTestCase("Foo", Seq("%foo%", "%oo"), "UTF8_LCASE", false),
      NotLikeAllTestCase("foo", Seq("%goo%", "%bar%"), "UTF8_BINARY", true)
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"notlikeall(collate(Foo, UNICODE_CI))\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(Foo, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"NOT LIKE ALL ('${t.p.mkString("','")}')",
          start = 36,
          stop = 63)
      )
    })
  }

  test("Support LikeAny string expression with collation") {
    // Supported collations
    case class LikeAnyTestCase[R](s: String, p: Seq[String], c: String, result: R)
    val testCases = Seq(
      LikeAnyTestCase("foo", Seq("%foo%", "%bar"), "UTF8_BINARY", true),
      LikeAnyTestCase("Foo", Seq("%foo%", "%bar"), "UTF8_LCASE", true),
      LikeAnyTestCase("foo", Seq("%goo%", "%hoo%"), "UTF8_BINARY", false)
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"likeany(collate(Foo, UNICODE_CI))\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(Foo, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"LIKE ANY ('${t.p.mkString("','")}')",
          start = 36,
          stop = 59)
      )
    })
  }

  test("Support NotLikeAny string expression with collation") {
    // Supported collations
    case class NotLikeAnyTestCase[R](s: String, p: Seq[String], c: String, result: R)
    val testCases = Seq(
      NotLikeAnyTestCase("foo", Seq("%foo%", "%hoo"), "UTF8_BINARY", true),
      NotLikeAnyTestCase("Foo", Seq("%foo%", "%hoo"), "UTF8_LCASE", true),
      NotLikeAnyTestCase("foo", Seq("%foo%", "%oo%"), "UTF8_BINARY", false)
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"notlikeany(collate(Foo, UNICODE_CI))\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(Foo, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"NOT LIKE ANY ('${t.p.mkString("','")}')",
          start = 36,
          stop = 63)
      )
    })
  }

  test("Support RLike string expression with collation") {
    // Supported collations
    case class RLikeTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RLikeTestCase("ABC", ".B.", "UTF8_BINARY", true),
      RLikeTestCase("AḂC", ".ḃ.", "UTF8_LCASE", true),
      RLikeTestCase("ABC", ".b.", "UTF8_BINARY", false)
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"RLIKE(collate(ABC, UNICODE_CI), .b.)\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(ABC, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"rlike(collate('${t.l}', '${t.c}'), '${t.r}')",
          start = 7,
          stop = 48)
      )
    })
  }

  test("Support StringSplit string expression with collation") {
    // Supported collations
    case class StringSplitTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      StringSplitTestCase("ABC", "[B]", "UTF8_BINARY", Seq("A", "C")),
      StringSplitTestCase("AḂC", "[ḃ]", "UTF8_LCASE", Seq("A", "C")),
      StringSplitTestCase("ABC", "[B]", "UTF8_BINARY", Seq("A", "C"))
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"split(collate(ABC, UNICODE_CI), [b], -1)\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(ABC, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"split(collate('${t.l}', '${t.c}'), '${t.r}')",
          start = 7,
          stop = 48)
      )
    })
  }

  test("Support RegExpReplace string expression with collation") {
    // Supported collations
    case class RegExpReplaceTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpReplaceTestCase("ABCDE", ".C.", "UTF8_BINARY", "AFFFE"),
      RegExpReplaceTestCase("ABĆDE", ".ć.", "UTF8_LCASE", "AFFFE"),
      RegExpReplaceTestCase("ABCDE", ".c.", "UTF8_BINARY", "ABCDE")
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
    val (c1, c2) = ("UTF8_BINARY", "UTF8_LCASE")
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"SELECT regexp_replace(collate('ABCDE','$c1'), '.c.', collate('FFF','$c2'))")
      },
      condition = "COLLATION_MISMATCH.EXPLICIT",
      parameters = Map(
        "explicitTypes" -> """"STRING", "STRING COLLATE UTF8_LCASE""""
      )
    )
    // Unsupported collations
    case class RegExpReplaceTestFail(l: String, r: String, c: String)
    val failCases = Seq(
      RegExpReplaceTestFail("ABCDE", ".c.", "UNICODE_CI")
    )
    failCases.foreach(t => {
      val query =
        s"SELECT regexp_replace(collate('${t.l}', '${t.c}'), '${t.r}', 'FFF')"
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"regexp_replace(collate(ABCDE, UNICODE_CI), .c., FFF, 1)\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(ABCDE, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"regexp_replace(collate('${t.l}', '${t.c}'), '${t.r}', 'FFF')",
          start = 7,
          stop = 66)
      )
    })
  }

  test("Support RegExpExtract string expression with collation") {
    // Supported collations
    case class RegExpExtractTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpExtractTestCase("ABCDE", ".C.", "UTF8_BINARY", "BCD"),
      RegExpExtractTestCase("ABĆDE", ".ć.", "UTF8_LCASE", "BĆD"),
      RegExpExtractTestCase("ABCDE", ".c.", "UTF8_BINARY", "")
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"regexp_extract(collate(ABCDE, UNICODE_CI), .c., 0)\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(ABCDE, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"regexp_extract(collate('${t.l}', '${t.c}'), '${t.r}', 0)",
          start = 7,
          stop = 62)
      )
    })
  }

  test("Support RegExpExtractAll string expression with collation") {
    // Supported collations
    case class RegExpExtractAllTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpExtractAllTestCase("ABCDE", ".C.", "UTF8_BINARY", Seq("BCD")),
      RegExpExtractAllTestCase("ABĆDE", ".ć.", "UTF8_LCASE", Seq("BĆD")),
      RegExpExtractAllTestCase("ABCDE", ".c.", "UTF8_BINARY", Seq())
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"regexp_extract_all(collate(ABCDE, UNICODE_CI), .c., 0)\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(ABCDE, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"regexp_extract_all(collate('${t.l}', '${t.c}'), '${t.r}', 0)",
          start = 7,
          stop = 66)
      )
    })
  }

  test("Support RegExpCount string expression with collation") {
    // Supported collations
    case class RegExpCountTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpCountTestCase("ABCDE", ".C.", "UTF8_BINARY", 1),
      RegExpCountTestCase("ABĆDE", ".ć.", "UTF8_LCASE", 1),
      RegExpCountTestCase("ABCDE", ".c.", "UTF8_BINARY", 0)
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"regexp_count(collate(ABCDE, UNICODE_CI), .c.)\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(ABCDE, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"regexp_count(collate('${t.l}', '${t.c}'), '${t.r}')",
          start = 7,
          stop = 57)
      )
    })
  }

  test("Support RegExpSubStr string expression with collation") {
    // Supported collations
    case class RegExpSubStrTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpSubStrTestCase("ABCDE", ".C.", "UTF8_BINARY", "BCD"),
      RegExpSubStrTestCase("ABĆDE", ".ć.", "UTF8_LCASE", "BĆD"),
      RegExpSubStrTestCase("ABCDE", ".c.", "UTF8_BINARY", null)
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"regexp_substr(collate(ABCDE, UNICODE_CI), .c.)\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(ABCDE, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"regexp_substr(collate('${t.l}', '${t.c}'), '${t.r}')",
          start = 7,
          stop = 58)
      )
    })
  }

  test("Support RegExpInStr string expression with collation") {
    // Supported collations
    case class RegExpInStrTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      RegExpInStrTestCase("ABCDE", ".C.", "UTF8_BINARY", 2),
      RegExpInStrTestCase("ABĆDE", ".ć.", "UTF8_LCASE", 2),
      RegExpInStrTestCase("ABCDE", ".c.", "UTF8_BINARY", 0)
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
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"regexp_instr(collate(ABCDE, UNICODE_CI), .c., 0)\"",
          "paramIndex" -> "first",
          "inputSql" -> "\"collate(ABCDE, UNICODE_CI)\"",
          "inputType" -> "\"STRING COLLATE UNICODE_CI\"",
          "requiredType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = s"regexp_instr(collate('${t.l}', '${t.c}'), '${t.r}')",
          start = 7,
          stop = 57)
      )
    })
  }
}
// scalastyle:on nonascii
