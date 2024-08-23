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
import org.apache.spark.sql.catalyst.analysis.CollationTypeCasts
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

// scalastyle:off nonascii
class CollationStringExpressionsSuite
  extends QueryTest
  with SharedSparkSession
  with ExpressionEvalHelper {

  test("Support `ConcatWs` string expression with collation") {
    case class ConcatWsTestCase[R](
        sep: String,
        arrayStr: Array[String],
        collation: String,
        result: R)
    val testCases = Seq(
      ConcatWsTestCase(" ", Array("Spark", "SQL"), "UTF8_BINARY", "Spark SQL"),
      ConcatWsTestCase(" ", Array("Spark", "SQL"), "UTF8_LCASE", "Spark SQL"),
      ConcatWsTestCase(" ", Array("Spark", "SQL"), "UNICODE", "Spark SQL"),
      ConcatWsTestCase(" ", Array("Spark", "SQL"), "UNICODE_CI", "Spark SQL")
    )
    testCases.foreach(t => {
      // Unit test.
      val inputExprs = t.arrayStr.map {
        case null => Literal.create(null, StringType(t.collation))
        case s: String => Literal.create(s, StringType(t.collation))
      }
      val sepExpr = Literal.create(t.sep, StringType(t.collation))
      checkEvaluation(ConcatWs(sepExpr +: inputExprs.toIndexedSeq), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val array = t.arrayStr.map(s => s"'$s'").mkString(", ")
        val query = s"select concat_ws('${t.sep}', $array)"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `Elt` string expression with collation") {
    case class EltTestCase[R](index: Integer, inputs: Array[String], collation: String, result: R)
    val testCases = Seq(
      EltTestCase(1, Array("Spark", "SQL"), "UTF8_BINARY", "Spark"),
      EltTestCase(1, Array("Spark", "SQL"), "UTF8_LCASE", "Spark"),
      EltTestCase(2, Array("Spark", "SQL"), "UNICODE", "SQL"),
      EltTestCase(2, Array("Spark", "SQL"), "UNICODE_CI", "SQL")
    )
    testCases.foreach(t => {
      // Unit test.
      val inputExprs = t.inputs.map {
        case null => Literal.create(null, StringType(t.collation))
        case s: String => Literal.create(s, StringType(t.collation))
      }
      val intExpr = Literal.create(t.index, IntegerType)
      checkEvaluation(Elt(intExpr +: inputExprs.toIndexedSeq), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select elt(${t.index}, '${t.inputs(0)}', '${t.inputs(1)}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `SplitPart` string expression with collation") {
    case class SplitPartTestCase[R](
        str: String,
        delimiter: String,
        partNum: Integer,
        collation: String,
        result: R)
    val testCases = Seq(
      SplitPartTestCase("1a2", "a", 2, "UTF8_BINARY", "2"),
      SplitPartTestCase("1a2", "a", 2, "UNICODE", "2"),
      SplitPartTestCase("1a2", "A", 2, "UTF8_LCASE", "2"),
      SplitPartTestCase("1a2", "A", 2, "UNICODE_CI", "2")
    )
    testCases.foreach(t => {
      // Unit test.
      val str = Literal.create(t.str, StringType(t.collation))
      val delimiter = Literal.create(t.delimiter, StringType(t.collation))
      val partNum = Literal.create(t.partNum, IntegerType)
      checkEvaluation(SplitPart(str, delimiter, partNum), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select split_part('${t.str}', '${t.delimiter}', ${t.partNum})"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `StringSplitSQL` string expression with collation") {
    case class SplitPartTestCase[R](
        str: String,
        delimiter: String,
        collation: String,
        result: R)
    val testCases = Seq(
      SplitPartTestCase("1a2", "a", "UTF8_BINARY", Array("1", "2")),
      SplitPartTestCase("1a2", "a", "UNICODE", Array("1", "2")),
      SplitPartTestCase("1a2", "A", "UTF8_LCASE", Array("1", "2")),
      SplitPartTestCase("1a2", "A", "UNICODE_CI", Array("1", "2"))
    )
    testCases.foreach(t => {
      // Unit test.
      val str = Literal.create(t.str, StringType(t.collation))
      val delimiter = Literal.create(t.delimiter, StringType(t.collation))
      checkEvaluation(StringSplitSQL(str, delimiter), t.result)
    })

    // Because `StringSplitSQL` is an internal expression, E2E SQL test cannot be performed.
    checkError(
      exception = intercept[AnalysisException] {
        val expr = StringSplitSQL(
          Cast(Literal.create("1a2"), StringType("UTF8_BINARY")),
          Cast(Literal.create("a"), StringType("UTF8_LCASE")))
        CollationTypeCasts.transform.apply(expr)
      },
      errorClass = "COLLATION_MISMATCH.IMPLICIT",
      sqlState = "42P21",
      parameters = Map.empty
    )
    checkError(
      exception = intercept[AnalysisException] {
        val expr = StringSplitSQL(
          Collate(Literal.create("1a2"), "UTF8_BINARY"),
          Collate(Literal.create("a"), "UTF8_LCASE"))
        CollationTypeCasts.transform.apply(expr)
      },
      errorClass = "COLLATION_MISMATCH.EXPLICIT",
      sqlState = "42P21",
      parameters = Map("explicitTypes" -> "`string`, `string collate UTF8_LCASE`")
    )
  }

  test("Support `Contains` string expression with collation") {
    case class ContainsTestCase[R](left: String, right: String, collation: String, result: R)
    val testCases = Seq(
      ContainsTestCase("", "", "UTF8_BINARY", true),
      ContainsTestCase("abcde", "C", "UNICODE", false),
      ContainsTestCase("abcde", "FGH", "UTF8_LCASE", false),
      ContainsTestCase("abcde", "BCD", "UNICODE_CI", true)
    )
    testCases.foreach(t => {
      // Unit test.
      val left = Literal.create(t.left, StringType(t.collation))
      val right = Literal.create(t.right, StringType(t.collation))
      checkEvaluation(Contains(left, right), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select contains('${t.left}', '${t.right}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      }
    })
  }

  test("Support `SubstringIndex` expression with collation") {
    case class SubstringIndexTestCase[R](
        strExpr: String,
        delimExpr: String,
        countExpr: Integer,
        collation: String,
        result: R)
    val testCases = Seq(
      SubstringIndexTestCase("wwwgapachegorg", "g", -3, "UTF8_BINARY", "apachegorg"),
      SubstringIndexTestCase("www||apache||org", "||", 2, "UTF8_BINARY", "www||apache"),
      SubstringIndexTestCase("wwwXapacheXorg", "x", 2, "UTF8_LCASE", "wwwXapache"),
      SubstringIndexTestCase("aaaaaaaaaa", "aa", 2, "UNICODE", "a"),
      SubstringIndexTestCase("wwwmapacheMorg", "M", -2, "UNICODE_CI", "apacheMorg")
    )
    testCases.foreach(t => {
      // Unit test.
      val strExpr = Literal.create(t.strExpr, StringType(t.collation))
      val delimExpr = Literal.create(t.delimExpr, StringType(t.collation))
      val countExpr = Literal.create(t.countExpr, IntegerType)
      checkEvaluation(SubstringIndex(strExpr, delimExpr, countExpr), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select substring_index('${t.strExpr}', '${t.delimExpr}', ${t.countExpr})"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `StringInStr` string expression with collation") {
    case class StringInStrTestCase[R](str: String, substr: String, collation: String, result: R)
    val testCases = Seq(
      StringInStrTestCase("test大千世界X大千世界", "大千", "UTF8_BINARY", 5),
      StringInStrTestCase("test大千世界X大千世界", "界x", "UTF8_LCASE", 8),
      StringInStrTestCase("test大千世界X大千世界", "界x", "UNICODE", 0),
      StringInStrTestCase("test大千世界X大千世界", "界y", "UNICODE_CI", 0),
      StringInStrTestCase("test大千世界X大千世界", "界x", "UNICODE_CI", 8),
      StringInStrTestCase("abİo12", "i̇o", "UNICODE_CI", 3)
    )
    testCases.foreach(t => {
      // Unit test.
      val str = Literal.create(t.str, StringType(t.collation))
      val substr = Literal.create(t.substr, StringType(t.collation))
      checkEvaluation(StringInstr(str, substr), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select instr('${t.str}', '${t.substr}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      }
    })
  }

  test("Support `FindInSet` string expression with collation") {
    case class FindInSetTestCase[R](left: String, right: String, collation: String, result: R)
    val testCases = Seq(
      FindInSetTestCase("AB", "abc,b,ab,c,def", "UTF8_BINARY", 0),
      FindInSetTestCase("C", "abc,b,ab,c,def", "UTF8_LCASE", 4),
      FindInSetTestCase("d,ef", "abc,b,ab,c,def", "UNICODE", 0),
      FindInSetTestCase("i̇o", "ab,İo,12", "UNICODE_CI", 2),
      FindInSetTestCase("İo", "ab,i̇o,12", "UNICODE_CI", 2)
    )
    testCases.foreach(t => {
      // Unit test.
      val left = Literal.create(t.left, StringType(t.collation))
      val right = Literal.create(t.right, StringType(t.collation))
      checkEvaluation(FindInSet(left, right), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select find_in_set('${t.left}', '${t.right}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      }
    })
  }

  test("Support `StartsWith` string expression with collation") {
    case class StartsWithTestCase[R](left: String, right: String, collation: String, result: R)
    val testCases = Seq(
      StartsWithTestCase("", "", "UTF8_BINARY", true),
      StartsWithTestCase("abcde", "A", "UNICODE", false),
      StartsWithTestCase("abcde", "FGH", "UTF8_LCASE", false),
      StartsWithTestCase("abcde", "ABC", "UNICODE_CI", true)
    )
    testCases.foreach(t => {
      // Unit test.
      val left = Literal.create(t.left, StringType(t.collation))
      val right = Literal.create(t.right, StringType(t.collation))
      checkEvaluation(StartsWith(left, right), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select startswith('${t.left}', '${t.right}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      }
    })
  }

  test("Support `StringTranslate` string expression with collation") {
    case class StringTranslateTestCase[R](
        srcExpr: String,
        matchingExpr: String,
        replaceExpr: String,
        collation: String,
        result: R)
    val testCases = Seq(
      StringTranslateTestCase("Translate", "Rnlt", "12", "UTF8_BINARY", "Tra2sae"),
      StringTranslateTestCase("Translate", "Rnlt", "1234", "UTF8_LCASE", "41a2s3a4e"),
      StringTranslateTestCase("Translate", "Rn", "\u0000\u0000", "UNICODE", "Traslate"),
      StringTranslateTestCase("Translate", "Rn", "1234", "UNICODE_CI", "T1a2slate")
    )
    testCases.foreach(t => {
      // Unit test.
      val srcExpr = Literal.create(t.srcExpr, StringType(t.collation))
      val matchingExpr = Literal.create(t.matchingExpr, StringType(t.collation))
      val replaceExpr = Literal.create(t.replaceExpr, StringType(t.collation))
      checkEvaluation(StringTranslate(srcExpr, matchingExpr, replaceExpr), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select translate('${t.srcExpr}', '${t.matchingExpr}', '${t.replaceExpr}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `StringReplace` string expression with collation") {
    case class StringReplaceTestCase[R](
        srcExpr: String,
        searchExpr: String,
        replaceExpr: String,
        collation: String,
        result: R)
    val testCases = Seq(
      StringReplaceTestCase("r世eplace", "pl", "123", "UTF8_BINARY", "r世e123ace"),
      StringReplaceTestCase("repl世ace", "PL", "AB", "UTF8_LCASE", "reAB世ace"),
      StringReplaceTestCase("abcdabcd", "bc", "", "UNICODE", "adad"),
      StringReplaceTestCase("aBc世abc", "b", "12", "UNICODE_CI", "a12c世a12c"),
      StringReplaceTestCase("abi̇o12i̇o", "İo", "yy", "UNICODE_CI", "abyy12yy"),
      StringReplaceTestCase("abİo12i̇o", "i̇o", "xx", "UNICODE_CI", "abxx12xx")
    )
    testCases.foreach(t => {
      // Unit test.
      val srcExpr = Literal.create(t.srcExpr, StringType(t.collation))
      val searchExpr = Literal.create(t.searchExpr, StringType(t.collation))
      val replaceExpr = Literal.create(t.replaceExpr, StringType(t.collation))
      checkEvaluation(StringReplace(srcExpr, searchExpr, replaceExpr), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select replace('${t.srcExpr}', '${t.searchExpr}', '${t.replaceExpr}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `EndsWith` string expression with collation") {
    case class EndsWithTestCase[R](left: String, right: String, collation: String, result: R)
    val testCases = Seq(
      EndsWithTestCase("", "", "UTF8_BINARY", true),
      EndsWithTestCase("abcde", "E", "UNICODE", false),
      EndsWithTestCase("abcde", "FGH", "UTF8_LCASE", false),
      EndsWithTestCase("abcde", "CDE", "UNICODE_CI", true)
    )
    testCases.foreach(t => {
      // Unit test.
      val left = Literal.create(t.left, StringType(t.collation))
      val right = Literal.create(t.right, StringType(t.collation))
      checkEvaluation(EndsWith(left, right), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select endswith('${t.left}', '${t.right}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      }
    })
  }

  test("Support `StringRepeat` string expression with collation") {
    case class StringRepeatTestCase[R](str: String, times: Integer, collation: String, result: R)
    val testCases = Seq(
      StringRepeatTestCase("", 1, "UTF8_BINARY", ""),
      StringRepeatTestCase("a", 0, "UNICODE", ""),
      StringRepeatTestCase("XY", 3, "UTF8_LCASE", "XYXYXY"),
      StringRepeatTestCase("123", 2, "UNICODE_CI", "123123")
    )
    testCases.foreach(t => {
      // Unit test.
      val str = Literal.create(t.str, StringType(t.collation))
      val times = Literal.create(t.times, IntegerType)
      checkEvaluation(StringRepeat(str, times), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select repeat('${t.str}', ${t.times})"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `Ascii` string expression with collation") {
    case class AsciiTestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      AsciiTestCase("a", "UTF8_BINARY", 97),
      AsciiTestCase("B", "UTF8_LCASE", 66),
      AsciiTestCase("#", "UNICODE", 35),
      AsciiTestCase("!", "UNICODE_CI", 33)
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(Ascii(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select ascii('${t.input}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      }
    })
  }

  test("Support `Chr` string expression with collation") {
    case class ChrTestCase[R](input: Long, collation: String, result: R)
    val testCases = Seq(
      ChrTestCase(65, "UTF8_BINARY", "A"),
      ChrTestCase(66, "UTF8_LCASE", "B"),
      ChrTestCase(97, "UNICODE", "a"),
      ChrTestCase(98, "UNICODE_CI", "b")
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(Chr(Literal(t.input)), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select chr(${t.input})"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `UnBase64` string expression with collation") {
    case class UnBase64TestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      UnBase64TestCase("QUJD", "UTF8_BINARY", Array(65, 66, 67)),
      UnBase64TestCase("eHl6", "UTF8_LCASE", Array(120, 121, 122)),
      UnBase64TestCase("IyMj", "UNICODE", Array(35, 35, 35)),
      UnBase64TestCase("IQ==", "UNICODE_CI", Array(33))
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(Base64(UnBase64(Literal.create(t.input, StringType(t.collation)))), t.input)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select unbase64('${t.input}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(BinaryType))
      }
    })
  }

  test("Support `Base64` string expression with collation") {
    case class Base64TestCase[R](input: Array[Byte], collation: String, result: R)
    val testCases = Seq(
      Base64TestCase(Array(65, 66, 67), "UTF8_BINARY", "QUJD"),
      Base64TestCase(Array(120, 121, 122), "UTF8_LCASE", "eHl6"),
      Base64TestCase(Array(35, 35, 35), "UNICODE", "IyMj"),
      Base64TestCase(Array(33), "UNICODE_CI", "IQ==")
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(Base64(Literal.create(t.input, BinaryType)), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val str = new String(t.input.map(_.toChar))
        val query = s"select base64('$str')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `FormatNumber` string expression with collation") {
    case class FormatNumberTestCase[R](x: Double, d: String, collation: String, r: R)
    val testCases = Seq(
      FormatNumberTestCase(123.123, "###.###", "UTF8_BINARY", "123.123"),
      FormatNumberTestCase(99.99, "##.##", "UTF8_LCASE", "99.99"),
      FormatNumberTestCase(123.123, "###.###", "UNICODE", "123.123"),
      FormatNumberTestCase(99.99, "##.##", "UNICODE_CI", "99.99")
    )
    testCases.foreach(t => {
      // Unit test.
      val x = Literal(t.x)
      val d = Literal.create(t.d, StringType(t.collation))
      checkEvaluation(FormatNumber(x, d), t.r)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select format_number(${t.x}, '${t.d}')"
        checkAnswer(sql(query), Row(t.r))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `Decode` string expression with collation") {
    case class DecodeTestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      DecodeTestCase("a", "UTF8_BINARY", "a"),
      DecodeTestCase("A", "UTF8_LCASE", "A"),
      DecodeTestCase("b", "UNICODE", "b"),
      DecodeTestCase("B", "UNICODE_CI", "B")
    )
    testCases.foreach(t => {
      // Unit test.
      val input = Literal.create(t.input, StringType(t.collation))
      val default = Literal.create("default", StringType(t.collation))
      val params = Seq(Literal(1), Literal(1), input, default)
      checkEvaluation(Decode(params, Decode.createExpr(params)), t.input)
      val encoding = Literal.create("UTF-8", StringType(t.collation))
      val encodeExpr = Encode(Literal.create(t.input, StringType(t.collation)), encoding)
      checkEvaluation(StringDecode(encodeExpr, encoding), t.input)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val queryDecode = s"select decode(1, 1, '${t.input}', 'default')"
        checkAnswer(sql(queryDecode), Row(t.result))
        assert(sql(queryDecode).schema.fields.head.dataType.sameType(StringType(t.collation)))
        val queryStrDecode = s"select decode(encode('${t.input}', 'utf-8'), 'utf-8')"
        checkAnswer(sql(queryStrDecode), Row(t.result))
        assert(sql(queryStrDecode).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `Encode` string expression with collation") {
    case class EncodeTestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      EncodeTestCase("a", "UTF8_BINARY", Array(97)),
      EncodeTestCase("A", "UTF8_LCASE", Array(65)),
      EncodeTestCase("b", "UNICODE", Array(98)),
      EncodeTestCase("B", "UNICODE_CI", Array(66))
    )
    testCases.foreach(t => {
      // Unit test.
      val encoding = Literal.create("UTF-8", StringType(t.collation))
      val encodeExpr = Encode(Literal.create(t.input, StringType(t.collation)), encoding)
      checkEvaluation(StringDecode(encodeExpr, encoding), t.input)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select encode('${t.input}', 'utf-8')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(BinaryType))
      }
    })
  }

  test("Support `ToBinary` string expression with collation") {
    case class ToBinaryTestCase[R](expr: String, format: String, collation: String, result: R)
    val testCases = Seq(
      ToBinaryTestCase("a", "utf-8", "UTF8_BINARY", Array(97)),
      ToBinaryTestCase("A", "utf-8", "UTF8_LCASE", Array(65)),
      ToBinaryTestCase("b", "utf-8", "UNICODE", Array(98)),
      ToBinaryTestCase("B", "utf-8", "UNICODE_CI", Array(66))
    )
    testCases.foreach(t => {
      // Unit test.
      val expr = Literal.create(t.expr, StringType(t.collation))
      val format = Literal.create(t.format, StringType(t.collation))
      checkEvaluation(StringDecode(ToBinary(expr, Some(format)), format), t.expr)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select to_binary('${t.expr}', 'utf-8')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(BinaryType))
      }
    })
  }

  test("Support `Sentences` string expression with collation") {
    case class SentencesTestCase[R](str: String, collation: String, result: R)
    val testCases = Seq(
      SentencesTestCase(
        "Hello, world! Nice day.",
        "UTF8_BINARY",
        Seq(Seq("Hello", "world"), Seq("Nice", "day"))
      ),
      SentencesTestCase(
        "Something else. Nothing here.",
        "UTF8_LCASE",
        Seq(Seq("Something", "else"), Seq("Nothing", "here"))
      ),
      SentencesTestCase(
        "Hello, world! Nice day.",
        "UNICODE",
        Seq(Seq("Hello", "world"), Seq("Nice", "day"))
      ),
      SentencesTestCase(
        "Something else. Nothing here.",
        "UNICODE_CI",
        Seq(Seq("Something", "else"), Seq("Nothing", "here"))
      )
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(Sentences(Literal.create(t.str, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select sentences('${t.str}')"
        checkAnswer(sql(query), Row(t.result))
        val expectedDataType = ArrayType(ArrayType(StringType(t.collation)))
        assert(sql(query).schema.fields.head.dataType.sameType(expectedDataType))
      }
    })
  }

  test("Support `Upper` string expression with collation") {
    case class UpperTestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      UpperTestCase("aBc", "UTF8_BINARY", "ABC"),
      UpperTestCase("aBc", "UTF8_LCASE", "ABC"),
      UpperTestCase("aBc", "UNICODE", "ABC"),
      UpperTestCase("aBc", "UNICODE_CI", "ABC")
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(Upper(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select upper('${t.input}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `Lower` string expression with collation") {
    case class LowerTestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      LowerTestCase("aBc", "UTF8_BINARY", "abc"),
      LowerTestCase("aBc", "UTF8_LCASE", "abc"),
      LowerTestCase("aBc", "UNICODE", "abc"),
      LowerTestCase("aBc", "UNICODE_CI", "abc")
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(Lower(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select lower('${t.input}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `InitCap` string expression with collation") {
    case class InitCapTestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      InitCapTestCase("aBc ABc", "UTF8_BINARY", "Abc Abc"),
      InitCapTestCase("aBc ABc", "UTF8_LCASE", "Abc Abc"),
      InitCapTestCase("aBc ABc", "UNICODE", "Abc Abc"),
      InitCapTestCase("aBc ABc", "UNICODE_CI", "Abc Abc")
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(InitCap(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select initcap('${t.input}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `Overlay` string expression with collation") {
    case class OverlayTestCase[R](
        input: String,
        replace: String,
        pos: Integer,
        len: Integer,
        collation: String,
        result: R)
    val testCases = Seq(
      OverlayTestCase("hello", " world", 6, -1, "UTF8_BINARY", "hello world"),
      OverlayTestCase("nice", " day", 5, -1, "UTF8_LCASE", "nice day"),
      OverlayTestCase("A", "B", 1, -1, "UNICODE", "B"),
      OverlayTestCase("!", "!!!", 1, -1, "UNICODE_CI", "!!!")
    )
    testCases.foreach(t => {
      // Unit test.
      val input = Literal.create(t.input, StringType(t.collation))
      val replace = Literal.create(t.replace, StringType(t.collation))
      val pos = Literal.create(t.pos, IntegerType)
      val len = Literal.create(t.len, IntegerType)
      checkEvaluation(Overlay(input, replace, pos, len), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select overlay('${t.input}' placing '${t.replace}' from ${t.pos})"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `FormatString` string expression with collation") {
    case class FormatStringTestCase[R](
        format: String,
        input: Seq[Any],
        collation: String,
        result: R)
    val testCases = Seq(
      FormatStringTestCase("%s%s", Seq("a", "b"), "UTF8_BINARY", "ab"),
      FormatStringTestCase("%d", Seq(123), "UTF8_LCASE", "123"),
      FormatStringTestCase("%s%d", Seq("A", 0), "UNICODE", "A0"),
      FormatStringTestCase("%s%s", Seq("Hello", "!!!"), "UNICODE_CI", "Hello!!!")
    )
    testCases.foreach(t => {
      // Unit test.
      val format = Literal.create(t.format, StringType(t.collation))
      val arguments = t.input.map {
        case s: String => Literal.create(s, StringType(t.collation))
        case i: Integer => Literal.create(i, IntegerType)
      }
      checkEvaluation(FormatString(format +: arguments: _*), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val args = t.input
          .map {
            case s: String => s"'$s'"
            case other => other.toString
          }
          .mkString(", ")
        val query = s"select format_string('${t.format}', $args)"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `SoundEx` string expression with collation") {
    case class SoundExTestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      SoundExTestCase("A", "UTF8_BINARY", "A000"),
      SoundExTestCase("!", "UTF8_LCASE", "!"),
      SoundExTestCase("$", "UNICODE", "$"),
      SoundExTestCase("X", "UNICODE_CI", "X000")
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(SoundEx(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select soundex('${t.input}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `Length` string expression with collation") {
    case class LengthTestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      LengthTestCase("", "UTF8_BINARY", 0),
      LengthTestCase("abc", "UTF8_LCASE", 3),
      LengthTestCase("hello", "UNICODE", 5),
      LengthTestCase("ﬀ", "UNICODE_CI", 1)
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(Length(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select length('${t.input}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      }
    })
  }

  test("Support `BitLength` string expression with collation") {
    case class BitLengthTestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      BitLengthTestCase("", "UTF8_BINARY", 0),
      BitLengthTestCase("abc", "UTF8_LCASE", 24),
      BitLengthTestCase("hello", "UNICODE", 40),
      BitLengthTestCase("ﬀ", "UNICODE_CI", 24)
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(BitLength(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select bit_length('${t.input}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      }
    })
  }

  test("Support `OctetLength` string expression with collation") {
    case class OctetLengthTestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      OctetLengthTestCase("", "UTF8_BINARY", 0),
      OctetLengthTestCase("abc", "UTF8_LCASE", 3),
      OctetLengthTestCase("hello", "UNICODE", 5),
      OctetLengthTestCase("ﬀ", "UNICODE_CI", 3)
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(OctetLength(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select octet_length('${t.input}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      }
    })
  }

  test("Support `Luhncheck` string expression with collation") {
    case class LuhncheckTestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      LuhncheckTestCase("123", "UTF8_BINARY", false),
      LuhncheckTestCase("000", "UTF8_LCASE", true),
      LuhncheckTestCase("111", "UNICODE", false),
      LuhncheckTestCase("222", "UNICODE_CI", false)
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(Luhncheck(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select luhn_check(${t.input})"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      }
    })
  }

  test("Support `Levenshtein` string expression with collation") {
    case class LevenshteinTestCase[R](
        left: String,
        right: String,
        collation: String,
        threshold: Option[Integer],
        result: R)
    val testCases = Seq(
      LevenshteinTestCase("kitten", "sitTing", "UTF8_BINARY", None, 4),
      LevenshteinTestCase("kitten", "sitTing", "UTF8_LCASE", None, 4),
      LevenshteinTestCase("kitten", "sitTing", "UNICODE", Some(3), -1),
      LevenshteinTestCase("kitten", "sitTing", "UNICODE_CI", Some(3), -1)
    )
    testCases.foreach(t => {
      // Unit test.
      val left = Literal.create(t.left, StringType(t.collation))
      val right = Literal.create(t.right, StringType(t.collation))
      val threshold = t.threshold.map(Literal(_))
      checkEvaluation(Levenshtein(left, right, threshold), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val th = if (t.threshold.isDefined) s", ${t.threshold.get}" else ""
        val query = s"select levenshtein('${t.left}', '${t.right}'$th)"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      }
    })
  }

  test("Support `IsValidUTF8` string expression with collation") {
    case class IsValidUTF8TestCase[R](input: Any, collation: String, result: R)
    val testCases = Seq(
      IsValidUTF8TestCase(null, "UTF8_BINARY", null),
      IsValidUTF8TestCase("", "UTF8_LCASE", true),
      IsValidUTF8TestCase("abc", "UNICODE", true),
      IsValidUTF8TestCase("hello", "UNICODE_CI", true)
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(IsValidUTF8(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val str = if (t.input == null) "null" else s"'${t.input}'"
        val query = s"select is_valid_utf8($str)"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      }
    })
  }

  test("Support `MakeValidUTF8` string expression with collation") {
    case class MakeValidUTF8TestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      MakeValidUTF8TestCase(null, "UTF8_BINARY", null),
      MakeValidUTF8TestCase("", "UTF8_LCASE", ""),
      MakeValidUTF8TestCase("abc", "UNICODE", "abc"),
      MakeValidUTF8TestCase("hello", "UNICODE_CI", "hello")
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(MakeValidUTF8(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val str = if (t.input == null) "null" else s"'${t.input}'"
        val query = s"select make_valid_utf8($str)"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `ValidateUTF8` string expression with collation") {
    case class ValidateUTF8TestCase[R](input: String, collation: String, result: R)
    val testCases = Seq(
      ValidateUTF8TestCase(null, "UTF8_BINARY", null),
      ValidateUTF8TestCase("", "UTF8_LCASE", ""),
      ValidateUTF8TestCase("abc", "UNICODE", "abc"),
      ValidateUTF8TestCase("hello", "UNICODE_CI", "hello")
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(ValidateUTF8(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val str = if (t.input == null) "null" else s"'${t.input}'"
        val query = s"select validate_utf8($str)"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `TryValidateUTF8` string expression with collation") {
    case class ValidateUTF8TestCase(input: String, collation: String, result: Any)
    val testCases = Seq(
      ValidateUTF8TestCase(null, "UTF8_BINARY", null),
      ValidateUTF8TestCase("", "UTF8_LCASE", ""),
      ValidateUTF8TestCase("abc", "UNICODE", "abc"),
      ValidateUTF8TestCase("hello", "UNICODE_CI", "hello")
    )
    testCases.foreach(t => {
      // Unit test.
      checkEvaluation(TryValidateUTF8(Literal.create(t.input, StringType(t.collation))), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val str = if (t.input == null) "null" else s"'${t.input}'"
        val query = s"select try_validate_utf8($str)"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `Substring` string expression with collation") {
    case class SubstringTestCase[R](
        str: String,
        pos: Integer,
        len: Option[Integer],
        collation: String,
        result: R)
    val testCases = Seq(
      SubstringTestCase("example", 1, Some(100), "UTF8_LCASE", "example"),
      SubstringTestCase("example", 2, Some(2), "UTF8_BINARY", "xa"),
      SubstringTestCase("example", 0, Some(0), "UNICODE", ""),
      SubstringTestCase("example", -3, Some(2), "UNICODE_CI", "pl"),
      SubstringTestCase(" a世a ", 2, Some(3), "UTF8_LCASE", "a世a"),
      SubstringTestCase("", 1, Some(1), "UTF8_LCASE", ""),
      SubstringTestCase("", 1, Some(1), "UNICODE", ""),
      SubstringTestCase(null, 1, None, "UTF8_BINARY", null),
      SubstringTestCase(null, 1, Some(1), "UNICODE_CI", null),
      SubstringTestCase(null, null, Some(null), "UTF8_BINARY", null),
      SubstringTestCase(null, null, None, "UNICODE_CI", null),
      SubstringTestCase("ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ", null, None, "UTF8_BINARY", null),
      SubstringTestCase("", null, None, "UNICODE_CI", null)
    )
    testCases.foreach(t => {
      // Unit test.
      val str = Literal.create(t.str, StringType(t.collation))
      val pos = Literal.create(t.pos, IntegerType)
      val len = Literal.create(t.len.getOrElse(Integer.MAX_VALUE), IntegerType)
      checkEvaluation(Substring(str, pos, len), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val input = if (t.str == null) "null" else s"'${t.str}'"
        val length = if (t.len.isDefined) s", ${t.len.get}" else ""
        val query = s"select substring($input, ${t.pos}$length)"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `Left` string expression with collation") {
    case class LeftTestCase[R](str: String, len: Integer, collation: String, result: R)
    val testCases = Seq(
      LeftTestCase(null, null, "UTF8_BINARY", null),
      LeftTestCase(" a世a ", 3, "UTF8_LCASE", " a世"),
      LeftTestCase("", 1, "UNICODE", ""),
      LeftTestCase("ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ", 3, "UNICODE", "ÀÃÂ")
    )
    testCases.foreach(t => {
      // Unit test.
      val str = Literal.create(t.str, StringType)
      val len = Literal.create(t.len, IntegerType)
      checkEvaluation(Left(str, len), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val input = if (t.str == null) "null" else s"'${t.str}'"
        val query = s"select left($input, ${t.len})"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `Right` string expression with collation") {
    case class RightTestCase[R](str: String, len: Integer, collation: String, result: R)
    val testCases = Seq(
      RightTestCase(null, null, "UTF8_BINARY", null),
      RightTestCase(" a世a ", 3, "UTF8_LCASE", "世a "),
      RightTestCase("", 1, "UNICODE", ""),
      RightTestCase("ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ", 3, "UNICODE", "ǢǼÆ")
    )
    testCases.foreach(t => {
      // Unit test.
      val str = Literal.create(t.str, StringType)
      val len = Literal.create(t.len, IntegerType)
      checkEvaluation(Right(str, len), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val input = if (t.str == null) "null" else s"'${t.str}'"
        val query = s"select right($input, ${t.len})"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `StringRPad` string expression with collation") {
    case class StringRPadTestCase[R](
        str: String,
        len: Integer,
        pad: String,
        collation: String,
        result: R)
    val testCases = Seq(
      StringRPadTestCase("", 5, " ", "UTF8_BINARY", "     "),
      StringRPadTestCase("abc", 5, " ", "UNICODE", "abc  "),
      StringRPadTestCase("Hello", 7, "Wörld", "UTF8_LCASE", "HelloWö"),
      StringRPadTestCase("1234567890", 5, "aaaAAa", "UNICODE_CI", "12345"),
      StringRPadTestCase("aaAA", 2, " ", "UTF8_BINARY", "aa"),
      StringRPadTestCase("ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ℀℃", 2, "1", "UTF8_LCASE", "ÀÃ"),
      StringRPadTestCase("ĂȦÄäåäá", 20, "ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ", "UNICODE", "ĂȦÄäåäáÀÃÂĀĂȦÄäåäáâã"),
      StringRPadTestCase("aȦÄä", 8, "a1", "UNICODE_CI", "aȦÄäa1a1")
    )
    testCases.foreach(t => {
      // Unit test.
      val str = Literal.create(t.str, StringType(t.collation))
      val len = Literal.create(t.len, IntegerType)
      val pad = Literal.create(t.pad, StringType(t.collation))
      checkEvaluation(StringRPad(str, len, pad), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select rpad('${t.str}', ${t.len}, '${t.pad}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `StringLPad` string expression with collation") {
    case class StringLPadTestCase[R](
        str: String,
        len: Integer,
        pad: String,
        collation: String,
        result: R)
    val testCases = Seq(
      StringLPadTestCase("", 5, " ", "UTF8_BINARY", "     "),
      StringLPadTestCase("abc", 5, " ", "UNICODE", "  abc"),
      StringLPadTestCase("Hello", 7, "Wörld", "UTF8_LCASE", "WöHello"),
      StringLPadTestCase("1234567890", 5, "aaaAAa", "UNICODE_CI", "12345"),
      StringLPadTestCase("aaAA", 2, " ", "UTF8_BINARY", "aa"),
      StringLPadTestCase("ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ℀℃", 2, "1", "UTF8_LCASE", "ÀÃ"),
      StringLPadTestCase("ĂȦÄäåäá", 20, "ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ", "UNICODE", "ÀÃÂĀĂȦÄäåäáâãĂȦÄäåäá"),
      StringLPadTestCase("aȦÄä", 8, "a1", "UNICODE_CI", "a1a1aȦÄä")
    )
    testCases.foreach(t => {
      // Unit test.
      val str = Literal.create(t.str, StringType(t.collation))
      val len = Literal.create(t.len, IntegerType)
      val pad = Literal.create(t.pad, StringType(t.collation))
      checkEvaluation(StringLPad(str, len, pad), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select lpad('${t.str}', ${t.len}, '${t.pad}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `StringLocate` string expression with collation") {
    case class StringLocateTestCase[R](
        substr: String,
        str: String,
        start: Integer,
        collation: String,
        result: R)
    val testCases = Seq(
      StringLocateTestCase("aa", "aaads", 0, "UTF8_BINARY", 0),
      StringLocateTestCase("aa", "Aaads", 0, "UTF8_LCASE", 0),
      StringLocateTestCase("界x", "test大千世界X大千世界", 1, "UTF8_LCASE", 8),
      StringLocateTestCase("aBc", "abcabc", 4, "UTF8_LCASE", 4),
      StringLocateTestCase("aa", "Aaads", 0, "UNICODE", 0),
      StringLocateTestCase("abC", "abCabC", 2, "UNICODE", 4),
      StringLocateTestCase("aa", "Aaads", 0, "UNICODE_CI", 0),
      StringLocateTestCase("界x", "test大千世界X大千世界", 1, "UNICODE_CI", 8)
    )
    testCases.foreach(t => {
      // Unit test.
      val substr = Literal.create(t.substr, StringType(t.collation))
      val str = Literal.create(t.str, StringType(t.collation))
      val start = Literal.create(t.start, IntegerType)
      checkEvaluation(StringLocate(substr, str, start), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"select locate('${t.substr}', '${t.str}', ${t.start})"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      }
    })
  }

  test("Support `StringTrimLeft` string expression with collation") {
    case class StringTrimLeftTestCase[R](
        srcStr: String,
        trimStr: Option[String],
        collation: String,
        result: R)
    val testCases = Seq(
      StringTrimLeftTestCase("xxasdxx", Some("x"), "UTF8_BINARY", "asdxx"),
      StringTrimLeftTestCase("xxasdxx", Some("X"), "UTF8_LCASE", "asdxx"),
      StringTrimLeftTestCase("xxasdxx", Some("y"), "UNICODE", "xxasdxx"),
      StringTrimLeftTestCase("  asd  ", None, "UNICODE_CI", "asd  ")
    )
    testCases.foreach(t => {
      // Unit test.
      val srcStr = Literal.create(t.srcStr, StringType(t.collation))
      val trimStr = t.trimStr.map(Literal.create(_, StringType(t.collation)))
      checkEvaluation(StringTrimLeft(srcStr, trimStr), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val trimString = if (t.trimStr.isDefined) s"'${t.trimStr.get}', " else ""
        val query = s"select ltrim($trimString'${t.srcStr}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `StringTrimRight` string expression with collation") {
    case class StringTrimRightTestCase[R](
        srcStr: String,
        trimStr: Option[String],
        collation: String,
        result: R)
    val testCases = Seq(
      StringTrimRightTestCase("xxasdxx", Some("x"), "UTF8_BINARY", "xxasd"),
      StringTrimRightTestCase("xxasdxx", Some("X"), "UTF8_LCASE", "xxasd"),
      StringTrimRightTestCase("xxasdxx", Some("y"), "UNICODE", "xxasdxx"),
      StringTrimRightTestCase("  asd  ", None, "UNICODE_CI", "  asd")
    )
    testCases.foreach(t => {
      // Unit test.
      val srcStr = Literal.create(t.srcStr, StringType(t.collation))
      val trimStr = t.trimStr.map(Literal.create(_, StringType(t.collation)))
      checkEvaluation(StringTrimRight(srcStr, trimStr), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val trimString = if (t.trimStr.isDefined) s"'${t.trimStr.get}', " else ""
        val query = s"select rtrim($trimString'${t.srcStr}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `StringTrim` string expression with collation") {
    case class StringTrimTestCase[R](
        srcStr: String,
        trimStr: Option[String],
        collation: String,
        result: R)
    val testCases = Seq(
      StringTrimTestCase("xxasdxx", Some("x"), "UTF8_BINARY", "asd"),
      StringTrimTestCase("xxasdxx", Some("X"), "UTF8_LCASE", "asd"),
      StringTrimTestCase("xxasdxx", Some("y"), "UNICODE", "xxasdxx"),
      StringTrimTestCase("  asd  ", None, "UNICODE_CI", "asd")
    )
    testCases.foreach(t => {
      // Unit test.
      val srcStr = Literal.create(t.srcStr, StringType(t.collation))
      val trimStr = t.trimStr.map(Literal.create(_, StringType(t.collation)))
      checkEvaluation(StringTrim(srcStr, trimStr), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val trimString = if (t.trimStr.isDefined) s"'${t.trimStr.get}', " else ""
        val query = s"select trim($trimString'${t.srcStr}')"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

  test("Support `StringTrimBoth` string expression with collation") {
    case class StringTrimBothTestCase[R](
        srcStr: String,
        trimStr: Option[String],
        collation: String,
        result: R)
    val testCases = Seq(
      StringTrimBothTestCase("xxasdxx", Some("x"), "UTF8_BINARY", "asd"),
      StringTrimBothTestCase("xxasdxx", Some("X"), "UTF8_LCASE", "asd"),
      StringTrimBothTestCase("xxasdxx", Some("y"), "UNICODE", "xxasdxx"),
      StringTrimBothTestCase("  asd  ", None, "UNICODE_CI", "asd")
    )
    testCases.foreach(t => {
      // Unit test.
      val srcStr = Literal.create(t.srcStr, StringType(t.collation))
      val trimStr = t.trimStr.map(Literal.create(_, StringType(t.collation)))
      val replacement = StringTrim(srcStr, trimStr)
      checkEvaluation(StringTrimBoth(srcStr, trimStr, replacement), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val trimString = if (t.trimStr.isDefined) s", '${t.trimStr.get}'" else ""
        val query = s"select btrim('${t.srcStr}'$trimString)"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
      }
    })
  }

}
// scalastyle:on nonascii

class CollationStringExpressionsANSISuite extends CollationStringExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)

  // TODO: If needed, add more tests for other string expressions (with ANSI mode enabled)

}
