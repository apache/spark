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
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, DataType, IntegerType, StringType}

// scalastyle:off nonascii
class CollationStringExpressionsSuite
  extends QueryTest
  with SharedSparkSession {

  test("Support ConcatWs string expression with collation") {
    // Supported collations
    case class ConcatWsTestCase[R](s: String, a: Array[String], c: String, result: R)
    val testCases = Seq(
      ConcatWsTestCase(" ", Array("Spark", "SQL"), "UTF8_BINARY", "Spark SQL"),
      ConcatWsTestCase(" ", Array("Spark", "SQL"), "UTF8_BINARY_LCASE", "Spark SQL"),
      ConcatWsTestCase(" ", Array("Spark", "SQL"), "UNICODE", "Spark SQL"),
      ConcatWsTestCase(" ", Array("Spark", "SQL"), "UNICODE_CI", "Spark SQL")
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
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT concat_ws(' ',collate('Spark', 'UTF8_BINARY_LCASE'),collate('SQL', 'UNICODE'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("Support Elt string expression with collation") {
    // Supported collations
    case class EltTestCase[R](index: Int, inputs: Array[String], c: String, result: R)
    val testCases = Seq(
      EltTestCase(1, Array("Spark", "SQL"), "UTF8_BINARY", "Spark"),
      EltTestCase(1, Array("Spark", "SQL"), "UTF8_BINARY_LCASE", "Spark"),
      EltTestCase(2, Array("Spark", "SQL"), "UNICODE", "SQL"),
      EltTestCase(2, Array("Spark", "SQL"), "UNICODE_CI", "SQL")
    )
    testCases.foreach(t => {
      var query = s"SELECT elt(${t.index}, collate('${t.inputs(0)}', '${t.c}')," +
        s" collate('${t.inputs(1)}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      // Implicit casting
      query = s"SELECT elt(${t.index}, collate('${t.inputs(0)}', '${t.c}'), '${t.inputs(1)}')"
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      query = s"SELECT elt(${t.index}, '${t.inputs(0)}', collate('${t.inputs(1)}', '${t.c}'))"
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT elt(0 ,collate('Spark', 'UTF8_BINARY_LCASE'), collate('SQL', 'UNICODE'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("Support SplitPart string expression with collation") {
    // Supported collations
    case class SplitPartTestCase[R](s: String, d: String, p: Int, c: String, result: R)
    val testCases = Seq(
      SplitPartTestCase("1a2", "a", 2, "UTF8_BINARY", "2"),
      SplitPartTestCase("1a2", "a", 2, "UNICODE", "2"),
      SplitPartTestCase("1a2", "A", 2, "UTF8_BINARY_LCASE", "2"),
      SplitPartTestCase("1a2", "A", 2, "UNICODE_CI", "2")
    )
    testCases.foreach(t => {
      val query = s"SELECT split_part(collate('${t.s}','${t.c}'),collate('${t.d}','${t.c}'),${t.p})"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
    })
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

  test("Support StringInStr string expression with collation") {
    case class StringInStrTestCase[R](string: String, substring: String, c: String, result: R)
    val testCases = Seq(
      // scalastyle:off
      StringInStrTestCase("test大千世界X大千世界", "大千", "UTF8_BINARY", 5),
      StringInStrTestCase("test大千世界X大千世界", "界x", "UTF8_BINARY_LCASE", 8),
      StringInStrTestCase("test大千世界X大千世界", "界x", "UNICODE", 0),
      StringInStrTestCase("test大千世界X大千世界", "界y", "UNICODE_CI", 0),
      StringInStrTestCase("test大千世界X大千世界", "界x", "UNICODE_CI", 8),
      StringInStrTestCase("abİo12", "i̇o", "UNICODE_CI", 3)
      // scalastyle:on
    )
    testCases.foreach(t => {
      val query = s"SELECT instr(collate('${t.string}','${t.c}')," +
        s"collate('${t.substring}','${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      // Implicit casting
      checkAnswer(sql(s"SELECT instr(collate('${t.string}','${t.c}')," +
        s"'${t.substring}')"), Row(t.result))
      checkAnswer(sql(s"SELECT instr('${t.string}'," +
        s"collate('${t.substring}','${t.c}'))"), Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql(s"SELECT instr(collate('aaads','UTF8_BINARY'), collate('Aa','UTF8_BINARY_LCASE'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("Support FindInSet string expression with collation") {
    case class FindInSetTestCase[R](word: String, set: String, c: String, result: R)
    val testCases = Seq(
      FindInSetTestCase("AB", "abc,b,ab,c,def", "UTF8_BINARY", 0),
      FindInSetTestCase("C", "abc,b,ab,c,def", "UTF8_BINARY_LCASE", 4),
      FindInSetTestCase("d,ef", "abc,b,ab,c,def", "UNICODE", 0),
      // scalastyle:off
      FindInSetTestCase("i̇o", "ab,İo,12", "UNICODE_CI", 2),
      FindInSetTestCase("İo", "ab,i̇o,12", "UNICODE_CI", 2)
      // scalastyle:on
    )
    testCases.foreach(t => {
      val query = s"SELECT find_in_set(collate('${t.word}', '${t.c}')," +
        s"collate('${t.set}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      // Implicit casting
      checkAnswer(sql(s"SELECT find_in_set(collate('${t.word}', '${t.c}')," +
        s"'${t.set}')"), Row(t.result))
      checkAnswer(sql(s"SELECT find_in_set('${t.word}'," +
        s"collate('${t.set}', '${t.c}'))"), Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql(s"SELECT find_in_set(collate('AB','UTF8_BINARY')," +
        s"collate('ab,xyz,fgh','UTF8_BINARY_LCASE'))")
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

  test("Support Replace string expression with collation") {
    case class ReplaceTestCase[R](source: String, search: String, replace: String,
        c: String, result: R)
    val testCases = Seq(
      // scalastyle:off
      ReplaceTestCase("r世eplace", "pl", "123", "UTF8_BINARY", "r世e123ace"),
      ReplaceTestCase("repl世ace", "PL", "AB", "UTF8_BINARY_LCASE", "reAB世ace"),
      ReplaceTestCase("abcdabcd", "bc", "", "UNICODE", "adad"),
      ReplaceTestCase("aBc世abc", "b", "12", "UNICODE_CI", "a12c世a12c"),
      ReplaceTestCase("abi̇o12i̇o", "İo", "yy", "UNICODE_CI", "abyy12yy"),
      ReplaceTestCase("abİo12i̇o", "i̇o", "xx", "UNICODE_CI", "abxx12xx")
      // scalastyle:on
    )
    testCases.foreach(t => {
      val query = s"SELECT replace(collate('${t.source}','${t.c}'),collate('${t.search}'," +
        s"'${t.c}'),collate('${t.replace}','${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(
        StringType(CollationFactory.collationNameToId(t.c))))
      // Implicit casting
      checkAnswer(sql(s"SELECT replace(collate('${t.source}','${t.c}'),'${t.search}'," +
        s"'${t.replace}')"), Row(t.result))
      checkAnswer(sql(s"SELECT replace('${t.source}',collate('${t.search}','${t.c}')," +
        s"'${t.replace}')"), Row(t.result))
      checkAnswer(sql(s"SELECT replace('${t.source}','${t.search}'," +
        s"collate('${t.replace}','${t.c}'))"), Row(t.result))
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

  test("Ascii & UnBase64 string expressions with collation") {
    case class AsciiUnBase64TestCase[R](q: String, dt: DataType, r: R)
    val testCases = Seq(
      AsciiUnBase64TestCase("select ascii('a' collate utf8_binary)", IntegerType, 97),
      AsciiUnBase64TestCase("select ascii('B' collate utf8_binary_lcase)", IntegerType, 66),
      AsciiUnBase64TestCase("select ascii('#' collate unicode)", IntegerType, 35),
      AsciiUnBase64TestCase("select ascii('!' collate unicode_ci)", IntegerType, 33),
      AsciiUnBase64TestCase("select unbase64('QUJD' collate utf8_binary)", BinaryType,
        Seq(65, 66, 67)),
      AsciiUnBase64TestCase("select unbase64('eHl6' collate utf8_binary_lcase)", BinaryType,
        Seq(120, 121, 122)),
      AsciiUnBase64TestCase("select unbase64('IyMj' collate utf8_binary)", BinaryType,
        Seq(35, 35, 35)),
      AsciiUnBase64TestCase("select unbase64('IQ==' collate utf8_binary_lcase)", BinaryType,
        Seq(33))
    )
    testCases.foreach(t => {
      // Result & data type
      checkAnswer(sql(t.q), Row(t.r))
      assert(sql(t.q).schema.fields.head.dataType.sameType(t.dt))
    })
  }

  test("Chr, Base64, Decode & FormatNumber string expressions with collation") {
    case class DefaultCollationTestCase[R](q: String, c: String, r: R)
    val testCases = Seq(
      DefaultCollationTestCase("select chr(97)", "UTF8_BINARY", "a"),
      DefaultCollationTestCase("select chr(66)", "UTF8_BINARY_LCASE", "B"),
      DefaultCollationTestCase("select base64('xyz')", "UNICODE", "eHl6"),
      DefaultCollationTestCase("select base64('!')", "UNICODE_CI", "IQ=="),
      DefaultCollationTestCase("select decode(encode('$', 'utf-8'), 'utf-8')", "UTF8_BINARY", "$"),
      DefaultCollationTestCase("select decode(encode('X', 'utf-8'), 'utf-8')", "UTF8_BINARY_LCASE",
        "X"),
      DefaultCollationTestCase("select format_number(123.123, '###.###')", "UNICODE", "123.123"),
      DefaultCollationTestCase("select format_number(99.99, '##.##')", "UNICODE_CI", "99.99")
    )
    testCases.foreach(t => {
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.c) {
        // Result & data type
        checkAnswer(sql(t.q), Row(t.r))
        assert(sql(t.q).schema.fields.head.dataType.sameType(StringType(t.c)))
      }
    })
  }

  test("Encode, ToBinary & Sentences string expressions with collation") {
    case class EncodeToBinarySentencesTestCase[R](q: String, dt: DataType, r: R)
    val testCases = Seq(
      EncodeToBinarySentencesTestCase("select encode('a' collate utf8_binary, 'utf-8')",
        BinaryType, Seq(97)),
      EncodeToBinarySentencesTestCase("select encode('$' collate utf8_binary_lcase, 'utf-8')",
        BinaryType, Seq(36)),
      EncodeToBinarySentencesTestCase("select to_binary('B' collate unicode, 'utf-8')",
        BinaryType, Seq(66)),
      EncodeToBinarySentencesTestCase("select to_binary('#' collate unicode_ci, 'utf-8')",
        BinaryType, Seq(35)),
      EncodeToBinarySentencesTestCase(
        """
          |select sentences('Hello, world! Nice day.' collate utf8_binary)
          |""".stripMargin,
        ArrayType(ArrayType(StringType)), Seq(Seq("Hello", "world"), Seq("Nice", "day"))),
      EncodeToBinarySentencesTestCase(
        """
          |select sentences('Something else. Nothing here.' collate utf8_binary_lcase)
          |""".stripMargin,
        ArrayType(ArrayType(StringType("UTF8_BINARY_LCASE"))),
        Seq(Seq("Something", "else"), Seq("Nothing", "here")))
    )
    testCases.foreach(t => {
      // Result & data type
      checkAnswer(sql(t.q), Row(t.r))
      assert(sql(t.q).schema.fields.head.dataType.sameType(t.dt))
    })
  }

  test("SPARK-47357: Support Upper string expression with collation") {
    // Supported collations
    case class UpperTestCase[R](s: String, c: String, result: R)
    val testCases = Seq(
      UpperTestCase("aBc", "UTF8_BINARY", "ABC"),
      UpperTestCase("aBc", "UTF8_BINARY_LCASE", "ABC"),
      UpperTestCase("aBc", "UNICODE", "ABC"),
      UpperTestCase("aBc", "UNICODE_CI", "ABC")
    )
    testCases.foreach(t => {
      val query = s"SELECT upper(collate('${t.s}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
    })
  }

  test("SPARK-47357: Support Lower string expression with collation") {
    // Supported collations
    case class LowerTestCase[R](s: String, c: String, result: R)
    val testCases = Seq(
      LowerTestCase("aBc", "UTF8_BINARY", "abc"),
      LowerTestCase("aBc", "UTF8_BINARY_LCASE", "abc"),
      LowerTestCase("aBc", "UNICODE", "abc"),
      LowerTestCase("aBc", "UNICODE_CI", "abc")
    )
    testCases.foreach(t => {
      val query = s"SELECT lower(collate('${t.s}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
    })
  }

  test("SPARK-47357: Support InitCap string expression with collation") {
    // Supported collations
    case class InitCapTestCase[R](s: String, c: String, result: R)
    val testCases = Seq(
      InitCapTestCase("aBc ABc", "UTF8_BINARY", "Abc Abc"),
      InitCapTestCase("aBc ABc", "UTF8_BINARY_LCASE", "Abc Abc"),
      InitCapTestCase("aBc ABc", "UNICODE", "Abc Abc"),
      InitCapTestCase("aBc ABc", "UNICODE_CI", "Abc Abc")
    )
    testCases.foreach(t => {
      val query = s"SELECT initcap(collate('${t.s}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
    })
  }

  test("Overlay string expression with collation") {
    // Supported collations
    case class OverlayTestCase(l: String, r: String, pos: Int, c: String, result: String)
    val testCases = Seq(
      OverlayTestCase("hello", " world", 6, "UTF8_BINARY", "hello world"),
      OverlayTestCase("nice", " day", 5, "UTF8_BINARY_LCASE", "nice day"),
      OverlayTestCase("A", "B", 1, "UNICODE", "B"),
      OverlayTestCase("!", "!!!", 1, "UNICODE_CI", "!!!")
    )
    testCases.foreach(t => {
      val query =
        s"""
           |select overlay(collate('${t.l}', '${t.c}') placing
           |collate('${t.r}', '${t.c}') from ${t.pos})
           |""".stripMargin
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      // Implicit casting
      checkAnswer(sql(
        s"""
           |select overlay(collate('${t.l}', '${t.c}') placing '${t.r}' from ${t.pos})
           |""".stripMargin), Row(t.result))
      checkAnswer(sql(
        s"""
           |select overlay('${t.l}' placing collate('${t.r}', '${t.c}') from ${t.pos})
           |""".stripMargin), Row(t.result))
      checkAnswer(sql(
        s"""
           |select overlay(collate('${t.l}', '${t.c}')
           |placing '${t.r}' from collate('${t.pos}', '${t.c}'))
           |""".stripMargin), Row(t.result))
    })
    // Collation mismatch
    assert(
      intercept[AnalysisException] {
        sql("SELECT overlay('a' collate UNICODE PLACING 'b' collate UNICODE_CI FROM 1)")
      }.getErrorClass == "COLLATION_MISMATCH.EXPLICIT"
    )
  }

  test("FormatString string expression with collation") {
    // Supported collations
    case class FormatStringTestCase(f: String, a: Seq[Any], c: String, r: String)
    val testCases = Seq(
      FormatStringTestCase("%s%s", Seq("'a'", "'b'"), "UTF8_BINARY", "ab"),
      FormatStringTestCase("%d", Seq(123), "UTF8_BINARY_LCASE", "123"),
      FormatStringTestCase("%s%d", Seq("'A'", 0), "UNICODE", "A0"),
      FormatStringTestCase("%s%s", Seq("'Hello'", "'!!!'"), "UNICODE_CI", "Hello!!!")
    )
    testCases.foreach(t => {
      val query =
        s"""
           |select format_string(collate('${t.f}', '${t.c}'), ${t.a.mkString(", ")})
           |""".stripMargin
      // Result & data type
      checkAnswer(sql(query), Row(t.r))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
    })
  }

  test("SoundEx string expression with collation") {
    // Supported collations
    case class SoundExTestCase(q: String, c: String, r: String)
    val testCases = Seq(
      SoundExTestCase("select soundex('A' collate utf8_binary)", "UTF8_BINARY", "A000"),
      SoundExTestCase("select soundex('!' collate utf8_binary_lcase)", "UTF8_BINARY_LCASE", "!"),
      SoundExTestCase("select soundex('$' collate unicode)", "UNICODE", "$"),
      SoundExTestCase("select soundex('X' collate unicode_ci)", "UNICODE_CI", "X000")
    )
    testCases.foreach(t => {
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.c) {
        // Result & data type
        checkAnswer(sql(t.q), Row(t.r))
        assert(sql(t.q).schema.fields.head.dataType.sameType(StringType(t.c)))
      }
    })
  }

  test("Length, BitLength & OctetLength string expressions with collations") {
    // Supported collations
    case class LenTestCase(q: String, r: Int)
    val testCases = Seq(
      LenTestCase("select length('hello' collate utf8_binary)", 5),
      LenTestCase("select length('world' collate utf8_binary_lcase)", 5),
      LenTestCase("select length('ﬀ' collate unicode)", 1),
      LenTestCase("select bit_length('hello' collate unicode_ci)", 40),
      LenTestCase("select bit_length('world' collate utf8_binary)", 40),
      LenTestCase("select bit_length('ﬀ' collate utf8_binary_lcase)", 24),
      LenTestCase("select octet_length('hello' collate unicode)", 5),
      LenTestCase("select octet_length('world' collate unicode_ci)", 5),
      LenTestCase("select octet_length('ﬀ' collate utf8_binary)", 3)
    )
    testCases.foreach(t => {
      // Result & data type
      checkAnswer(sql(t.q), Row(t.r))
      assert(sql(t.q).schema.fields.head.dataType.sameType(IntegerType))
    })
  }

  test("Luhncheck string expression with collation") {
    // Supported collations
    case class LuhncheckTestCase(q: String, c: String, r: Boolean)
    val testCases = Seq(
      LuhncheckTestCase("123", "UTF8_BINARY", r = false),
      LuhncheckTestCase("000", "UTF8_BINARY_LCASE", r = true),
      LuhncheckTestCase("111", "UNICODE", r = false),
      LuhncheckTestCase("222", "UNICODE_CI", r = false)
    )
    testCases.foreach(t => {
      val query = s"select luhn_check(${t.q})"
      // Result & data type
      checkAnswer(sql(query), Row(t.r))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
    })
  }

  test("Support Left/Right/Substr with collation") {
    case class SubstringTestCase(
        method: String,
        str: String,
        len: String,
        pad: Option[String],
        collation: String,
        result: Row) {
      val strString = if (str == "null") "null" else s"'$str'"
      val query =
        s"SELECT $method(collate($strString, '$collation')," +
          s" $len${pad.map(p => s", '$p'").getOrElse("")})"
    }

    val checks = Seq(
      SubstringTestCase("substr", "example", "1", Some("100"), "utf8_binary_lcase", Row("example")),
      SubstringTestCase("substr", "example", "2", Some("2"), "utf8_binary", Row("xa")),
      SubstringTestCase("right", "", "1", None, "utf8_binary_lcase", Row("")),
      SubstringTestCase("substr", "example", "0", Some("0"), "unicode", Row("")),
      SubstringTestCase("substr", "example", "-3", Some("2"), "unicode_ci", Row("pl")),
      SubstringTestCase("substr", " a世a ", "2", Some("3"), "utf8_binary_lcase", Row("a世a")),
      SubstringTestCase("left", " a世a ", "3", None, "utf8_binary", Row(" a世")),
      SubstringTestCase("right", " a世a ", "3", None, "unicode", Row("世a ")),
      SubstringTestCase("left", "ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ", "3", None, "unicode_ci", Row("ÀÃÂ")),
      SubstringTestCase("right", "ÀÃÂĀĂȦÄäâãȻȻȻȻȻǢǼÆ", "3", None, "utf8_binary_lcase", Row("ǢǼÆ")),
      SubstringTestCase("substr", "", "1", Some("1"), "utf8_binary_lcase", Row("")),
      SubstringTestCase("substr", "", "1", Some("1"), "unicode", Row("")),
      SubstringTestCase("left", "", "1", None, "utf8_binary", Row("")),
      SubstringTestCase("left", "null", "1", None, "utf8_binary_lcase", Row(null)),
      SubstringTestCase("right", "null", "1", None, "unicode", Row(null)),
      SubstringTestCase("substr", "null", "1", None, "utf8_binary", Row(null)),
      SubstringTestCase("substr", "null", "1", Some("1"), "unicode_ci", Row(null)),
      SubstringTestCase("left", "null", "null", None, "utf8_binary_lcase", Row(null)),
      SubstringTestCase("right", "null", "null", None, "unicode", Row(null)),
      SubstringTestCase("substr", "null", "null", Some("null"), "utf8_binary", Row(null)),
      SubstringTestCase("substr", "null", "null", None, "unicode_ci", Row(null)),
      SubstringTestCase("left", "ÀÃÂȦÄäåäáâãȻȻȻǢǼÆ", "null", None, "utf8_binary_lcase", Row(null)),
      SubstringTestCase("right", "ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ", "null", None, "unicode", Row(null)),
      SubstringTestCase("substr", "ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ", "null", None, "utf8_binary", Row(null)),
      SubstringTestCase("substr", "", "null", None, "unicode_ci", Row(null))
    )

    checks.foreach { check =>
      // Result & data type
      checkAnswer(sql(check.query), check.result)
      assert(sql(check.query).schema.fields.head.dataType.sameType(StringType(check.collation)))
    }
  }

  test("Support StringRPad string expressions with collation") {
    // Supported collations
    case class StringRPadTestCase[R](s: String, len: Int, pad: String, c: String, result: R)
    val testCases = Seq(
      StringRPadTestCase("", 5, " ", "UTF8_BINARY", "     "),
      StringRPadTestCase("abc", 5, " ", "UNICODE", "abc  "),
      StringRPadTestCase("Hello", 7, "Wörld", "UTF8_BINARY_LCASE", "HelloWö"),
      StringRPadTestCase("1234567890", 5, "aaaAAa", "UNICODE_CI", "12345"),
      StringRPadTestCase("aaAA", 2, " ", "UTF8_BINARY", "aa"),
      StringRPadTestCase("ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ℀℃", 2, "1", "UTF8_BINARY_LCASE", "ÀÃ"),
      StringRPadTestCase("ĂȦÄäåäá", 20, "ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ", "UNICODE", "ĂȦÄäåäáÀÃÂĀĂȦÄäåäáâã"),
      StringRPadTestCase("aȦÄä", 8, "a1", "UNICODE_CI", "aȦÄäa1a1")
    )
    testCases.foreach(t => {
      val query = s"SELECT rpad(collate('${t.s}', '${t.c}')," +
        s" ${t.len}, collate('${t.pad}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      // Implicit casting
      checkAnswer(
        sql(s"SELECT rpad(collate('${t.s}', '${t.c}'), ${t.len}, '${t.pad}')"),
        Row(t.result))
      checkAnswer(
        sql(s"SELECT rpad('${t.s}', ${t.len}, collate('${t.pad}', '${t.c}'))"),
        Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT rpad(collate('abcde', 'UNICODE_CI'),1,collate('C', 'UTF8_BINARY_LCASE'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("Support StringLPad string expressions with collation") {
    // Supported collations
    case class StringLPadTestCase[R](s: String, len: Int, pad: String, c: String, result: R)
    val testCases = Seq(
      StringLPadTestCase("", 5, " ", "UTF8_BINARY", "     "),
      StringLPadTestCase("abc", 5, " ", "UNICODE", "  abc"),
      StringLPadTestCase("Hello", 7, "Wörld", "UTF8_BINARY_LCASE", "WöHello"),
      StringLPadTestCase("1234567890", 5, "aaaAAa", "UNICODE_CI", "12345"),
      StringLPadTestCase("aaAA", 2, " ", "UTF8_BINARY", "aa"),
      StringLPadTestCase("ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ℀℃", 2, "1", "UTF8_BINARY_LCASE", "ÀÃ"),
      StringLPadTestCase("ĂȦÄäåäá", 20, "ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ", "UNICODE", "ÀÃÂĀĂȦÄäåäáâãĂȦÄäåäá"),
      StringLPadTestCase("aȦÄä", 8, "a1", "UNICODE_CI", "a1a1aȦÄä")
    )
    testCases.foreach(t => {
      val query = s"SELECT lpad(collate('${t.s}', '${t.c}')," +
        s" ${t.len}, collate('${t.pad}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      // Implicit casting
      checkAnswer(
        sql(s"SELECT lpad(collate('${t.s}', '${t.c}'), ${t.len}, '${t.pad}')"),
        Row(t.result))
      checkAnswer(
        sql(s"SELECT lpad('${t.s}', ${t.len}, collate('${t.pad}', '${t.c}'))"),
        Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT lpad(collate('abcde', 'UNICODE_CI'),1,collate('C', 'UTF8_BINARY_LCASE'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("Support StringLPad string expressions with explicit collation on second parameter") {
    val query = "SELECT lpad('abc', collate('5', 'unicode_ci'), ' ')"
    checkAnswer(sql(query), Row("  abc"))
    assert(sql(query).schema.fields.head.dataType.sameType(StringType(0)))
  }

  test("Support Locate string expression with collation") {
    case class StringLocateTestCase[R](substring: String, string: String, start: Integer,
        c: String, result: R)
    val testCases = Seq(
      // scalastyle:off
      StringLocateTestCase("aa", "aaads", 0, "UTF8_BINARY", 0),
      StringLocateTestCase("aa", "Aaads", 0, "UTF8_BINARY_LCASE", 0),
      StringLocateTestCase("界x", "test大千世界X大千世界", 1, "UTF8_BINARY_LCASE", 8),
      StringLocateTestCase("aBc", "abcabc", 4, "UTF8_BINARY_LCASE", 4),
      StringLocateTestCase("aa", "Aaads", 0, "UNICODE", 0),
      StringLocateTestCase("abC", "abCabC", 2, "UNICODE", 4),
      StringLocateTestCase("aa", "Aaads", 0, "UNICODE_CI", 0),
      StringLocateTestCase("界x", "test大千世界X大千世界", 1, "UNICODE_CI", 8)
      // scalastyle:on
    )
    testCases.foreach(t => {
      val query = s"SELECT locate(collate('${t.substring}','${t.c}')," +
        s"collate('${t.string}','${t.c}'),${t.start})"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(IntegerType))
      // Implicit casting
      checkAnswer(sql(s"SELECT locate(collate('${t.substring}','${t.c}')," +
        s"'${t.string}',${t.start})"), Row(t.result))
      checkAnswer(sql(s"SELECT locate('${t.substring}',collate('${t.string}'," +
        s"'${t.c}'),${t.start})"), Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT locate(collate('aBc', 'UTF8_BINARY'),collate('abcabc', 'UTF8_BINARY_LCASE'),4)")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  // TODO: Add more tests for other string expressions

}
// scalastyle:on nonascii

class CollationStringExpressionsANSISuite extends CollationStringExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)

  // TODO: If needed, add more tests for other string expressions (with ANSI mode enabled)

}
