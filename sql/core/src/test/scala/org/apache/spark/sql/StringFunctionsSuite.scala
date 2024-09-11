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

import org.apache.spark.{SPARK_DOC_ROOT, SparkIllegalArgumentException, SparkRuntimeException}
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.execution.FormattedMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class StringFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("string concat") {
    val df = Seq[(String, String, String)](("a", "b", null)).toDF("a", "b", "c")

    checkAnswer(
      df.select(concat($"a", $"b"), concat($"a", $"b", $"c")),
      Row("ab", null))

    checkAnswer(
      df.selectExpr("concat(a, b)", "concat(a, b, c)"),
      Row("ab", null))
  }

  test("string concat_ws") {
    val df = Seq[(String, String, String)](("a", "b", null)).toDF("a", "b", "c")

    checkAnswer(
      df.select(concat_ws("||", $"a", $"b", $"c")),
      Row("a||b"))

    checkAnswer(
      df.selectExpr("concat_ws('||', a, b, c)"),
      Row("a||b"))
  }

  test("SPARK-31993: concat_ws in agg function with plenty of string/array types columns") {
    withSQLConf(SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1024",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY") {

      val (df, genColNames, genColValues) = prepareTestConcatWsColumns()
      val groupedCols = Seq($"a") ++ genColNames.map(col)
      val concatCols = Seq(collect_list($"b"), collect_list($"c")) ++ genColNames.map(col)
      val df2 = df
        .groupBy(groupedCols: _*)
        .agg(concat_ws(",", concatCols: _*).as("con"))
        .select("con")

      val expected = Seq(
        Row((Seq("b1", "b2") ++ genColValues).mkString(",")),
        Row((Seq("b3", "b4") ++ genColValues).mkString(","))
      )

      checkAnswer(df2, expected)
    }
  }

  // This test doesn't fail without SPARK-31993, but still be useful for regression test.
  test("SPARK-31993: concat_ws in agg function with plenty of string types columns") {
    withSQLConf(SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1024",
      SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY") {

      val (df, genColNames, genColValues) = prepareTestConcatWsColumns()
      val groupedCols = Seq($"a") ++ genColNames.map(col)
      val concatCols = groupedCols
      val df2 = df
        .groupBy(groupedCols: _*)
        .agg(concat_ws(",", concatCols: _*).as("con"))
        .select("con")

      val expected = Seq(
        Row((Seq("a") ++ genColValues).mkString(",")),
        Row((Seq("b") ++ genColValues).mkString(","))
      )

      checkAnswer(df2, expected)
    }
  }

  private def prepareTestConcatWsColumns(): (DataFrame, Seq[String], Seq[String]) = {
    val genColNames = (1 to 30).map { idx => s"col_$idx" }
    val genColValues = (1 to 30).map { _.toString }
    val genCols = genColValues.map(lit)

    val df = Seq[(String, String, String)](
      ("a", "b1", null),
      ("a", "b2", null),
      ("b", "b3", null),
      ("b", "b4", null))
      .toDF("a", "b", "c")
      .withColumns(genColNames, genCols)

    (df, genColNames, genColValues)
  }

  test("string elt") {
    val df = Seq[(String, String, String, Int)](("hello", "world", null, 15))
      .toDF("a", "b", "c", "d")

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkAnswer(
        df.selectExpr("elt(0, a, b, c)", "elt(1, a, b, c)", "elt(4, a, b, c)"),
        Row(null, "hello", null))
    }

    // check implicit type cast
    checkAnswer(
      df.selectExpr("elt(4, a, b, c, d)", "elt('2', a, b, c, d)"),
      Row("15", "world"))
  }

  test("string Levenshtein distance") {
    val df = Seq(("kitten", "sitting"), ("frog", "fog")).toDF("l", "r")
    checkAnswer(df.select(levenshtein($"l", $"r")), Seq(Row(3), Row(1)))
    checkAnswer(df.selectExpr("levenshtein(l, r)"), Seq(Row(3), Row(1)))
    checkAnswer(df.select(levenshtein($"l", lit(null))), Seq(Row(null), Row(null)))
    checkAnswer(df.selectExpr("levenshtein(l, null)"), Seq(Row(null), Row(null)))

    checkAnswer(df.select(levenshtein($"l", $"r", 3)), Seq(Row(3), Row(1)))
    checkAnswer(df.selectExpr("levenshtein(l, r, 3)"), Seq(Row(3), Row(1)))
    checkAnswer(df.select(levenshtein(lit(null), $"r", 3)), Seq(Row(null), Row(null)))
    checkAnswer(df.selectExpr("levenshtein(null, r, 3)"), Seq(Row(null), Row(null)))

    checkAnswer(df.select(levenshtein($"l", $"r", 0)), Seq(Row(-1), Row(-1)))
    checkAnswer(df.selectExpr("levenshtein(l, r, 0)"), Seq(Row(-1), Row(-1)))
    checkAnswer(df.select(levenshtein($"l", lit(null), 0)), Seq(Row(null), Row(null)))
    checkAnswer(df.selectExpr("levenshtein(l, null, 0)"), Seq(Row(null), Row(null)))
  }

  test("string rlike / regexp / regexp_like") {
    val df = Seq(
      ("1a 2b 14m", "\\d+b"),
      ("1a 2b 14m", "[a-z]+b")).toDF("a", "b")

    checkAnswer(
      df.select(
        rlike($"a", $"b"),
        regexp($"a", $"b"),
        regexp_like($"a", $"b")),
      Row(false, false, false) :: Row(true, true, true) :: Nil)
    checkAnswer(
      df.selectExpr(
        "rlike(a, b)",
        "regexp(a, b)",
        "regexp_like(a, b)"),
      Row(false, false, false) :: Row(true, true, true) :: Nil)
  }

  test("string regexp_count") {
    val df = Seq(
      ("1a 2b 14m", "\\d+"),
      ("1a 2b 14m", "mmm")).toDF("a", "b")

    checkAnswer(
      df.select(regexp_count($"a", $"b")),
      Row(0) :: Row(3) :: Nil)

    checkAnswer(
      df.selectExpr("regexp_count(a, b)"),
      Row(0) :: Row(3) :: Nil)
  }

  test("string regex_replace / regex_extract") {
    val df = Seq(
      ("100-200", "(\\d+)-(\\d+)", "300"),
      ("100-200", "(\\d+)-(\\d+)", "400"),
      ("100-200", "(\\d+)", "400")).toDF("a", "b", "c")

    checkAnswer(
      df.select(
        regexp_replace($"a", "(\\d+)", "num"),
        regexp_replace($"a", $"b", $"c"),
        regexp_extract($"a", "(\\d+)-(\\d+)", 1)),
      Row("num-num", "300", "100") :: Row("num-num", "400", "100") ::
        Row("num-num", "400-400", "100") :: Nil)

    // for testing the mutable state of the expression in code gen.
    // This is a hack way to enable the codegen, thus the codegen is enable by default,
    // it will still use the interpretProjection if projection followed by a LocalRelation,
    // hence we add a filter operator.
    // See the optimizer rule `ConvertToLocalRelation`
    checkAnswer(
      df.filter("isnotnull(a)").selectExpr(
        "regexp_replace(a, b, c)",
        "regexp_extract(a, b, 1)"),
      Row("300", "100") :: Row("400", "100") :: Row("400-400", "100") :: Nil)
  }

  test("non-matching optional group") {
    val df = Seq(Tuple1("aaaac")).toDF("s")

    checkAnswer(
      df.select(regexp_extract($"s", "(foo)", 1)),
      Row("")
    )
    checkAnswer(
      df.select(regexp_extract($"s", "(a+)(b)?(c)", 2)),
      Row("")
    )
  }

  test("string regexp_extract_all") {
    val df = Seq(("1a 2b 14m", "(\\d+)([a-z]+)")).toDF("a", "b")

    checkAnswer(
      df.select(
        regexp_extract_all($"a", $"b", lit(0)),
        regexp_extract_all($"a", $"b"),
        regexp_extract_all($"a", $"b", lit(2))),
      Row(Seq("1a", "2b", "14m"), Seq("1", "2", "14"), Seq("a", "b", "m")) :: Nil)
    checkAnswer(
      df.selectExpr(
        "regexp_extract_all(a, b, 0)",
        "regexp_extract_all(a, b)",
        "regexp_extract_all(a, b, 2)"),
      Row(Seq("1a", "2b", "14m"), Seq("1", "2", "14"), Seq("a", "b", "m")) :: Nil)
  }

  test("string regexp_substr") {
    val df = Seq(
      ("1a 2b 14m", "\\d+"),
      ("1a 2b 14m", "\\d+ "),
      ("1a 2b 14m", "\\d+(a|b|m)"),
      ("1a 2b 14m", "\\d{2}(a|b|m)"),
      ("1a 2b 14m", "")).toDF("a", "b")

    checkAnswer(
      df.select(regexp_substr($"a", $"b")),
      Row("14m") :: Row("1") :: Row("1a") :: Row(null) :: Row(null) :: Nil)
    checkAnswer(
      df.selectExpr("regexp_substr(a, b)"),
      Row("14m") :: Row("1") :: Row("1a") :: Row(null) :: Row(null) :: Nil)
  }

  test("string regexp_instr") {
    val df = Seq(
      ("1a 2b 14m", "\\d+(a|b|m)"),
      ("1a 2b 14m", "\\d{2}(a|b|m)")).toDF("a", "b")

    checkAnswer(
      df.select(
        regexp_instr($"a", $"b"),
        regexp_instr($"a", $"b", lit(1)),
        regexp_instr($"a", $"b", lit(2))),
      Row(1, 1, 1) :: Row(7, 7, 7) :: Nil)
    checkAnswer(
      df.selectExpr(
        "regexp_instr(a, b)",
        "regexp_instr(a, b, 1)",
        "regexp_instr(a, b, 2)"),
      Row(1, 1, 1) :: Row(7, 7, 7) :: Nil)
  }

  test("string ascii function") {
    val df = Seq(("abc", "")).toDF("a", "b")
    checkAnswer(
      df.select(ascii($"a"), ascii($"b")),
      Row(97, 0))

    checkAnswer(
      df.selectExpr("ascii(a)", "ascii(b)"),
      Row(97, 0))
  }

  test("string base64/unbase64 function") {
    val bytes = Array[Byte](1, 2, 3, 4)
    val df = Seq((bytes, "AQIDBA==")).toDF("a", "b")
    checkAnswer(
      df.select(base64($"a"), unbase64($"b")),
      Row("AQIDBA==", bytes))

    checkAnswer(
      df.selectExpr("base64(a)", "unbase64(b)"),
      Row("AQIDBA==", bytes))
  }

  test("string overlay function") {
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    val df = Seq(("Spark SQL", "Spark的SQL", "_", "CORE", "ANSI ", "tructured", 6, 7, 0, 2, 4)).
      toDF("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
    checkAnswer(df.select(overlay($"a", $"c", $"g")), Row("Spark_SQL"))
    checkAnswer(df.select(overlay($"a", $"d", $"h")), Row("Spark CORE"))
    checkAnswer(df.select(overlay($"a", $"e", $"h", $"i")), Row("Spark ANSI SQL"))
    checkAnswer(df.select(overlay($"a", $"f", $"j", $"k")), Row("Structured SQL"))
    checkAnswer(df.select(overlay($"b", $"c", $"g")), Row("Spark_SQL"))
    // scalastyle:on
  }

  test("binary overlay function") {
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    val df = Seq((
      Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9),
      Array[Byte](-1),
      Array[Byte](-1, -1, -1, -1),
      Array[Byte](-1, -1),
      Array[Byte](-1, -1, -1, -1, -1),
      6, 7, 0, 2, 4)).toDF("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
    checkAnswer(df.select(overlay($"a", $"b", $"f")), Row(Array[Byte](1, 2, 3, 4, 5, -1, 7, 8, 9)))
    checkAnswer(df.select(overlay($"a", $"c", $"g")),
      Row(Array[Byte](1, 2, 3, 4, 5, 6, -1, -1, -1, -1)))
    checkAnswer(df.select(overlay($"a", $"d", $"g", $"h")),
      Row(Array[Byte](1, 2, 3, 4, 5, 6, -1, -1, 7, 8, 9)))
    checkAnswer(df.select(overlay($"a", $"e", $"i", $"j")),
      Row(Array[Byte](1, -1, -1, -1, -1, -1, 6, 7, 8, 9)))
  }

  test("string / binary substring function") {
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    val df = Seq(("1世3", Array[Byte](1, 2, 3, 4))).toDF("a", "b")
    checkAnswer(df.select(substring($"a", 1, 2)), Row("1世"))
    checkAnswer(df.select(substring($"b", 2, 2)), Row(Array[Byte](2,3)))
    checkAnswer(df.selectExpr("substring(a, 1, 2)"), Row("1世"))
    // scalastyle:on
  }

  test("string substring function using columns") {
    val df = Seq(("Spark", 2, 3)).toDF("a", "b", "c")
    checkAnswer(df.select(substring($"a", $"b", $"c")), Row("par"))
  }

  test("string encode/decode function") {
    val bytes = Array[Byte](-27, -92, -89, -27, -115, -125, -28, -72, -106, -25, -107, -116)
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    val df = Seq(("大千世界", "utf-8", bytes)).toDF("a", "b", "c")
    checkAnswer(
      df.select(encode($"a", "utf-8"), decode($"c", "utf-8")),
      Row(bytes, "大千世界"))

    checkAnswer(
      df.selectExpr("encode(a, 'utf-8')", "decode(c, 'utf-8')"),
      Row(bytes, "大千世界"))
    // scalastyle:on
  }

  test("string translate") {
    val df = Seq(("translate", "")).toDF("a", "b")
    checkAnswer(df.select(translate($"a", "rnlt", "123")), Row("1a2s3ae"))
    checkAnswer(df.selectExpr("""translate(a, "rnlt", "")"""), Row("asae"))
  }

  test("string trim functions") {
    val df = Seq(("  example  ", "", "example")).toDF("a", "b", "c")

    checkAnswer(
      df.select(ltrim($"a"), rtrim($"a"), trim($"a")),
      Row("example  ", "  example", "example"))

    checkAnswer(
      df.select(ltrim($"c", "e"), rtrim($"c", "e"), trim($"c", "e")),
      Row("xample", "exampl", "xampl"))

    checkAnswer(
      df.select(ltrim($"c", "xe"), rtrim($"c", "emlp"), trim($"c", "elxp")),
      Row("ample", "exa", "am"))

    checkAnswer(
      df.select(trim($"c", "xyz")),
      Row("example"))

    checkAnswer(
      df.selectExpr("ltrim(a)", "rtrim(a)", "trim(a)"),
      Row("example  ", "  example", "example"))
  }

  test("string formatString function") {
    val df = Seq(("aa%d%s", 123, "cc")).toDF("a", "b", "c")

    checkAnswer(
      df.select(format_string("aa%d%s", $"b", $"c")),
      Row("aa123cc"))

    checkAnswer(
      df.selectExpr("printf(a, b, c)"),
      Row("aa123cc"))
  }

  test("soundex function") {
    val df = Seq(("MARY", "SU")).toDF("l", "r")
    checkAnswer(
      df.select(soundex($"l"), soundex($"r")), Row("M600", "S000"))

    checkAnswer(
      df.selectExpr("SoundEx(l)", "SoundEx(r)"), Row("M600", "S000"))
  }

  test("string instr function") {
    val df = Seq(("aaads", "aa", "zz")).toDF("a", "b", "c")

    checkAnswer(
      df.select(instr($"a", "aa")),
      Row(1))

    checkAnswer(
      df.selectExpr("instr(a, b)"),
      Row(1))
  }

  test("string substring_index function") {
    val df = Seq(("www.apache.org", ".", "zz")).toDF("a", "b", "c")
    checkAnswer(
      df.select(substring_index($"a", ".", 2)),
      Row("www.apache"))
    checkAnswer(
      df.selectExpr("substring_index(a, '.', 2)"),
      Row("www.apache")
    )

    // TODO SPARK-48779 Move E2E SQL tests with column input to collations.sql golden file.
    val testTable = "test_substring_index"
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (num int) USING parquet")
      sql(s"INSERT INTO $testTable VALUES (1), (2), (3), (NULL)")
      Seq("UTF8_BINARY", "UTF8_LCASE", "UNICODE", "UNICODE_CI").foreach(collation =>
        withSQLConf(SQLConf.DEFAULT_COLLATION.key -> collation) {
          val query = s"SELECT num, SUBSTRING_INDEX('a_a_a', '_', num) as sub_str FROM $testTable"
          checkAnswer(sql(query), Seq(Row(1, "a"), Row(2, "a_a"), Row(3, "a_a_a"), Row(null, null)))
        }
      )
    }
  }

  test("string locate function") {
    val df = Seq(("aaads", "aa", "zz", 2)).toDF("a", "b", "c", "d")

    checkAnswer(
      df.select(locate("aa", $"a"), locate("aa", $"a", 2), locate("aa", $"a", 0)),
      Row(1, 2, 0))

    checkAnswer(
      df.selectExpr("locate(b, a)", "locate(b, a, d)", "locate(b, a, 3)"),
      Row(1, 2, 0))
  }

  test("string padding functions") {
    val df = Seq(("hi", 5, "??")).toDF("a", "b", "c")

    checkAnswer(
      df.select(lpad($"a", 1, "c"), lpad($"a", 5, "??"), rpad($"a", 1, "c"), rpad($"a", 5, "??")),
      Row("h", "???hi", "h", "hi???"))

    checkAnswer(
      df.selectExpr("lpad(a, b, c)", "rpad(a, b, c)", "lpad(a, 1, c)", "rpad(a, 1, c)"),
      Row("???hi", "hi???", "h", "h"))
  }

  test("string repeat function") {
    val df = Seq(("hi", 2)).toDF("a", "b")

    checkAnswer(
      df.select(repeat($"a", 2)),
      Row("hihi"))

    checkAnswer(
      df.selectExpr("repeat(a, 2)", "repeat(a, b)"),
      Row("hihi", "hihi"))
  }

  test("string reverse function") {
    val df = Seq(("hi", "hhhi")).toDF("a", "b")

    checkAnswer(
      df.select(reverse($"a"), reverse($"b")),
      Row("ih", "ihhh"))

    checkAnswer(
      df.selectExpr("reverse(b)"),
      Row("ihhh"))
  }

  test("string space function") {
    val df = Seq((2, 3)).toDF("a", "b")

    checkAnswer(
      df.selectExpr("space(b)"),
      Row("   "))
  }

  test("string split function with no limit") {
    val df = Seq(("aa2bb3cc4", "[1-9]+")).toDF("a", "b")

    checkAnswer(
      df.select(split($"a", "[1-9]+")),
      Row(Seq("aa", "bb", "cc", "")))

    checkAnswer(
      df.selectExpr("split(a, '[1-9]+')"),
      Row(Seq("aa", "bb", "cc", "")))
  }

  test("string split function with limit explicitly set to 0") {
    val df = Seq(("aa2bb3cc4", "[1-9]+")).toDF("a", "b")

    checkAnswer(
      df.select(split($"a", "[1-9]+", 0)),
      Row(Seq("aa", "bb", "cc", "")))

    checkAnswer(
      df.selectExpr("split(a, '[1-9]+', 0)"),
      Row(Seq("aa", "bb", "cc", "")))
  }

  test("string split function with positive limit") {
    val df = Seq(("aa2bb3cc4", "[1-9]+")).toDF("a", "b")

    checkAnswer(
      df.select(split($"a", "[1-9]+", 2)),
      Row(Seq("aa", "bb3cc4")))

    checkAnswer(
      df.selectExpr("split(a, '[1-9]+', 2)"),
      Row(Seq("aa", "bb3cc4")))
  }

  test("string split function with negative limit") {
    val df = Seq(("aa2bb3cc4", "[1-9]+")).toDF("a", "b")

    checkAnswer(
      df.select(split($"a", "[1-9]+", -2)),
      Row(Seq("aa", "bb", "cc", "")))

    checkAnswer(
      df.selectExpr("split(a, '[1-9]+', -2)"),
      Row(Seq("aa", "bb", "cc", "")))
  }

  test("SPARK-47845: string split function with column types") {
    val df = Seq(
      ("aa2bb3cc4", "[1-9]+", 0),
      ("aa2bb3cc4", "[1-9]+", 2),
      ("aa2bb3cc4", "[1-9]+", -2)).toDF("a", "b", "c")

    // without limit
    val expectedNoLimit = Seq(
      Row(Seq("aa", "bb", "cc", "")),
      Row(Seq("aa", "bb", "cc", "")),
      Row(Seq("aa", "bb", "cc", "")))

    checkAnswer(df.select(split($"a", $"b")), expectedNoLimit)

    checkAnswer(df.selectExpr("split(a, b)"), expectedNoLimit)

    // with limit
    val expectedWithLimit = Seq(
      Row(Seq("aa", "bb", "cc", "")),
      Row(Seq("aa", "bb3cc4")),
      Row(Seq("aa", "bb", "cc", "")))

    checkAnswer(df.select(split($"a", $"b", $"c")), expectedWithLimit)

    checkAnswer(df.selectExpr("split(a, b, c)"), expectedWithLimit)
  }

  test("string / binary length function") {
    val df = Seq(("123", Array[Byte](1, 2, 3, 4), 123, 2.0f, 3.015))
      .toDF("a", "b", "c", "d", "e")
    checkAnswer(
      df.select(length($"a"), length($"b")),
      Row(3, 4))

    checkAnswer(
      df.select(len($"a"), len($"b")),
      Row(3, 4))

    Seq("length", "len").foreach { len =>
      checkAnswer(df.selectExpr(s"$len(a)", s"$len(b)"), Row(3, 4))
      checkAnswer(df.selectExpr(s"$len(c)", s"$len(d)", s"$len(e)"), Row(3, 3, 5))
    }
  }

  test("SPARK-36751: add octet length api for scala") {
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    val df = Seq(("123", Array[Byte](1, 2, 3, 4), 123, 2.0f, 3.015, "\ud83d\udc08"))
      .toDF("a", "b", "c", "d", "e", "f")
    // string and binary input
    checkAnswer(
      df.select(octet_length($"a"), octet_length($"b")),
      Row(3, 4))
    // string and binary input
    checkAnswer(
      df.selectExpr("octet_length(a)", "octet_length(b)"),
      Row(3, 4))
    // integer, float and double input
    checkAnswer(
      df.selectExpr("octet_length(c)", "octet_length(d)", "octet_length(e)"),
      Row(3, 3, 5)
    )
    // multi-byte character input
    checkAnswer(
      df.selectExpr("octet_length(f)"),
      Row(4)
    )
    // scalastyle:on
  }

  test("SPARK-36751: add bit length api for scala") {
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    val df = Seq(("123", Array[Byte](1, 2, 3, 4), 123, 2.0f, 3.015, "\ud83d\udc08"))
      .toDF("a", "b", "c", "d", "e", "f")
    // string and binary input
    checkAnswer(
      df.select(bit_length($"a"), bit_length($"b")),
      Row(24, 32))
    // string and binary input
    checkAnswer(
      df.selectExpr("bit_length(a)", "bit_length(b)"),
      Row(24, 32))
    // integer, float and double input
    checkAnswer(
      df.selectExpr("bit_length(c)", "bit_length(d)", "bit_length(e)"),
      Row(24, 24, 40)
    )
    // multi-byte character input
    checkAnswer(
      df.selectExpr("bit_length(f)"),
      Row(32)
    )
    // scalastyle:on
  }

  test("initcap function") {
    val df = Seq(("ab", "a B", "sParK")).toDF("x", "y", "z")
    checkAnswer(
      df.select(initcap($"x"), initcap($"y"), initcap($"z")), Row("Ab", "A B", "Spark"))

    checkAnswer(
      df.selectExpr("InitCap(x)", "InitCap(y)", "InitCap(z)"), Row("Ab", "A B", "Spark"))
  }

  test("number format function") {
    val df = spark.range(1)

    checkAnswer(
      df.select(format_number(lit(5L), 4)),
      Row("5.0000"))

    checkAnswer(
      df.select(format_number(lit(1.toByte), 4)), // convert the 1st argument to integer
      Row("1.0000"))

    checkAnswer(
      df.select(format_number(lit(2.toShort), 4)), // convert the 1st argument to integer
      Row("2.0000"))

    checkAnswer(
      df.select(format_number(lit(3.1322.toFloat), 4)), // convert the 1st argument to double
      Row("3.1322"))

    checkAnswer(
      df.select(format_number(lit(4), 4)), // not convert anything
      Row("4.0000"))

    checkAnswer(
      df.select(format_number(lit(5L), 4)), // not convert anything
      Row("5.0000"))

    checkAnswer(
      df.select(format_number(lit(6.48173), 4)), // not convert anything
      Row("6.4817"))

    checkAnswer(
      df.select(format_number(lit(BigDecimal("7.128381")), 4)), // not convert anything
      Row("7.1284"))

    intercept[AnalysisException] {
      df.select(format_number(lit("aa"), 4)) // string type of the 1st argument is unacceptable
    }

    intercept[AnalysisException] {
      df.selectExpr("format_number(4, 6.48173)") // non-integral type 2nd argument is unacceptable
    }

    // for testing the mutable state of the expression in code gen.
    // This is a hack way to enable the codegen, thus the codegen is enable by default,
    // it will still use the interpretProjection if projection follows by a LocalRelation,
    // hence we add a filter operator.
    // See the optimizer rule `ConvertToLocalRelation`
    val df2 = Seq((5L, 4), (4L, 3), (4L, 3), (4L, 3), (3L, 2)).toDF("a", "b")
    checkAnswer(
      df2.filter("b>0").selectExpr("format_number(a, b)"),
      Row("5.0000") :: Row("4.000") :: Row("4.000") :: Row("4.000") :: Row("3.00") :: Nil)
  }

  test("string sentences function") {
    val df = Seq(("Hi there! The price was $1,234.56.... But, not now.", "en", "US"))
      .toDF("str", "language", "country")

    checkAnswer(
      df.selectExpr("sentences(str, language, country)"),
      Row(Seq(Seq("Hi", "there"), Seq("The", "price", "was"), Seq("But", "not", "now"))))

    checkAnswer(
      df.select(sentences($"str", $"language", $"country")),
      Row(Seq(Seq("Hi", "there"), Seq("The", "price", "was"), Seq("But", "not", "now"))))

    // Type coercion
    checkAnswer(
      df.selectExpr("sentences(null)", "sentences(10)", "sentences(3.14)"),
      Row(null, Seq(Seq("10")), Seq(Seq("3.14"))))

    checkAnswer(df.select(sentences(lit(null)), sentences(lit(10)), sentences(lit(3.14))),
      Row(null, Seq(Seq("10")), Seq(Seq("3.14"))))

    // Argument number exception
    checkError(
      exception = intercept[AnalysisException] {
        df.selectExpr("sentences()")
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> toSQLId("sentences"),
        "expectedNum" -> "[1, 2, 3]",
        "actualNum" -> "0",
        "docroot" -> SPARK_DOC_ROOT
      ),
      context = ExpectedContext(
        fragment = "sentences()",
        start = 0,
        stop = 10)
    )
  }

  test("str_to_map function") {
    val df1 = Seq(
      ("a=1,b=2", "y"),
      ("a=1,b=2,c=3", "y")
    ).toDF("a", "b")

    checkAnswer(
      df1.selectExpr("str_to_map(a,',','=')"),
      Seq(
        Row(Map("a" -> "1", "b" -> "2")),
        Row(Map("a" -> "1", "b" -> "2", "c" -> "3"))
      )
    )
    checkAnswer(
      df1.select(str_to_map(col("a"), lit(","), lit("="))),
      Seq(
        Row(Map("a" -> "1", "b" -> "2")),
        Row(Map("a" -> "1", "b" -> "2", "c" -> "3"))
      )
    )

    val df2 = Seq(("a:1,b:2,c:3", "y")).toDF("a", "b")

    checkAnswer(
      df2.selectExpr("str_to_map(a)"),
      Seq(Row(Map("a" -> "1", "b" -> "2", "c" -> "3")))
    )
    checkAnswer(
      df2.select(str_to_map(col("a"))),
      Seq(Row(Map("a" -> "1", "b" -> "2", "c" -> "3")))
    )

    checkAnswer(
      df2.selectExpr("str_to_map(a, ',')"),
      Seq(Row(Map("a" -> "1", "b" -> "2", "c" -> "3")))
    )
    checkAnswer(
      df2.select(str_to_map(col("a"), lit(","))),
      Seq(Row(Map("a" -> "1", "b" -> "2", "c" -> "3")))
    )

    val df3 = Seq(
      ("a=1&b=2", "&", "="),
      ("k#2%v#3", "%", "#")
    ).toDF("str", "delim1", "delim2")

    checkAnswer(
      df3.selectExpr("str_to_map(str, delim1, delim2)"),
      Seq(
        Row(Map("a" -> "1", "b" -> "2")),
        Row(Map("k" -> "2", "v" -> "3"))
      )
    )
    checkAnswer(
      df3.select(str_to_map(col("str"), col("delim1"), col("delim2"))),
      Seq(
        Row(Map("a" -> "1", "b" -> "2")),
        Row(Map("k" -> "2", "v" -> "3"))
      )
    )

    val df4 = Seq(
      ("a:1&b:2", "&"),
      ("k:2%v:3", "%")
    ).toDF("str", "delim1")

    checkAnswer(
      df4.selectExpr("str_to_map(str, delim1)"),
      Seq(
        Row(Map("a" -> "1", "b" -> "2")),
        Row(Map("k" -> "2", "v" -> "3"))
      )
    )
    checkAnswer(
      df4.select(str_to_map(col("str"), col("delim1"))),
      Seq(
        Row(Map("a" -> "1", "b" -> "2")),
        Row(Map("k" -> "2", "v" -> "3"))
      )
    )
  }

  test("SPARK-36148: check input data types of regexp_replace") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("select regexp_replace(collect_list(1), '1', '2')")
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      sqlState = None,
      parameters = Map(
        "sqlExpr" -> "\"regexp_replace(collect_list(1), 1, 2, 1)\"",
        "paramIndex" -> "first",
        "inputSql" -> "\"collect_list(1)\"",
        "inputType" -> "\"ARRAY<INT>\"",
        "requiredType" -> "\"STRING\""),
      context = ExpectedContext(
        fragment = "regexp_replace(collect_list(1), '1', '2')",
        start = 7,
        stop = 47))
  }

  test("SPARK-41780: INVALID_PARAMETER_VALUE.PATTERN - " +
    "invalid parameters `regexp` in regexp_replace & regexp_extract") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("select regexp_replace('', '[a\\\\d]{0, 2}', 'x')").collect()
      },
      condition = "INVALID_PARAMETER_VALUE.PATTERN",
      parameters = Map(
        "parameter" -> toSQLId("regexp"),
        "functionName" -> toSQLId("regexp_replace"),
        "value" -> "'[a\\\\d]{0, 2}'"
      )
    )
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("select regexp_extract('', '[a\\\\d]{0, 2}', 1)").collect()
      },
      condition = "INVALID_PARAMETER_VALUE.PATTERN",
      parameters = Map(
        "parameter" -> toSQLId("regexp"),
        "functionName" -> toSQLId("regexp_extract"),
        "value" -> "'[a\\\\d]{0, 2}'"
      )
    )
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("select rlike('', '[a\\\\d]{0, 2}')").collect()
      },
      condition = "INVALID_PARAMETER_VALUE.PATTERN",
      parameters = Map(
        "parameter" -> toSQLId("regexp"),
        "functionName" -> toSQLId("rlike"),
        "value" -> "'[a\\\\d]{0, 2}'"
      )
    )
  }

  test("SPARK-42384: mask with null input") {
    val df = Seq(
      ("AbCD123-@$#"),
      (null)
    ).toDF("a")

    checkAnswer(
      df.selectExpr("mask(a,'Q','q','d','o')"),
      Row("QqQQdddoooo") :: Row(null) :: Nil
    )
  }

  test("to_binary") {
    val df = Seq("abc").toDF("a")
    checkAnswer(
      df.selectExpr("to_binary(a, 'utf-8')"),
      df.select(to_binary(col("a"), lit("utf-8")))
    )
  }

  test("to_char/to_varchar") {
    Seq(
      "to_char" -> ((e: Column, fmt: Column) => to_char(e, fmt)),
      "to_varchar" -> ((e: Column, fmt: Column) => to_varchar(e, fmt))
    ).foreach { case (funcName, func) =>
      val df = Seq(78.12).toDF("a")
      checkAnswer(df.selectExpr(s"$funcName(a, '$$99.99')"), Seq(Row("$78.12")))
      checkAnswer(df.select(func(col("a"), lit("$99.99"))), Seq(Row("$78.12")))

      val df2 = Seq((Array(100.toByte), "base64")).toDF("input", "format")
      checkAnswer(df2.selectExpr(s"$funcName(input, 'hex')"), Seq(Row("64")))
      checkAnswer(df2.select(func(col("input"), lit("hex"))), Seq(Row("64")))
      checkAnswer(df2.selectExpr(s"$funcName(input, 'base64')"), Seq(Row("ZA==")))
      checkAnswer(df2.select(func(col("input"), lit("base64"))), Seq(Row("ZA==")))
      checkAnswer(df2.selectExpr(s"$funcName(input, 'utf-8')"), Seq(Row("d")))
      checkAnswer(df2.select(func(col("input"), lit("utf-8"))), Seq(Row("d")))

      checkError(
        exception = intercept[AnalysisException] {
          df2.select(func(col("input"), col("format"))).collect()
        },
        condition = "NON_FOLDABLE_ARGUMENT",
        parameters = Map(
          "funcName" -> s"`$funcName`",
          "paramName" -> "`format`",
          "paramType" -> "\"STRING\""),
        context = ExpectedContext(
          fragment = funcName,
          callSitePattern = getCurrentClassCallSitePattern))
      checkError(
        exception = intercept[AnalysisException] {
          df2.select(func(col("input"), lit("invalid_format"))).collect()
        },
        condition = "INVALID_PARAMETER_VALUE.BINARY_FORMAT",
        parameters = Map(
          "parameter" -> "`format`",
          "functionName" -> s"`$funcName`",
          "invalidFormat" -> "'invalid_format'"),
        context = ExpectedContext(
          fragment = funcName,
          callSitePattern = getCurrentClassCallSitePattern))
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"select $funcName('a', 'b', 'c')")
        },
        condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
        parameters = Map(
          "functionName" -> s"`$funcName`",
          "expectedNum" -> "2",
          "actualNum" -> "3",
          "docroot" -> SPARK_DOC_ROOT),
        context = ExpectedContext("", "", 7, 21 + funcName.length, s"$funcName('a', 'b', 'c')"))
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"select $funcName(x'537061726b2053514c', CAST(NULL AS STRING))")
        },
        condition = "INVALID_PARAMETER_VALUE.NULL",
        parameters = Map(
          "functionName" -> s"`$funcName`",
          "parameter" -> "`format`"),
        context = ExpectedContext(
          "",
          "",
          7,
          51 + funcName.length,
          s"$funcName(x'537061726b2053514c', CAST(NULL AS STRING))"))
    }
  }

  test("to_number") {
    val df = Seq("$78.12").toDF("a")
    checkAnswer(
      df.selectExpr("to_number(a, '$99.99')"),
      Seq(Row(78.12))
    )
    checkAnswer(
      df.select(to_number(col("a"), lit("$99.99"))),
      Seq(Row(78.12))
    )
  }

  test("char & chr function") {
    val df = Seq(65).toDF("a")
    checkAnswer(df.selectExpr("char(a)"), Seq(Row("A")))
    checkAnswer(df.select(char(col("a"))), Seq(Row("A")))

    checkAnswer(df.selectExpr("chr(a)"), Seq(Row("A")))
    checkAnswer(df.select(chr(col("a"))), Seq(Row("A")))
  }

  test("btrim function") {
    val df = Seq(("SSparkSQLS", "SL")).toDF("a", "b")

    checkAnswer(df.selectExpr("btrim(a)"), Seq(Row("SSparkSQLS")))
    checkAnswer(df.select(btrim(col("a"))), Seq(Row("SSparkSQLS")))

    checkAnswer(df.selectExpr("btrim(a, b)"), Seq(Row("parkSQ")))
    checkAnswer(df.select(btrim(col("a"), col("b"))), Seq(Row("parkSQ")))
  }

  test("char_length & character_length function") {
    val df = Seq("SSparkSQLS").toDF("a")
    checkAnswer(df.selectExpr("char_length(a)"), Seq(Row(10)))
    checkAnswer(df.select(char_length(col("a"))), Seq(Row(10)))

    checkAnswer(df.selectExpr("character_length(a)"), Seq(Row(10)))
    checkAnswer(df.select(character_length(col("a"))), Seq(Row(10)))
  }

  test("contains function") {
    val df = Seq(("Spark SQL", "Spark", Array[Byte](1, 2, 3, 4), Array[Byte](1, 2))).
      toDF("a", "b", "c", "d")

    checkAnswer(df.selectExpr("contains(a, b)"), Seq(Row(true)))
    checkAnswer(df.select(contains(col("a"), col("b"))), Seq(Row(true)))

    // test binary
    checkAnswer(df.selectExpr("contains(c, d)"), Seq(Row(true)))
    checkAnswer(df.select(contains(col("c"), col("d"))), Seq(Row(true)))
  }

  test("elt function") {
    val df = Seq((1, "scala", "java")).toDF("a", "b", "c")
    checkAnswer(df.selectExpr("elt(a, b, c)"), Seq(Row("scala")))
    checkAnswer(df.select(elt(col("a"), col("b"), col("c"))), Seq(Row("scala")))
  }

  test("find_in_set function") {
    val df = Seq(("ab", "abc,b,ab,c,def")).toDF("a", "b")
    checkAnswer(df.selectExpr("find_in_set(a, b)"), Seq(Row(3)))
    checkAnswer(df.select(find_in_set(col("a"), col("b"))), Seq(Row(3)))
  }

  test("like & ilike function") {
    val df = Seq(("Spark", "_park")).toDF("a", "b")

    checkAnswer(df.selectExpr("a like b"), Seq(Row(true)))
    checkAnswer(df.select(like(col("a"), col("b"))), Seq(Row(true)))

    checkAnswer(df.selectExpr("a ilike b"), Seq(Row(true)))
    checkAnswer(df.select(ilike(col("a"), col("b"))), Seq(Row(true)))

    val df1 = Seq(("%SystemDrive%/Users/John", "/%SystemDrive/%//Users%")).toDF("a", "b")

    checkAnswer(df1.selectExpr("a like b escape '/'"), Seq(Row(true)))
    checkAnswer(df1.select(like(col("a"), col("b"), lit('/'))), Seq(Row(true)))

    checkAnswer(df.selectExpr("a ilike b escape '/'"), Seq(Row(true)))
    checkAnswer(df.select(ilike(col("a"), col("b"), lit('/'))), Seq(Row(true)))

    val df2 = Seq(("""abc\""", """%\\""")).toDF("i", "p")
    checkAnswer(df2.select(like(col("i"), col("p"))), Seq(Row(true)))
    val df3 = Seq(("""\abc""", """\\abc""")).toDF("i", "p")
    checkAnswer(df3.select(like(col("i"), col("p"))), Seq(Row(true)))

    checkError(
      exception = intercept[AnalysisException] {
        df1.select(like(col("a"), col("b"), lit(618))).collect()
      },
      condition = "INVALID_ESCAPE_CHAR",
      parameters = Map("sqlExpr" -> "\"618\""),
      context = ExpectedContext("like", getCurrentClassCallSitePattern)
    )

    checkError(
      exception = intercept[AnalysisException] {
        df1.select(ilike(col("a"), col("b"), lit(618))).collect()
      },
      condition = "INVALID_ESCAPE_CHAR",
      parameters = Map("sqlExpr" -> "\"618\""),
      context = ExpectedContext("ilike", getCurrentClassCallSitePattern)
    )

    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    checkError(
      exception = intercept[AnalysisException] {
        df1.select(like(col("a"), col("b"), lit("中国"))).collect()
      },
      condition = "INVALID_ESCAPE_CHAR",
      parameters = Map("sqlExpr" -> "\"中国\""),
      context = ExpectedContext("like", getCurrentClassCallSitePattern)
    )

    checkError(
      exception = intercept[AnalysisException] {
        df1.select(ilike(col("a"), col("b"), lit("中国"))).collect()
      },
      condition = "INVALID_ESCAPE_CHAR",
      parameters = Map("sqlExpr" -> "\"中国\""),
      context = ExpectedContext("ilike", getCurrentClassCallSitePattern)
    )
    // scalastyle:on
  }

  test("lcase & ucase function") {
    val df = Seq("Spark").toDF("a")

    checkAnswer(df.selectExpr("lcase(a)"), Seq(Row("spark")))
    checkAnswer(df.select(lcase(col("a"))), Seq(Row("spark")))

    checkAnswer(df.selectExpr("ucase(a)"), Seq(Row("SPARK")))
    checkAnswer(df.select(ucase(col("a"))), Seq(Row("SPARK")))
  }

  test("left & right function") {
    val df = Seq(("Spark SQL", 3)).toDF("a", "b")

    checkAnswer(df.selectExpr("left(a, b)"), Seq(Row("Spa")))
    checkAnswer(df.select(left(col("a"), col("b"))), Seq(Row("Spa")))

    checkAnswer(df.selectExpr("right(a, b)"), Seq(Row("SQL")))
    checkAnswer(df.select(right(col("a"), col("b"))), Seq(Row("SQL")))
  }

  test("replace") {
    val df = Seq(("ABCabc", "abc", "DEF")).toDF("a", "b", "c")

    checkAnswer(
      df.selectExpr("replace(a, b, c)"),
      Seq(Row("ABCDEF"))
    )
    checkAnswer(
      df.select(replace(col("a"), col("b"), col("c"))),
      Seq(Row("ABCDEF"))
    )

    checkAnswer(
      df.selectExpr("replace(a, b)"),
      Seq(Row("ABC"))
    )
    checkAnswer(
      df.select(replace(col("a"), col("b"))),
      Seq(Row("ABC"))
    )
  }

  test("split_part") {
    val df = Seq(("11.12.13", ".", 3)).toDF("a", "b", "c")
    checkAnswer(
      df.selectExpr("split_part(a, b, c)"),
      Seq(Row("13"))
    )
    checkAnswer(
      df.select(split_part(col("a"), col("b"), col("c"))),
      Seq(Row("13"))
    )
  }

  test("substr") {
    val df = Seq(("Spark SQL", 5, 1)).toDF("a", "b", "c")
    checkAnswer(
      df.selectExpr("substr(a, b, c)"),
      Seq(Row("k"))
    )
    checkAnswer(
      df.select(substr(col("a"), col("b"), col("c"))),
      Seq(Row("k"))
    )

    checkAnswer(
      df.selectExpr("substr(a, b)"),
      Seq(Row("k SQL"))
    )
    checkAnswer(
      df.select(substr(col("a"), col("b"))),
      Seq(Row("k SQL"))
    )
  }

  test("parse_url") {
    val df = Seq(("http://spark.apache.org/path?query=1", "QUERY", "query")).toDF("a", "b", "c")

    checkAnswer(
      df.selectExpr("parse_url(a, b, c)"),
      Seq(Row("1"))
    )
    checkAnswer(
      df.select(parse_url(col("a"), col("b"), col("c"))),
      Seq(Row("1"))
    )

    checkAnswer(
      df.selectExpr("parse_url(a, b)"),
      Seq(Row("query=1"))
    )
    checkAnswer(
      df.select(parse_url(col("a"), col("b"))),
      Seq(Row("query=1"))
    )
  }

  test("printf") {
    val df = Seq(("aa%d%s", 123, "cc")).toDF("a", "b", "c")
    checkAnswer(
      df.selectExpr("printf(a, b, c)"),
      Row("aa123cc"))
    checkAnswer(
      df.select(printf(col("a"), col("b"), col("c"))),
      Row("aa123cc"))
  }

  test("url_decode") {
    val df = Seq("https%3A%2F%2Fspark.apache.org").toDF("a")
    checkAnswer(
      df.selectExpr("url_decode(a)"),
      Row("https://spark.apache.org"))
    checkAnswer(
      df.select(url_decode(col("a"))),
      Row("https://spark.apache.org"))
  }

  test("url_encode") {
    val df = Seq("https://spark.apache.org").toDF("a")
    checkAnswer(
      df.selectExpr("url_encode(a)"),
      Row("https%3A%2F%2Fspark.apache.org"))
    checkAnswer(
      df.select(url_encode(col("a"))),
      Row("https%3A%2F%2Fspark.apache.org"))
  }

  test("position") {
    val df = Seq(("bar", "foobarbar", 5)).toDF("a", "b", "c")

    checkAnswer(df.selectExpr("position(a, b)"), Row(4))
    checkAnswer(df.select(position(col("a"), col("b"))), Row(4))

    checkAnswer(df.selectExpr("position(a, b, c)"), Row(7))
    checkAnswer(df.select(position(col("a"), col("b"), col("c"))), Row(7))
  }

  test("endswith") {
    val df = Seq(("Spark SQL", "Spark", Array[Byte](1, 2, 3, 4), Array[Byte](3, 4)))
      .toDF("a", "b", "c", "d")

    checkAnswer(df.selectExpr("endswith(a, b)"), Row(false))
    checkAnswer(df.select(endswith(col("a"), col("b"))), Row(false))

    // test binary
    checkAnswer(df.selectExpr("endswith(c, d)"), Row(true))
    checkAnswer(df.select(endswith(col("c"), col("d"))), Row(true))
  }

  test("startswith") {
    val df = Seq(("Spark SQL", "Spark", Array[Byte](1, 2, 3, 4), Array[Byte](1, 2)))
      .toDF("a", "b", "c", "d")

    checkAnswer(df.selectExpr("startswith(a, b)"), Row(true))
    checkAnswer(df.select(startswith(col("a"), col("b"))), Row(true))

    // test binary
    checkAnswer(df.selectExpr("startswith(c, d)"), Row(true))
    checkAnswer(df.select(startswith(col("c"), col("d"))), Row(true))
  }

  test("try_to_binary") {
    val df = Seq("abc").toDF("a")

    checkAnswer(df.selectExpr("try_to_binary(a, 'utf-8')"),
      df.select(try_to_binary(col("a"), lit("utf-8"))))

    checkAnswer(df.selectExpr("try_to_binary(a)"),
      df.select(try_to_binary(col("a"))))
  }

  test("try_to_number") {
    val df = Seq("$78.12").toDF("a")

    checkAnswer(df.selectExpr("try_to_number(a, '$99.99')"), Seq(Row(78.12)))
    checkAnswer(df.select(try_to_number(col("a"), lit("$99.99"))), Seq(Row(78.12)))
  }

  test("SPARK-47646: try_to_number should return NULL for malformed input") {
    val df = spark.createDataset(spark.sparkContext.parallelize(Seq("11")))
    checkAnswer(df.select(try_to_number($"value", lit("$99.99"))), Seq(Row(null)))
  }

  test("SPARK-44905: stateful lastRegex causes NullPointerException on eval for regexp_replace") {
    val df = sql("select regexp_replace('', '[a\\\\d]{0, 2}', 'x')")
    intercept[SparkRuntimeException](df.queryExecution.optimizedPlan)
    checkError(
      exception = intercept[SparkRuntimeException](df.queryExecution.explainString(FormattedMode)),
      condition = "INVALID_PARAMETER_VALUE.PATTERN",
      parameters = Map(
        "parameter" -> toSQLId("regexp"),
        "functionName" -> toSQLId("regexp_replace"),
        "value" -> "'[a\\\\d]{0, 2}'"
      )
    )
  }

  test("SPARK-48806: url_decode exception") {
    val e = intercept[SparkIllegalArgumentException] {
      sql("select url_decode('https%3A%2F%2spark.apache.org')").collect()
    }
    assert(e.getCause.isInstanceOf[IllegalArgumentException] &&
      e.getCause.getMessage
        .startsWith("URLDecoder: Illegal hex characters in escape (%) pattern - "))
  }

  test("SPARK-49188: concat_ws called on array of arrays of string - invalid cast") {
    for (enableANSI <- Seq(false, true)) {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> enableANSI.toString) {
        val testTable = "invalidCastTestTable"
        withTable(testTable) {
          sql(s"create table $testTable(dat array<string>) using parquet")
          checkError(
            exception = intercept[AnalysisException] {
              sql(s"select concat_ws(',', collect_list(dat)) FROM $testTable")
            },
            condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
            parameters = Map(
              "sqlExpr" -> """"concat_ws(,, collect_list(dat))"""",
              "paramIndex" -> "second",
              "inputSql" -> """"collect_list(dat)"""",
              "inputType" -> """"ARRAY<ARRAY<STRING>>"""",
              "requiredType" -> """("ARRAY<STRING>" or "STRING")"""
            ),
            context = ExpectedContext(
              fragment = """concat_ws(',', collect_list(dat))""",
              start = 7,
              stop = 39
            )
          )
        }
      }
    }
  }
}
