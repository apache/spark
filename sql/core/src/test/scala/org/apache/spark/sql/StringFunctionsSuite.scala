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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.Decimal


class StringFunctionsSuite extends QueryTest {

  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext
  import ctx.implicits._

  test("string concat") {
    val df = Seq[(String, String, String)](("a", "b", null)).toDF("a", "b", "c")

    checkAnswer(
      df.select(concat($"a", $"b", $"c")),
      Row("ab"))

    checkAnswer(
      df.selectExpr("concat(a, b, c)"),
      Row("ab"))
  }


  test("string Levenshtein distance") {
    val df = Seq(("kitten", "sitting"), ("frog", "fog")).toDF("l", "r")
    checkAnswer(df.select(levenshtein("l", "r")), Seq(Row(3), Row(1)))
    checkAnswer(df.selectExpr("levenshtein(l, r)"), Seq(Row(3), Row(1)))
  }

  test("string ascii function") {
    val df = Seq(("abc", "")).toDF("a", "b")
    checkAnswer(
      df.select(ascii($"a"), ascii("b")),
      Row(97, 0))

    checkAnswer(
      df.selectExpr("ascii(a)", "ascii(b)"),
      Row(97, 0))
  }

  test("string base64/unbase64 function") {
    val bytes = Array[Byte](1, 2, 3, 4)
    val df = Seq((bytes, "AQIDBA==")).toDF("a", "b")
    checkAnswer(
      df.select(base64("a"), base64($"a"), unbase64("b"), unbase64($"b")),
      Row("AQIDBA==", "AQIDBA==", bytes, bytes))

    checkAnswer(
      df.selectExpr("base64(a)", "unbase64(b)"),
      Row("AQIDBA==", bytes))
  }

  test("string encode/decode function") {
    val bytes = Array[Byte](-27, -92, -89, -27, -115, -125, -28, -72, -106, -25, -107, -116)
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    val df = Seq(("大千世界", "utf-8", bytes)).toDF("a", "b", "c")
    checkAnswer(
      df.select(
        encode($"a", "utf-8"),
        encode("a", "utf-8"),
        decode($"c", "utf-8"),
        decode("c", "utf-8")),
      Row(bytes, bytes, "大千世界", "大千世界"))

    checkAnswer(
      df.selectExpr("encode(a, 'utf-8')", "decode(c, 'utf-8')"),
      Row(bytes, "大千世界"))
    // scalastyle:on
  }

  test("string trim functions") {
    val df = Seq(("  example  ", "")).toDF("a", "b")

    checkAnswer(
      df.select(ltrim($"a"), rtrim($"a"), trim($"a")),
      Row("example  ", "  example", "example"))

    checkAnswer(
      df.selectExpr("ltrim(a)", "rtrim(a)", "trim(a)"),
      Row("example  ", "  example", "example"))
  }

  test("string formatString function") {
    val df = Seq(("aa%d%s", 123, "cc")).toDF("a", "b", "c")

    checkAnswer(
      df.select(formatString($"a", $"b", $"c"), formatString("aa%d%s", "b", "c")),
      Row("aa123cc", "aa123cc"))

    checkAnswer(
      df.selectExpr("printf(a, b, c)"),
      Row("aa123cc"))
  }

  test("string instr function") {
    val df = Seq(("aaads", "aa", "zz")).toDF("a", "b", "c")

    checkAnswer(
      df.select(instr($"a", $"b"), instr("a", "b")),
      Row(1, 1))

    checkAnswer(
      df.selectExpr("instr(a, b)"),
      Row(1))
  }

  test("string locate function") {
    val df = Seq(("aaads", "aa", "zz", 1)).toDF("a", "b", "c", "d")

    checkAnswer(
      df.select(
        locate($"b", $"a"), locate("b", "a"), locate($"b", $"a", 1),
        locate("b", "a", 1), locate($"b", $"a", $"d"), locate("b", "a", "d")),
      Row(1, 1, 2, 2, 2, 2))

    checkAnswer(
      df.selectExpr("locate(b, a)", "locate(b, a, d)"),
      Row(1, 2))
  }

  test("string padding functions") {
    val df = Seq(("hi", 5, "??")).toDF("a", "b", "c")

    checkAnswer(
      df.select(
        lpad($"a", $"b", $"c"), rpad("a", "b", "c"),
        lpad($"a", 1, $"c"), rpad("a", 1, "c")),
      Row("???hi", "hi???", "h", "h"))

    checkAnswer(
      df.selectExpr("lpad(a, b, c)", "rpad(a, b, c)", "lpad(a, 1, c)", "rpad(a, 1, c)"),
      Row("???hi", "hi???", "h", "h"))
  }

  test("string repeat function") {
    val df = Seq(("hi", 2)).toDF("a", "b")

    checkAnswer(
      df.select(
        repeat($"a", 2), repeat("a", 2), repeat($"a", $"b"), repeat("a", "b")),
      Row("hihi", "hihi", "hihi", "hihi"))

    checkAnswer(
      df.selectExpr("repeat(a, 2)", "repeat(a, b)"),
      Row("hihi", "hihi"))
  }

  test("string reverse function") {
    val df = Seq(("hi", "hhhi")).toDF("a", "b")

    checkAnswer(
      df.select(reverse($"a"), reverse("b")),
      Row("ih", "ihhh"))

    checkAnswer(
      df.selectExpr("reverse(b)"),
      Row("ihhh"))
  }

  test("string space function") {
    val df = Seq((2, 3)).toDF("a", "b")

    checkAnswer(
      df.select(space($"a"), space("b")),
      Row("  ", "   "))

    checkAnswer(
      df.selectExpr("space(b)"),
      Row("   "))
  }

  test("string split function") {
    val df = Seq(("aa2bb3cc", "[1-9]+")).toDF("a", "b")

    checkAnswer(
      df.select(
        split($"a", "[1-9]+"),
        split("a", "[1-9]+")),
      Row(Seq("aa", "bb", "cc"), Seq("aa", "bb", "cc")))

    checkAnswer(
      df.selectExpr("split(a, '[1-9]+')"),
      Row(Seq("aa", "bb", "cc")))
  }

  test("string / binary length function") {
    val df = Seq(("123", Array[Byte](1, 2, 3, 4), 123)).toDF("a", "b", "c")
    checkAnswer(
      df.select(length($"a"), length("a"), length($"b"), length("b")),
      Row(3, 3, 4, 4))

    checkAnswer(
      df.selectExpr("length(a)", "length(b)"),
      Row(3, 4))

    intercept[AnalysisException] {
      checkAnswer(
        df.selectExpr("length(c)"), // int type of the argument is unacceptable
        Row("5.0000"))
    }
  }

  test("number format function") {
    val tuple =
      ("aa", 1.asInstanceOf[Byte], 2.asInstanceOf[Short],
        3.13223f, 4, 5L, 6.48173d, Decimal(7.128381))
    val df =
      Seq(tuple)
        .toDF(
          "a", // string "aa"
          "b", // byte    1
          "c", // short   2
          "d", // float   3.13223f
          "e", // integer 4
          "f", // long    5L
          "g", // double  6.48173d
          "h") // decimal 7.128381

    checkAnswer(
      df.select(
        format_number($"f", 4),
        format_number("f", 4)),
      Row("5.0000", "5.0000"))

    checkAnswer(
      df.selectExpr("format_number(b, e)"), // convert the 1st argument to integer
      Row("1.0000"))

    checkAnswer(
      df.selectExpr("format_number(c, e)"), // convert the 1st argument to integer
      Row("2.0000"))

    checkAnswer(
      df.selectExpr("format_number(d, e)"), // convert the 1st argument to double
      Row("3.1322"))

    checkAnswer(
      df.selectExpr("format_number(e, e)"), // not convert anything
      Row("4.0000"))

    checkAnswer(
      df.selectExpr("format_number(f, e)"), // not convert anything
      Row("5.0000"))

    checkAnswer(
      df.selectExpr("format_number(g, e)"), // not convert anything
      Row("6.4817"))

    checkAnswer(
      df.selectExpr("format_number(h, e)"), // not convert anything
      Row("7.1284"))

    intercept[AnalysisException] {
      checkAnswer(
        df.selectExpr("format_number(a, e)"), // string type of the 1st argument is unacceptable
        Row("5.0000"))
    }

    intercept[AnalysisException] {
      checkAnswer(
        df.selectExpr("format_number(e, g)"), // decimal type of the 2nd argument is unacceptable
        Row("5.0000"))
    }
  }
}
