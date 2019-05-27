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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._

class StringExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("concat") {
    def testConcat(inputs: String*): Unit = {
      val expected = if (inputs.contains(null)) null else inputs.mkString
      checkEvaluation(Concat(inputs.map(Literal.create(_, StringType))), expected, EmptyRow)
    }

    testConcat()
    testConcat(null)
    testConcat("")
    testConcat("ab")
    testConcat("a", "b")
    testConcat("a", "b", "C")
    testConcat("a", null, "C")
    testConcat("a", null, null)
    testConcat(null, null, null)

    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    testConcat("数据", null, "砖头")
    // scalastyle:on
  }

  test("SPARK-22498: Concat should not generate codes beyond 64KB") {
    val N = 5000
    val strs = (1 to N).map(x => s"s$x")
    checkEvaluation(Concat(strs.map(Literal.create(_, StringType))), strs.mkString, EmptyRow)
  }

  test("SPARK-22771 Check Concat.checkInputDataTypes results") {
    assert(Concat(Seq.empty[Expression]).checkInputDataTypes().isSuccess)
    assert(Concat(Literal.create("a") :: Literal.create("b") :: Nil)
      .checkInputDataTypes().isSuccess)
    assert(Concat(Literal.create("a".getBytes) :: Literal.create("b".getBytes) :: Nil)
      .checkInputDataTypes().isSuccess)
    assert(Concat(Literal.create(1) :: Literal.create(2) :: Nil)
      .checkInputDataTypes().isFailure)
    assert(Concat(Literal.create("a") :: Literal.create("b".getBytes) :: Nil)
      .checkInputDataTypes().isFailure)
  }

  test("concat_ws") {
    def testConcatWs(expected: String, sep: String, inputs: Any*): Unit = {
      val inputExprs = inputs.map {
        case s: Seq[_] => Literal.create(s, ArrayType(StringType))
        case null => Literal.create(null, StringType)
        case s: String => Literal.create(s, StringType)
      }
      val sepExpr = Literal.create(sep, StringType)
      checkEvaluation(ConcatWs(sepExpr +: inputExprs), expected, EmptyRow)
    }

    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    testConcatWs(null, null)
    testConcatWs(null, null, "a", "b")
    testConcatWs("", "")
    testConcatWs("ab", "哈哈", "ab")
    testConcatWs("a哈哈b", "哈哈", "a", "b")
    testConcatWs("a哈哈b", "哈哈", "a", null, "b")
    testConcatWs("a哈哈b哈哈c", "哈哈", null, "a", null, "b", "c")

    testConcatWs("ab", "哈哈", Seq("ab"))
    testConcatWs("a哈哈b", "哈哈", Seq("a", "b"))
    testConcatWs("a哈哈b哈哈c哈哈d", "哈哈", Seq("a", null, "b"), null, "c", Seq(null, "d"))
    testConcatWs("a哈哈b哈哈c", "哈哈", Seq("a", null, "b"), null, "c", Seq.empty[String])
    testConcatWs("a哈哈b哈哈c", "哈哈", Seq("a", null, "b"), null, "c", Seq[String](null))
    // scalastyle:on
  }

  test("SPARK-22549: ConcatWs should not generate codes beyond 64KB") {
    val N = 5000
    val sepExpr = Literal.create("#", StringType)
    val strings1 = (1 to N).map(x => s"s$x")
    val inputsExpr1 = strings1.map(Literal.create(_, StringType))
    checkEvaluation(ConcatWs(sepExpr +: inputsExpr1), strings1.mkString("#"), EmptyRow)

    val strings2 = (1 to N).map(x => Seq(s"s$x"))
    val inputsExpr2 = strings2.map(Literal.create(_, ArrayType(StringType)))
    checkEvaluation(
      ConcatWs(sepExpr +: inputsExpr2), strings2.map(s => s(0)).mkString("#"), EmptyRow)
  }

  test("elt") {
    def testElt(result: String, n: java.lang.Integer, args: String*): Unit = {
      checkEvaluation(
        Elt(Literal.create(n, IntegerType) +: args.map(Literal.create(_, StringType))),
        result)
    }

    testElt("hello", 1, "hello", "world")
    testElt(null, 1, null, "world")
    testElt(null, null, "hello", "world")

    // Invalid ranages
    testElt(null, 3, "hello", "world")
    testElt(null, 0, "hello", "world")
    testElt(null, -1, "hello", "world")

    // type checking
    assert(Elt(Seq.empty).checkInputDataTypes().isFailure)
    assert(Elt(Seq(Literal(1))).checkInputDataTypes().isFailure)
    assert(Elt(Seq(Literal(1), Literal("A"))).checkInputDataTypes().isSuccess)
    assert(Elt(Seq(Literal(1), Literal(2))).checkInputDataTypes().isFailure)
  }

  test("SPARK-22550: Elt should not generate codes beyond 64KB") {
    val N = 10000
    val strings = (1 to N).map(x => s"s$x")
    val args = Literal.create(N, IntegerType) +: strings.map(Literal.create(_, StringType))
    checkEvaluation(Elt(args), s"s$N")
  }

  test("StringComparison") {
    val row = create_row("abc", null)
    val c1 = 'a.string.at(0)
    val c2 = 'a.string.at(1)

    checkEvaluation(c1 contains "b", true, row)
    checkEvaluation(c1 contains "x", false, row)
    checkEvaluation(c2 contains "b", null, row)
    checkEvaluation(c1 contains Literal.create(null, StringType), null, row)

    checkEvaluation(c1 startsWith "a", true, row)
    checkEvaluation(c1 startsWith "b", false, row)
    checkEvaluation(c2 startsWith "a", null, row)
    checkEvaluation(c1 startsWith Literal.create(null, StringType), null, row)

    checkEvaluation(c1 endsWith "c", true, row)
    checkEvaluation(c1 endsWith "b", false, row)
    checkEvaluation(c2 endsWith "b", null, row)
    checkEvaluation(c1 endsWith Literal.create(null, StringType), null, row)
  }

  test("Substring") {
    val row = create_row("example", "example".toArray.map(_.toByte))

    val s = 'a.string.at(0)

    // substring from zero position with less-than-full length
    checkEvaluation(
      Substring(s, Literal.create(0, IntegerType), Literal.create(2, IntegerType)), "ex", row)
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(2, IntegerType)), "ex", row)

    // substring from zero position with full length
    checkEvaluation(
      Substring(s, Literal.create(0, IntegerType), Literal.create(7, IntegerType)), "example", row)
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(7, IntegerType)), "example", row)

    // substring from zero position with greater-than-full length
    checkEvaluation(Substring(s, Literal.create(0, IntegerType), Literal.create(100, IntegerType)),
      "example", row)
    checkEvaluation(Substring(s, Literal.create(1, IntegerType), Literal.create(100, IntegerType)),
      "example", row)

    // substring from nonzero position with less-than-full length
    checkEvaluation(Substring(s, Literal.create(2, IntegerType), Literal.create(2, IntegerType)),
      "xa", row)

    // substring from nonzero position with full length
    checkEvaluation(Substring(s, Literal.create(2, IntegerType), Literal.create(6, IntegerType)),
      "xample", row)

    // substring from nonzero position with greater-than-full length
    checkEvaluation(Substring(s, Literal.create(2, IntegerType), Literal.create(100, IntegerType)),
      "xample", row)

    // zero-length substring (within string bounds)
    checkEvaluation(Substring(s, Literal.create(0, IntegerType), Literal.create(0, IntegerType)),
      "", row)

    // zero-length substring (beyond string bounds)
    checkEvaluation(Substring(s, Literal.create(100, IntegerType), Literal.create(4, IntegerType)),
      "", row)

    // substring(null, _, _) -> null
    checkEvaluation(Substring(s, Literal.create(100, IntegerType), Literal.create(4, IntegerType)),
      null, create_row(null))

    // substring(_, null, _) -> null
    checkEvaluation(Substring(s, Literal.create(null, IntegerType), Literal.create(4, IntegerType)),
      null, row)

    // substring(_, _, null) -> null
    checkEvaluation(
      Substring(s, Literal.create(100, IntegerType), Literal.create(null, IntegerType)),
      null,
      row)

    // 2-arg substring from zero position
    checkEvaluation(
      Substring(s, Literal.create(0, IntegerType), Literal.create(Integer.MAX_VALUE, IntegerType)),
      "example",
      row)
    checkEvaluation(
      Substring(s, Literal.create(1, IntegerType), Literal.create(Integer.MAX_VALUE, IntegerType)),
      "example",
      row)

    // 2-arg substring from nonzero position
    checkEvaluation(
      Substring(s, Literal.create(2, IntegerType), Literal.create(Integer.MAX_VALUE, IntegerType)),
      "xample",
      row)

    val s_notNull = 'a.string.notNull.at(0)

    assert(Substring(s, Literal.create(0, IntegerType), Literal.create(2, IntegerType)).nullable)
    assert(
      Substring(s_notNull, Literal.create(0, IntegerType), Literal.create(2, IntegerType)).nullable
        === false)
    assert(Substring(s_notNull,
      Literal.create(null, IntegerType), Literal.create(2, IntegerType)).nullable)
    assert(Substring(s_notNull,
      Literal.create(0, IntegerType), Literal.create(null, IntegerType)).nullable)

    checkEvaluation(s.substr(0, 2), "ex", row)
    checkEvaluation(s.substr(0), "example", row)
    checkEvaluation(s.substring(0, 2), "ex", row)
    checkEvaluation(s.substring(0), "example", row)

    val bytes = Array[Byte](1, 2, 3, 4)
    checkEvaluation(Substring(bytes, 0, 2), Array[Byte](1, 2))
    checkEvaluation(Substring(bytes, 1, 2), Array[Byte](1, 2))
    checkEvaluation(Substring(bytes, 2, 2), Array[Byte](2, 3))
    checkEvaluation(Substring(bytes, 3, 2), Array[Byte](3, 4))
    checkEvaluation(Substring(bytes, 4, 2), Array[Byte](4))
    checkEvaluation(Substring(bytes, 8, 2), Array.empty[Byte])
    checkEvaluation(Substring(bytes, -1, 2), Array[Byte](4))
    checkEvaluation(Substring(bytes, -2, 2), Array[Byte](3, 4))
    checkEvaluation(Substring(bytes, -3, 2), Array[Byte](2, 3))
    checkEvaluation(Substring(bytes, -4, 2), Array[Byte](1, 2))
    checkEvaluation(Substring(bytes, -5, 2), Array[Byte](1))
    checkEvaluation(Substring(bytes, -8, 2), Array.empty[Byte])
  }

  test("string substring_index function") {
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(3)), "www.apache.org")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(2)), "www.apache")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(1)), "www")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(0)), "")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(-3)), "www.apache.org")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(-2)), "apache.org")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(-1)), "org")
    checkEvaluation(
      SubstringIndex(Literal(""), Literal("."), Literal(-2)), "")
    checkEvaluation(
      SubstringIndex(Literal.create(null, StringType), Literal("."), Literal(-2)), null)
    checkEvaluation(SubstringIndex(
        Literal("www.apache.org"), Literal.create(null, StringType), Literal(-2)), null)
    // non ascii chars
    // scalastyle:off
    checkEvaluation(
      SubstringIndex(Literal("大千世界大千世界"), Literal( "千"), Literal(2)), "大千世界大")
    // scalastyle:on
    checkEvaluation(
      SubstringIndex(Literal("www||apache||org"), Literal( "||"), Literal(2)), "www||apache")
  }

  test("ascii for string") {
    val a = 'a.string.at(0)
    checkEvaluation(Ascii(Literal("efg")), 101, create_row("abdef"))
    checkEvaluation(Ascii(a), 97, create_row("abdef"))
    checkEvaluation(Ascii(a), 0, create_row(""))
    checkEvaluation(Ascii(a), null, create_row(null))
    checkEvaluation(Ascii(Literal.create(null, StringType)), null, create_row("abdef"))
  }

  test("string for ascii") {
    val a = 'a.long.at(0)
    checkEvaluation(Chr(Literal(48L)), "0", create_row("abdef"))
    checkEvaluation(Chr(a), "a", create_row(97L))
    checkEvaluation(Chr(a), "a", create_row(97L + 256L))
    checkEvaluation(Chr(a), "", create_row(-9L))
    checkEvaluation(Chr(a), Character.MIN_VALUE.toString, create_row(0L))
    checkEvaluation(Chr(a), Character.MIN_VALUE.toString, create_row(256L))
    checkEvaluation(Chr(a), null, create_row(null))
    checkEvaluation(Chr(a), 149.toChar.toString, create_row(149L))
    checkEvaluation(Chr(Literal.create(null, LongType)), null, create_row("abdef"))
  }

  test("base64/unbase64 for string") {
    val a = 'a.string.at(0)
    val b = 'b.binary.at(0)
    val bytes = Array[Byte](1, 2, 3, 4)

    checkEvaluation(Base64(Literal(bytes)), "AQIDBA==", create_row("abdef"))
    checkEvaluation(Base64(UnBase64(Literal("AQIDBA=="))), "AQIDBA==", create_row("abdef"))
    checkEvaluation(Base64(UnBase64(Literal(""))), "", create_row("abdef"))
    checkEvaluation(Base64(UnBase64(Literal.create(null, StringType))), null, create_row("abdef"))
    checkEvaluation(Base64(UnBase64(a)), "AQIDBA==", create_row("AQIDBA=="))

    checkEvaluation(Base64(b), "AQIDBA==", create_row(bytes))
    checkEvaluation(Base64(b), "", create_row(Array.empty[Byte]))
    checkEvaluation(Base64(b), null, create_row(null))
    checkEvaluation(Base64(Literal.create(null, BinaryType)), null, create_row("abdef"))

    checkEvaluation(UnBase64(a), null, create_row(null))
    checkEvaluation(UnBase64(Literal.create(null, StringType)), null, create_row("abdef"))
  }

  test("encode/decode for string") {
    val a = 'a.string.at(0)
    val b = 'b.binary.at(0)
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    checkEvaluation(
      Decode(Encode(Literal("大千世界"), Literal("UTF-16LE")), Literal("UTF-16LE")), "大千世界")
    checkEvaluation(
      Decode(Encode(a, Literal("utf-8")), Literal("utf-8")), "大千世界", create_row("大千世界"))
    checkEvaluation(
      Decode(Encode(a, Literal("utf-8")), Literal("utf-8")), "", create_row(""))
    // scalastyle:on
    checkEvaluation(Encode(a, Literal("utf-8")), null, create_row(null))
    checkEvaluation(Encode(Literal.create(null, StringType), Literal("utf-8")), null)
    checkEvaluation(Encode(a, Literal.create(null, StringType)), null, create_row(""))

    checkEvaluation(Decode(b, Literal("utf-8")), null, create_row(null))
    checkEvaluation(Decode(Literal.create(null, BinaryType), Literal("utf-8")), null)
    checkEvaluation(Decode(b, Literal.create(null, StringType)), null, create_row(null))
  }

  test("initcap unit test") {
    checkEvaluation(InitCap(Literal.create(null, StringType)), null)
    checkEvaluation(InitCap(Literal("a b")), "A B")
    checkEvaluation(InitCap(Literal(" a")), " A")
    checkEvaluation(InitCap(Literal("the test")), "The Test")
    checkEvaluation(InitCap(Literal("sParK")), "Spark")
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    checkEvaluation(InitCap(Literal("世界")), "世界")
    // scalastyle:on
  }


  test("Levenshtein distance") {
    checkEvaluation(Levenshtein(Literal.create(null, StringType), Literal("")), null)
    checkEvaluation(Levenshtein(Literal(""), Literal.create(null, StringType)), null)
    checkEvaluation(Levenshtein(Literal(""), Literal("")), 0)
    checkEvaluation(Levenshtein(Literal("abc"), Literal("abc")), 0)
    checkEvaluation(Levenshtein(Literal("kitten"), Literal("sitting")), 3)
    checkEvaluation(Levenshtein(Literal("frog"), Literal("fog")), 1)
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    checkEvaluation(Levenshtein(Literal("千世"), Literal("fog")), 3)
    checkEvaluation(Levenshtein(Literal("世界千世"), Literal("大a界b")), 4)
    // scalastyle:on
  }

  test("soundex unit test") {
    checkEvaluation(SoundEx(Literal("ZIN")), "Z500")
    checkEvaluation(SoundEx(Literal("SU")), "S000")
    checkEvaluation(SoundEx(Literal("")), "")
    checkEvaluation(SoundEx(Literal.create(null, StringType)), null)

    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    checkEvaluation(SoundEx(Literal("测试")), "测试")
    checkEvaluation(SoundEx(Literal("Tschüss")), "T220")
    // scalastyle:on
    checkEvaluation(SoundEx(Literal("zZ")), "Z000", create_row("s8"))
    checkEvaluation(SoundEx(Literal("RAGSSEEESSSVEEWE")), "R221")
    checkEvaluation(SoundEx(Literal("Ashcraft")), "A261")
    checkEvaluation(SoundEx(Literal("Aswcraft")), "A261")
    checkEvaluation(SoundEx(Literal("Tymczak")), "T522")
    checkEvaluation(SoundEx(Literal("Pfister")), "P236")
    checkEvaluation(SoundEx(Literal("Miller")), "M460")
    checkEvaluation(SoundEx(Literal("Peterson")), "P362")
    checkEvaluation(SoundEx(Literal("Peters")), "P362")
    checkEvaluation(SoundEx(Literal("Auerbach")), "A612")
    checkEvaluation(SoundEx(Literal("Uhrbach")), "U612")
    checkEvaluation(SoundEx(Literal("Moskowitz")), "M232")
    checkEvaluation(SoundEx(Literal("Moskovitz")), "M213")
    checkEvaluation(SoundEx(Literal("relyheewsgeessg")), "R422")
    checkEvaluation(SoundEx(Literal("!!")), "!!")
  }

  test("replace") {
    checkEvaluation(
      StringReplace(Literal("replace"), Literal("pl"), Literal("123")), "re123ace")
    checkEvaluation(StringReplace(Literal("replace"), Literal("pl"), Literal("")), "reace")
    checkEvaluation(StringReplace(Literal("replace"), Literal(""), Literal("123")), "replace")
    checkEvaluation(StringReplace(Literal.create(null, StringType),
      Literal("pl"), Literal("123")), null)
    checkEvaluation(StringReplace(Literal("replace"),
      Literal.create(null, StringType), Literal("123")), null)
    checkEvaluation(StringReplace(Literal("replace"),
      Literal("pl"), Literal.create(null, StringType)), null)
    // test for multiple replace
    checkEvaluation(StringReplace(Literal("abcabc"), Literal("b"), Literal("12")), "a12ca12c")
    checkEvaluation(StringReplace(Literal("abcdabcd"), Literal("bc"), Literal("")), "adad")
    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkEvaluation(StringReplace(Literal("花花世界"), Literal("花世"), Literal("ab")), "花ab界")
    // scalastyle:on
  }

  test("translate") {
    checkEvaluation(
      StringTranslate(Literal("translate"), Literal("rnlt"), Literal("123")), "1a2s3ae")
    checkEvaluation(StringTranslate(Literal("translate"), Literal(""), Literal("123")), "translate")
    checkEvaluation(StringTranslate(Literal("translate"), Literal("rnlt"), Literal("")), "asae")
    // test for multiple mapping
    checkEvaluation(StringTranslate(Literal("abcd"), Literal("aba"), Literal("123")), "12cd")
    checkEvaluation(StringTranslate(Literal("abcd"), Literal("aba"), Literal("12")), "12cd")
    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkEvaluation(StringTranslate(Literal("花花世界"), Literal("花界"), Literal("ab")), "aa世b")
    // scalastyle:on
  }

  test("TRIM") {
    val s = 'a.string.at(0)
    checkEvaluation(StringTrim(Literal(" aa  ")), "aa", create_row(" abdef "))
    checkEvaluation(StringTrim("aa", "a"), "", create_row(" abdef "))
    checkEvaluation(StringTrim(Literal(" aabbtrimccc"), "ab cd"), "trim", create_row("bdef"))
    checkEvaluation(StringTrim(Literal("a<a >@>.,>"), "a.,@<>"), " ", create_row(" abdef "))
    checkEvaluation(StringTrim(s), "abdef", create_row(" abdef "))
    checkEvaluation(StringTrim(s, "abd"), "ef", create_row("abdefa"))
    checkEvaluation(StringTrim(s, "a"), "bdef", create_row("aaabdefaaaa"))
    checkEvaluation(StringTrim(s, "SLSQ"), "park", create_row("SSparkSQLS"))

    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkEvaluation(StringTrim(s), "花花世界", create_row("  花花世界 "))
    checkEvaluation(StringTrim(s, "花世界"), "", create_row("花花世界花花"))
    checkEvaluation(StringTrim(s, "花 "), "世界", create_row(" 花花世界花花"))
    checkEvaluation(StringTrim(s, "花 "), "世界", create_row(" 花 花 世界 花 花 "))
    checkEvaluation(StringTrim(s, "a花世"), "界", create_row("aa花花世界花花aa"))
    checkEvaluation(StringTrim(s, "a@#( )"), "花花世界花花", create_row("aa()花花世界花花@ #"))
    checkEvaluation(StringTrim(Literal("花trim"), "花 "), "trim", create_row(" abdef "))
    // scalastyle:on
    checkEvaluation(StringTrim(Literal("a"), Literal.create(null, StringType)), null)
    checkEvaluation(StringTrim(Literal.create(null, StringType), Literal("a")), null)
  }

  test("LTRIM") {
    val s = 'a.string.at(0)
    checkEvaluation(StringTrimLeft(Literal(" aa  ")), "aa  ", create_row(" abdef "))
    checkEvaluation(StringTrimLeft(Literal("aa"), "a"), "", create_row(" abdef "))
    checkEvaluation(StringTrimLeft(Literal("aa "), "a "), "", create_row(" abdef "))
    checkEvaluation(StringTrimLeft(Literal("aabbcaaaa"), "ab"), "caaaa", create_row(" abdef "))
    checkEvaluation(StringTrimLeft(s), "abdef ", create_row(" abdef "))
    checkEvaluation(StringTrimLeft(s, "a"), "bdefa", create_row("abdefa"))
    checkEvaluation(StringTrimLeft(s, "a "), "bdefaaaa", create_row(" aaabdefaaaa"))
    checkEvaluation(StringTrimLeft(s, "Spk"), "arkSQLS", create_row("SSparkSQLS"))

    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkEvaluation(StringTrimLeft(s), "花花世界 ", create_row("  花花世界 "))
    checkEvaluation(StringTrimLeft(s, "花"), "世界花花", create_row("花花世界花花"))
    checkEvaluation(StringTrimLeft(s, "花 世"), "界花花", create_row(" 花花世界花花"))
    checkEvaluation(StringTrimLeft(s, "花"), "a花花世界花花 ", create_row("a花花世界花花 "))
    checkEvaluation(StringTrimLeft(s, "a花界"), "世界花花aa", create_row("aa花花世界花花aa"))
    checkEvaluation(StringTrimLeft(s, "a世界"), "花花世界花花", create_row("花花世界花花"))
    // scalastyle:on
    checkEvaluation(StringTrimLeft(Literal.create(null, StringType), Literal("a")), null)
    checkEvaluation(StringTrimLeft(Literal("a"), Literal.create(null, StringType)), null)
  }

  test("RTRIM") {
    val s = 'a.string.at(0)
    checkEvaluation(StringTrimRight(Literal(" aa  ")), " aa", create_row(" abdef "))
    checkEvaluation(StringTrimRight(Literal("a"), "a"), "", create_row(" abdef "))
    checkEvaluation(StringTrimRight(Literal("ab"), "ab"), "", create_row(" abdef "))
    checkEvaluation(StringTrimRight(Literal("aabbaaaa %"), "a %"), "aabb", create_row("def"))
    checkEvaluation(StringTrimRight(s), " abdef", create_row(" abdef "))
    checkEvaluation(StringTrimRight(s, "a"), "abdef", create_row("abdefa"))
    checkEvaluation(StringTrimRight(s, "abf de"), "", create_row(" aaabdefaaaa"))
    checkEvaluation(StringTrimRight(s, "S*&"), "SSparkSQL", create_row("SSparkSQLS*"))

    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkEvaluation(StringTrimRight(Literal("a"), "花"), "a", create_row(" abdef "))
    checkEvaluation(StringTrimRight(Literal("花"), "a"), "花", create_row(" abdef "))
    checkEvaluation(StringTrimRight(Literal("花花世界"), "界花世"), "", create_row(" abdef "))
    checkEvaluation(StringTrimRight(s), "  花花世界", create_row("  花花世界 "))
    checkEvaluation(StringTrimRight(s, "花a#"), "花花世界", create_row("花花世界花花###aa花"))
    checkEvaluation(StringTrimRight(s, "花"), "", create_row("花花花花"))
    checkEvaluation(StringTrimRight(s, "花 界b@"), " 花花世", create_row(" 花花世 b界@花花 "))
    // scalastyle:on
    checkEvaluation(StringTrimRight(Literal("a"), Literal.create(null, StringType)), null)
    checkEvaluation(StringTrimRight(Literal.create(null, StringType), Literal("a")), null)
  }

  test("FORMAT") {
    checkEvaluation(FormatString(Literal("aa%d%s"), Literal(123), Literal("a")), "aa123a")
    checkEvaluation(FormatString(Literal("aa")), "aa", create_row(null))
    checkEvaluation(FormatString(Literal("aa%d%s"), Literal(123), Literal("a")), "aa123a")
    checkEvaluation(FormatString(Literal("aa%d%s"), 12, "cc"), "aa12cc")

    checkEvaluation(FormatString(Literal.create(null, StringType), 12, "cc"), null)
    checkEvaluation(
      FormatString(Literal("aa%d%s"), Literal.create(null, IntegerType), "cc"), "aanullcc")
    checkEvaluation(
      FormatString(Literal("aa%d%s"), 12, Literal.create(null, StringType)), "aa12null")
  }

  test("SPARK-22603: FormatString should not generate codes beyond 64KB") {
    val N = 4500
    val args = (1 to N).map(i => Literal.create(i.toString, StringType))
    val format = "%s" * N
    val expected = (1 to N).map(i => i.toString).mkString
    checkEvaluation(FormatString(Literal(format) +: args: _*), expected)
  }

  test("INSTR") {
    val s1 = 'a.string.at(0)
    val s2 = 'b.string.at(1)
    val s3 = 'c.string.at(2)
    val row1 = create_row("aaads", "aa", "zz")

    checkEvaluation(StringInstr(Literal("aaads"), Literal("aa")), 1, row1)
    checkEvaluation(StringInstr(Literal("aaads"), Literal("de")), 0, row1)
    checkEvaluation(StringInstr(Literal.create(null, StringType), Literal("de")), null, row1)
    checkEvaluation(StringInstr(Literal("aaads"), Literal.create(null, StringType)), null, row1)

    checkEvaluation(StringInstr(s1, s2), 1, row1)
    checkEvaluation(StringInstr(s1, s3), 0, row1)

    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkEvaluation(StringInstr(s1, s2), 3, create_row("花花世界", "世界"))
    checkEvaluation(StringInstr(s1, s2), 1, create_row("花花世界", "花"))
    checkEvaluation(StringInstr(s1, s2), 0, create_row("花花世界", "小"))
    // scalastyle:on
  }

  test("LOCATE") {
    val s1 = 'a.string.at(0)
    val s2 = 'b.string.at(1)
    val s3 = 'c.string.at(2)
    val s4 = 'd.int.at(3)
    val row1 = create_row("aaads", "aa", "zz", 2)
    val row2 = create_row(null, "aa", "zz", 1)
    val row3 = create_row("aaads", null, "zz", 1)
    val row4 = create_row(null, null, null, 1)

    checkEvaluation(new StringLocate(Literal("aa"), Literal("aaads")), 1, row1)
    checkEvaluation(StringLocate(Literal("aa"), Literal("aaads"), Literal(0)), 0, row1)
    checkEvaluation(StringLocate(Literal("aa"), Literal("aaads"), Literal(1)), 1, row1)
    checkEvaluation(StringLocate(Literal("aa"), Literal("aaads"), Literal(2)), 2, row1)
    checkEvaluation(StringLocate(Literal("aa"), Literal("aaads"), Literal(3)), 0, row1)
    checkEvaluation(new StringLocate(Literal("de"), Literal("aaads")), 0, row1)
    checkEvaluation(StringLocate(Literal("de"), Literal("aaads"), 2), 0, row1)

    checkEvaluation(new StringLocate(s2, s1), 1, row1)
    checkEvaluation(StringLocate(s2, s1, s4), 2, row1)
    checkEvaluation(new StringLocate(s3, s1), 0, row1)
    checkEvaluation(StringLocate(s3, s1, Literal.create(null, IntegerType)), 0, row1)
    checkEvaluation(new StringLocate(s2, s1), null, row2)
    checkEvaluation(new StringLocate(s2, s1), null, row3)
    checkEvaluation(new StringLocate(s2, s1, Literal.create(null, IntegerType)), 0, row4)
  }

  test("LPAD/RPAD") {
    val s1 = 'a.string.at(0)
    val s2 = 'b.int.at(1)
    val s3 = 'c.string.at(2)
    val row1 = create_row("hi", 5, "??")
    val row2 = create_row("hi", 1, "?")
    val row3 = create_row(null, 1, "?")
    val row4 = create_row("hi", null, "?")
    val row5 = create_row("hi", 1, null)

    checkEvaluation(StringLPad(Literal("hi"), Literal(5), Literal("??")), "???hi", row1)
    checkEvaluation(StringLPad(Literal("hi"), Literal(1), Literal("??")), "h", row1)
    checkEvaluation(StringLPad(s1, s2, s3), "???hi", row1)
    checkEvaluation(StringLPad(s1, s2, s3), "h", row2)
    checkEvaluation(StringLPad(s1, s2, s3), null, row3)
    checkEvaluation(StringLPad(s1, s2, s3), null, row4)
    checkEvaluation(StringLPad(s1, s2, s3), null, row5)

    checkEvaluation(StringRPad(Literal("hi"), Literal(5), Literal("??")), "hi???", row1)
    checkEvaluation(StringRPad(Literal("hi"), Literal(1), Literal("??")), "h", row1)
    checkEvaluation(StringRPad(s1, s2, s3), "hi???", row1)
    checkEvaluation(StringRPad(s1, s2, s3), "h", row2)
    checkEvaluation(StringRPad(s1, s2, s3), null, row3)
    checkEvaluation(StringRPad(s1, s2, s3), null, row4)
    checkEvaluation(StringRPad(s1, s2, s3), null, row5)
  }

  test("REPEAT") {
    val s1 = 'a.string.at(0)
    val s2 = 'b.int.at(1)
    val row1 = create_row("hi", 2)
    val row2 = create_row(null, 1)

    checkEvaluation(StringRepeat(Literal("hi"), Literal(2)), "hihi", row1)
    checkEvaluation(StringRepeat(Literal("hi"), Literal(-1)), "", row1)
    checkEvaluation(StringRepeat(s1, s2), "hihi", row1)
    checkEvaluation(StringRepeat(s1, s2), null, row2)
  }

  test("REVERSE") {
    val s = 'a.string.at(0)
    val row1 = create_row("abccc")
    checkEvaluation(Reverse(Literal("abccc")), "cccba", row1)
    checkEvaluation(Reverse(s), "cccba", row1)
    checkEvaluation(Reverse(Literal.create(null, StringType)), null, row1)
  }

  test("SPACE") {
    val s1 = 'b.int.at(0)
    val row1 = create_row(2)
    val row2 = create_row(null)

    checkEvaluation(StringSpace(Literal(2)), "  ", row1)
    checkEvaluation(StringSpace(Literal(-1)), "", row1)
    checkEvaluation(StringSpace(Literal(0)), "", row1)
    checkEvaluation(StringSpace(s1), "  ", row1)
    checkEvaluation(StringSpace(s1), null, row2)
  }

  test("length for string / binary") {
    val a = 'a.string.at(0)
    val b = 'b.binary.at(0)
    val bytes = Array[Byte](1, 2, 3, 1, 2)
    val string = "abdef"

    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkEvaluation(Length(Literal("a花花c")), 4, create_row(string))
    checkEvaluation(OctetLength(Literal("a花花c")), 8, create_row(string))
    checkEvaluation(BitLength(Literal("a花花c")), 8 * 8, create_row(string))
    // scalastyle:on
    checkEvaluation(Length(Literal(bytes)), 5, create_row(Array.empty[Byte]))
    checkEvaluation(OctetLength(Literal(bytes)), 5, create_row(Array.empty[Byte]))
    checkEvaluation(BitLength(Literal(bytes)), 5 * 8, create_row(Array.empty[Byte]))

    checkEvaluation(Length(a), 5, create_row(string))
    checkEvaluation(OctetLength(a), 5, create_row(string))
    checkEvaluation(BitLength(a), 5 * 8, create_row(string))
    checkEvaluation(Length(b), 5, create_row(bytes))
    checkEvaluation(OctetLength(b), 5, create_row(bytes))
    checkEvaluation(BitLength(b), 5 * 8, create_row(bytes))

    checkEvaluation(Length(a), 0, create_row(""))
    checkEvaluation(OctetLength(a), 0, create_row(""))
    checkEvaluation(BitLength(a), 0, create_row(""))
    checkEvaluation(Length(b), 0, create_row(Array.empty[Byte]))
    checkEvaluation(OctetLength(b), 0, create_row(Array.empty[Byte]))
    checkEvaluation(BitLength(b), 0, create_row(Array.empty[Byte]))

    checkEvaluation(Length(a), null, create_row(null))
    checkEvaluation(OctetLength(a), null, create_row(null))
    checkEvaluation(BitLength(a), null, create_row(null))
    checkEvaluation(Length(b), null, create_row(null))
    checkEvaluation(OctetLength(b), null, create_row(null))
    checkEvaluation(BitLength(b), null, create_row(null))

    checkEvaluation(Length(Literal.create(null, StringType)), null, create_row(string))
    checkEvaluation(OctetLength(Literal.create(null, StringType)), null, create_row(string))
    checkEvaluation(BitLength(Literal.create(null, StringType)), null, create_row(string))
    checkEvaluation(Length(Literal.create(null, BinaryType)), null, create_row(bytes))
    checkEvaluation(OctetLength(Literal.create(null, BinaryType)), null, create_row(bytes))
    checkEvaluation(BitLength(Literal.create(null, BinaryType)), null, create_row(bytes))
  }

  test("format_number / FormatNumber") {
    checkEvaluation(FormatNumber(Literal(4.asInstanceOf[Byte]), Literal(3)), "4.000")
    checkEvaluation(FormatNumber(Literal(4.asInstanceOf[Short]), Literal(3)), "4.000")
    checkEvaluation(FormatNumber(Literal(4.0f), Literal(3)), "4.000")
    checkEvaluation(FormatNumber(Literal(4), Literal(3)), "4.000")
    checkEvaluation(FormatNumber(Literal(12831273.23481d), Literal(3)), "12,831,273.235")
    checkEvaluation(FormatNumber(Literal(12831273.83421d), Literal(0)), "12,831,274")
    checkEvaluation(FormatNumber(Literal(123123324123L), Literal(3)), "123,123,324,123.000")
    checkEvaluation(FormatNumber(Literal(123123324123L), Literal(-1)), null)
    checkEvaluation(
      FormatNumber(
        Literal(Decimal(123123324123L) * Decimal(123123.21234d)), Literal(4)),
      "15,159,339,180,002,773.2778")
    checkEvaluation(FormatNumber(Literal.create(null, IntegerType), Literal(3)), null)
    assert(FormatNumber(Literal.create(null, NullType), Literal(3)).resolved === false)

    checkEvaluation(FormatNumber(Literal(12332.123456), Literal("##############.###")), "12332.123")
    checkEvaluation(FormatNumber(Literal(12332.123456), Literal("##.###")), "12332.123")
    checkEvaluation(FormatNumber(Literal(4.asInstanceOf[Byte]), Literal("##.####")), "4")
    checkEvaluation(FormatNumber(Literal(4.asInstanceOf[Short]), Literal("##.####")), "4")
    checkEvaluation(FormatNumber(Literal(4.0f), Literal("##.###")), "4")
    checkEvaluation(FormatNumber(Literal(4), Literal("##.###")), "4")
    checkEvaluation(FormatNumber(Literal(12831273.23481d),
      Literal("###,###,###,###,###.###")), "12,831,273.235")
    checkEvaluation(FormatNumber(Literal(12831273.83421d), Literal("")), "12,831,274")
    checkEvaluation(FormatNumber(Literal(123123324123L), Literal("###,###,###,###,###.###")),
      "123,123,324,123")
    checkEvaluation(
      FormatNumber(Literal(Decimal(123123324123L) * Decimal(123123.21234d)),
        Literal("###,###,###,###,###.####")), "15,159,339,180,002,773.2778")
    checkEvaluation(FormatNumber(Literal.create(null, IntegerType), Literal("##.###")), null)
    assert(FormatNumber(Literal.create(null, NullType), Literal("##.###")).resolved === false)

    checkEvaluation(FormatNumber(Literal(12332.123456), Literal("#,###,###,###,###,###,##0")),
      "12,332")
    checkEvaluation(FormatNumber(
      Literal.create(null, IntegerType), Literal.create(null, StringType)), null)
    checkEvaluation(FormatNumber(
      Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null)
  }

  test("find in set") {
    checkEvaluation(
      FindInSet(Literal.create(null, StringType), Literal.create(null, StringType)), null)
    checkEvaluation(FindInSet(Literal("ab"), Literal.create(null, StringType)), null)
    checkEvaluation(FindInSet(Literal.create(null, StringType), Literal("abc,b,ab,c,def")), null)
    checkEvaluation(FindInSet(Literal("ab"), Literal("abc,b,ab,c,def")), 3)
    checkEvaluation(FindInSet(Literal("abf"), Literal("abc,b,ab,c,def")), 0)
    checkEvaluation(FindInSet(Literal("ab,"), Literal("abc,b,ab,c,def")), 0)
  }

  test("ParseUrl") {
    def checkParseUrl(expected: String, urlStr: String, partToExtract: String): Unit = {
      checkEvaluation(ParseUrl(Seq(urlStr, partToExtract)), expected)
    }
    def checkParseUrlWithKey(
        expected: String,
        urlStr: String,
        partToExtract: String,
        key: String): Unit = {
      checkEvaluation(ParseUrl(Seq(urlStr, partToExtract, key)), expected)
    }

    checkParseUrl("spark.apache.org", "http://spark.apache.org/path?query=1", "HOST")
    checkParseUrl("/path", "http://spark.apache.org/path?query=1", "PATH")
    checkParseUrl("query=1", "http://spark.apache.org/path?query=1", "QUERY")
    checkParseUrl("Ref", "http://spark.apache.org/path?query=1#Ref", "REF")
    checkParseUrl("http", "http://spark.apache.org/path?query=1", "PROTOCOL")
    checkParseUrl("/path?query=1", "http://spark.apache.org/path?query=1", "FILE")
    checkParseUrl("spark.apache.org:8080", "http://spark.apache.org:8080/path?query=1", "AUTHORITY")
    checkParseUrl("userinfo", "http://userinfo@spark.apache.org/path?query=1", "USERINFO")
    checkParseUrlWithKey("1", "http://spark.apache.org/path?query=1", "QUERY", "query")

    // Null checking
    checkParseUrl(null, null, "HOST")
    checkParseUrl(null, "http://spark.apache.org/path?query=1", null)
    checkParseUrl(null, null, null)
    checkParseUrl(null, "test", "HOST")
    checkParseUrl(null, "http://spark.apache.org/path?query=1", "NO")
    checkParseUrl(null, "http://spark.apache.org/path?query=1", "USERINFO")
    checkParseUrlWithKey(null, "http://spark.apache.org/path?query=1", "HOST", "query")
    checkParseUrlWithKey(null, "http://spark.apache.org/path?query=1", "QUERY", "quer")
    checkParseUrlWithKey(null, "http://spark.apache.org/path?query=1", "QUERY", null)
    checkParseUrlWithKey(null, "http://spark.apache.org/path?query=1", "QUERY", "")

    // exceptional cases
    intercept[java.util.regex.PatternSyntaxException] {
      evaluateWithoutCodegen(ParseUrl(Seq(Literal("http://spark.apache.org/path?"),
        Literal("QUERY"), Literal("???"))))
    }

    // arguments checking
    assert(ParseUrl(Seq(Literal("1"))).checkInputDataTypes().isFailure)
    assert(ParseUrl(Seq(Literal("1"), Literal("2"), Literal("3"), Literal("4")))
      .checkInputDataTypes().isFailure)
    assert(ParseUrl(Seq(Literal("1"), Literal(2))).checkInputDataTypes().isFailure)
    assert(ParseUrl(Seq(Literal(1), Literal("2"))).checkInputDataTypes().isFailure)
    assert(ParseUrl(Seq(Literal("1"), Literal("2"), Literal(3))).checkInputDataTypes().isFailure)
  }

  test("Sentences") {
    val nullString = Literal.create(null, StringType)
    checkEvaluation(Sentences(nullString, nullString, nullString), null)
    checkEvaluation(Sentences(nullString, nullString), null)
    checkEvaluation(Sentences(nullString), null)
    checkEvaluation(Sentences("", nullString, nullString), Seq.empty)
    checkEvaluation(Sentences("", nullString), Seq.empty)
    checkEvaluation(Sentences(""), Seq.empty)

    val answer = Seq(
      Seq("Hi", "there"),
      Seq("The", "price", "was"),
      Seq("But", "not", "now"))

    checkEvaluation(Sentences("Hi there! The price was $1,234.56.... But, not now."), answer)
    checkEvaluation(Sentences("Hi there! The price was $1,234.56.... But, not now.", "en"), answer)
    checkEvaluation(Sentences("Hi there! The price was $1,234.56.... But, not now.", "en", "US"),
      answer)
    checkEvaluation(Sentences("Hi there! The price was $1,234.56.... But, not now.", "XXX", "YYY"),
      answer)
  }
}
