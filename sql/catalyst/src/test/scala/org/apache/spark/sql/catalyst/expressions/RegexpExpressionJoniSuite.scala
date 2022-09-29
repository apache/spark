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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._


class RegexpExpressionJoniSuite extends SparkFunSuite with ExpressionEvalHelper {
  def checkLiteralRow[A](mkExpr: Expression => Expression, input: A, expected: Any)
                        (implicit inputToExpression: A => Expression): Unit = {
    checkEvaluation(mkExpr(input), expected) // check literal input

    val regex = 'a.string.at(0)
    checkEvaluation(mkExpr(regex), expected, create_row(input)) // check row input
  }

  test("LIKE ALL") {
    checkEvaluation(Literal.create(null, StringType).jLikeAll("%foo%", "%oo"), null)
    checkEvaluation(Literal.create("foo", StringType).jLikeAll("%foo%", "%oo"), true)
    checkEvaluation(Literal.create("foo", StringType).jLikeAll("%foo%", "%bar%"), false)
    checkEvaluation(Literal.create("foo", StringType)
      .jLikeAll("%foo%", Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create("foo", StringType)
      .jLikeAll(Literal.create(null, StringType), "%foo%"), null)
    checkEvaluation(Literal.create("foo", StringType)
      .jLikeAll("%feo%", Literal.create(null, StringType)), false)
    checkEvaluation(Literal.create("foo", StringType)
      .jLikeAll(Literal.create(null, StringType), "%feo%"), false)
    checkEvaluation(Literal.create("foo", StringType).jNotLikeAll("tee", "%yoo%"), true)
    checkEvaluation(Literal.create("foo", StringType).jNotLikeAll("%oo%", "%yoo%"), false)
    checkEvaluation(Literal.create("foo", StringType)
      .jNotLikeAll("%foo%", Literal.create(null, StringType)), false)
    checkEvaluation(Literal.create("foo", StringType)
      .jNotLikeAll(Literal.create(null, StringType), "%foo%"), false)
    checkEvaluation(Literal.create("foo", StringType)
      .jNotLikeAll("%yoo%", Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create("foo", StringType)
      .jNotLikeAll(Literal.create(null, StringType), "%yoo%"), null)
  }

  test("LIKE ANY") {
    checkEvaluation(Literal.create(null, StringType).jLikeAny("%foo%", "%oo"), null)
    checkEvaluation(Literal.create("foo", StringType).jLikeAny("%foo%", "%oo"), true)
    checkEvaluation(Literal.create("foo", StringType).jLikeAny("%foo%", "%bar%"), true)
    checkEvaluation(Literal.create("foo", StringType).jLikeAny("%fee%", "%bar%"), false)
    checkEvaluation(Literal.create("foo", StringType)
      .jLikeAny("%foo%", Literal.create(null, StringType)), true)
    checkEvaluation(Literal.create("foo", StringType)
      .jLikeAny(Literal.create(null, StringType), "%foo%"), true)
    checkEvaluation(Literal.create("foo", StringType)
      .jLikeAny("%feo%", Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create("foo", StringType)
      .jLikeAny(Literal.create(null, StringType), "%feo%"), null)
    checkEvaluation(Literal.create("foo", StringType).jNotLikeAny("tee", "%yoo%"), true)
    checkEvaluation(Literal.create("foo", StringType).jNotLikeAny("%oo%", "%yoo%"), true)
    checkEvaluation(Literal.create("foo", StringType).jNotLikeAny("%foo%", "%oo"), false)
    checkEvaluation(Literal.create("foo", StringType)
      .jNotLikeAny("%foo%", Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create("foo", StringType)
      .jNotLikeAny(Literal.create(null, StringType), "%foo%"), null)
    checkEvaluation(Literal.create("foo", StringType)
      .jNotLikeAny("%yoo%", Literal.create(null, StringType)), true)
    checkEvaluation(Literal.create("foo", StringType)
      .jNotLikeAny(Literal.create(null, StringType), "%yoo%"), true)
  }

  test("RLIKE literal Regular Expression") {
    checkLiteralRow(Literal.create(null, StringType) jRlike _, "abdef", null)
    checkEvaluation("abdef" jRlike Literal.create(null, StringType), null)
    checkEvaluation(Literal.create(null, StringType) jRlike Literal.create(null, StringType), null)
    checkEvaluation("abdef" jRlike NonFoldableLiteral.create("abdef", StringType), true)
    checkEvaluation("abdef" jRlike NonFoldableLiteral.create(null, StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) jRlike NonFoldableLiteral.create("abdef", StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) jRlike NonFoldableLiteral.create(null, StringType), null)

    checkLiteralRow("abdef" jRlike _, "abdef", true)
    checkLiteralRow("abbbbc" jRlike _, "a.*c", true)

    checkLiteralRow("fofo" jRlike _, "^fo", true)
    checkLiteralRow("fo\no" jRlike _, "^fo\no$", true)
    checkLiteralRow("Bn" jRlike _, "^Ba*n", true)
    checkLiteralRow("afofo" jRlike _, "fo", true)
    checkLiteralRow("afofo" jRlike _, "^fo", false)
    checkLiteralRow("Baan" jRlike _, "^Ba?n", false)
    checkLiteralRow("axe" jRlike _, "pi|apa", false)
    checkLiteralRow("pip" jRlike _, "^(pi)*$", false)

    checkLiteralRow("abc" jRlike _, "^ab", true)
    checkLiteralRow("abc" jRlike _, "^bc", false)
    checkLiteralRow("abc" jRlike _, "^ab", true)
    checkLiteralRow("abc" jRlike _, "^bc", false)

    intercept[org.joni.exception.SyntaxException] {
      evaluateWithoutCodegen("abbbbc" jRlike "**")
    }
    intercept[org.joni.exception.SyntaxException] {
      val regex = 'a.string.at(0)
      evaluateWithoutCodegen("abbbbc" jRlike regex, create_row("**"))
    }

    intercept[org.joni.exception.SyntaxException] {
      evaluateWithoutCodegen("abbbbc" jRlike "**")
    }
  }

  test("RLIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abdef" jRlike regEx, true, create_row("abdef"))
    checkEvaluation("abbbbc" jRlike regEx, true, create_row("a.*c"))
    checkEvaluation("fofo" jRlike regEx, true, create_row("^fo"))
    checkEvaluation("fo\no" jRlike regEx, true, create_row("^fo\no$"))
    checkEvaluation("Bn" jRlike regEx, true, create_row("^Ba*n"))

    intercept[org.joni.exception.SyntaxException] {
      evaluateWithoutCodegen("abbbbc" jRlike regEx, create_row("**"))
    }
  }

  test("RLIKE Regular Expression") {
    checkLiteralRow(Literal.create(null, StringType) jRlike _, "abdef", null)
    checkEvaluation("abdef" jRlike Literal.create(null, StringType), null)
    checkEvaluation(Literal.create(null, StringType) jRlike Literal.create(null, StringType), null)
    checkEvaluation("abdef" jRlike NonFoldableLiteral.create("abdef", StringType), true)
    checkEvaluation("abdef" jRlike NonFoldableLiteral.create(null, StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) jRlike NonFoldableLiteral.create("abdef", StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) jRlike NonFoldableLiteral.create(null, StringType), null)

    checkLiteralRow("abdef" jRlike _, "abdef", true)
    checkLiteralRow("abbbbc" jRlike _, "a.*c", true)

    checkLiteralRow("fofo" jRlike _, "^fo", true)
    checkLiteralRow("fo\no" jRlike _, "^fo\no$", true)
    checkLiteralRow("Bn" jRlike _, "^Ba*n", true)
    checkLiteralRow("afofo" jRlike _, "fo", true)
    checkLiteralRow("afofo" jRlike _, "^fo", false)
    checkLiteralRow("Baan" jRlike _, "^Ba?n", false)
    checkLiteralRow("axe" jRlike _, "pi|apa", false)
    checkLiteralRow("pip" jRlike _, "^(pi)*$", false)

    checkLiteralRow("abc" jRlike _, "^ab", true)
    checkLiteralRow("abc" jRlike _, "^bc", false)
    checkLiteralRow("abc" jRlike _, "^ab", true)
    checkLiteralRow("abc" jRlike _, "^bc", false)

    intercept[org.joni.exception.SyntaxException] {
      evaluateWithoutCodegen("abbbbc" jRlike "**")
    }
    intercept[org.joni.exception.SyntaxException] {
      val regex = 'a.string.at(0)
      evaluateWithoutCodegen("abbbbc" jRlike regex, create_row("**"))
    }
  }

  test("LIKE Pattern") {

    // null handling
    checkLiteralRow("a" jLike _, "", false)
    checkLiteralRow(Literal.create(null, StringType).jLike(_), "a", null)
    checkEvaluation(Literal.create("a", StringType).jLike(Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create(null, StringType).jLike(Literal.create(null, StringType)), null)
    checkEvaluation(
      Literal.create("a", StringType).jLike(NonFoldableLiteral.create("a", StringType)), true)
    checkEvaluation(
      Literal.create("a", StringType).jLike(NonFoldableLiteral.create(null, StringType)), null)
    checkEvaluation(
      Literal.create(null, StringType).jLike(NonFoldableLiteral.create("a", StringType)), null)
    checkEvaluation(
      Literal.create(null, StringType).jLike(NonFoldableLiteral.create(null, StringType)), null)

    // simple patterns
    checkLiteralRow("abdef" jLike _, "abdef", true)
    checkLiteralRow("a_%b" jLike _, "a\\__b", true)
    checkLiteralRow("addb" jLike _, "a_%b", true)
    checkLiteralRow("addb" jLike _, "a\\__b", false)
    checkLiteralRow("addb" jLike _, "a%\\%b", false)
    checkLiteralRow("a_%b" jLike _, "a%\\%b", true)
    checkLiteralRow("addb" jLike _, "a%", true)
    checkLiteralRow("addb" jLike _, "**", false)
    checkLiteralRow("abc" jLike _, "a%", true)
    checkLiteralRow("abc" jLike _, "b%", false)
    checkLiteralRow("abc" jLike _, "bc%", false)
    checkLiteralRow("a\nb" jLike _, "a_b", true)
    checkLiteralRow("ab" jLike _, "a%b", true)
    checkLiteralRow("a\nb" jLike _, "a%b", true)
    checkLiteralRow("" jLike _, "", true)

    // empty input
    checkLiteralRow("" jLike _, "", true)
    checkLiteralRow("a" jLike _, "", false)
    checkLiteralRow("" jLike _, "a", false)

    // SI-17647 double-escaping backslash
    checkLiteralRow("""\\\\""" jLike _, """%\\%""", true)
    checkLiteralRow("""%%""" jLike _, """%%""", true)
    checkLiteralRow("""\__""" jLike _, """\\\__""", true)
    checkLiteralRow("""\\\__""" jLike _, """%\\%\%""", false)
    checkLiteralRow("""_\\\%""" jLike _, """%\\""", false)

    // unicode
    // scalastyle:off nonascii
    checkLiteralRow("a\u20ACa" jLike _, "_\u20AC_", true)
    checkLiteralRow("a€a" jLike _, "_€_", true)
    checkLiteralRow("a€a" jLike _, "_\u20AC_", true)
    checkLiteralRow("a\u20ACa" jLike _, "_€_", true)
    // scalastyle:on nonascii

    // invalid escaping
    val invalidEscape = intercept[AnalysisException] {
      evaluateWithoutCodegen("""a""" jLike """\a""")
    }
    assert(invalidEscape.getMessage.contains("pattern"))

    val endEscape = intercept[AnalysisException] {
      evaluateWithoutCodegen("""a""" jLike """a\""")
    }
    assert(endEscape.getMessage.contains("pattern"))

    // case
    checkLiteralRow("A" jLike _, "a%", false)
    checkLiteralRow("a" jLike _, "A%", false)
    checkLiteralRow("AaA" jLike _, "_a_", true)

    // example
    checkLiteralRow("""%SystemDrive%\Users\John""" jLike _, """\%SystemDrive\%\\Users%""", true)
  }

  Seq('/', '#', '\"').foreach { escapeChar =>
    test(s"LIKE Pattern ESCAPE '$escapeChar'") {
      // null handling
      checkLiteralRow(Literal.create(null, StringType).jLike(_, escapeChar), "a", null)
      checkEvaluation(
        Literal.create("a", StringType).jLike(Literal.create(null, StringType), escapeChar), null)
      checkEvaluation(
        Literal.create(null, StringType).jLike(Literal.create(null, StringType), escapeChar), null)
      checkEvaluation(Literal.create("a", StringType).jLike(
        NonFoldableLiteral.create("a", StringType), escapeChar), true)
      checkEvaluation(Literal.create("a", StringType).jLike(
        NonFoldableLiteral.create(null, StringType), escapeChar), null)
      checkEvaluation(Literal.create(null, StringType).jLike(
        NonFoldableLiteral.create("a", StringType), escapeChar), null)
      checkEvaluation(Literal.create(null, StringType).jLike(
        NonFoldableLiteral.create(null, StringType), escapeChar), null)

      // simple patterns
      checkLiteralRow("abdef" jLike(_, escapeChar), "abdef", true)
      checkLiteralRow("a_%b" jLike(_, escapeChar), s"a${escapeChar}__b", true)
      checkLiteralRow("addb" jLike(_, escapeChar), "a_%b", true)
      checkLiteralRow("addb" jLike(_, escapeChar), s"a${escapeChar}__b", false)
      checkLiteralRow("addb" jLike(_, escapeChar), s"a%$escapeChar%b", false)
      checkLiteralRow("a_%b" jLike(_, escapeChar), s"a%$escapeChar%b", true)
      checkLiteralRow("addb" jLike(_, escapeChar), "a%", true)
      checkLiteralRow("addb" jLike(_, escapeChar), "**", false)
      checkLiteralRow("abc" jLike(_, escapeChar), "a%", true)
      checkLiteralRow("abc" jLike(_, escapeChar), "b%", false)
      checkLiteralRow("abc" jLike(_, escapeChar), "bc%", false)
      checkLiteralRow("a\nb" jLike(_, escapeChar), "a_b", true)
      checkLiteralRow("ab" jLike(_, escapeChar), "a%b", true)
      checkLiteralRow("a\nb" jLike(_, escapeChar), "a%b", true)

      // empty input
      checkLiteralRow("" jLike(_, escapeChar), "", true)
      checkLiteralRow("a" jLike(_, escapeChar), "", false)
      checkLiteralRow("" jLike(_, escapeChar), "a", false)

      // SI-17647 double-escaping backslash
      checkLiteralRow(s"""$escapeChar$escapeChar$escapeChar$escapeChar""" jLike(_, escapeChar),
        s"""%$escapeChar$escapeChar%""", true)
      checkLiteralRow("""%%""" jLike(_, escapeChar), """%%""", true)
      checkLiteralRow(s"""${escapeChar}__""" jLike(_, escapeChar),
        s"""$escapeChar$escapeChar${escapeChar}__""", true)
      checkLiteralRow(s"""$escapeChar$escapeChar${escapeChar}__""" jLike(_, escapeChar),
        s"""%$escapeChar$escapeChar%$escapeChar%""", false)
      checkLiteralRow(s"""_$escapeChar$escapeChar$escapeChar%""" jLike(_, escapeChar),
        s"""%$escapeChar${escapeChar}""", false)

      // unicode
      // scalastyle:off nonascii
      checkLiteralRow("a\u20ACa" jLike(_, escapeChar), "_\u20AC_", true)
      checkLiteralRow("a€a" jLike(_, escapeChar), "_€_", true)
      checkLiteralRow("a€a" jLike(_, escapeChar), "_\u20AC_", true)
      checkLiteralRow("a\u20ACa" jLike(_, escapeChar), "_€_", true)
      // scalastyle:on nonascii

      // invalid escaping
      val invalidEscape = intercept[AnalysisException] {
        evaluateWithoutCodegen("""a""" jLike(s"""${escapeChar}a""", escapeChar))
      }
      assert(invalidEscape.getMessage.contains("pattern"))
      val endEscape = intercept[AnalysisException] {
        evaluateWithoutCodegen("""a""" jLike(s"""a$escapeChar""", escapeChar))
      }
      assert(endEscape.getMessage.contains("pattern"))

      // case
      checkLiteralRow("A" jLike(_, escapeChar), "a%", false)
      checkLiteralRow("a" jLike(_, escapeChar), "A%", false)
      checkLiteralRow("AaA" jLike(_, escapeChar), "_a_", true)

      // example
      checkLiteralRow(s"""%SystemDrive%${escapeChar}Users${escapeChar}John""" jLike(_, escapeChar),
        s"""$escapeChar%SystemDrive$escapeChar%$escapeChar${escapeChar}Users%""", true)
    }
  }


  test("failed") {

    checkLiteralRow("a" jLike _, "", false)
    checkLiteralRow("""aa""" jLike _, """""", false)
    checkLiteralRow("""bbaa""" jLike _, """""", false)
    checkLiteralRow("""bbaa""" jLike _, """aa""", false)


    // checkLiteralRow("a" jlike _, "", false)

    checkLiteralRow("""bbaa""" jLike _, """bba""", false)
    checkLiteralRow("""_\\\%aaa""" jLike _, """%\\""", false)

    // unicode
    // scalastyle:off nonascii
    checkLiteralRow("a\u20ACa" jLike _, "_\u20AC_", true)
    checkLiteralRow("a€a" jLike _, "_€_", true)
    checkLiteralRow("a€a" jLike _, "_\u20AC_", true)
    checkLiteralRow("a\u20ACa" jLike _, "_€_", true)
  }

}
