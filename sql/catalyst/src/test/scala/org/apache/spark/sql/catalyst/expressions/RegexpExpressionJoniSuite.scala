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

  test("RLIKE literal Regular Expression") {
    checkLiteralRow(Literal.create(null, StringType) jrlike _, "abdef", null)
    checkEvaluation("abdef" jrlike Literal.create(null, StringType), null)
    checkEvaluation(Literal.create(null, StringType) jrlike Literal.create(null, StringType), null)
    checkEvaluation("abdef" jrlike NonFoldableLiteral.create("abdef", StringType), true)
    checkEvaluation("abdef" jrlike NonFoldableLiteral.create(null, StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) jrlike NonFoldableLiteral.create("abdef", StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) jrlike NonFoldableLiteral.create(null, StringType), null)

    checkLiteralRow("abdef" jrlike _, "abdef", true)
    checkLiteralRow("abbbbc" jrlike _, "a.*c", true)

    checkLiteralRow("fofo" jrlike _, "^fo", true)
    checkLiteralRow("fo\no" jrlike _, "^fo\no$", true)
    checkLiteralRow("Bn" jrlike _, "^Ba*n", true)
    checkLiteralRow("afofo" jrlike _, "fo", true)
    checkLiteralRow("afofo" jrlike _, "^fo", false)
    checkLiteralRow("Baan" jrlike _, "^Ba?n", false)
    checkLiteralRow("axe" jrlike _, "pi|apa", false)
    checkLiteralRow("pip" jrlike _, "^(pi)*$", false)

    checkLiteralRow("abc" jrlike _, "^ab", true)
    checkLiteralRow("abc" jrlike _, "^bc", false)
    checkLiteralRow("abc" jrlike _, "^ab", true)
    checkLiteralRow("abc" jrlike _, "^bc", false)

    intercept[org.joni.exception.SyntaxException] {
      evaluateWithoutCodegen("abbbbc" jrlike "**")
    }
    intercept[org.joni.exception.SyntaxException] {
      val regex = 'a.string.at(0)
      evaluateWithoutCodegen("abbbbc" jrlike regex, create_row("**"))
    }

    intercept[org.joni.exception.SyntaxException] {
      evaluateWithoutCodegen("abbbbc" jrlike "**")
    }
  }

  test("RLIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abdef" jrlike regEx, true, create_row("abdef"))
    checkEvaluation("abbbbc" jrlike regEx, true, create_row("a.*c"))
    checkEvaluation("fofo" jrlike regEx, true, create_row("^fo"))
    checkEvaluation("fo\no" jrlike regEx, true, create_row("^fo\no$"))
    checkEvaluation("Bn" jrlike regEx, true, create_row("^Ba*n"))

    intercept[org.joni.exception.SyntaxException] {
      evaluateWithoutCodegen("abbbbc" jrlike regEx, create_row("**"))
    }
  }

  test("RLIKE Regular Expression") {
    checkLiteralRow(Literal.create(null, StringType) jrlike _, "abdef", null)
    checkEvaluation("abdef" jrlike Literal.create(null, StringType), null)
    checkEvaluation(Literal.create(null, StringType) jrlike Literal.create(null, StringType), null)
    checkEvaluation("abdef" jrlike NonFoldableLiteral.create("abdef", StringType), true)
    checkEvaluation("abdef" jrlike NonFoldableLiteral.create(null, StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) jrlike NonFoldableLiteral.create("abdef", StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) jrlike NonFoldableLiteral.create(null, StringType), null)

    checkLiteralRow("abdef" jrlike _, "abdef", true)
    checkLiteralRow("abbbbc" jrlike _, "a.*c", true)

    checkLiteralRow("fofo" jrlike _, "^fo", true)
    checkLiteralRow("fo\no" jrlike _, "^fo\no$", true)
    checkLiteralRow("Bn" jrlike _, "^Ba*n", true)
    checkLiteralRow("afofo" jrlike _, "fo", true)
    checkLiteralRow("afofo" jrlike _, "^fo", false)
    checkLiteralRow("Baan" jrlike _, "^Ba?n", false)
    checkLiteralRow("axe" jrlike _, "pi|apa", false)
    checkLiteralRow("pip" jrlike _, "^(pi)*$", false)

    checkLiteralRow("abc" jrlike _, "^ab", true)
    checkLiteralRow("abc" jrlike _, "^bc", false)
    checkLiteralRow("abc" jrlike _, "^ab", true)
    checkLiteralRow("abc" jrlike _, "^bc", false)

    intercept[org.joni.exception.SyntaxException] {
      evaluateWithoutCodegen("abbbbc" jrlike "**")
    }
    intercept[org.joni.exception.SyntaxException] {
      val regex = 'a.string.at(0)
      evaluateWithoutCodegen("abbbbc" jrlike regex, create_row("**"))
    }
  }

  test("LIKE Pattern") {

    // null handling
    checkLiteralRow("a" jlike _, "", false)
    checkLiteralRow(Literal.create(null, StringType).jlike(_), "a", null)
    checkEvaluation(Literal.create("a", StringType).jlike(Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create(null, StringType).jlike(Literal.create(null, StringType)), null)
    checkEvaluation(
      Literal.create("a", StringType).jlike(NonFoldableLiteral.create("a", StringType)), true)
    checkEvaluation(
      Literal.create("a", StringType).jlike(NonFoldableLiteral.create(null, StringType)), null)
    checkEvaluation(
      Literal.create(null, StringType).jlike(NonFoldableLiteral.create("a", StringType)), null)
    checkEvaluation(
      Literal.create(null, StringType).jlike(NonFoldableLiteral.create(null, StringType)), null)

    // simple patterns
    checkLiteralRow("abdef" jlike _, "abdef", true)
    checkLiteralRow("a_%b" jlike _, "a\\__b", true)
    checkLiteralRow("addb" jlike _, "a_%b", true)
    checkLiteralRow("addb" jlike _, "a\\__b", false)
    checkLiteralRow("addb" jlike _, "a%\\%b", false)
    checkLiteralRow("a_%b" jlike _, "a%\\%b", true)
    checkLiteralRow("addb" jlike _, "a%", true)
    checkLiteralRow("addb" jlike _, "**", false)
    checkLiteralRow("abc" jlike _, "a%", true)
    checkLiteralRow("abc" jlike _, "b%", false)
    checkLiteralRow("abc" jlike _, "bc%", false)
    checkLiteralRow("a\nb" jlike _, "a_b", true)
    checkLiteralRow("ab" jlike _, "a%b", true)
    checkLiteralRow("a\nb" jlike _, "a%b", true)
    checkLiteralRow("" jlike _, "", true)

    // empty input
    checkLiteralRow("" jlike _, "", true)
    checkLiteralRow("a" jlike _, "", false)
    checkLiteralRow("" jlike _, "a", false)

    // SI-17647 double-escaping backslash
    checkLiteralRow("""\\\\""" jlike _, """%\\%""", true)
    checkLiteralRow("""%%""" jlike _, """%%""", true)
    checkLiteralRow("""\__""" jlike _, """\\\__""", true)
    checkLiteralRow("""\\\__""" jlike _, """%\\%\%""", false)
    checkLiteralRow("""_\\\%""" jlike _, """%\\""", false)

    // unicode
    // scalastyle:off nonascii
    checkLiteralRow("a\u20ACa" jlike _, "_\u20AC_", true)
    checkLiteralRow("a€a" jlike _, "_€_", true)
    checkLiteralRow("a€a" jlike _, "_\u20AC_", true)
    checkLiteralRow("a\u20ACa" jlike _, "_€_", true)
    // scalastyle:on nonascii

    // invalid escaping
    val invalidEscape = intercept[AnalysisException] {
      evaluateWithoutCodegen("""a""" jlike """\a""")
    }
    assert(invalidEscape.getMessage.contains("pattern"))

    val endEscape = intercept[AnalysisException] {
      evaluateWithoutCodegen("""a""" jlike """a\""")
    }
    assert(endEscape.getMessage.contains("pattern"))

    // case
    checkLiteralRow("A" jlike _, "a%", false)
    checkLiteralRow("a" jlike _, "A%", false)
    checkLiteralRow("AaA" jlike _, "_a_", true)

    // example
    checkLiteralRow("""%SystemDrive%\Users\John""" jlike _, """\%SystemDrive\%\\Users%""", true)
  }

  Seq('/', '#', '\"').foreach { escapeChar =>
    test(s"LIKE Pattern ESCAPE '$escapeChar'") {
      // null handling
      checkLiteralRow(Literal.create(null, StringType).jlike(_, escapeChar), "a", null)
      checkEvaluation(
        Literal.create("a", StringType).jlike(Literal.create(null, StringType), escapeChar), null)
      checkEvaluation(
        Literal.create(null, StringType).jlike(Literal.create(null, StringType), escapeChar), null)
      checkEvaluation(Literal.create("a", StringType).jlike(
        NonFoldableLiteral.create("a", StringType), escapeChar), true)
      checkEvaluation(Literal.create("a", StringType).jlike(
        NonFoldableLiteral.create(null, StringType), escapeChar), null)
      checkEvaluation(Literal.create(null, StringType).jlike(
        NonFoldableLiteral.create("a", StringType), escapeChar), null)
      checkEvaluation(Literal.create(null, StringType).jlike(
        NonFoldableLiteral.create(null, StringType), escapeChar), null)

      // simple patterns
      checkLiteralRow("abdef" jlike(_, escapeChar), "abdef", true)
      checkLiteralRow("a_%b" jlike(_, escapeChar), s"a${escapeChar}__b", true)
      checkLiteralRow("addb" jlike(_, escapeChar), "a_%b", true)
      checkLiteralRow("addb" jlike(_, escapeChar), s"a${escapeChar}__b", false)
      checkLiteralRow("addb" jlike(_, escapeChar), s"a%$escapeChar%b", false)
      checkLiteralRow("a_%b" jlike(_, escapeChar), s"a%$escapeChar%b", true)
      checkLiteralRow("addb" jlike(_, escapeChar), "a%", true)
      checkLiteralRow("addb" jlike(_, escapeChar), "**", false)
      checkLiteralRow("abc" jlike(_, escapeChar), "a%", true)
      checkLiteralRow("abc" jlike(_, escapeChar), "b%", false)
      checkLiteralRow("abc" jlike(_, escapeChar), "bc%", false)
      checkLiteralRow("a\nb" jlike(_, escapeChar), "a_b", true)
      checkLiteralRow("ab" jlike(_, escapeChar), "a%b", true)
      checkLiteralRow("a\nb" jlike(_, escapeChar), "a%b", true)

      // empty input
      checkLiteralRow("" jlike(_, escapeChar), "", true)
      checkLiteralRow("a" jlike(_, escapeChar), "", false)
      checkLiteralRow("" jlike(_, escapeChar), "a", false)

      // SI-17647 double-escaping backslash
      checkLiteralRow(s"""$escapeChar$escapeChar$escapeChar$escapeChar""" jlike(_, escapeChar),
        s"""%$escapeChar$escapeChar%""", true)
      checkLiteralRow("""%%""" jlike(_, escapeChar), """%%""", true)
      checkLiteralRow(s"""${escapeChar}__""" jlike(_, escapeChar),
        s"""$escapeChar$escapeChar${escapeChar}__""", true)
      checkLiteralRow(s"""$escapeChar$escapeChar${escapeChar}__""" jlike(_, escapeChar),
        s"""%$escapeChar$escapeChar%$escapeChar%""", false)
      checkLiteralRow(s"""_$escapeChar$escapeChar$escapeChar%""" jlike(_, escapeChar),
        s"""%$escapeChar${escapeChar}""", false)

      // unicode
      // scalastyle:off nonascii
      checkLiteralRow("a\u20ACa" jlike(_, escapeChar), "_\u20AC_", true)
      checkLiteralRow("a€a" jlike(_, escapeChar), "_€_", true)
      checkLiteralRow("a€a" jlike(_, escapeChar), "_\u20AC_", true)
      checkLiteralRow("a\u20ACa" jlike(_, escapeChar), "_€_", true)
      // scalastyle:on nonascii

      // invalid escaping
      val invalidEscape = intercept[AnalysisException] {
        evaluateWithoutCodegen("""a""" jlike(s"""${escapeChar}a""", escapeChar))
      }
      assert(invalidEscape.getMessage.contains("pattern"))
      val endEscape = intercept[AnalysisException] {
        evaluateWithoutCodegen("""a""" jlike(s"""a$escapeChar""", escapeChar))
      }
      assert(endEscape.getMessage.contains("pattern"))

      // case
      checkLiteralRow("A" jlike(_, escapeChar), "a%", false)
      checkLiteralRow("a" jlike(_, escapeChar), "A%", false)
      checkLiteralRow("AaA" jlike(_, escapeChar), "_a_", true)

      // example
      checkLiteralRow(s"""%SystemDrive%${escapeChar}Users${escapeChar}John""" jlike(_, escapeChar),
        s"""$escapeChar%SystemDrive$escapeChar%$escapeChar${escapeChar}Users%""", true)
    }
  }


  test("failed") {

    checkLiteralRow("a" jlike _, "", false)
    checkLiteralRow("""aa""" jlike _, """""", false)
    checkLiteralRow("""bbaa""" jlike _, """""", false)
    checkLiteralRow("""bbaa""" jlike _, """aa""", false)


    // checkLiteralRow("a" jlike _, "", false)

    checkLiteralRow("""bbaa""" jlike _, """bba""", false)
    checkLiteralRow("""_\\\%aaa""" jlike _, """%\\""", false)

    // unicode
    // scalastyle:off nonascii
    checkLiteralRow("a\u20ACa" jlike _, "_\u20AC_", true)
    checkLiteralRow("a€a" jlike _, "_€_", true)
    checkLiteralRow("a€a" jlike _, "_\u20AC_", true)
    checkLiteralRow("a\u20ACa" jlike _, "_€_", true)
  }

}
