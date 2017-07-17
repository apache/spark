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
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Unit tests for regular expression (regexp) related SQL expressions.
 */
class RegexpExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  /**
   * Check if a given expression evaluates to an expected output, in case the input is
   * a literal and in case the input is in the form of a row.
   * @tparam A type of input
   * @param mkExpr the expression to test for a given input
   * @param input value that will be used to create the expression, as literal and in the form
   *        of a row
   * @param expected the expected output of the expression
   * @param inputToExpression an implicit conversion from the input type to its corresponding
   *        sql expression
   */
  def checkLiteralRow[A](mkExpr: Expression => Expression, input: A, expected: Any)
    (implicit inputToExpression: A => Expression): Unit = {
    checkEvaluation(mkExpr(input), expected) // check literal input

    val regex = 'a.string.at(0)
    checkEvaluation(mkExpr(regex), expected, create_row(input)) // check row input
  }

  test("LIKE Pattern") {

    // null handling
    checkLiteralRow(Literal.create(null, StringType).like(_), "a", null)
    checkEvaluation(Literal.create("a", StringType).like(Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create(null, StringType).like(Literal.create(null, StringType)), null)
    checkEvaluation(
      Literal.create("a", StringType).like(NonFoldableLiteral.create("a", StringType)), true)
    checkEvaluation(
      Literal.create("a", StringType).like(NonFoldableLiteral.create(null, StringType)), null)
    checkEvaluation(
      Literal.create(null, StringType).like(NonFoldableLiteral.create("a", StringType)), null)
    checkEvaluation(
      Literal.create(null, StringType).like(NonFoldableLiteral.create(null, StringType)), null)

    // simple patterns
    checkLiteralRow("abdef" like _, "abdef", true)
    checkLiteralRow("a_%b" like _, "a\\__b", true)
    checkLiteralRow("addb" like _, "a_%b", true)
    checkLiteralRow("addb" like _, "a\\__b", false)
    checkLiteralRow("addb" like _, "a%\\%b", false)
    checkLiteralRow("a_%b" like _, "a%\\%b", true)
    checkLiteralRow("addb" like _, "a%", true)
    checkLiteralRow("addb" like _, "**", false)
    checkLiteralRow("abc" like _, "a%", true)
    checkLiteralRow("abc"  like _, "b%", false)
    checkLiteralRow("abc"  like _, "bc%", false)
    checkLiteralRow("a\nb" like _, "a_b", true)
    checkLiteralRow("ab" like _, "a%b", true)
    checkLiteralRow("a\nb" like _, "a%b", true)

    // empty input
    checkLiteralRow("" like _, "", true)
    checkLiteralRow("a" like _, "", false)
    checkLiteralRow("" like _, "a", false)

    // SI-17647 double-escaping backslash
    checkLiteralRow("""\\\\""" like _, """%\\%""", true)
    checkLiteralRow("""%%""" like _, """%%""", true)
    checkLiteralRow("""\__""" like _, """\\\__""", true)
    checkLiteralRow("""\\\__""" like _, """%\\%\%""", false)
    checkLiteralRow("""_\\\%""" like _, """%\\""", false)

    // unicode
    // scalastyle:off nonascii
    checkLiteralRow("a\u20ACa" like _, "_\u20AC_", true)
    checkLiteralRow("a€a" like _, "_€_", true)
    checkLiteralRow("a€a" like _, "_\u20AC_", true)
    checkLiteralRow("a\u20ACa" like _, "_€_", true)
    // scalastyle:on nonascii

    // invalid escaping
    val invalidEscape = intercept[AnalysisException] {
      evaluate("""a""" like """\a""")
    }
    assert(invalidEscape.getMessage.contains("pattern"))

    val endEscape = intercept[AnalysisException] {
      evaluate("""a""" like """a\""")
    }
    assert(endEscape.getMessage.contains("pattern"))

    // case
    checkLiteralRow("A" like _, "a%", false)
    checkLiteralRow("a" like _, "A%", false)
    checkLiteralRow("AaA" like _, "_a_", true)

    // example
    checkLiteralRow("""%SystemDrive%\Users\John""" like _, """\%SystemDrive\%\\Users%""", true)
  }

  test("RLIKE Regular Expression") {
    checkLiteralRow(Literal.create(null, StringType) rlike _, "abdef", null)
    checkEvaluation("abdef" rlike Literal.create(null, StringType), null)
    checkEvaluation(Literal.create(null, StringType) rlike Literal.create(null, StringType), null)
    checkEvaluation("abdef" rlike NonFoldableLiteral.create("abdef", StringType), true)
    checkEvaluation("abdef" rlike NonFoldableLiteral.create(null, StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) rlike NonFoldableLiteral.create("abdef", StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) rlike NonFoldableLiteral.create(null, StringType), null)

    checkLiteralRow("abdef" rlike _, "abdef", true)
    checkLiteralRow("abbbbc" rlike _, "a.*c", true)

    checkLiteralRow("fofo" rlike _, "^fo", true)
    checkLiteralRow("fo\no" rlike _, "^fo\no$", true)
    checkLiteralRow("Bn" rlike _, "^Ba*n", true)
    checkLiteralRow("afofo" rlike _, "fo", true)
    checkLiteralRow("afofo" rlike _, "^fo", false)
    checkLiteralRow("Baan" rlike _, "^Ba?n", false)
    checkLiteralRow("axe" rlike _, "pi|apa", false)
    checkLiteralRow("pip" rlike _, "^(pi)*$", false)

    checkLiteralRow("abc"  rlike _, "^ab", true)
    checkLiteralRow("abc"  rlike _, "^bc", false)
    checkLiteralRow("abc"  rlike _, "^ab", true)
    checkLiteralRow("abc"  rlike _, "^bc", false)

    intercept[java.util.regex.PatternSyntaxException] {
      evaluate("abbbbc" rlike "**")
    }
    intercept[java.util.regex.PatternSyntaxException] {
      val regex = 'a.string.at(0)
      evaluate("abbbbc" rlike regex, create_row("**"))
    }
  }

  test("RegexReplace") {
    val row1 = create_row("100-200", "(\\d+)", "num")
    val row2 = create_row("100-200", "(\\d+)", "###")
    val row3 = create_row("100-200", "(-)", "###")
    val row4 = create_row(null, "(\\d+)", "###")
    val row5 = create_row("100-200", null, "###")
    val row6 = create_row("100-200", "(-)", null)

    val s = 's.string.at(0)
    val p = 'p.string.at(1)
    val r = 'r.string.at(2)

    val expr = RegExpReplace(s, p, r)
    checkEvaluation(expr, "num-num", row1)
    checkEvaluation(expr, "###-###", row2)
    checkEvaluation(expr, "100###200", row3)
    checkEvaluation(expr, null, row4)
    checkEvaluation(expr, null, row5)
    checkEvaluation(expr, null, row6)

    val nonNullExpr = RegExpReplace(Literal("100-200"), Literal("(\\d+)"), Literal("num"))
    checkEvaluation(nonNullExpr, "num-num", row1)
  }

  test("RegexExtract") {
    val row1 = create_row("100-200", "(\\d+)-(\\d+)", 1)
    val row2 = create_row("100-200", "(\\d+)-(\\d+)", 2)
    val row3 = create_row("100-200", "(\\d+).*", 1)
    val row4 = create_row("100-200", "([a-z])", 1)
    val row5 = create_row(null, "([a-z])", 1)
    val row6 = create_row("100-200", null, 1)
    val row7 = create_row("100-200", "([a-z])", null)

    val s = 's.string.at(0)
    val p = 'p.string.at(1)
    val r = 'r.int.at(2)

    val expr = RegExpExtract(s, p, r)
    checkEvaluation(expr, "100", row1)
    checkEvaluation(expr, "200", row2)
    checkEvaluation(expr, "100", row3)
    checkEvaluation(expr, "", row4) // will not match anything, empty string get
    checkEvaluation(expr, null, row5)
    checkEvaluation(expr, null, row6)
    checkEvaluation(expr, null, row7)

    val expr1 = new RegExpExtract(s, p)
    checkEvaluation(expr1, "100", row1)

    val nonNullExpr = RegExpExtract(Literal("100-200"), Literal("(\\d+)-(\\d+)"), Literal(1))
    checkEvaluation(nonNullExpr, "100", row1)
  }

  test("SPLIT") {
    val s1 = 'a.string.at(0)
    val s2 = 'b.string.at(1)
    val row1 = create_row("aa2bb3cc", "[1-9]+")
    val row2 = create_row(null, "[1-9]+")
    val row3 = create_row("aa2bb3cc", null)

    checkEvaluation(
      StringSplit(Literal("aa2bb3cc"), Literal("[1-9]+")), Seq("aa", "bb", "cc"), row1)
    checkEvaluation(
      StringSplit(s1, s2), Seq("aa", "bb", "cc"), row1)
    checkEvaluation(StringSplit(s1, s2), null, row2)
    checkEvaluation(StringSplit(s1, s2), null, row3)
  }

}
