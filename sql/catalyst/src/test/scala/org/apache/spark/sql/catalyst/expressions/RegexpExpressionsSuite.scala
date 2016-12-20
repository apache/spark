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
import org.apache.spark.sql.types.StringType

/**
 * Unit tests for regular expression (regexp) related SQL expressions.
 */
class RegexpExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("LIKE literal Regular Expression") {
    checkEvaluation(Literal.create(null, StringType).like("a"), null)
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

    checkEvaluation("abdef" like "abdef", true)
    checkEvaluation("a_%b" like "a\\__b", true)
    checkEvaluation("addb" like "a_%b", true)
    checkEvaluation("addb" like "a\\__b", false)
    checkEvaluation("addb" like "a%\\%b", false)
    checkEvaluation("a_%b" like "a%\\%b", true)
    checkEvaluation("addb" like "a%", true)
    checkEvaluation("addb" like "**", false)
    checkEvaluation("abc" like "a%", true)
    checkEvaluation("abc"  like "b%", false)
    checkEvaluation("abc"  like "bc%", false)
    checkEvaluation("a\nb" like "a_b", true)
    checkEvaluation("ab" like "a%b", true)
    checkEvaluation("a\nb" like "a%b", true)
  }

  test("LIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abcd" like regEx, null, create_row(null))
    checkEvaluation("abdef" like regEx, true, create_row("abdef"))
    checkEvaluation("a_%b" like regEx, true, create_row("a\\__b"))
    checkEvaluation("addb" like regEx, true, create_row("a_%b"))
    checkEvaluation("addb" like regEx, false, create_row("a\\__b"))
    checkEvaluation("addb" like regEx, false, create_row("a%\\%b"))
    checkEvaluation("a_%b" like regEx, true, create_row("a%\\%b"))
    checkEvaluation("addb" like regEx, true, create_row("a%"))
    checkEvaluation("addb" like regEx, false, create_row("**"))
    checkEvaluation("abc" like regEx, true, create_row("a%"))
    checkEvaluation("abc" like regEx, false, create_row("b%"))
    checkEvaluation("abc" like regEx, false, create_row("bc%"))
    checkEvaluation("a\nb" like regEx, true, create_row("a_b"))
    checkEvaluation("ab" like regEx, true, create_row("a%b"))
    checkEvaluation("a\nb" like regEx, true, create_row("a%b"))

    checkEvaluation(Literal.create(null, StringType) like regEx, null, create_row("bc%"))
  }

  test("RLIKE literal Regular Expression") {
    checkEvaluation(Literal.create(null, StringType) rlike "abdef", null)
    checkEvaluation("abdef" rlike Literal.create(null, StringType), null)
    checkEvaluation(Literal.create(null, StringType) rlike Literal.create(null, StringType), null)
    checkEvaluation("abdef" rlike NonFoldableLiteral.create("abdef", StringType), true)
    checkEvaluation("abdef" rlike NonFoldableLiteral.create(null, StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) rlike NonFoldableLiteral.create("abdef", StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) rlike NonFoldableLiteral.create(null, StringType), null)

    checkEvaluation("abdef" rlike "abdef", true)
    checkEvaluation("abbbbc" rlike "a.*c", true)

    checkEvaluation("fofo" rlike "^fo", true)
    checkEvaluation("fo\no" rlike "^fo\no$", true)
    checkEvaluation("Bn" rlike "^Ba*n", true)
    checkEvaluation("afofo" rlike "fo", true)
    checkEvaluation("afofo" rlike "^fo", false)
    checkEvaluation("Baan" rlike "^Ba?n", false)
    checkEvaluation("axe" rlike "pi|apa", false)
    checkEvaluation("pip" rlike "^(pi)*$", false)

    checkEvaluation("abc"  rlike "^ab", true)
    checkEvaluation("abc"  rlike "^bc", false)
    checkEvaluation("abc"  rlike "^ab", true)
    checkEvaluation("abc"  rlike "^bc", false)

    intercept[java.util.regex.PatternSyntaxException] {
      evaluate("abbbbc" rlike "**")
    }
  }

  test("RLIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abdef" rlike regEx, true, create_row("abdef"))
    checkEvaluation("abbbbc" rlike regEx, true, create_row("a.*c"))
    checkEvaluation("fofo" rlike regEx, true, create_row("^fo"))
    checkEvaluation("fo\no" rlike regEx, true, create_row("^fo\no$"))
    checkEvaluation("Bn" rlike regEx, true, create_row("^Ba*n"))

    intercept[java.util.regex.PatternSyntaxException] {
      evaluate("abbbbc" rlike regEx, create_row("**"))
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
