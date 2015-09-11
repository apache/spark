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


class RegexpExpressionJavaFallbackSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("LIKE literal Regular Expression") {
    checkEvaluation(Literal.create(null, StringType).jlike("a"), null)
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

    checkEvaluation("abdef" jlike "abdef", true)
    checkEvaluation("a_%b" jlike "a\\__b", true)
    checkEvaluation("addb" jlike "a_%b", true)
    checkEvaluation("addb" jlike "a\\__b", false)
    checkEvaluation("addb" jlike "a%\\%b", false)
    checkEvaluation("a_%b" jlike "a%\\%b", true)
    checkEvaluation("addb" jlike "a%", true)
    checkEvaluation("addb" jlike "**", false)
    checkEvaluation("abc" jlike "a%", true)
    checkEvaluation("abc"  jlike "b%", false)
    checkEvaluation("abc"  jlike "bc%", false)
    checkEvaluation("a\nb" jlike "a_b", true)
    checkEvaluation("ab" jlike "a%b", true)
    checkEvaluation("a\nb" jlike "a%b", true)
  }

  test("LIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abcd" jlike regEx, null, create_row(null))
    checkEvaluation("abdef" jlike regEx, true, create_row("abdef"))
    checkEvaluation("a_%b" jlike regEx, true, create_row("a\\__b"))
    checkEvaluation("addb" jlike regEx, true, create_row("a_%b"))
    checkEvaluation("addb" jlike regEx, false, create_row("a\\__b"))
    checkEvaluation("addb" jlike regEx, false, create_row("a%\\%b"))
    checkEvaluation("a_%b" jlike regEx, true, create_row("a%\\%b"))
    checkEvaluation("addb" jlike regEx, true, create_row("a%"))
    checkEvaluation("addb" jlike regEx, false, create_row("**"))
    checkEvaluation("abc" jlike regEx, true, create_row("a%"))
    checkEvaluation("abc" jlike regEx, false, create_row("b%"))
    checkEvaluation("abc" jlike regEx, false, create_row("bc%"))
    checkEvaluation("a\nb" jlike regEx, true, create_row("a_b"))
    checkEvaluation("ab" jlike regEx, true, create_row("a%b"))
    checkEvaluation("a\nb" jlike regEx, true, create_row("a%b"))

    checkEvaluation(Literal.create(null, StringType) jlike regEx, null, create_row("bc%"))
  }

  test("RLIKE literal Regular Expression") {
    checkEvaluation(Literal.create(null, StringType) jrlike "abdef", null)
    checkEvaluation("abdef" jrlike Literal.create(null, StringType), null)
    checkEvaluation(Literal.create(null, StringType) jrlike Literal.create(null, StringType), null)
    checkEvaluation("abdef" jrlike NonFoldableLiteral.create("abdef", StringType), true)
    checkEvaluation("abdef" jrlike NonFoldableLiteral.create(null, StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) jrlike NonFoldableLiteral.create("abdef", StringType), null)
    checkEvaluation(
      Literal.create(null, StringType) jrlike NonFoldableLiteral.create(null, StringType), null)

    checkEvaluation("abdef" jrlike "abdef", true)
    checkEvaluation("abbbbc" jrlike "a.*c", true)

    checkEvaluation("fofo" jrlike "^fo", true)
    checkEvaluation("fo\no" jrlike "^fo\no$", true)
    checkEvaluation("Bn" jrlike "^Ba*n", true)
    checkEvaluation("afofo" jrlike "fo", true)
    checkEvaluation("afofo" jrlike "^fo", false)
    checkEvaluation("Baan" jrlike "^Ba?n", false)
    checkEvaluation("axe" jrlike "pi|apa", false)
    checkEvaluation("pip" jrlike "^(pi)*$", false)

    checkEvaluation("abc"  jrlike "^ab", true)
    checkEvaluation("abc"  jrlike "^bc", false)
    checkEvaluation("abc"  jrlike "^ab", true)
    checkEvaluation("abc"  jrlike "^bc", false)

    intercept[java.util.regex.PatternSyntaxException] {
      evaluate("abbbbc" jrlike "**")
    }
  }

  test("RLIKE Non-literal Regular Expression") {
    val regEx = 'a.string.at(0)
    checkEvaluation("abdef" jrlike regEx, true, create_row("abdef"))
    checkEvaluation("abbbbc" jrlike regEx, true, create_row("a.*c"))
    checkEvaluation("fofo" jrlike regEx, true, create_row("^fo"))
    checkEvaluation("fo\no" jrlike regEx, true, create_row("^fo\no$"))
    checkEvaluation("Bn" jrlike regEx, true, create_row("^Ba*n"))

    intercept[java.util.regex.PatternSyntaxException] {
      evaluate("abbbbc" jrlike regEx, create_row("**"))
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

    val expr = RegExpReplaceJavaFallback(s, p, r)
    checkEvaluation(expr, "num-num", row1)
    checkEvaluation(expr, "###-###", row2)
    checkEvaluation(expr, "100###200", row3)
    checkEvaluation(expr, null, row4)
    checkEvaluation(expr, null, row5)
    checkEvaluation(expr, null, row6)
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

    val expr = RegExpExtractJavaFallback(s, p, r)
    checkEvaluation(expr, "100", row1)
    checkEvaluation(expr, "200", row2)
    checkEvaluation(expr, "100", row3)
    checkEvaluation(expr, "", row4) // will not match anything, empty string get
    checkEvaluation(expr, null, row5)
    checkEvaluation(expr, null, row6)
    checkEvaluation(expr, null, row7)

    val expr1 = new RegExpExtract(s, p)
    checkEvaluation(expr1, "100", row1)
  }
}
