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
import org.apache.spark.sql.types.{IntegerType, StringType}


class StringFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

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

    assert(Substring(s, Literal.create(0, IntegerType), Literal.create(2, IntegerType)).nullable
      === true)
    assert(
      Substring(s_notNull, Literal.create(0, IntegerType), Literal.create(2, IntegerType)).nullable
        === false)
    assert(Substring(s_notNull,
      Literal.create(null, IntegerType), Literal.create(2, IntegerType)).nullable === true)
    assert(Substring(s_notNull,
      Literal.create(0, IntegerType), Literal.create(null, IntegerType)).nullable === true)

    checkEvaluation(s.substr(0, 2), "ex", row)
    checkEvaluation(s.substr(0), "example", row)
    checkEvaluation(s.substring(0, 2), "ex", row)
    checkEvaluation(s.substring(0), "example", row)
  }

  test("LIKE literal Regular Expression") {
    checkEvaluation(Literal.create(null, StringType).like("a"), null)
    checkEvaluation(Literal.create("a", StringType).like(Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create(null, StringType).like(Literal.create(null, StringType)), null)
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

  test("length for string") {
    val regEx = 'a.string.at(0)
    checkEvaluation(Literal("abc").length(), 3, create_row("abdef"))
    checkEvaluation(regEx.length(), 5, create_row("abdef"))
    checkEvaluation(regEx.length(), 0, create_row(""))
    checkEvaluation(Literal.create(null, StringType).length(), null, create_row(""))
  }


}
