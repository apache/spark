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
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

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

    val regex = $"a".string.at(0)
    checkEvaluation(mkExpr(regex), expected, create_row(input)) // check row input
  }

  test("LIKE ALL") {
    checkEvaluation(Literal.create(null, StringType).likeAll("%foo%", "%oo"), null)
    checkEvaluation(Literal.create("foo", StringType).likeAll("%foo%", "%oo"), true)
    checkEvaluation(Literal.create("foo", StringType).likeAll("%foo%", "%bar%"), false)
    checkEvaluation(Literal.create("foo", StringType)
      .likeAll("%foo%", Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create("foo", StringType)
      .likeAll(Literal.create(null, StringType), "%foo%"), null)
    checkEvaluation(Literal.create("foo", StringType)
      .likeAll("%feo%", Literal.create(null, StringType)), false)
    checkEvaluation(Literal.create("foo", StringType)
      .likeAll(Literal.create(null, StringType), "%feo%"), false)
    checkEvaluation(Literal.create("foo", StringType).notLikeAll("tee", "%yoo%"), true)
    checkEvaluation(Literal.create("foo", StringType).notLikeAll("%oo%", "%yoo%"), false)
    checkEvaluation(Literal.create("foo", StringType)
      .notLikeAll("%foo%", Literal.create(null, StringType)), false)
    checkEvaluation(Literal.create("foo", StringType)
      .notLikeAll(Literal.create(null, StringType), "%foo%"), false)
    checkEvaluation(Literal.create("foo", StringType)
      .notLikeAll("%yoo%", Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create("foo", StringType)
      .notLikeAll(Literal.create(null, StringType), "%yoo%"), null)
  }

  test("LIKE ANY") {
    checkEvaluation(Literal.create(null, StringType).likeAny("%foo%", "%oo"), null)
    checkEvaluation(Literal.create("foo", StringType).likeAny("%foo%", "%oo"), true)
    checkEvaluation(Literal.create("foo", StringType).likeAny("%foo%", "%bar%"), true)
    checkEvaluation(Literal.create("foo", StringType).likeAny("%fee%", "%bar%"), false)
    checkEvaluation(Literal.create("foo", StringType)
      .likeAny("%foo%", Literal.create(null, StringType)), true)
    checkEvaluation(Literal.create("foo", StringType)
      .likeAny(Literal.create(null, StringType), "%foo%"), true)
    checkEvaluation(Literal.create("foo", StringType)
      .likeAny("%feo%", Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create("foo", StringType)
      .likeAny(Literal.create(null, StringType), "%feo%"), null)
    checkEvaluation(Literal.create("foo", StringType).notLikeAny("tee", "%yoo%"), true)
    checkEvaluation(Literal.create("foo", StringType).notLikeAny("%oo%", "%yoo%"), true)
    checkEvaluation(Literal.create("foo", StringType).notLikeAny("%foo%", "%oo"), false)
    checkEvaluation(Literal.create("foo", StringType)
      .notLikeAny("%foo%", Literal.create(null, StringType)), null)
    checkEvaluation(Literal.create("foo", StringType)
      .notLikeAny(Literal.create(null, StringType), "%foo%"), null)
    checkEvaluation(Literal.create("foo", StringType)
      .notLikeAny("%yoo%", Literal.create(null, StringType)), true)
    checkEvaluation(Literal.create("foo", StringType)
      .notLikeAny(Literal.create(null, StringType), "%yoo%"), true)
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
      evaluateWithoutCodegen("""a""" like """\a""")
    }
    assert(invalidEscape.getMessage.contains("pattern"))

    val endEscape = intercept[AnalysisException] {
      evaluateWithoutCodegen("""a""" like """a\""")
    }
    assert(endEscape.getMessage.contains("pattern"))

    // case
    checkLiteralRow("A" like _, "a%", false)
    checkLiteralRow("a" like _, "A%", false)
    checkLiteralRow("AaA" like _, "_a_", true)

    // example
    checkLiteralRow("""%SystemDrive%\Users\John""" like _, """\%SystemDrive\%\\Users%""", true)
  }

  Seq('/', '#', '\"').foreach { escapeChar =>
    test(s"LIKE Pattern ESCAPE '$escapeChar'") {
      // null handling
      checkLiteralRow(Literal.create(null, StringType).like(_, escapeChar), "a", null)
      checkEvaluation(
        Literal.create("a", StringType).like(Literal.create(null, StringType), escapeChar), null)
      checkEvaluation(
        Literal.create(null, StringType).like(Literal.create(null, StringType), escapeChar), null)
      checkEvaluation(Literal.create("a", StringType).like(
        NonFoldableLiteral.create("a", StringType), escapeChar), true)
      checkEvaluation(Literal.create("a", StringType).like(
        NonFoldableLiteral.create(null, StringType), escapeChar), null)
      checkEvaluation(Literal.create(null, StringType).like(
        NonFoldableLiteral.create("a", StringType), escapeChar), null)
      checkEvaluation(Literal.create(null, StringType).like(
        NonFoldableLiteral.create(null, StringType), escapeChar), null)

      // simple patterns
      checkLiteralRow("abdef" like(_, escapeChar), "abdef", true)
      checkLiteralRow("a_%b" like(_, escapeChar), s"a${escapeChar}__b", true)
      checkLiteralRow("addb" like(_, escapeChar), "a_%b", true)
      checkLiteralRow("addb" like(_, escapeChar), s"a${escapeChar}__b", false)
      checkLiteralRow("addb" like(_, escapeChar), s"a%$escapeChar%b", false)
      checkLiteralRow("a_%b" like(_, escapeChar), s"a%$escapeChar%b", true)
      checkLiteralRow("addb" like(_, escapeChar), "a%", true)
      checkLiteralRow("addb" like(_, escapeChar), "**", false)
      checkLiteralRow("abc" like(_, escapeChar), "a%", true)
      checkLiteralRow("abc"  like(_, escapeChar), "b%", false)
      checkLiteralRow("abc"  like(_, escapeChar), "bc%", false)
      checkLiteralRow("a\nb" like(_, escapeChar), "a_b", true)
      checkLiteralRow("ab" like(_, escapeChar), "a%b", true)
      checkLiteralRow("a\nb" like(_, escapeChar), "a%b", true)

      // empty input
      checkLiteralRow("" like(_, escapeChar), "", true)
      checkLiteralRow("a" like(_, escapeChar), "", false)
      checkLiteralRow("" like(_, escapeChar), "a", false)

      // SI-17647 double-escaping backslash
      checkLiteralRow(s"""$escapeChar$escapeChar$escapeChar$escapeChar""" like(_, escapeChar),
        s"""%$escapeChar$escapeChar%""", true)
      checkLiteralRow("""%%""" like(_, escapeChar), """%%""", true)
      checkLiteralRow(s"""${escapeChar}__""" like(_, escapeChar),
        s"""$escapeChar$escapeChar${escapeChar}__""", true)
      checkLiteralRow(s"""$escapeChar$escapeChar${escapeChar}__""" like(_, escapeChar),
        s"""%$escapeChar$escapeChar%$escapeChar%""", false)
      checkLiteralRow(s"""_$escapeChar$escapeChar$escapeChar%""" like(_, escapeChar),
        s"""%$escapeChar${escapeChar}""", false)

      // unicode
      // scalastyle:off nonascii
      checkLiteralRow("a\u20ACa" like(_, escapeChar), "_\u20AC_", true)
      checkLiteralRow("a€a" like(_, escapeChar), "_€_", true)
      checkLiteralRow("a€a" like(_, escapeChar), "_\u20AC_", true)
      checkLiteralRow("a\u20ACa" like(_, escapeChar), "_€_", true)
      // scalastyle:on nonascii

      // invalid escaping
      val invalidEscape = intercept[AnalysisException] {
        evaluateWithoutCodegen("""a""" like(s"""${escapeChar}a""", escapeChar))
      }
      assert(invalidEscape.getMessage.contains("pattern"))
      val endEscape = intercept[AnalysisException] {
        evaluateWithoutCodegen("""a""" like(s"""a$escapeChar""", escapeChar))
      }
      assert(endEscape.getMessage.contains("pattern"))

      // case
      checkLiteralRow("A" like(_, escapeChar), "a%", false)
      checkLiteralRow("a" like(_, escapeChar), "A%", false)
      checkLiteralRow("AaA" like(_, escapeChar), "_a_", true)

      // example
      checkLiteralRow(s"""%SystemDrive%${escapeChar}Users${escapeChar}John""" like(_, escapeChar),
        s"""$escapeChar%SystemDrive$escapeChar%$escapeChar${escapeChar}Users%""", true)
    }
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
      evaluateWithoutCodegen("abbbbc" rlike "**")
    }
    intercept[java.util.regex.PatternSyntaxException] {
      val regex = $"a".string.at(0)
      evaluateWithoutCodegen("abbbbc" rlike regex, create_row("**"))
    }
  }

  test("RegexReplace") {
    val row1 = create_row("100-200", "(\\d+)", "num")
    val row2 = create_row("100-200", "(\\d+)", "###")
    val row3 = create_row("100-200", "(-)", "###")
    val row4 = create_row(null, "(\\d+)", "###")
    val row5 = create_row("100-200", null, "###")
    val row6 = create_row("100-200", "(-)", null)

    val s = $"s".string.at(0)
    val p = $"p".string.at(1)
    val r = $"r".string.at(2)

    val expr = RegExpReplace(s, p, r)
    checkEvaluation(expr, "num-num", row1)
    checkEvaluation(expr, "###-###", row2)
    checkEvaluation(expr, "100###200", row3)
    checkEvaluation(expr, null, row4)
    checkEvaluation(expr, null, row5)
    checkEvaluation(expr, null, row6)
    // test position
    val exprWithPos = RegExpReplace(s, p, r, 4)
    checkEvaluation(exprWithPos, "100-num", row1)
    checkEvaluation(exprWithPos, "100-###", row2)
    checkEvaluation(exprWithPos, "100###200", row3)
    checkEvaluation(exprWithPos, null, row4)
    checkEvaluation(exprWithPos, null, row5)
    checkEvaluation(exprWithPos, null, row6)
    val exprWithLargePos = RegExpReplace(s, p, r, 7)
    checkEvaluation(exprWithLargePos, "100-20num", row1)
    checkEvaluation(exprWithLargePos, "100-20###", row2)
    val exprWithExceedLength = RegExpReplace(s, p, r, 8)
    checkEvaluation(exprWithExceedLength, "100-200", row1)
    checkEvaluation(exprWithExceedLength, "100-200", row2)

    val nonNullExpr = RegExpReplace(Literal("100-200"), Literal("(\\d+)"), Literal("num"))
    checkEvaluation(nonNullExpr, "num-num", row1)

    // Test escaping of arguments
    GenerateUnsafeProjection.generate(
      RegExpReplace(Literal("\"quote"), Literal("\"quote"), Literal("\"quote")) :: Nil)
  }

  test("SPARK-22570: RegExpReplace should not create a lot of global variables") {
    val ctx = new CodegenContext
    RegExpReplace(Literal("100"), Literal("(\\d+)"), Literal("num")).genCode(ctx)
    // four global variables (lastRegex, pattern, lastReplacement, and lastReplacementInUTF8)
    // are always required, which are allocated in type-based global array
    assert(ctx.inlinedMutableStates.length == 0)
    assert(ctx.mutableStateInitCode.length == 4)
  }

  test("RegexExtract") {
    val row1 = create_row("100-200", "(\\d+)-(\\d+)", 1)
    val row2 = create_row("100-200", "(\\d+)-(\\d+)", 2)
    val row3 = create_row("100-200", "(\\d+).*", 1)
    val row4 = create_row("100-200", "([a-z])", 1)
    val row5 = create_row(null, "([a-z])", 1)
    val row6 = create_row("100-200", null, 1)
    val row7 = create_row("100-200", "([a-z])", null)

    val s = $"s".string.at(0)
    val p = $"p".string.at(1)
    val r = $"r".int.at(2)

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

    // invalid group index
    val row8 = create_row("100-200", "(\\d+)-(\\d+)", 3)
    val row9 = create_row("100-200", "(\\d+).*", 2)
    val row10 = create_row("100-200", "\\d+", 1)
    val row11 = create_row("100-200", "(\\d+)-(\\d+)", -1)
    val row12 = create_row("100-200", "\\d+", -1)

    checkExceptionInExpression[IllegalArgumentException](
      expr, row8, "Regex group count is 2, but the specified group index is 3")
    checkExceptionInExpression[IllegalArgumentException](
      expr, row9, "Regex group count is 1, but the specified group index is 2")
    checkExceptionInExpression[IllegalArgumentException](
      expr, row10, "Regex group count is 0, but the specified group index is 1")
    checkExceptionInExpression[IllegalArgumentException](
      expr, row11, "The specified group index cannot be less than zero")
    checkExceptionInExpression[IllegalArgumentException](
      expr, row12, "The specified group index cannot be less than zero")

    // Test escaping of arguments
    GenerateUnsafeProjection.generate(
      RegExpExtract(Literal("\"quote"), Literal("\"quote"), Literal(1)) :: Nil)
  }

  test("RegexExtractAll") {
    val row1 = create_row("100-200,300-400,500-600", "(\\d+)-(\\d+)", 0)
    val row2 = create_row("100-200,300-400,500-600", "(\\d+)-(\\d+)", 1)
    val row3 = create_row("100-200,300-400,500-600", "(\\d+)-(\\d+)", 2)
    val row4 = create_row("100-200,300-400,500-600", "(\\d+).*", 1)
    val row5 = create_row("100-200,300-400,500-600", "([a-z])", 1)
    val row6 = create_row(null, "([a-z])", 1)
    val row7 = create_row("100-200,300-400,500-600", null, 1)
    val row8 = create_row("100-200,300-400,500-600", "([a-z])", null)

    val s = $"s".string.at(0)
    val p = $"p".string.at(1)
    val r = $"r".int.at(2)

    val expr = RegExpExtractAll(s, p, r)
    checkEvaluation(expr, Seq("100-200", "300-400", "500-600"), row1)
    checkEvaluation(expr, Seq("100", "300", "500"), row2)
    checkEvaluation(expr, Seq("200", "400", "600"), row3)
    checkEvaluation(expr, Seq("100"), row4)
    checkEvaluation(expr, Seq(), row5)
    checkEvaluation(expr, null, row6)
    checkEvaluation(expr, null, row7)
    checkEvaluation(expr, null, row8)

    val expr1 = new RegExpExtractAll(s, p)
    checkEvaluation(expr1, Seq("100", "300", "500"), row2)

    val nonNullExpr = RegExpExtractAll(Literal("100-200,300-400,500-600"),
      Literal("(\\d+)-(\\d+)"), Literal(1))
    checkEvaluation(nonNullExpr, Seq("100", "300", "500"), row2)

    // invalid group index
    val row9 = create_row("100-200,300-400,500-600", "(\\d+)-(\\d+)", 3)
    val row10 = create_row("100-200,300-400,500-600", "(\\d+).*", 2)
    val row11 = create_row("100-200,300-400,500-600", "\\d+", 1)
    val row12 = create_row("100-200,300-400,500-600", "(\\d+)-(\\d+)", -1)
    val row13 = create_row("100-200,300-400,500-600", "\\d+", -1)

    checkExceptionInExpression[IllegalArgumentException](
      expr, row9, "Regex group count is 2, but the specified group index is 3")
    checkExceptionInExpression[IllegalArgumentException](
      expr, row10, "Regex group count is 1, but the specified group index is 2")
    checkExceptionInExpression[IllegalArgumentException](
      expr, row11, "Regex group count is 0, but the specified group index is 1")
    checkExceptionInExpression[IllegalArgumentException](
      expr, row12, "The specified group index cannot be less than zero")
    checkExceptionInExpression[IllegalArgumentException](
      expr, row13, "The specified group index cannot be less than zero")
  }

  test("SPLIT") {
    val s1 = $"a".string.at(0)
    val s2 = $"b".string.at(1)
    val row1 = create_row("aa2bb3cc", "[1-9]+")
    val row2 = create_row(null, "[1-9]+")
    val row3 = create_row("aa2bb3cc", null)

    checkEvaluation(
      StringSplit(Literal("aa2bb3cc"), Literal("[1-9]+"), -1), Seq("aa", "bb", "cc"), row1)
    checkEvaluation(
      StringSplit(Literal("aa2bb3cc"), Literal("[1-9]+"), 2), Seq("aa", "bb3cc"), row1)
    // limit = 0 should behave just like limit = -1
    checkEvaluation(
      StringSplit(Literal("aacbbcddc"), Literal("c"), 0), Seq("aa", "bb", "dd", ""), row1)
    checkEvaluation(
      StringSplit(Literal("aacbbcddc"), Literal("c"), -1), Seq("aa", "bb", "dd", ""), row1)
    checkEvaluation(
      StringSplit(s1, s2, -1), Seq("aa", "bb", "cc"), row1)
    checkEvaluation(StringSplit(s1, s2, -1), null, row2)
    checkEvaluation(StringSplit(s1, s2, -1), null, row3)

    // Test escaping of arguments
    GenerateUnsafeProjection.generate(
      StringSplit(Literal("\"quote"), Literal("\"quote"), Literal(-1)) :: Nil)
  }

  test("SPARK-30759: cache initialization for literal patterns") {
    val expr = "A" like Literal.create("a", StringType)
    expr.eval()
    val cache = expr.getClass.getSuperclass
      .getDeclaredFields.filter(_.getName.endsWith("cache")).head
    cache.setAccessible(true)
    assert(cache.get(expr).asInstanceOf[java.util.regex.Pattern].pattern().contains("a"))
  }

  test("SPARK-34814: LikeSimplification should handle NULL") {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
      ConstantFolding.getClass.getName.stripSuffix("$")) {
      checkEvaluation(Literal.create("foo", StringType)
        .likeAll("%foo%", Literal.create(null, StringType)), null)
    }
  }
}
