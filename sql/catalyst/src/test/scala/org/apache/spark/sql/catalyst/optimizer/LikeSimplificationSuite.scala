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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class LikeSimplificationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Like Simplification", Once,
        LikeSimplification) :: Nil
  }

  val testRelation = LocalRelation($"a".string)

  test("simplify Like into StartsWith") {
    val originalQuery =
      testRelation
        .where(($"a" like "abc%") || ($"a" like "abc\\%"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(StartsWith($"a", "abc") || ($"a" === "abc%"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into EndsWith") {
    val originalQuery =
      testRelation
        .where($"a" like "%xyz")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(EndsWith($"a", "xyz"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into startsWith and EndsWith") {
    val originalQuery =
      testRelation
        .where(($"a" like "abc\\%def") || ($"a" like "abc%def"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(($"a" === "abc%def") ||
        (OctetLength($"a") >= 6 && (StartsWith($"a", "abc") && EndsWith($"a", "def"))))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into Contains") {
    val originalQuery =
      testRelation
        .where(($"a" like "%mn%") || ($"a" like "%mn\\%"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(Contains($"a", "mn") || EndsWith($"a", "mn%"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into EqualTo") {
    val originalQuery =
      testRelation
        .where(($"a" like "") || ($"a" like "abc"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(($"a" === "") || ($"a" === "abc"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("null pattern") {
    val originalQuery = testRelation.where($"a" like Literal(null, StringType)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, testRelation.where(Literal(null, BooleanType)).analyze)
  }

  test("test like escape syntax") {
    // '#%' with escape '#' is a literal '%', so these decode to exact-match EqualTo.
    val originalQuery1 = testRelation.where($"a".like("abc#%", '#'))
    val optimized1 = Optimize.execute(originalQuery1.analyze)
    comparePlans(optimized1, testRelation.where($"a" === "abc%").analyze)

    val originalQuery2 = testRelation.where($"a".like("abc#%abc", '#'))
    val optimized2 = Optimize.execute(originalQuery2.analyze)
    comparePlans(optimized2, testRelation.where($"a" === "abc%abc").analyze)
  }

  test("simplify LIKE patterns with escaped wildcards") {
    // Escaped wildcard/escape characters decode to literals, so escaped patterns simplify too.
    // EqualTo (no unescaped wildcard):
    comparePlans(
      Optimize.execute(testRelation.where($"a" like "ma\\%ca").analyze),
      testRelation.where($"a" === "ma%ca").analyze)
    comparePlans(
      Optimize.execute(testRelation.where($"a" like "ma\\\\ca").analyze),
      testRelation.where($"a" === "ma\\ca").analyze)
    comparePlans(
      Optimize.execute(testRelation.where($"a" like "a\\_b").analyze),
      testRelation.where($"a" === "a_b").analyze)
    // StartsWith / EndsWith / Contains with an escaped literal in the fixed part:
    comparePlans(
      Optimize.execute(testRelation.where($"a" like "abc\\%def%").analyze),
      testRelation.where(StartsWith($"a", "abc%def")).analyze)
    comparePlans(
      Optimize.execute(testRelation.where($"a" like "%xyz\\%").analyze),
      testRelation.where(EndsWith($"a", "xyz%")).analyze)
    comparePlans(
      Optimize.execute(testRelation.where($"a" like "%a\\%b%").analyze),
      testRelation.where(Contains($"a", "a%b")).analyze)
    // startsAndEndsWith with escaped literals on both sides ("a%b" and "c_d", 3 bytes each):
    comparePlans(
      Optimize.execute(testRelation.where($"a" like "a\\%b%c\\_d").analyze),
      testRelation.where(OctetLength($"a") >= 6 &&
        (StartsWith($"a", "a%b") && EndsWith($"a", "c_d"))).analyze)
  }

  test("do not simplify LIKE with invalid or pathological escapes") {
    // Invalid escape (escape char not followed by %, _, or itself) -> kept (LIKE errors at eval).
    val invalid = testRelation.where($"a" like "m\\aca").analyze
    comparePlans(Optimize.execute(invalid), invalid)
    // Trailing escape -> kept.
    val trailing = testRelation.where($"a" like "abc\\").analyze
    comparePlans(Optimize.execute(trailing), trailing)
    // Escape char is itself a wildcard character -> kept.
    val escPercent = testRelation.where($"a".like("a%%b", '%')).analyze
    comparePlans(Optimize.execute(escPercent), escPercent)
    val escUnderscore = testRelation.where($"a".like("a__b", '_')).analyze
    comparePlans(Optimize.execute(escUnderscore), escUnderscore)
    // Unescaped '_' single-char wildcard -> not handled by this rule.
    val underscore = testRelation.where($"a" like "a_b").analyze
    comparePlans(Optimize.execute(underscore), underscore)
  }

  test("SPARK-33677: LikeSimplification skips invalid/pathological escapes, simplifies valid") {
    val originalQuery1 =
      testRelation
        .where(($"a" like "abc%") || ($"a" like "\\abc%"))
    val optimized1 = Optimize.execute(originalQuery1.analyze)
    val correctAnswer1 = testRelation
      .where(StartsWith($"a", "abc") || ($"a" like "\\abc%"))
      .analyze
    comparePlans(optimized1, correctAnswer1)

    val originalQuery2 =
      testRelation
        .where(($"a" like "%xyz") || ($"a" like "%xyz\\"))
    val optimized2 = Optimize.execute(originalQuery2.analyze)
    val correctAnswer2 = testRelation
      .where(EndsWith($"a", "xyz") || ($"a" like "%xyz\\"))
      .analyze
    comparePlans(optimized2, correctAnswer2)

    val originalQuery3 =
      testRelation
        .where(($"a" like ("@bc%def", '@')) || ($"a" like "abc%def"))
    val optimized3 = Optimize.execute(originalQuery3.analyze)
    val correctAnswer3 = testRelation
      .where(($"a" like ("@bc%def", '@')) ||
        (OctetLength($"a") >= 6 && (StartsWith($"a", "abc") && EndsWith($"a", "def"))))
      .analyze
    comparePlans(optimized3, correctAnswer3)

    val originalQuery4 =
      testRelation
        .where(($"a" like "%mn%") || ($"a" like ("%mn%", '%')))
    val optimized4 = Optimize.execute(originalQuery4.analyze)
    val correctAnswer4 = testRelation
      .where(Contains($"a", "mn") || ($"a" like ("%mn%", '%')))
      .analyze
    comparePlans(optimized4, correctAnswer4)

    // 'abbc' with escape 'b': the 'b' escapes the next 'b' (a valid escaped-escape), so this is
    // the exact literal "abc" and is now simplified rather than skipped.
    val originalQuery5 =
      testRelation
        .where(($"a" like "abc") || ($"a" like ("abbc", 'b')))
    val optimized5 = Optimize.execute(originalQuery5.analyze)
    val correctAnswer5 = testRelation
      .where(($"a" === "abc") || ($"a" === "abc"))
      .analyze
    comparePlans(optimized5, correctAnswer5)
  }

  test("SPARK-52817: Spark SQL LIKE expressions show poor performance when using multiple '%'") {
    val originalQuery1 =
      testRelation
        .where($"a" like "abc%%")
    val optimized1 = Optimize.execute(originalQuery1.analyze)
    val correctAnswer1 = testRelation
      .where(StartsWith($"a", "abc"))
      .analyze
    comparePlans(optimized1, correctAnswer1)

    val originalQuery2 =
      testRelation
        .where($"a" like "%%xyz")
    val optimized2 = Optimize.execute(originalQuery2.analyze)
    val correctAnswer2 = testRelation
      .where(EndsWith($"a", "xyz"))
      .analyze
    comparePlans(optimized2, correctAnswer2)

    val originalQuery3 =
      testRelation
        .where($"a" like "abc%%def")
    val optimized3 = Optimize.execute(originalQuery3.analyze)
    val correctAnswer3 = testRelation
      .where(
        (OctetLength($"a") >= 6 && (StartsWith($"a", "abc") && EndsWith($"a", "def"))))
      .analyze
    comparePlans(optimized3, correctAnswer3)

    val originalQuery4 =
      testRelation
        .where(($"a" like "%%mn%%"))
    val optimized4 = Optimize.execute(originalQuery4.analyze)
    val correctAnswer4 = testRelation
      .where(Contains($"a", "mn"))
      .analyze
    comparePlans(optimized4, correctAnswer4)

    val originalQuery5 =
      testRelation
        .where(($"a" like "%%%mn%%%"))
    val optimized5 = Optimize.execute(originalQuery5.analyze)
    val correctAnswer5 = testRelation
      .where(Contains($"a", "mn"))
      .analyze
    comparePlans(optimized5, correctAnswer5)
  }

  test("simplify LikeAll") {
    val originalQuery =
      testRelation
        .where(($"a" likeAll(
    "abc%", "abc\\%", "%xyz", "abc\\%def", "abc%def", "%mn%", "%mn\\%", "", "abc")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(StartsWith($"a", "abc") && ($"a" === "abc%") && EndsWith($"a", "xyz") &&
        ($"a" === "abc%def") &&
        (OctetLength($"a") >= 6 && (StartsWith($"a", "abc") && EndsWith($"a", "def"))) &&
        Contains($"a", "mn") && EndsWith($"a", "mn%") && ($"a" === "") && ($"a" === "abc"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify NotLikeAll") {
    val originalQuery =
      testRelation
        .where(($"a" notLikeAll(
          "abc%", "abc\\%", "%xyz", "abc\\%def", "abc%def", "%mn%", "%mn\\%", "", "abc")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(Not(StartsWith($"a", "abc")) && Not($"a" === "abc%") && Not(EndsWith($"a", "xyz")) &&
        Not($"a" === "abc%def") &&
        Not(OctetLength($"a") >= 6 && (StartsWith($"a", "abc") && EndsWith($"a", "def"))) &&
        Not(Contains($"a", "mn")) && Not(EndsWith($"a", "mn%")) && Not($"a" === "") &&
        Not($"a" === "abc"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify LikeAny") {
    val originalQuery =
      testRelation
        .where(($"a" likeAny(
          "abc%", "abc\\%", "%xyz", "abc\\%def", "abc%def", "%mn%", "%mn\\%", "", "abc")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(
        (((StartsWith($"a", "abc") || ($"a" === "abc%")) ||
          (EndsWith($"a", "xyz") || ($"a" === "abc%def"))) ||
          (((OctetLength($"a") >= 6 && (StartsWith($"a", "abc") && EndsWith($"a", "def"))) ||
            Contains($"a", "mn")) || (EndsWith($"a", "mn%") || ($"a" === "")))) ||
          ($"a" === "abc"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify NotLikeAny") {
    val originalQuery =
      testRelation
        .where(($"a" notLikeAny(
          "abc%", "abc\\%", "%xyz", "abc\\%def", "abc%def", "%mn%", "%mn\\%", "", "abc")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(
        (((Not(StartsWith($"a", "abc")) || Not($"a" === "abc%")) ||
          (Not(EndsWith($"a", "xyz")) || Not($"a" === "abc%def"))) ||
          ((Not(OctetLength($"a") >= 6 && (StartsWith($"a", "abc") && EndsWith($"a", "def"))) ||
            Not(Contains($"a", "mn"))) || (Not(EndsWith($"a", "mn%")) || Not($"a" === "")))) ||
          Not($"a" === "abc"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-39251: Simplify MultiLike if remainPatterns is empty") {
    comparePlans(
      Optimize.execute(testRelation.where($"a" likeAll("abc%")).analyze),
      testRelation.where(StartsWith($"a", "abc")).analyze)

    comparePlans(
      Optimize.execute(testRelation.where($"a" notLikeAll("abc%")).analyze),
      testRelation.where(Not(StartsWith($"a", "abc"))).analyze)

    comparePlans(
      Optimize.execute(testRelation.where($"a" likeAny("abc%")).analyze),
      testRelation.where(StartsWith($"a", "abc")).analyze)

    comparePlans(
      Optimize.execute(testRelation.where($"a" notLikeAny("abc%")).analyze),
      testRelation.where(Not(StartsWith($"a", "abc"))).analyze)
  }

  test("SPARK-40228: Simplify multiLike if child is foldable expression") {
    comparePlans(Optimize.execute(testRelation.where("a" likeAny("abc%", "", "ab")).analyze),
      testRelation.where(StartsWith("a", "abc") || EqualTo("a", "") || EqualTo("a", "ab")).analyze)
  }

  test("SPARK-40228: Do not simplify multiLike if child is not a cheap expression") {
    val originalQuery = testRelation.where($"a".substring(1, 5) likeAny("abc%", "", "ab")).analyze

    comparePlans(Optimize.execute(originalQuery), originalQuery)
  }

  // scalastyle:off nonascii
  test("SPARK-59063: LikeSimplification preserves LIKE semantics under non-binary collation") {
    // Under UTF8_LCASE, StartsWith/EndsWith are collation-aware, so a single code point
    // whose case-folded form equals the anchor satisfies BOTH StartsWith(prefix) and
    // EndsWith(suffix). The Kelvin sign (U+212A) is one code point of three UTF-8 bytes
    // that folds to 'k'. LIKE 'k%k' is false for it -- the pattern requires two 'k's --
    // so the rewrite's length guard must reject it. A byte-length guard (OctetLength >=
    // numBytes("k") + numBytes("k") = 2) is satisfied by the 3-byte Kelvin sign and would
    // wrongly accept it; the code-point guard kept for non-binary collations rejects it.
    val lcase = StringType("UTF8_LCASE")
    val relation = LocalRelation(AttributeReference("a", lcase)())
    val attr = relation.output.head
    val like = Like(attr, Literal.create("k%k", lcase), '\\')

    val optimized = Optimize.execute(relation.where(like).analyze)
    val simplified = optimized.asInstanceOf[Filter].condition

    // A single Kelvin sign: one code point, three UTF-8 bytes.
    val row = InternalRow(UTF8String.fromString("\u212a"))
    val likeResult = BindReferences.bindReference(like, relation.output).eval(row)
    val simplifiedResult = BindReferences.bindReference(simplified, relation.output).eval(row)

    assert(likeResult === false,
      "LIKE 'k%k' should not match a single Kelvin sign under UTF8_LCASE")
    // The rewrite must be behavior-preserving: it must reject the single Kelvin sign too.
    assert(simplifiedResult === likeResult,
      s"LikeSimplification changed the LIKE 'k%k' result under UTF8_LCASE: expected " +
        s"$likeResult but the rewritten predicate returned $simplifiedResult")
  }

  test("SPARK-59063: LikeSimplification preserves LIKE semantics for multibyte UTF8_BINARY") {
    // The byte-length guard exists to reject inputs too short to hold both the prefix and
    // the suffix (a single code point must not match 'x%x'). Confirm the rewrite evaluates
    // exactly like LIKE for a multibyte UTF8_BINARY pattern: the a-umlaut U+00E4 is one code
    // point of two UTF-8 bytes, so 'a-umlaut % a-umlaut' rewrites to OctetLength >= 4 guarded
    // by StartsWith/EndsWith.
    val relation = LocalRelation($"a".string) // default StringType is UTF8_BINARY
    val attr = relation.output.head
    val like = Like(attr, Literal.create("\u00e4%\u00e4", StringType), '\\')

    val optimized = Optimize.execute(relation.where(like).analyze)
    val simplified = optimized.asInstanceOf[Filter].condition
    val boundLike = BindReferences.bindReference(like, relation.output)
    val boundSimplified = BindReferences.bindReference(simplified, relation.output)

    // A single a-umlaut: one code point, two UTF-8 bytes -- too short to match the pattern.
    val single = InternalRow(UTF8String.fromString("\u00e4"))
    assert(boundLike.eval(single) === false)
    assert(boundSimplified.eval(single) === boundLike.eval(single),
      "the rewrite must reject a single a-umlaut, matching LIKE")

    // Two a-umlauts: two code points, four UTF-8 bytes -- long enough to match.
    val pair = InternalRow(UTF8String.fromString("\u00e4\u00e4"))
    assert(boundLike.eval(pair) === true)
    assert(boundSimplified.eval(pair) === boundLike.eval(pair),
      "the rewrite must accept two a-umlauts, matching LIKE")
  }

  test("LikeSimplification with emojis") {
    val originalQuery =
      testRelation
        .where($"a" like "😀%🥑")

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      // Byte-length guard: '😀' and '🥑' are 4 UTF-8 bytes each, so the threshold is 8
      // bytes rather than 2 code points. This is equivalent to the char-length guard
      // because StartsWith/EndsWith already pin the prefix and suffix at byte boundaries.
      .where(OctetLength($"a") >= 8 && (StartsWith($"a", "😀") && EndsWith($"a", "🥑")))
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("LikeSimplification StartsWith/EndsWith/Contains with emojis") {
    comparePlans(
      Optimize.execute(testRelation.where($"a" like "😀%").analyze),
      testRelation.where(StartsWith($"a", "😀")).analyze)

    comparePlans(
      Optimize.execute(testRelation.where($"a" like "%🥑").analyze),
      testRelation.where(EndsWith($"a", "🥑")).analyze)

    comparePlans(
      Optimize.execute(testRelation.where($"a" like "%😇%").analyze),
      testRelation.where(Contains($"a", "😇")).analyze)

    comparePlans(
      Optimize.execute(testRelation.where($"a" like "😀😇🥑").analyze),
      testRelation.where($"a" === "😀😇🥑").analyze)
  }
  // scalastyle:on nonascii
}
