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
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types._

class CollationRegexpExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("Like/ILike/RLike expressions with collated strings") {
    case class LikeTestCase[R](l: String, regexLike: String, regexRLike: String, collation: String,
      expectedLike: R, expectedILike: R, expectedRLike: R)
    val testCases = Seq(
      LikeTestCase("AbC", "%AbC%", ".b.", "UTF8_BINARY", true, true, true),
      LikeTestCase("AbC", "%ABC%", ".B.", "UTF8_BINARY", false, true, false),
      LikeTestCase("AbC", "%abc%", ".b.", "UTF8_LCASE", true, true, true),
      LikeTestCase("", "", "", "UTF8_LCASE", true, true, true),
      LikeTestCase("Foo", "", "", "UTF8_LCASE", false, false, true),
      LikeTestCase("", "%foo%", ".o.", "UTF8_LCASE", false, false, false),
      LikeTestCase("AbC", "%ABC%", ".B.", "UTF8_BINARY", false, true, false),
      LikeTestCase(null, "%foo%", ".o.", "UTF8_BINARY", null, null, null),
      LikeTestCase("Foo", null, null, "UTF8_BINARY", null, null, null),
      LikeTestCase(null, null, null, "UTF8_BINARY", null, null, null)
    )
    testCases.foreach(t => {
      // Like
      checkEvaluation(Like(
        Literal.create(t.l, StringType(CollationFactory.collationNameToId(t.collation))),
        Literal.create(t.regexLike, StringType), '\\'), t.expectedLike)
      // ILike
      checkEvaluation(ILike(
        Literal.create(t.l, StringType(CollationFactory.collationNameToId(t.collation))),
        Literal.create(t.regexLike, StringType), '\\').replacement, t.expectedILike)
      // RLike
      checkEvaluation(RLike(
        Literal.create(t.l, StringType(CollationFactory.collationNameToId(t.collation))),
        Literal.create(t.regexRLike, StringType)), t.expectedRLike)
    })
  }

  test("StringSplit expression with collated strings") {
    case class StringSplitTestCase[R](s: String, r: String, collation: String, expected: R)
    val testCases = Seq(
      StringSplitTestCase("1A2B3C", "[ABC]", "UTF8_BINARY", Seq("1", "2", "3", "")),
      StringSplitTestCase("1A2B3C", "[abc]", "UTF8_BINARY", Seq("1A2B3C")),
      StringSplitTestCase("1A2B3C", "[ABC]", "UTF8_LCASE", Seq("1", "2", "3", "")),
      StringSplitTestCase("1A2B3C", "[abc]", "UTF8_LCASE", Seq("1", "2", "3", "")),
      StringSplitTestCase("1A2B3C", "[1-9]+", "UTF8_BINARY", Seq("", "A", "B", "C")),
      StringSplitTestCase("", "", "UTF8_BINARY", Seq("")),
      StringSplitTestCase("1A2B3C", "", "UTF8_BINARY", Seq("1", "A", "2", "B", "3", "C")),
      StringSplitTestCase("", "[1-9]+", "UTF8_BINARY", Seq("")),
      StringSplitTestCase(null, "[1-9]+", "UTF8_BINARY", null),
      StringSplitTestCase("1A2B3C", null, "UTF8_BINARY", null),
      StringSplitTestCase(null, null, "UTF8_BINARY", null)
    )
    testCases.foreach(t => {
      // StringSplit
      checkEvaluation(StringSplit(
        Literal.create(t.s, StringType(CollationFactory.collationNameToId(t.collation))),
        Literal.create(t.r, StringType), -1), t.expected)
    })
  }

  test("Regexp expressions with collated strings") {
    case class RegexpTestCase[R](l: String, r: String, collation: String,
      expectedExtract: R, expectedExtractAll: R, expectedCount: R)
    val testCases = Seq(
      RegexpTestCase("AbC-aBc", ".b.", "UTF8_BINARY", "AbC", Seq("AbC"), 1),
      RegexpTestCase("AbC-abc", ".b.", "UTF8_BINARY", "AbC", Seq("AbC", "abc"), 2),
      RegexpTestCase("AbC-aBc", ".b.", "UTF8_LCASE", "AbC", Seq("AbC", "aBc"), 2),
      RegexpTestCase("ABC-abc", ".b.", "UTF8_LCASE", "ABC", Seq("ABC", "abc"), 2),
      RegexpTestCase("", "", "UTF8_LCASE", "", Seq(""), 1),
      RegexpTestCase("Foo", "", "UTF8_LCASE", "", Seq("", "", "", ""), 4),
      RegexpTestCase("", ".o.", "UTF8_LCASE", "", Seq(), 0),
      RegexpTestCase("Foo", ".O.", "UTF8_BINARY", "", Seq(), 0),
      RegexpTestCase(null, ".O.", "UTF8_BINARY", null, null, null),
      RegexpTestCase("Foo", null, "UTF8_BINARY", null, null, null),
      RegexpTestCase(null, null, "UTF8_BINARY", null, null, null)
    )
    testCases.foreach(t => {
      // RegExpExtract
      checkEvaluation(RegExpExtract(
        Literal.create(t.l, StringType(CollationFactory.collationNameToId(t.collation))),
        Literal.create(t.r, StringType), 0), t.expectedExtract)
      // RegExpExtractAll
      checkEvaluation(RegExpExtractAll(
        Literal.create(t.l, StringType(CollationFactory.collationNameToId(t.collation))),
        Literal.create(t.r, StringType), 0), t.expectedExtractAll)
      // RegExpCount
      checkEvaluation(RegExpCount(
        Literal.create(t.l, StringType(CollationFactory.collationNameToId(t.collation))),
        Literal.create(t.r, StringType)).replacement, t.expectedCount)
      // RegExpInStr
      def expectedInStr(count: Any): Any = count match {
        case null => null
        case 0 => 0
        case n: Int if n >= 1 => 1
      }
      checkEvaluation(RegExpInStr(
        Literal.create(t.l, StringType(CollationFactory.collationNameToId(t.collation))),
        Literal.create(t.r, StringType), 0), expectedInStr(t.expectedCount))
    })
  }

  test("MultiLikeBase regexp expressions with collated strings") {
    val nullStr = Literal.create(null, StringType)
    // Supported collations (StringTypeBinaryLcase)
    val binaryCollation = StringType(CollationFactory.collationNameToId("UTF8_BINARY"))
    val lowercaseCollation = StringType(CollationFactory.collationNameToId("UTF8_LCASE"))
    // LikeAll
    checkEvaluation(Literal.create("foo", binaryCollation).likeAll("%foo%", "%oo"), true)
    checkEvaluation(Literal.create("foo", binaryCollation).likeAll("%foo%", "%bar%"), false)
    checkEvaluation(Literal.create("Foo", lowercaseCollation).likeAll("%foo%", "%oo"), true)
    checkEvaluation(Literal.create("Foo", lowercaseCollation).likeAll("%foo%", "%bar%"), false)
    checkEvaluation(Literal.create("foo", binaryCollation).likeAll("%foo%", "%oo"), true)
    checkEvaluation(Literal.create("foo", binaryCollation).likeAll("%foo%", "%bar%"), false)
    checkEvaluation(Literal.create("foo", binaryCollation).likeAll("%foo%", nullStr), null)
    checkEvaluation(Literal.create("foo", binaryCollation).likeAll("%feo%", nullStr), false)
    checkEvaluation(Literal.create(null, binaryCollation).likeAll("%foo%", "%oo"), null)
    // NotLikeAll
    checkEvaluation(Literal.create("foo", binaryCollation).notLikeAll("%foo%", "%oo"), false)
    checkEvaluation(Literal.create("foo", binaryCollation).notLikeAll("%goo%", "%bar%"), true)
    checkEvaluation(Literal.create("Foo", lowercaseCollation).notLikeAll("%foo%", "%oo"), false)
    checkEvaluation(Literal.create("Foo", lowercaseCollation).notLikeAll("%goo%", "%bar%"), true)
    checkEvaluation(Literal.create("foo", binaryCollation).notLikeAll("%foo%", "%oo"), false)
    checkEvaluation(Literal.create("foo", binaryCollation).notLikeAll("%goo%", "%bar%"), true)
    checkEvaluation(Literal.create("foo", binaryCollation).notLikeAll("%foo%", nullStr), false)
    checkEvaluation(Literal.create("foo", binaryCollation).notLikeAll("%feo%", nullStr), null)
    checkEvaluation(Literal.create(null, binaryCollation).notLikeAll("%foo%", "%oo"), null)
    // LikeAny
    checkEvaluation(Literal.create("foo", binaryCollation).likeAny("%goo%", "%hoo"), false)
    checkEvaluation(Literal.create("foo", binaryCollation).likeAny("%foo%", "%bar%"), true)
    checkEvaluation(Literal.create("Foo", lowercaseCollation).likeAny("%goo%", "%hoo"), false)
    checkEvaluation(Literal.create("Foo", lowercaseCollation).likeAny("%foo%", "%bar%"), true)
    checkEvaluation(Literal.create("foo", binaryCollation).likeAny("%goo%", "%hoo"), false)
    checkEvaluation(Literal.create("foo", binaryCollation).likeAny("%foo%", "%bar%"), true)
    checkEvaluation(Literal.create("foo", binaryCollation).likeAny("%foo%", nullStr), true)
    checkEvaluation(Literal.create("foo", binaryCollation).likeAny("%feo%", nullStr), null)
    checkEvaluation(Literal.create(null, binaryCollation).likeAny("%foo%", "%oo"), null)
    // NotLikeAny
    checkEvaluation(Literal.create("foo", binaryCollation).notLikeAny("%foo%", "%hoo"), true)
    checkEvaluation(Literal.create("foo", binaryCollation).notLikeAny("%foo%", "%oo%"), false)
    checkEvaluation(Literal.create("Foo", lowercaseCollation).notLikeAny("%Foo%", "%hoo"), true)
    checkEvaluation(Literal.create("Foo", lowercaseCollation).notLikeAny("%foo%", "%oo%"), false)
    checkEvaluation(Literal.create("foo", binaryCollation).notLikeAny("%Foo%", "%hoo"), true)
    checkEvaluation(Literal.create("foo", binaryCollation).notLikeAny("%foo%", "%oo%"), false)
    checkEvaluation(Literal.create("foo", binaryCollation).notLikeAny("%foo%", nullStr), null)
    checkEvaluation(Literal.create("foo", binaryCollation).notLikeAny("%feo%", nullStr), true)
    checkEvaluation(Literal.create(null, binaryCollation).notLikeAny("%foo%", "%oo"), null)
  }

}
