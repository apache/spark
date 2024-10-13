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
import org.apache.spark.sql.catalyst.collation.CollationFactory
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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
        Literal.create(t.regexLike, StringType), '\\'), t.expectedILike)
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
        Literal.create(t.r, StringType)), t.expectedCount)
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
    // LikeAll
    case class LikeAllTestCase[R](l: String, p1: String, p2: String, collation: String,
      expectedLikeAll: R)
    val likeAllTestCases = Seq(
      LikeAllTestCase("foo", "%foo%", "%oo", "UTF8_BINARY", true),
      LikeAllTestCase("foo", "%foo%", "%bar%", "UTF8_BINARY", false),
      LikeAllTestCase("Foo", "%foo%", "%oo", "UTF8_LCASE", true),
      LikeAllTestCase("Foo", "%foo%", "%bar%", "UTF8_LCASE", false),
      LikeAllTestCase("foo", "%foo%", "%oo", "UTF8_BINARY", true),
      LikeAllTestCase("foo", "%foo%", "%bar%", "UTF8_BINARY", false),
      LikeAllTestCase("foo", "%foo%", null, "UTF8_BINARY", null),
      LikeAllTestCase("foo", "%feo%", null, "UTF8_BINARY", false),
      LikeAllTestCase(null, "%foo%", "%oo", "UTF8_BINARY", null)
    )
    likeAllTestCases.foreach(t => {
      checkEvaluation(LikeAll(
        Literal.create(t.l, StringType(CollationFactory.collationNameToId(t.collation))),
          Seq(UTF8String.fromString(t.p1), UTF8String.fromString(t.p2))), t.expectedLikeAll)
    })

    // NotLikeAll
    case class NotLikeAllTestCase[R](l: String, p1: String, p2: String, collation: String,
      expectedNotLikeAll: R)
    val notLikeAllTestCases = Seq(
      NotLikeAllTestCase("foo", "%foo%", "%oo", "UTF8_BINARY", false),
      NotLikeAllTestCase("foo", "%goo%", "%bar%", "UTF8_BINARY", true),
      NotLikeAllTestCase("Foo", "%foo%", "%oo", "UTF8_LCASE", false),
      NotLikeAllTestCase("Foo", "%goo%", "%bar%", "UTF8_LCASE", true),
      NotLikeAllTestCase("foo", "%foo%", "%oo", "UTF8_BINARY", false),
      NotLikeAllTestCase("foo", "%goo%", "%bar%", "UTF8_BINARY", true),
      NotLikeAllTestCase("foo", "%foo%", null, "UTF8_BINARY", false),
      NotLikeAllTestCase("foo", "%feo%", null, "UTF8_BINARY", null),
      NotLikeAllTestCase(null, "%foo%", "%oo", "UTF8_BINARY", null)
    )
    notLikeAllTestCases.foreach(t => {
      checkEvaluation(NotLikeAll(
        Literal.create(t.l, StringType(CollationFactory.collationNameToId(t.collation))),
        Seq(UTF8String.fromString(t.p1), UTF8String.fromString(t.p2))), t.expectedNotLikeAll)
    })

    // LikeAny
    case class LikeAnyTestCase[R](l: String, p1: String, p2: String, collation: String,
      expectedLikeAny: R)
    val likeAnyTestCases = Seq(
      LikeAnyTestCase("foo", "%goo%", "%hoo", "UTF8_BINARY", false),
      LikeAnyTestCase("foo", "%foo%", "%bar%", "UTF8_BINARY", true),
      LikeAnyTestCase("Foo", "%goo%", "%hoo", "UTF8_LCASE", false),
      LikeAnyTestCase("Foo", "%foo%", "%bar%", "UTF8_LCASE", true),
      LikeAnyTestCase("foo", "%goo%", "%hoo", "UTF8_BINARY", false),
      LikeAnyTestCase("foo", "%foo%", "%bar%", "UTF8_BINARY", true),
      LikeAnyTestCase("foo", "%foo%", null, "UTF8_BINARY", true),
      LikeAnyTestCase("foo", "%feo%", null, "UTF8_BINARY", null),
      LikeAnyTestCase(null, "%foo%", "%oo", "UTF8_BINARY", null)
    )
    likeAnyTestCases.foreach(t => {
      checkEvaluation(LikeAny(
        Literal.create(t.l, StringType(CollationFactory.collationNameToId(t.collation))),
        Seq(UTF8String.fromString(t.p1), UTF8String.fromString(t.p2))), t.expectedLikeAny)
    })

    // NotLikeAny
    case class NotLikeAnyTestCase[R](l: String, p1: String, p2: String, collation: String,
      expectedNotLikeAny: R)
    val notLikeAnyTestCases = Seq(
      NotLikeAnyTestCase("foo", "%foo%", "%hoo", "UTF8_BINARY", true),
      NotLikeAnyTestCase("foo", "%foo%", "%oo%", "UTF8_BINARY", false),
      NotLikeAnyTestCase("Foo", "%Foo%", "%hoo", "UTF8_LCASE", true),
      NotLikeAnyTestCase("Foo", "%foo%", "%oo%", "UTF8_LCASE", false),
      NotLikeAnyTestCase("foo", "%Foo%", "%hoo", "UTF8_BINARY", true),
      NotLikeAnyTestCase("foo", "%foo%", "%oo%", "UTF8_BINARY", false),
      NotLikeAnyTestCase("foo", "%foo%", null, "UTF8_BINARY", null),
      NotLikeAnyTestCase("foo", "%feo%", null, "UTF8_BINARY", true),
      NotLikeAnyTestCase(null, "%foo%", "%oo", "UTF8_BINARY", null)
    )
    notLikeAnyTestCases.foreach(t => {
      checkEvaluation(NotLikeAny(
        Literal.create(t.l, StringType(CollationFactory.collationNameToId(t.collation))),
        Seq(UTF8String.fromString(t.p1), UTF8String.fromString(t.p2))), t.expectedNotLikeAny)
    })
  }
}
