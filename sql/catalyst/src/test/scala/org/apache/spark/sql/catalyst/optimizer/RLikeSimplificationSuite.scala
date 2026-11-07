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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, StringType}

class RLikeSimplificationSuite extends PlanTest with ExpressionEvalHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("RLike Simplification", Once, RLikeSimplification) :: Nil
  }

  private val testRelation = LocalRelation($"a".string)
  private val input = testRelation.output.head

  private def withRuleEnabled[T](f: => T): T = {
    withSQLConf(SQLConf.RLIKE_SIMPLIFICATION_ENABLED.key -> "true")(f)
  }

  private def checkSimplified(pattern: String, expected: Expression): Unit = withRuleEnabled {
    val original = testRelation.where(RLike(input, Literal(pattern))).analyze
    val correctAnswer = testRelation.where(expected).analyze
    comparePlans(Optimize.execute(original), correctAnswer)
  }

  private def checkNotSimplified(pattern: String): Unit = withRuleEnabled {
    val original = testRelation.where(RLike(input, Literal(pattern))).analyze
    comparePlans(Optimize.execute(original), original)
  }

  test("RLike simplification is disabled by default") {
    assert(!conf.getConf(SQLConf.RLIKE_SIMPLIFICATION_ENABLED))

    def checkDisabled(): Unit = {
      val alternation = testRelation.where(RLike(input, Literal("foo|bar"))).analyze
      comparePlans(Optimize.execute(alternation), alternation)

      val nullPattern = testRelation.where(RLike(input, Literal(null, StringType))).analyze
      comparePlans(Optimize.execute(nullPattern), nullPattern)
    }

    checkDisabled()
    withSQLConf(SQLConf.RLIKE_SIMPLIFICATION_ENABLED.key -> "false") {
      checkDisabled()
    }
  }

  test("simplify a literal and redundant dot-star wrappers") {
    Seq("foo", ".*foo", "foo.*", ".*foo.*", "(.*foo.*)", "((foo))").foreach { pattern =>
      checkSimplified(pattern, Contains(input, Literal("foo")))
    }
  }

  test("simplify literal alternatives into balanced Contains predicates") {
    checkSimplified(
      ".*(foo|bar).*",
      Or(Contains(input, Literal("foo")), Contains(input, Literal("bar"))))
    checkSimplified(
      "(foo|bar|baz)",
      Or(
        Or(Contains(input, Literal("foo")), Contains(input, Literal("bar"))),
        Contains(input, Literal("baz"))))
    checkSimplified(
      "(foo|bar|baz|qux)",
      Or(
        Or(Contains(input, Literal("foo")), Contains(input, Literal("bar"))),
        Or(Contains(input, Literal("baz")), Contains(input, Literal("qux")))))
  }

  test("simplify a leading or trailing dot-star on one alternative") {
    val expected = Or(Contains(input, Literal("foo")), Contains(input, Literal("bar")))
    Seq("foo|bar.*", ".*foo|bar", "foo.*|bar", "foo.*|bar.*").foreach { pattern =>
      checkSimplified(pattern, expected)
    }
  }

  // scalastyle:off nonascii
  test("simplify multibyte literal alternatives") {
    checkSimplified(
      "\u03b1|\u03b2",
      Or(Contains(input, Literal("\u03b1")), Contains(input, Literal("\u03b2"))))
  }
  // scalastyle:on nonascii

  // scalastyle:off nonascii
  test("simplify Chinese literal alternatives") {
    // BMP Han characters are single UTF-16 literals. The replacement must agree
    // with RLIKE find() on both the pattern and the input text.
    val simplified = Or(
      Contains(input, Literal("北京")),
      Contains(input, Literal("上海")))
    checkSimplified("北京|上海", simplified)
    checkSimplified(".*(北京|上海).*", simplified)

    val bound = BoundReference(0, StringType, nullable = true)
    val original = RLike(bound, Literal("北京|上海"))
    val rewritten = Or(
      Contains(bound, Literal("北京")),
      Contains(bound, Literal("上海")))
    Seq(
      "北京市",
      "去上海",
      "广州",
      "北京上海",
      "",
      "foo",
      null).foreach { value =>
      val row = create_row(value)
      checkEvaluation(rewritten, original.eval(row), row)
    }
  }
  // scalastyle:on nonascii

  test("do not simplify regex metacharacters or anchors") {
    // `a*` and `a?` match every string under find(); `*foo` is invalid.
    // `foo.*bar` still contains `.`, so it does not pin `*` or `?`.
    Seq("[a-z]", "a.b", "a+", "foo.*bar", "a{b,c}", "^foo", "foo$", "^foo$",
      "a*", "a?", "*foo").foreach(checkNotSimplified)
  }

  test("do not simplify escapes, empty alternatives, or inner groups") {
    Seq("\\d", "a\\.b", "\\1", "foo|", "|foo", "", ".*", ".*|foo", "foo|.*|bar",
      "(foo|(bar))", "(foo)(bar)").foreach(checkNotSimplified)
  }

  test("do not simplify one unpaired grouping character") {
    // A paired pattern still fails when only its opener is rejected.
    Seq("a{b", "a}b", "[a", "a]", "(foo", "foo)").foreach(checkNotSimplified)
  }

  test("do not simplify a non-literal pattern") {
    val relation = LocalRelation($"a".string, $"b".string)
    val original = relation.where(RLike(relation.output.head, relation.output(1))).analyze
    withRuleEnabled {
      comparePlans(Optimize.execute(original), original)
    }
  }

  test("fold a null pattern only when enabled") {
    val original = testRelation.where(RLike(input, Literal(null, StringType))).analyze
    withRuleEnabled {
      comparePlans(
        Optimize.execute(original),
        testRelation.where(Literal(null, BooleanType)).analyze)
    }
  }

  test("do not duplicate a nondeterministic input across alternatives") {
    val nondeterministicInput = Cast(Rand(0), StringType)
    val original = testRelation.where(RLike(nondeterministicInput, Literal("foo|bar"))).analyze
    withRuleEnabled {
      comparePlans(Optimize.execute(original), original)
      comparePlans(
        Optimize.execute(testRelation.where(RLike(nondeterministicInput, Literal("foo"))).analyze),
        testRelation.where(Contains(nondeterministicInput, Literal("foo"))).analyze)
    }
  }

  test("do not simplify a non-binary collation") {
    val lcase = StringType("UTF8_LCASE")
    val relation = LocalRelation(AttributeReference("a", lcase)())
    val original = relation.where(
      RLike(relation.output.head, Literal.create("i", lcase))).analyze
    withRuleEnabled {
      comparePlans(Optimize.execute(original), original)
    }
  }

  test("fold a null literal pattern for a non-binary collation") {
    val lcase = StringType("UTF8_LCASE")
    val relation = LocalRelation(AttributeReference("a", lcase)())
    val original = relation.where(
      RLike(relation.output.head, Literal.create(null, lcase))).analyze
    withRuleEnabled {
      comparePlans(
        Optimize.execute(original),
        relation.where(Literal(null, BooleanType)).analyze)
    }
  }
}
