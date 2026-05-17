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
import org.apache.spark.sql.catalyst.expressions.{Expression, If, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class RewriteCountDistinctConditionalSuite extends PlanTest {

  val testRelation = LocalRelation(
    Symbol("a").int, Symbol("b").int, Symbol("c").int, Symbol("d").string)

  private def countDistinctIf(cond: Expression, base: Expression): Expression = {
    Count(If(cond, base, Literal(null))).toAggregateExpression(isDistinct = true)
  }

  private def countDistinctCaseWhen(cond: Expression, base: Expression): Expression = {
    val caseWhen = org.apache.spark.sql.catalyst.expressions.CaseWhen(
      Seq((cond, base)),
      None)
    Count(caseWhen).toAggregateExpression(isDistinct = true)
  }

  private def countDistinctCaseWhenElseNull(cond: Expression, base: Expression): Expression = {
    val caseWhen = org.apache.spark.sql.catalyst.expressions.CaseWhen(
      Seq((cond, base)),
      Some(Literal(null)))
    Count(caseWhen).toAggregateExpression(isDistinct = true)
  }

  test("disabled by default") {
    val input = testRelation
      .groupBy(Symbol("a"))(
        countDistinctIf(Symbol("b") > 1, Symbol("c")).as("cnt1"))
      .analyze

    val optimized = RewriteCountDistinctConditional(input)
    comparePlans(optimized, input)
  }

  test("rewrite COUNT(DISTINCT IF(cond, col, NULL)) to COUNT(DISTINCT col) FILTER") {
    val input = testRelation
      .groupBy(Symbol("a"))(
        countDistinctIf(Symbol("b") > 1, Symbol("c")).as("cnt1"))
      .analyze

    withSQLConf(SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
      val optimized = RewriteCountDistinctConditional(input)

      val expected = testRelation
        .groupBy(Symbol("a"))(
          countDistinctWithFilter(Symbol("b") > 1, Symbol("c")).as("cnt1"))
        .analyze

      comparePlans(optimized, expected)
    }
  }

  test("rewrite COUNT(DISTINCT CASE WHEN cond THEN col END) to COUNT(DISTINCT col) FILTER") {
    val input = testRelation
      .groupBy(Symbol("a"))(
        countDistinctCaseWhen(Symbol("b") > 1, Symbol("c")).as("cnt1"))
      .analyze

    withSQLConf(SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
      val optimized = RewriteCountDistinctConditional(input)

      val expected = testRelation
        .groupBy(Symbol("a"))(
          countDistinctWithFilter(Symbol("b") > 1, Symbol("c")).as("cnt1"))
        .analyze

      comparePlans(optimized, expected)
    }
  }

  test("rewrite COUNT(DISTINCT CASE WHEN cond THEN col ELSE NULL END)") {
    val input = testRelation
      .groupBy(Symbol("a"))(
        countDistinctCaseWhenElseNull(Symbol("b") > 1, Symbol("c")).as("cnt1"))
      .analyze

    withSQLConf(SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
      val optimized = RewriteCountDistinctConditional(input)

      val expected = testRelation
        .groupBy(Symbol("a"))(
          countDistinctWithFilter(Symbol("b") > 1, Symbol("c")).as("cnt1"))
        .analyze

      comparePlans(optimized, expected)
    }
  }

  test("multiple conditional distinct counts collapse to single distinct group") {
    val input = testRelation
      .groupBy(Symbol("a"))(
        countDistinctIf(Symbol("b") > 1, Symbol("c")).as("cnt1"),
        countDistinctIf(Symbol("b") > 2, Symbol("c")).as("cnt2"),
        countDistinctIf(Symbol("b") > 3, Symbol("c")).as("cnt3"))
      .analyze

    withSQLConf(SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
      val optimized = RewriteCountDistinctConditional(input)

      val expected = testRelation
        .groupBy(Symbol("a"))(
          countDistinctWithFilter(Symbol("b") > 1, Symbol("c")).as("cnt1"),
          countDistinctWithFilter(Symbol("b") > 2, Symbol("c")).as("cnt2"),
          countDistinctWithFilter(Symbol("b") > 3, Symbol("c")).as("cnt3"))
        .analyze

      comparePlans(optimized, expected)

      // Verify RewriteDistinctAggregates sees only 1 distinct group
      val rewritten = RewriteDistinctAggregates(optimized)
      // Should be rewritten (not same as input) because there are now multiple
      // distinct expressions with the same base column
      assert(rewritten != optimized)
    }
  }

  test("do not rewrite IF with non-null else branch") {
    val input = testRelation
      .groupBy(Symbol("a"))(
        Count(If(Symbol("b") > 1, Symbol("c"), Literal(0, IntegerType)))
          .toAggregateExpression(isDistinct = true)
          .as("cnt1"))
      .analyze

    withSQLConf(SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
      val optimized = RewriteCountDistinctConditional(input)
      comparePlans(optimized, input)
    }
  }

  test("do not rewrite non-distinct COUNT") {
    val input = testRelation
      .groupBy(Symbol("a"))(
        Count(If(Symbol("b") > 1, Symbol("c"), Literal(null, IntegerType)))
          .toAggregateExpression(isDistinct = false)
          .as("cnt1"))
      .analyze

    withSQLConf(SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
      val optimized = RewriteCountDistinctConditional(input)
      comparePlans(optimized, input)
    }
  }

  test("do not rewrite when FILTER already exists") {
    val input = testRelation
      .groupBy(Symbol("a"))(
        Count(If(Symbol("b") > 1, Symbol("c"), Literal(null, IntegerType)))
          .toAggregateExpression(isDistinct = true, filter = Some(Symbol("d") === "x"))
          .as("cnt1"))
      .analyze

    withSQLConf(SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
      val optimized = RewriteCountDistinctConditional(input)
      comparePlans(optimized, input)
    }
  }

  test("do not rewrite multi-branch CASE WHEN") {
    val caseWhen = new org.apache.spark.sql.catalyst.expressions.CaseWhen(
      Seq(
        (Symbol("b") > Literal(1), Symbol("c")),
        (Symbol("b") > Literal(2), Symbol("a"))),
      Some(Literal(null)))
    val input = testRelation
      .groupBy(Symbol("a"))(
        Count(caseWhen).toAggregateExpression(isDistinct = true).as("cnt1"))
      .analyze

    withSQLConf(SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
      val optimized = RewriteCountDistinctConditional(input)
      comparePlans(optimized, input)
    }
  }

  test("do not rewrite SUM(DISTINCT IF(...))") {
    val input = testRelation
      .groupBy(Symbol("a"))(
        org.apache.spark.sql.catalyst.expressions.aggregate.Sum(
          If(Symbol("b") > 1, Symbol("c"), Literal(null, IntegerType)))
          .toAggregateExpression(isDistinct = true)
          .as("sum1"))
      .analyze

    withSQLConf(SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
      val optimized = RewriteCountDistinctConditional(input)
      comparePlans(optimized, input)
    }
  }
}
