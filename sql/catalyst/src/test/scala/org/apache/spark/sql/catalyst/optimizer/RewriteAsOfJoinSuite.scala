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
import org.apache.spark.sql.catalyst.expressions.{CreateStruct, Expression, GetStructField, If, Literal, OuterReference, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.MinBy
import org.apache.spark.sql.catalyst.plans.{AsOfJoinDirection, Inner, JoinType, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, LocalRelation, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf

class RewriteAsOfJoinSuite extends PlanTest {

  private val left = LocalRelation($"a".int, $"b".int, $"c".int)
  private val right = LocalRelation($"a".int, $"b".int, $"d".int)

  // Builds the plan RewriteAsOfJoin should produce: per left row, a correlated scalar subquery
  // picks the nearest matching right row via MIN_BY(struct(right.*), orderExpression), then
  // projects the right columns back out. INNER drops non-matches via `__right__ IS NOT NULL`;
  // LEFT OUTER keeps them. Only `filter` and `orderExpression` vary per test.
  private def expectedRewrite(
      filter: Expression,
      orderExpression: Expression,
      joinType: JoinType): LogicalPlan = {
    val rightStruct = CreateStruct(right.output)
    val nearestRight = MinBy(rightStruct, orderExpression)
      .toAggregateExpression().as("__nearest_right__")

    val scalarSubquery = left.select(
      left.output :+ ScalarSubquery(
        right.where(filter).groupBy()(nearestRight),
        left.output).as("__right__"): _*)
    val withNullFilter = joinType match {
      case LeftOuter => scalarSubquery
      case _ => scalarSubquery.where(scalarSubquery.output.last.isNotNull)
    }
    withNullFilter.select(left.output ++ right.output.zipWithIndex.map {
      case (attr, idx) => GetStructField(scalarSubquery.output.last, idx).as(attr.name)
    }: _*)
  }

  test("simple") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val correctAnswer = expectedRewrite(
      filter = OuterReference(left.output(0)) >= right.output(0),
      orderExpression = OuterReference(left.output(0)) - right.output(0),
      joinType = Inner)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("condition") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0),
      Some(left.output(1) === right.output(1)), Inner,
      tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    // The join condition is AND-ed in front of the as-of condition.
    val correctAnswer = expectedRewrite(
      filter = OuterReference(left.output(1)) === right.output(1) &&
        OuterReference(left.output(0)) >= right.output(0),
      orderExpression = OuterReference(left.output(0)) - right.output(0),
      joinType = Inner)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("left outer") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, LeftOuter,
      tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    // LEFT OUTER keeps non-matching left rows, so the `IS NOT NULL` filter is omitted.
    val correctAnswer = expectedRewrite(
      filter = OuterReference(left.output(0)) >= right.output(0),
      orderExpression = OuterReference(left.output(0)) - right.output(0),
      joinType = LeftOuter)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("tolerance") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = Some(1), allowExactMatches = true, direction = AsOfJoinDirection("backward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val correctAnswer = expectedRewrite(
      filter = OuterReference(left.output(0)) >= right.output(0) &&
        right.output(0) >= OuterReference(left.output(0)) - 1,
      orderExpression = OuterReference(left.output(0)) - right.output(0),
      joinType = Inner)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("allowExactMatches = false") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, LeftOuter,
      tolerance = None, allowExactMatches = false, direction = AsOfJoinDirection("backward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val correctAnswer = expectedRewrite(
      filter = OuterReference(left.output(0)) > right.output(0),
      orderExpression = OuterReference(left.output(0)) - right.output(0),
      joinType = LeftOuter)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("tolerance & allowExactMatches = false") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = Some(1), allowExactMatches = false, direction = AsOfJoinDirection("backward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val correctAnswer = expectedRewrite(
      filter = OuterReference(left.output(0)) > right.output(0) &&
        right.output(0) > OuterReference(left.output(0)) - 1,
      orderExpression = OuterReference(left.output(0)) - right.output(0),
      joinType = Inner)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("direction = forward") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("forward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    // Forward flips the comparison (`<=`) and the ordering distance (right - left).
    val correctAnswer = expectedRewrite(
      filter = OuterReference(left.output(0)) <= right.output(0),
      orderExpression = right.output(0) - OuterReference(left.output(0)),
      joinType = Inner)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("direction = forward & allowExactMatches = false") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = None, allowExactMatches = false, direction = AsOfJoinDirection("forward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val correctAnswer = expectedRewrite(
      filter = OuterReference(left.output(0)) < right.output(0),
      orderExpression = right.output(0) - OuterReference(left.output(0)),
      joinType = Inner)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("tolerance & direction = forward") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = Some(1), allowExactMatches = true, direction = AsOfJoinDirection("forward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val correctAnswer = expectedRewrite(
      filter = OuterReference(left.output(0)) <= right.output(0) &&
        right.output(0) <= OuterReference(left.output(0)) + 1,
      orderExpression = right.output(0) - OuterReference(left.output(0)),
      joinType = Inner)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("tolerance & allowExactMatches = false & direction = forward") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = Some(1), allowExactMatches = false, direction = AsOfJoinDirection("forward"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val correctAnswer = expectedRewrite(
      filter = OuterReference(left.output(0)) < right.output(0) &&
        right.output(0) < OuterReference(left.output(0)) + 1,
      orderExpression = right.output(0) - OuterReference(left.output(0)),
      joinType = Inner)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("direction = nearest") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("nearest"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    // nearest + allowExactMatches + no tolerance: no match constraint, so the condition is
    // `true`. The ordering picks the smallest absolute distance in either direction.
    val correctAnswer = expectedRewrite(
      filter = Literal.TrueLiteral,
      orderExpression = If(OuterReference(left.output(0)) > right.output(0),
        OuterReference(left.output(0)) - right.output(0),
        right.output(0) - OuterReference(left.output(0))),
      joinType = Inner)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("allowExactMatches = false & direction = nearest") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = None, allowExactMatches = false, direction = AsOfJoinDirection("nearest"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    // nearest without tolerance and without exact matches: the only constraint is that the
    // right key differs from the left key (AsOfJoin.makeAsOfCond `case (false, Nearest)`).
    val correctAnswer = expectedRewrite(
      filter = !(OuterReference(left.output(0)) === right.output(0)),
      orderExpression = If(OuterReference(left.output(0)) > right.output(0),
        OuterReference(left.output(0)) - right.output(0),
        right.output(0) - OuterReference(left.output(0))),
      joinType = Inner)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("tolerance & allowExactMatches = false & direction = nearest") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = Some(1), allowExactMatches = false, direction = AsOfJoinDirection("nearest"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    val correctAnswer = expectedRewrite(
      filter = (!(OuterReference(left.output(0)) === right.output(0))) &&
        ((right.output(0) > OuterReference(left.output(0)) - 1) &&
          (right.output(0) < OuterReference(left.output(0)) + 1)),
      orderExpression = If(OuterReference(left.output(0)) > right.output(0),
        OuterReference(left.output(0)) - right.output(0),
        right.output(0) - OuterReference(left.output(0))),
      joinType = Inner)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("tolerance & direction = nearest") {
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = Some(1), allowExactMatches = true, direction = AsOfJoinDirection("nearest"))

    val rewritten = RewriteAsOfJoin(query.analyze)

    // nearest + allowExactMatches + tolerance intentionally drops the `true` base condition and
    // keeps only the two-sided tolerance band (AsOfJoin.makeAsOfCond `case (true, Nearest)`).
    val correctAnswer = expectedRewrite(
      filter = right.output(0) >= OuterReference(left.output(0)) - 1 &&
        right.output(0) <= OuterReference(left.output(0)) + 1,
      orderExpression = If(OuterReference(left.output(0)) > right.output(0),
        OuterReference(left.output(0)) - right.output(0),
        right.output(0) - OuterReference(left.output(0))),
      joinType = Inner)

    comparePlans(rewritten, correctAnswer, checkAnalysis = false)
  }

  test("no rewrite when the node requires the sort-merge as-of join operator") {
    // A node flagged `requiresSortMergeAsOfJoin = true` (e.g. from a SQL MATCH_CONDITION) is
    // left untouched so the sort-merge physical operator handles it instead.
    val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))
      .copy(requiresSortMergeAsOfJoin = true)
    val analyzed = query.analyze

    // The flag must survive analysis, so it, not a dropped flag, is what makes this a no-op.
    assert(analyzed.collectFirst { case a: AsOfJoin => a }.exists(_.requiresSortMergeAsOfJoin),
      "expected the analyzed plan to still carry requiresSortMergeAsOfJoin = true")

    val rewritten = RewriteAsOfJoin(analyzed)

    comparePlans(rewritten, analyzed)
  }

  test("no rewrite when the sort-merge as-of join operator is enabled by config") {
    // With the config on, the sort-merge operator is enabled globally, so even a plain node
    // (flag off) is left intact rather than rewritten to a subquery.
    withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true") {
      val query = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
        tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))
      val analyzed = query.analyze

      comparePlans(RewriteAsOfJoin(analyzed), analyzed)
    }
  }

  test("references above the join are remapped to the rewritten output") {
    // A Project above the join selects a right-side column, which the rewrite gives a fresh
    // exprId (a GetStructField alias). The parent reference must be remapped onto it, or the
    // plan is left with a dangling reference. Other tests root at AsOfJoin and never hit this.
    val join = AsOfJoin(left, right, left.output(0), right.output(0), None, Inner,
      tolerance = None, allowExactMatches = true, direction = AsOfJoinDirection("backward"))
    val originalRightExprId = join.output.last.exprId
    val query = join.select(join.output.last).analyze

    val rewritten = RewriteAsOfJoin(query)

    rewritten.foreach { node =>
      assert(node.missingInput.isEmpty,
        s"${node.nodeName} has dangling references: ${node.missingInput}")
    }
    assert(!rewritten.references.exists(_.exprId == originalRightExprId),
      "expected the parent projection to be remapped off the original AsOfJoin output")
  }
}
