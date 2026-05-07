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
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, CreateStruct, Inline, Literal, Rand, Uuid}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, First, MaxMinByK}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, NearestByDistance, NearestBySimilarity, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Generate, Join, JoinHint, LocalRelation, NearestByJoin, Project}
import org.apache.spark.sql.types.IntegerType

class RewriteNearestByJoinSuite extends PlanTest {

  // The rewrite synthesizes `Uuid(Some(<random>))` for `__qid`, whose seed is fresh per call;
  // expected plans below use `Uuid(Some(0L))`, and we normalize the actual plan's `Uuid`
  // seeds to 0L before `comparePlans` so the structural shape is the only thing being
  // compared, not the (necessarily different) random seed values.
  private def normalizeUuidSeed(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan)
      : org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
    plan.transformAllExpressions { case _: Uuid => Uuid(Some(0L)) }

  private def expectedRewrite(
      left: LocalRelation,
      right: LocalRelation,
      numResults: Int,
      ranking: org.apache.spark.sql.catalyst.expressions.Expression,
      reverse: Boolean,
      outer: Boolean) = {
    val qidAlias = Alias(Uuid(Some(0L)), "__qid")()
    val taggedLeft = Project(left.output :+ qidAlias, left)
    val join = Join(taggedLeft, right, LeftOuter, None, JoinHint.NONE)

    val rightStruct = CreateStruct(right.output)
    val topKAgg = MaxMinByK(
      rightStruct, ranking, Literal(numResults), reverse = reverse)
      .toAggregateExpression()
    val matchesAlias = Alias(topKAgg, "__nearest_matches__")()
    val firstLeftAggs = left.output.map { attr =>
      Alias(
        First(attr, ignoreNulls = false).toAggregateExpression(),
        attr.name)(exprId = attr.exprId, qualifier = attr.qualifier)
    }
    val aggregate = Aggregate(
      Seq(qidAlias.toAttribute), firstLeftAggs :+ matchesAlias, join)

    val generatorOutput = right.output.map { a =>
      AttributeReference(a.name, a.dataType, nullable = true)(
        exprId = a.exprId, qualifier = a.qualifier)
    }
    val generate = Generate(
      Inline(matchesAlias.toAttribute),
      unrequiredChildIndex = Seq(aggregate.output.indexOf(matchesAlias.toAttribute)),
      outer = outer,
      qualifier = None,
      generatorOutput = generatorOutput,
      child = aggregate)
    // Mirror the rewrite's final Project that constrains the output schema to
    // `NearestByJoin.output` (left and right widened to nullable).
    val expectedOutput =
      left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
    Project(expectedOutput, generate)
  }

  test("similarity, inner, k=5") {
    val left = LocalRelation($"a".int, $"b".int)
    val right = LocalRelation($"x".int, $"y".int)
    val query = NearestByJoin(
      left, right, Inner, approx = true, numResults = 5,
      rankingExpression = left.output(0) + right.output(0),
      direction = NearestBySimilarity)

    val rewritten = RewriteNearestByJoin(query.analyze)
    val expected = expectedRewrite(
      left, right, 5,
      ranking = left.output(0) + right.output(0),
      reverse = false, outer = false)

    comparePlans(normalizeUuidSeed(rewritten), expected, checkAnalysis = false)
  }

  test("distance, inner, k=3") {
    val left = LocalRelation($"a".int, $"b".int)
    val right = LocalRelation($"x".int, $"y".int)
    val query = NearestByJoin(
      left, right, Inner, approx = true, numResults = 3,
      rankingExpression = left.output(0) - right.output(0),
      direction = NearestByDistance)

    val rewritten = RewriteNearestByJoin(query.analyze)
    val expected = expectedRewrite(
      left, right, 3,
      ranking = left.output(0) - right.output(0),
      reverse = true, outer = false)

    comparePlans(normalizeUuidSeed(rewritten), expected, checkAnalysis = false)
  }

  test("similarity, left outer, k=1") {
    val left = LocalRelation($"a".int, $"b".int)
    val right = LocalRelation($"x".int, $"y".int)
    val query = NearestByJoin(
      left, right, LeftOuter, approx = true, numResults = 1,
      rankingExpression = left.output(0) + right.output(0),
      direction = NearestBySimilarity)

    val rewritten = RewriteNearestByJoin(query.analyze)
    val expected = expectedRewrite(
      left, right, 1,
      ranking = left.output(0) + right.output(0),
      reverse = false, outer = true)

    comparePlans(normalizeUuidSeed(rewritten), expected, checkAnalysis = false)
  }

  test("distance, left outer, k=2") {
    val left = LocalRelation($"a".int, $"b".int)
    val right = LocalRelation($"x".int, $"y".int)
    val query = NearestByJoin(
      left, right, LeftOuter, approx = true, numResults = 2,
      rankingExpression = left.output(0) - right.output(0),
      direction = NearestByDistance)

    val rewritten = RewriteNearestByJoin(query.analyze)
    val expected = expectedRewrite(
      left, right, 2,
      ranking = left.output(0) - right.output(0),
      reverse = true, outer = true)

    comparePlans(normalizeUuidSeed(rewritten), expected, checkAnalysis = false)
  }

  test("EXACT (approx = false) produces the same rewrite as APPROX") {
    // Locks in the current invariant that APPROX and EXACT lower through the same
    // brute-force rewrite. If a future change diverges them (e.g. an APPROX-only
    // indexed-ANN strategy lands), this test fails and forces an intentional update.
    val left = LocalRelation($"a".int, $"b".int)
    val right = LocalRelation($"x".int, $"y".int)
    val query = NearestByJoin(
      left, right, Inner, approx = false, numResults = 5,
      rankingExpression = left.output(0) + right.output(0),
      direction = NearestBySimilarity)

    val rewritten = RewriteNearestByJoin(query.analyze)
    val expected = expectedRewrite(
      left, right, 5,
      ranking = left.output(0) + right.output(0),
      reverse = false, outer = false)

    comparePlans(normalizeUuidSeed(rewritten), expected, checkAnalysis = false)
  }

  test("k = 1 (lower boundary)") {
    val left = LocalRelation($"a".int, $"b".int)
    val right = LocalRelation($"x".int, $"y".int)
    val query = NearestByJoin(
      left, right, Inner, approx = true, numResults = 1,
      rankingExpression = left.output(0) + right.output(0),
      direction = NearestBySimilarity)

    val rewritten = RewriteNearestByJoin(query.analyze)
    val expected = expectedRewrite(
      left, right, 1,
      ranking = left.output(0) + right.output(0),
      reverse = false, outer = false)

    comparePlans(normalizeUuidSeed(rewritten), expected, checkAnalysis = false)
  }

  test("k = NearestByJoin.MaxNumResults (upper boundary)") {
    val left = LocalRelation($"a".int, $"b".int)
    val right = LocalRelation($"x".int, $"y".int)
    val query = NearestByJoin(
      left, right, Inner, approx = true, numResults = NearestByJoin.MaxNumResults,
      rankingExpression = left.output(0) + right.output(0),
      direction = NearestBySimilarity)

    val rewritten = RewriteNearestByJoin(query.analyze)
    val expected = expectedRewrite(
      left, right, NearestByJoin.MaxNumResults,
      ranking = left.output(0) + right.output(0),
      reverse = false, outer = false)

    comparePlans(normalizeUuidSeed(rewritten), expected, checkAnalysis = false)
  }

  test("self-join: rewrite resolves duplicate ExprIds via DeduplicateRelations") {
    // Exercises the NearestByJoin arm in DeduplicateRelations. Without it, `.analyze` on
    // a self-join would leave the right side sharing ExprIds with the left and the
    // CheckAnalysis arm would throw an internal error.
    val t = LocalRelation($"a".int, $"b".int)
    val query = NearestByJoin(
      t, t, Inner, approx = true, numResults = 1,
      rankingExpression = t.output(0) + t.output(0),
      direction = NearestBySimilarity)

    val rewritten = RewriteNearestByJoin(query.analyze)
    val tDup = LocalRelation($"a".int, $"b".int)
    val expected = expectedRewrite(
      t, tDup, 1,
      ranking = t.output(0) + tDup.output(0),
      reverse = false, outer = false)

    comparePlans(normalizeUuidSeed(rewritten), expected, checkAnalysis = false)
  }

  test("APPROX with nondeterministic ranking pre-materializes via Project") {
    // Locks in the Project-injection shape: when the ranking expression is nondeterministic
    // (legal only under APPROX), the rewrite inserts a Project above the Join that aliases
    // the ranking value as `__ranking__`. MaxMinByK then sees a plain AttributeReference as
    // its ordering input. This relies on Projection's standard partition-aware initialization
    // to call `Rand.initialize` once per partition before any value is evaluated; otherwise
    // MaxMinByK would call `eval` on an uninitialized Rand and throw at runtime. If a future
    // optimizer change folds this Project away, this test fails and forces an intentional
    // update.
    val left = LocalRelation($"a".int, $"b".int)
    val right = LocalRelation($"x".int, $"y".int)
    val ranking = Rand(Literal(0L)) + right.output(0)
    val query = NearestByJoin(
      left, right, Inner, approx = true, numResults = 1,
      rankingExpression = ranking,
      direction = NearestBySimilarity)

    val rewritten = RewriteNearestByJoin(query.analyze)

    val agg = rewritten.collect { case a: Aggregate => a }.head
    assert(agg.child.isInstanceOf[Project],
      s"expected materializing Project above the Join when ranking is nondeterministic, " +
        s"got ${agg.child.getClass.getSimpleName}")
    val maxMinByK = agg.aggregateExpressions.collectFirst {
      case Alias(AggregateExpression(m: MaxMinByK, _, _, _, _), "__nearest_matches__") => m
    }.getOrElse(fail("expected MaxMinByK aggregate in the rewritten plan"))
    assert(maxMinByK.orderingExpr.isInstanceOf[AttributeReference],
      "ranking expression should be materialized as an attribute, not evaluated inside MaxMinByK")
    assert(maxMinByK.orderingExpr.asInstanceOf[AttributeReference].name == "__ranking__")
    assert(rewritten.exists(_.expressions.exists(_.exists(_.isInstanceOf[Rand]))),
      "Rand should still appear in the plan -- inside the materializing Project, not lost")
  }

  test("APPROX with deterministic ranking does NOT inject the materializing Project") {
    // Counterpart to the test above: confirms the Project-injection is gated on
    // `!rankingExpression.deterministic` so the deterministic path's plan shape is unchanged.
    val left = LocalRelation($"a".int, $"b".int)
    val right = LocalRelation($"x".int, $"y".int)
    val query = NearestByJoin(
      left, right, Inner, approx = true, numResults = 1,
      rankingExpression = left.output(0) + right.output(0),
      direction = NearestBySimilarity)

    val rewritten = RewriteNearestByJoin(query.analyze)
    val agg = rewritten.collect { case a: Aggregate => a }.head
    assert(agg.child.isInstanceOf[Join],
      s"expected Aggregate's child to be the Join directly when ranking is deterministic, " +
        s"got ${agg.child.getClass.getSimpleName}")
  }

  test("output declares both left- and right-side attributes nullable") {
    // The rewrite carries left columns through `First` aggregates (always nullable result type)
    // and right columns through `Inline` over `MaxMinByK`'s `ArrayType(.., containsNull = true)`
    // (every struct field becomes nullable). NearestByJoin.output must reflect both widenings
    // so the analyzed schema matches the optimized plan; otherwise cached / written outputs
    // would advertise a stricter nullability than the data actually carries.
    val left = LocalRelation(
      AttributeReference("a", IntegerType, nullable = false)(),
      AttributeReference("b", IntegerType, nullable = false)())
    val right = LocalRelation(
      AttributeReference("x", IntegerType, nullable = false)(),
      AttributeReference("y", IntegerType, nullable = false)())
    val query = NearestByJoin(
      left, right, Inner, approx = true, numResults = 1,
      rankingExpression = left.output(0) + right.output(0),
      direction = NearestBySimilarity)

    assert(left.output.forall(!_.nullable),
      "preconditions: left input attributes should start non-nullable")
    assert(right.output.forall(!_.nullable),
      "preconditions: right input attributes should start non-nullable")
    assert(query.output.forall(_.nullable),
      "NearestByJoin.output should declare every attribute nullable, regardless of the " +
        "nullability of the underlying inputs")
  }
}
