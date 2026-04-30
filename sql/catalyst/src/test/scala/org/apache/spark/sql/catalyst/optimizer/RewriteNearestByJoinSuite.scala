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
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, CreateStruct, Inline, Literal, MonotonicallyIncreasingID}
import org.apache.spark.sql.catalyst.expressions.aggregate.{First, MaxMinByK}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, NearestByDistance, NearestBySimilarity, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Generate, Join, JoinHint, LocalRelation, NearestByJoin, Project}

class RewriteNearestByJoinSuite extends PlanTest {

  private def expectedRewrite(
      left: LocalRelation,
      right: LocalRelation,
      numResults: Int,
      ranking: org.apache.spark.sql.catalyst.expressions.Expression,
      reverse: Boolean,
      outer: Boolean) = {
    val qidAlias = Alias(MonotonicallyIncreasingID(), "__qid")()
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
      AttributeReference(a.name, a.dataType, nullable = true)(qualifier = a.qualifier)
    }
    Generate(
      Inline(matchesAlias.toAttribute),
      unrequiredChildIndex = Seq(aggregate.output.indexOf(matchesAlias.toAttribute)),
      outer = outer,
      qualifier = None,
      generatorOutput = generatorOutput,
      child = aggregate)
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

    comparePlans(rewritten, expected, checkAnalysis = false)
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

    comparePlans(rewritten, expected, checkAnalysis = false)
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

    comparePlans(rewritten, expected, checkAnalysis = false)
  }
}
