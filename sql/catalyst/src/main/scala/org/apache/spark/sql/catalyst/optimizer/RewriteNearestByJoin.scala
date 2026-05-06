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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

/**
 * Replaces a logical [[NearestByJoin]] operator with a `Generate(Inline(...))` over an
 * `Aggregate` that tags each left row with a unique id, cross-joins with the right side, and
 * groups by the unique id to compute the top-K matches via `MAX_BY`/`MIN_BY` (K-overload).
 *
 * Input Pseudo-Query:
 * {{{
 *    SELECT * FROM left [INNER | LEFT OUTER] JOIN right
 *      {APPROX | EXACT} NEAREST k BY {DISTANCE | SIMILARITY} expr
 * }}}
 *
 * Rewritten Plan (SIMILARITY, INNER join type):
 * {{{
 *    Project [left.*, right.*]
 *      +- Generate inline(_matches), [N], outer=false, [right.col1, right.col2, ...]
 *         +- Aggregate [__qid],
 *              [first(left.col0) AS left.col0, ..., first(left.colN-1) AS left.colN-1,
 *               max_by(struct(right.*), expr, k) AS _matches]
 *             +- Join LeftOuter
 *                :- Project [left.*, uuid() AS __qid]
 *                :  +- left
 *                +- right
 * }}}
 *
 * For `DISTANCE`, `MIN_BY` is used instead of `MAX_BY`. For `LEFT OUTER`, the `Generate` is
 * constructed with `outer = true` so left rows with no matches (empty/null `_matches`) are
 * preserved with `NULL` right-side columns.
 *
 * The matches in `_matches` are produced by `MaxMinByK` ordered by the ranking value: best
 * match first (largest ranking value for `SIMILARITY`, smallest for `DISTANCE`). `Inline`
 * preserves array order, so the K rows emitted per left row appear best-first in the output
 * of this rule. (Downstream operators may reorder.)
 *
 * If `rankingExpression` is nondeterministic (legal only under `APPROX`), an extra
 * `Project` is inserted above the `Join` to materialize the value as `__ranking__`. The
 * standard projection machinery runs `Nondeterministic.initialize(partitionIndex)` on every
 * nondeterministic descendant before any value is evaluated, so `MaxMinByK` only ever sees a
 * plain `AttributeReference` and never evaluates a nondeterministic expression directly.
 *
 * Unlike [[RewriteAsOfJoin]], which uses a correlated scalar subquery, this rule materializes
 * the cross product directly. A scalar subquery returns a single value per left row, so it
 * cannot carry K matches without an array-valued subquery + `Generate(Inline(...))` -- which
 * collapses back to the same cross product after decorrelation. The aggregate-then-inline form
 * makes the intended shape explicit and avoids round-tripping through subquery decorrelation.
 */
object RewriteNearestByJoin extends Rule[LogicalPlan] {
  private lazy val random = new scala.util.Random()

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case j @ NearestByJoin(left, right, joinType, _, numResults, rankingExpression, direction) =>
      // 1. Tag each left row with a unique id so that rows from the same left row can later be
      //    grouped together after the cross-join with `right`.
      val qidAlias = Alias(Uuid(Some(random.nextLong())), "__qid")()
      val taggedLeft = Project(left.output :+ qidAlias, left)
      val qidAttr = qidAlias.toAttribute

      // 2. LEFT OUTER-join the tagged left with right (no join condition). LEFT OUTER
      //    (rather than INNER) preserves left rows even when `right` is empty, so that a
      //    `LEFT OUTER NEAREST BY` query still returns those rows with `NULL` right-side
      //    columns after the aggregate + inline below. When `right` is non-empty every left
      //    row already has right-row pairings, so LEFT OUTER and INNER are equivalent.
      //
      //    This synthetic join is an unconditioned cross-product, so `NEAREST BY` queries
      //    are subject to `CheckCartesianProducts` and will be rejected when the user has
      //    set `spark.sql.crossJoin.enabled = false`. That is intentional: if the user has
      //    opted out of cross-products, the NEAREST BY rewrite -- which is itself a bounded
      //    cross-product today -- should not silently bypass that choice.
      val join = Join(taggedLeft, right, LeftOuter, None, JoinHint.NONE)

      val (aggInput, rankingForAgg) = if (!rankingExpression.deterministic) {
        val rankingAlias = Alias(rankingExpression, "__ranking__")()
        Project(join.output :+ rankingAlias, join) -> rankingAlias.toAttribute
      } else {
        join -> rankingExpression
      }

      // 4. Aggregate grouped by `__qid`:
      //      - first(col) for every left column so it flows to the output.
      //      - max_by/min_by(struct(right.*), ranking, k) as `_matches`.
      //    The ranking expression references left and right columns directly; no outer
      //    reference is needed because both sides are present in the joined input.
      val rightStruct = CreateStruct(right.output)
      // reverse = true  -> MIN_BY (smallest ranking value first, for DISTANCE)
      // reverse = false -> MAX_BY (largest ranking value first, for SIMILARITY)
      val reverse = direction match {
        case NearestByDistance => true
        case NearestBySimilarity => false
      }
      val topK = MaxMinByK(
        rightStruct,
        rankingForAgg,
        Literal(numResults),
        reverse = reverse).toAggregateExpression()
      val matchesAlias = Alias(topK, "__nearest_matches__")()

      // Carry left columns through with `First`. Within a `__qid` group every row has the same
      // left values (each group corresponds to one left row), so `First` is effectively a no-op.
      // We use `First` rather than adding all left columns to the GROUP BY because grouping by
      // `__qid` alone keeps the shuffle key small.
      val firstLeftAggs = left.output.map { attr =>
        Alias(
          First(attr, ignoreNulls = false).toAggregateExpression(),
          attr.name)(exprId = attr.exprId, qualifier = attr.qualifier)
      }
      val aggregate = Aggregate(Seq(qidAttr), firstLeftAggs :+ matchesAlias, aggInput)

      // 4. Generate inline(_matches) expands the K-element array into K rows, exposing each
      //    struct field as a top-level column. `outer = true` for LEFT OUTER preserves the
      //    left row with NULL right columns when there are no matches. Preserving the right
      //    side's `ExprId`s in `generatorOutput` (rather than allocating fresh ones) keeps
      //    `generate.output` byte-for-byte equivalent to `j.output` -- which already used
      //    those ExprIds with `nullable = true` -- so parent-operator references continue to
      //    resolve naturally and the rule can use plain `transformUp` without an attrMapping.
      val generatorOutput = right.output.map { a =>
        AttributeReference(a.name, a.dataType, nullable = true, a.metadata)(
          exprId = a.exprId, qualifier = a.qualifier)
      }
      val generate = Generate(
        Inline(matchesAlias.toAttribute),
        unrequiredChildIndex = Seq(aggregate.output.indexOf(matchesAlias.toAttribute)),
        outer = joinType == LeftOuter,
        qualifier = None,
        generatorOutput = generatorOutput,
        child = aggregate)

      // 5. Final `Project` pinning the output schema to `NearestByJoin.output`.
      Project(j.output, generate)
  }
}
