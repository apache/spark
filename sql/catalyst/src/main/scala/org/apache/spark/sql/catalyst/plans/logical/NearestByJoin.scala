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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, NearestByDirection, NearestByJoinValidation}
import org.apache.spark.sql.catalyst.trees.TreePattern._

object NearestByJoin {
  /** @see [[NearestByJoinValidation.MaxNumResults]] */
  val MaxNumResults: Int = NearestByJoinValidation.MaxNumResults
}

/**
 * A logical plan for a nearest-by top-K ranking join. For each row on the left side it returns
 * up to `numResults` rows from the right side ordered by `rankingExpression`:
 *   - `NearestByDistance`: smallest values of `rankingExpression` first.
 *   - `NearestBySimilarity`: largest values of `rankingExpression` first.
 *
 * The `approx` field records the user's APPROX/EXACT choice. Today both modes use the same
 * brute-force rewrite. The flag is preserved on the logical plan so that future indexed
 * approximate-nearest-neighbor strategies can fire only when `approx = true`, leaving EXACT
 * queries unaffected.
 *
 * @param left  The left (query) side of the join.
 * @param right The right (base) side of the join, against which each left row finds matches.
 * @param joinType  Must be `Inner` or `LeftOuter`. `Inner` drops left rows with no matches;
 *                  `LeftOuter` preserves them with `NULL` right-side columns.
 * @param approx  `true` for `APPROX` mode, `false` for `EXACT` mode. `APPROX` permits a
 *                nondeterministic `rankingExpression` and is the contract future indexed
 *                approximate-nearest-neighbor strategies key off; `EXACT` requires
 *                determinism (enforced by `CheckAnalysis`).
 * @param numResults  The K in top-K: the maximum number of right-side matches returned per
 *                    left row. Bounded above by `NearestByJoin.MaxNumResults`.
 * @param rankingExpression  Scalar expression evaluated per (left, right) pair. Must return
 *                           an orderable type. Rows are ranked by its value, with ordering
 *                           determined by `direction`.
 * @param direction  `NearestByDistance` (smaller is better) or `NearestBySimilarity` (larger
 *                   is better). Selects whether the rewrite uses `MIN_BY` or `MAX_BY`.
 */
case class NearestByJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    approx: Boolean,
    numResults: Int,
    rankingExpression: Expression,
    direction: NearestByDirection)
  extends BinaryNode with SupportsNonDeterministicExpression {

  require(Seq(Inner, LeftOuter).contains(joinType),
    s"Unsupported nearest-by join type $joinType")

  // `APPROX` mode permits a nondeterministic ranking expression (e.g. `rand()` for randomized
  // tie-breaking). `EXACT` mode requires determinism, and that requirement is enforced
  // separately by the `NEAREST_BY_JOIN.EXACT_WITH_NONDETERMINISTIC_EXPRESSION` arm in
  // `CheckAnalysis`. Returning `approx` from this override is what lets APPROX queries pass
  // the generic `INVALID_NON_DETERMINISTIC_EXPRESSIONS` check that fires on operators not on
  // the analyzer's whitelist.
  override def allowNonDeterministicExpression: Boolean = approx

  // Both left- and right-side attributes are declared nullable to match the schema produced
  // by `RewriteNearestByJoin`. Right-side attributes are widened because the rewrite
  // materializes them through `Inline` over `MaxMinByK`'s `ArrayType(.., containsNull = true)`,
  // which widens every struct field to nullable. Left-side attributes are widened because the
  // rewrite carries each left column through a `First` aggregate, whose result type is always
  // nullable (`First` may return `null` for empty groups). Declaring both nullable here keeps
  // the analyzed schema consistent with the optimized plan (and with what users see in cached
  // or written outputs).
  override def output: Seq[Attribute] =
    left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))

  def duplicateResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

  override lazy val resolved: Boolean = {
    childrenResolved &&
      expressions.forall(_.resolved) &&
      duplicateResolved
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(NEAREST_BY_JOIN)

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan, newRight: LogicalPlan): NearestByJoin = {
    copy(left = newLeft, right = newRight)
  }
}
