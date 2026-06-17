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
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.internal.SQLConf

/**
 * Reduces data amplification from high-ratio Expand operators
 * produced by [[RewriteDistinctAggregates]].
 *
 * [[RewriteDistinctAggregates]] rewrites queries with multiple
 * distinct aggregates into:
 * {{{
 *   Aggregate(outer) -> Aggregate(inner) -> Expand(Nx) -> child
 * }}}
 * where the Expand duplicates each row N times. This rule inserts
 * a de-duplication step (grouping-only Aggregate) before the Expand:
 * {{{
 *   ... -> Expand(Nx) -> Aggregate(groupBy: keys + all distinct cols) -> child
 * }}}
 *
 * Applied only when:
 *  - Expand projection count >= configured threshold
 *  - No non-distinct aggregates or FILTER clauses on distinct
 *    aggregates (checked via `expand.producedAttributes` being
 *    a subset of the inner aggregate's GROUP BY)
 *  - The pre-aggregate's group-by column count does not exceed
 *    the inner aggregate's (minus gid), which rejects composite
 *    distinct expressions (e.g. `col1 + col2`) that introduce
 *    extra leaf attributes and inflate the Cartesian product.
 *  - The Expand child is not already an Aggregate (idempotency)
 *
 * Controlled by `spark.sql.optimizer.optimizeExpandRatio`
 * (default -1 = disabled). Excludable via
 * `spark.sql.optimizer.excludedRules`.
 */
object OptimizeExpand extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    val threshold = conf.getConf(SQLConf.OPTIMIZE_EXPAND_RATIO)
    if (threshold == -1) return plan

    plan.transformUpWithPruning(_.containsPattern(AGGREGATE)) {
      case outerAgg @ Aggregate(_, _, innerAgg @ Aggregate(_, _, expand @ Expand(
        projections, _, expandChild), _), _)
        if projections.size >= threshold &&
           !expandChild.isInstanceOf[Aggregate] && // idempotency guard
           canOptimize(innerAgg, expand) =>

        val preAggGroupBy = collectPreAggGroupBy(expand)

        val preAggregate = Aggregate(
          preAggGroupBy,
          preAggGroupBy.map(_.toAttribute),
          expandChild)

        val newExpand = expand.copy(child = preAggregate)
        val newInnerAgg = innerAgg.copy(child = newExpand)
        outerAgg.copy(child = newInnerAgg)
    }
  }

  /**
   * Returns true if the Expand is safe and beneficial to optimize.
   *
   * Checks (in order):
   *  1. The Expand is produced by [[RewriteDistinctAggregates]]
   *     (gid present in inner GROUP BY).
   *  2. All Expand-produced attributes are consumed by the inner
   *     GROUP BY (rejects non-distinct aggs and FILTER clauses).
   *  3. The pre-aggregate's group-by column count does not exceed
   *     the inner aggregate's (minus gid). Composite distinct
   *     expressions like `col1 + col2` fan out into more leaf
   *     attributes, inflating the Cartesian product and making
   *     pre-aggregation counterproductive.
   */
  private def canOptimize(
      innerAgg: Aggregate,
      expand: Expand): Boolean = {
    val hasGid = innerAgg.groupingExpressions.exists {
      case a: Attribute =>
        a.name == "gid" && expand.producedAttributes.contains(a)
      case _ => false
    }
    if (!hasGid) return false

    val innerGroupByAttrs = AttributeSet(
      innerAgg.groupingExpressions.flatMap(_.references))
    if (!expand.producedAttributes.subsetOf(innerGroupByAttrs)) return false

    // Check 3: composite expressions like col1 + col2 fan out into
    // more leaf attributes than inner group-by slots, making the
    // pre-aggregate's Cartesian product too large for effective dedup.
    val preAggSize = expand.child.output.count(expand.references.contains)
    val innerGroupBySize = innerAgg.groupingExpressions.size - 1 // minus gid
    preAggSize <= innerGroupBySize
  }

  /**
   * Collect the pre-aggregation GROUP BY: all attributes from
   * the Expand's child that the Expand references, preserving
   * the child's output order for plan determinism.
   */
  private def collectPreAggGroupBy(
      expand: Expand): Seq[Attribute] = {
    expand.child.output.filter(expand.references.contains)
  }
}
