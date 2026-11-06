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

import org.apache.spark.sql.catalyst.expressions.{ExplodeBase, Expression, GreaterThan, Literal, Size}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Generate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, GENERATE}

/**
 * Removes a [[Generate]] below a DISTINCT (group-only) [[Aggregate]] when the generated columns
 * are not referenced by the aggregate.
 *
 * `Generate` repeats each input row once per generated element, so a group-only aggregate above
 * it observes the same values it would observe when reading the input rows directly, except that
 * rows whose collection is empty or null are dropped. For a non-outer `Generate` this is
 * equivalent to a `size(expr) > 0` filter; for an outer one the `Generate` produces exactly one
 * row per input row, so it can simply be removed.
 *
 * For example, `SELECT DISTINCT id FROM t LATERAL VIEW explode(items) v AS item` is rewritten
 * from `Aggregate(id, Generate(explode(items), t))` to
 * `Aggregate(id, Filter(size(items) > 0, t))`, so the exploded rows never reach the aggregate.
 * The analyzer usually puts a projection between the aggregate and the `Generate`; the rule
 * looks through a single deterministic [[Project]] in that case.
 *
 * The rewrite is deliberately restricted to the case where the generated columns are unused.
 * Exploding is not injective: rows `(1, [1, 2])` and `(1, [1, 3])` both produce `(1, 1)`, so
 * pushing a DISTINCT whose keys contain generated columns below the `Generate` would change the
 * result. Aggregates with aggregate functions are excluded for the same reason: the number of
 * exploded rows is observable to functions such as `count` and `sum`.
 */
object RemoveRedundantGenerate extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDownWithPruning(
    _.containsAllPatterns(AGGREGATE, GENERATE), ruleId) {
    case agg @ Aggregate(grouping, _, g @ Generate(gen: ExplodeBase, _, outer, _, _, child), _)
        if isEligible(agg, grouping, g, None) =>
      agg.copy(child = removeGenerate(None, gen, outer, child))

    case agg @ Aggregate(
        grouping, _, p @ Project(_, g @ Generate(gen: ExplodeBase, _, outer, _, _, child)), _)
        if isEligible(agg, grouping, g, Some(p)) =>
      agg.copy(child = removeGenerate(Some(p), gen, outer, child))
  }

  /**
   * Returns whether the `Generate` can be removed: the aggregate must be a pure DISTINCT, every
   * expression producing its input must be deterministic, and no generated column may be
   * referenced by the aggregate or by the projection above the `Generate`.
   */
  private def isEligible(
      agg: Aggregate,
      grouping: Seq[Expression],
      g: Generate,
      project: Option[Project]): Boolean = {
    agg.groupOnly &&
      grouping.forall(_.deterministic) &&
      !g.child.isStreaming &&
      project.forall(_.projectList.forall(_.deterministic)) &&
      !referencesGeneratedColumns(agg, g, project)
  }

  private def referencesGeneratedColumns(
      agg: Aggregate,
      g: Generate,
      project: Option[Project]): Boolean = {
    def referencesGeneratedColumn(e: Expression): Boolean =
      e.references.exists(g.producedAttributes.contains)
    agg.references.exists(g.producedAttributes.contains) ||
      project.exists(_.projectList.exists(referencesGeneratedColumn))
  }

  /**
   * Replaces the `Generate` with a filter that keeps the rows whose collection is not empty, or
   * removes it entirely when it is an outer generate and produces exactly one row per input row.
   */
  private def removeGenerate(
      project: Option[Project],
      gen: ExplodeBase,
      outer: Boolean,
      child: LogicalPlan): LogicalPlan = {
    val newChild =
      if (outer) {
        child
      } else {
        Filter(GreaterThan(Size(gen.children.head), Literal(0)), child)
      }
    project.map(_.copy(child = newChild)).getOrElse(newChild)
  }
}
