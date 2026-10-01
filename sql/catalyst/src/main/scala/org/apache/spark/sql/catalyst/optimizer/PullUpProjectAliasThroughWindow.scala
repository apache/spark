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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeSet, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.WINDOW

/**
 * A [[Window]] operator does not project its output partitioning or ordering through aliases: its
 * physical `outputPartitioning`/`outputOrdering` are pure pass-throughs of the child's. So when a
 * window partitioned/ordered by `k` is followed by a consumer (aggregate, repartition, a chained
 * window, ...) that requires distribution/ordering on a rename `k AS a`, a redundant shuffle and/or
 * sort is inserted: the window already shuffled/sorted on `k`, but `HashPartitioning(k)` does not
 * satisfy `ClusteredDistribution(a)` because the rename lives in a [[Project]] *below* the window
 * while the analyzer-inserted [[Project]] *above* references `a` as a bare [[Attribute]] (empty
 * alias map), and `k` and `a`, though value-identical, have distinct expr ids.
 *
 * This rule pulls such aliases up from the bottom [[Project]] into the top [[Project]], across the
 * chain of one or more [[Window]] operators in between (`Project - Window+ - Project`; adjacent
 * windows that cannot be collapsed, e.g. with different order specs, leave no [[Project]] between
 * them). The top project's alias map then maps `k -> a`, and the
 * `PartitioningPreservingUnaryExecNode` / `OrderPreservingUnaryExecNode` machinery that
 * `ProjectExec` mixes in projects the windows' `HashPartitioning(k)`/`SortOrder(k)` up through the
 * alias, satisfying the consumer. The [[Window]] operators themselves are left untouched.
 *
 * Besides removing the redundant shuffle/sort, this also narrows the data crossing the window's
 * shuffle: the alias no longer flows through the window as a separate column (only its input `k`,
 * already needed for partitioning, does) and is recomputed cheaply in the top project above the
 * exchange.
 *
 * An entry of the bottom project is pulled up only when:
 *   - it is an [[Alias]] (bare pass-through attributes stay below so the windows keep producing
 *     them for the top project to reference);
 *   - it is deterministic: a nondeterministic alias (e.g. `rand()`, `spark_partition_id()`)
 *     evaluated above the window's exchange/sort instead of below it could produce different
 *     values, so it must stay below;
 *   - no window in the chain references it, so window semantics are unaffected;
 *   - all of its input attributes remain produced by the pruned bottom project (i.e. they are
 *     referenced by some window and thus retained), so a computed alias whose inputs would be
 *     dropped is not lifted above windows that no longer output them;
 *   - the top project consumes its output solely as a bare pass-through attribute (never inside a
 *     larger expression), so replacing that attribute with the alias fully preserves it.
 *
 * The rewrite is a no-op on its own output (a moved alias is no longer a bare attribute above nor
 * present below), so it converges immediately at the fixed point.
 */
object PullUpProjectAliasThroughWindow extends Rule[LogicalPlan] {

  /**
   * Matches a non-empty chain of [[Window]] operators bottomed out by a [[Project]], returning the
   * windows top-to-bottom and that bottom project.
   */
  private object WindowChain {
    def unapply(plan: LogicalPlan): Option[(Seq[Window], Project)] = plan match {
      case w @ Window(_, _, _, lower: Project, _) => Some((Seq(w), lower))
      case w @ Window(_, _, _, WindowChain(windows, lower), _) => Some((w +: windows, lower))
      case _ => None
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(WINDOW), ruleId) {
    // Match the `Project - Window+ - Project` shape: the top project is the windows' parent
    // scaffolding, and the bottom project is where the rename (`key AS userid`) is defined.
    case p @ Project(projectList, WindowChain(windows, lower)) =>
      // Entries any window references must stay below: they define what the bottom project must
      // keep producing, and in particular carry the partition/order key attributes the pulled-up
      // aliases reuse.
      val windowRefs = AttributeSet(windows.flatMap(_.references))
      val (retained, candidates) =
        lower.projectList.partition(e => windowRefs.contains(e.toAttribute))
      val retainedAttrs = AttributeSet(retained.map(_.toAttribute))
      // Attributes the top project consumes as bare pass-through entries, and those it consumes
      // inside a larger expression (an alias child, a window-output reference, ...).
      val bareAttrs = AttributeSet(projectList.collect { case a: Attribute => a })
      val referencedByExpr = AttributeSet(projectList.flatMap {
        case _: Attribute => Nil
        case other => other.references
      })
      // Pull up an alias only when: it is deterministic (a nondeterministic alias must not move
      // across the window's exchange/sort, which would change its per-partition/per-row values);
      // its inputs survive in the pruned bottom project; the top project passes its output through
      // as a bare attribute; and the top project does not also consume that output inside an
      // expression (which would require the windows to keep it).
      val pullUp = candidates.collect {
        case a: Alias
            if a.deterministic &&
              a.references.subsetOf(retainedAttrs) &&
              bareAttrs.contains(a.toAttribute) &&
              !referencedByExpr.contains(a.toAttribute) => a
      }
      if (pullUp.isEmpty) {
        p
      } else {
        val pullUpSet: Set[NamedExpression] = pullUp.toSet
        // Key the lookup by expr id (not by `Attribute.equals`, which also compares qualifier):
        // the top project may reference these attributes with a different qualifier than the alias
        // carries below (e.g. across a subquery alias).
        val pullUpMap = AttributeMap(pullUp.map(a => a.toAttribute -> a))
        val newProjectList = projectList.map {
          // Rebuild the alias so it keeps the lower alias's child and expr id but adopts the top
          // attribute's full identity (name, qualifier, metadata). The lookup is keyed by expr id
          // only, so a resolved top attribute may carry a different name/qualifier/metadata than
          // the lower alias (e.g. a different qualifier across a subquery alias); taking them from
          // the top attribute keeps the output schema byte-for-byte identical.
          case attr: Attribute =>
            pullUpMap.get(attr) match {
              case Some(a) => Alias(a.child, attr.name)(
                exprId = a.exprId,
                qualifier = attr.qualifier,
                explicitMetadata = Some(attr.metadata))
              case None => attr
            }
          case other => other
        }
        val newLower = lower.copy(projectList = lower.projectList.filterNot(pullUpSet.contains))
        val newChild = windows.foldRight(newLower: LogicalPlan) { (w, child) =>
          w.withNewChildren(child :: Nil)
        }
        Project(newProjectList, newChild)
      }
  }
}
