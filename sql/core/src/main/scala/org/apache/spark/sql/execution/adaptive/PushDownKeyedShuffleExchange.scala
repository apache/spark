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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.expressions.{AliasHelper, Attribute, AttributeSet, Expression, ExprUtils, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildSide}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning, KeyedPartitioning, PartitioningCollection}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{BinaryExecNode, FilterExec, GenerateExec, ProjectExec, SortExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, PartitioningOrigin, ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.execution.window.{Final => FinalWindowGroupLimit, WindowExecBase, WindowGroupLimitExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Moves a keyed shuffle down through operators clustered on its keys onto the hash shuffle that
 * feeds them, so that one shuffle serves both:
 *
 * {{{
 *   Before:                                     After:
 *   SortMergeJoin                               SortMergeJoin
 *    :- Sort                                     :- Sort
 *    :  +- Exchange(keyed [item_id])             :  +- HashAggregate(key = [item_id])
 *    :     +- HashAggregate(key = [item_id])     :     +- Exchange(keyed [item_id])
 *    :        +- Exchange(hash [item_id])        :        +- HashAggregate(partial)
 *    :           +- HashAggregate(partial)       :           +- ...
 *    +- BatchScan(items, keyed [item_id])        +- BatchScan(items, keyed [item_id])
 * }}}
 *
 * `EnsureRequirements` plans the join's keyed shuffle above the aggregate and the aggregate's own
 * hash shuffle below it independently; a layout that serves the aggregate's clustering as well as
 * the hash one lets the hash shuffle take the keyed layout. The walk crosses a grouped aggregate, a
 * partitioned window, a final window group limit, a deterministic projection or filter, a local
 * sort, a generate and a broadcast join's streamed side, and retargets only when the plan reuses
 * neither shuffle, the layout offers at least the hash shuffle's partition count, no crossed
 * expression evaluates differently, the layout stays evaluable on rows a crossed operator removes,
 * the rebuilt subtree reports the layout, and the whole plan still validates under the
 * preparation's root distribution.
 */
case class PushDownKeyedShuffleExchange(
    ensureRequirements: EnsureRequirements,
    queryContainsSubqueries: Boolean) extends Rule[SparkPlan] with AliasHelper {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED) ||
        (queryContainsSubqueries &&
          !conf.getConf(SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ALLOW_WITH_SUBQUERIES_ENABLED)) ||
        !conf.v2BucketingShuffleEnabled || conf.v2BucketingPartiallyClusteredDistributionEnabled) {
      return plan
    }

    lazy val sharedExchanges: Set[SparkPlan] =
      if (conf.exchangeReuseEnabled) AQEUtils.duplicatedShuffleForms(plan) else Set.empty

    // The layout has to be able to serve as a shuffle's template, as
    // `KeyedShuffleSpec.canCreatePartitioning` asks of one it lays another child out on.
    def isCandidate(plan: SparkPlan): Boolean = plan match {
      case ShuffleExchangeExec(keyed: KeyedPartitioning, _, ENSURE_REQUIREMENTS, _, _) =>
        keyed.canTemplateShuffle
      case _ => false
    }

    // The rewrite one candidate reports, or `current` when it declines.
    def rewrite(current: SparkPlan)(select: ShuffleExchangeExec => Boolean): SparkPlan =
      current.transformUp {
        case exchange: ShuffleExchangeExec if select(exchange) =>
          pushDown(exchange, sharedExchanges).getOrElse(exchange)
      }

    def validated(plan: SparkPlan): Boolean =
      ValidateRequirements.validateWithOrigins(plan, ensureRequirements.requiredDistribution)

    // One pass rewrites every candidate, and the whole plan is validated once. Only when that
    // check refuses a rewrite does the per-candidate retry below run, so a refusal on one branch
    // does not undo the others in the rare case that the whole-plan check fails.
    val pushed = rewrite(plan)(isCandidate)
    if (pushed.fastEquals(plan)) {
      plan
    } else if (validated(pushed)) {
      pushed
    } else {
      // `collect` visits a node before its children, so a candidate above another is retried
      // first. A rewrite rebuilds only the path from the candidate to the root, copies the hash
      // shuffle's child, and leaves a sibling subtree in place, so every other candidate keeps
      // its instance: one cannot sit in another's crossed operators, where a walk stops at a
      // keyed shuffle it cannot retarget.
      plan.collect { case exchange: ShuffleExchangeExec if isCandidate(exchange) => exchange }
        .foldLeft(plan) { (current, keyedExchange) =>
          val rewritten = rewrite(current)(_.eq(keyedExchange))
          if (!rewritten.fastEquals(current) && validated(rewritten)) rewritten else current
        }
    }
  }

  /**
   * `keyedExchange`'s child with the hash shuffle below it retargeted to the layout
   * `keyedExchange` reports, or `None` when the walk cannot reach a hash shuffle that can take
   * the layout, the plan reuses either shuffle, or the rebuilt subtree does not report the layout.
   */
  private def pushDown(
      keyedExchange: ShuffleExchangeExec,
      sharedExchanges: => Set[SparkPlan]): Option[SparkPlan] = {
    // The pattern in `apply` pinned the layout.
    val layout = keyedExchange.outputPartitioning.asInstanceOf[KeyedPartitioning]

    // A broadcast join reports the streamed side's partitioning for the join types that do, so
    // only that side is crossed and the build side is left alone. The final layout check rejects a
    // join type that reports something else.
    def retargetStreamed(
        join: BinaryExecNode,
        buildSide: BuildSide,
        layout: KeyedPartitioning,
        dropsRows: Boolean): Option[SparkPlan] = {
      val streamed = if (buildSide == BuildLeft) join.right else join.left
      retarget(streamed, layout, dropsRows).map { child =>
        val children = if (buildSide == BuildLeft) {
          Seq(join.left, child)
        } else {
          Seq(child, join.right)
        }
        join.withNewChildren(children)
      }
    }

    // `operator` with its child retargeted to `childLayout`, once `admit` accepts that layout.
    def retargetChild(
        operator: UnaryExecNode,
        childLayout: KeyedPartitioning,
        dropsRows: Boolean,
        admit: KeyedPartitioning => Boolean = _ => true): Option[SparkPlan] = {
      if (admit(childLayout)) {
        retarget(operator.child, childLayout, dropsRows)
          .map(child => operator.withNewChildren(Seq(child)))
      } else {
        None
      }
    }

    // `dropsRows` says that an operator above this point removes rows, so the layout is evaluated
    // on rows that operator would have removed.
    def retarget(
        plan: SparkPlan,
        layout: KeyedPartitioning,
        dropsRows: Boolean): Option[SparkPlan] = plan match {
      case hashExchange @ ShuffleExchangeExec(hash: HashPartitioning, _, ENSURE_REQUIREMENTS, _, _)
          if hash.numPartitions <= layout.numPartitions &&
            layout.expressions.forall(_.references.subsetOf(hashExchange.child.outputSet)) &&
            (!dropsRows || layout.expressions.forall(ExprUtils.canEvaluateUnconditionally)) &&
            !sharedExchanges.contains(keyedExchange.canonicalized) &&
            !sharedExchanges.contains(hashExchange.canonicalized) =>
        // `EnsureRequirements` builds the layout with `KeyedShuffleSpec.createPartitioning`, which
        // marks it `HASH_ROUTING`, and this shuffle routes undeclared rows by hash, as marked.
        val retargeted = hashExchange.copy(outputPartitioning = layout)
        // A custom strategy can plan a user repartition right above this shuffle, and its
        // elision records the user layout here; keep it so the check below can refuse. Only
        // union shuffle push-down makes `EnsureRequirements` record it, so with that off an
        // elided user layout on the walk is replaced by the keyed one.
        retargeted.copyTagsFrom(hashExchange)
        Option.when(PartitioningOrigin.validateNode(retargeted))(retargeted)

      case project: ProjectExec if project.expressions.forall(_.deterministic) =>
        rebind(layout, project.projectList, project.child.outputSet)
          .flatMap(childLayout => retargetChild(project, childLayout, dropsRows))
      case filter: FilterExec if filter.expressions.forall(_.deterministic) =>
        retargetChild(filter, layout, dropsRows = true)
      case sort: SortExec if !sort.global =>
        retargetChild(sort, layout, dropsRows)
      case generate: GenerateExec if generate.expressions.forall(_.deterministic) =>
        retargetChild(generate, layout, dropsRows || !generate.outer)

      case aggregate: BaseAggregateExec
          if !aggregate.isStreaming && aggregate.expressions.forall(_.deterministic) =>
        rebind(layout, aggregate.resultExpressions, aggregate.child.outputSet)
          .flatMap(childLayout =>
            retargetChild(aggregate, childLayout, dropsRows, candidate =>
              aggregate.requiredChildDistributionExpressions.exists(
                servesClustering(candidate, _))))
      case window: WindowExecBase if window.expressions.forall(_.deterministic) =>
        retargetChild(window, layout, dropsRows,
          childLayout => servesClustering(childLayout, window.partitionSpec))
      case wgl: WindowGroupLimitExec if wgl.mode == FinalWindowGroupLimit =>
        // The final limit keeps the top rows of each partition and drops the rest.
        retargetChild(wgl, layout, dropsRows = true,
          childLayout => servesClustering(childLayout, wgl.partitionSpec))

      // A crossed broadcast join can remove rows of its streamed side, so the layout has to be
      // evaluable on rows the join drops.
      case join: BroadcastHashJoinExec if join.condition.forall(_.deterministic) =>
        retargetStreamed(join, join.buildSide, layout, dropsRows = true)
      case join: BroadcastNestedLoopJoinExec if join.condition.forall(_.deterministic) =>
        retargetStreamed(join, join.buildSide, layout, dropsRows = true)

      case _ => None
    }

    retarget(keyedExchange.child, layout, dropsRows = false).filter { moved =>
      PartitioningCollection.flatten(moved.outputPartitioning).exists {
        case p: Expression => p.semanticEquals(layout)
        case _ => false
      }
    }
  }

  /**
   * Whether `layout` can serve an operator clustered on `keys`. Empty keys ask for all tuples in
   * one partition, which a keyed layout does not give.
   */
  private def servesClustering(layout: KeyedPartitioning, keys: Seq[Expression]): Boolean =
    keys.nonEmpty && layout.satisfies(ClusteredDistribution(keys))

  /**
   * `layout` rewritten from `outputExprs`' output attributes into the child's, or `None` when one
   * of its expressions still references an attribute the child does not produce, such as one an
   * expression alias computes. Only a plain rename is followed (`c1 AS x` becomes `c1`), as in
   * `PushDownLocalSort`.
   */
  private def rebind(
      layout: KeyedPartitioning,
      outputExprs: Seq[NamedExpression],
      childOutput: AttributeSet): Option[KeyedPartitioning] = {
    val renames = getRenameMap(outputExprs)
    val rebound = if (renames.isEmpty) {
      layout
    } else {
      layout.copy(expressions = layout.expressions.map(_.transformUp {
        case a: Attribute => renames.getOrElse(a, a)
      }))
    }
    Option.when(rebound.expressions.forall(_.references.subsetOf(childOutput)))(rebound)
  }
}
