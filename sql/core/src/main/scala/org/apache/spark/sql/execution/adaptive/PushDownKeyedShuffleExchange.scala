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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Expression}
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
 * expression evaluates differently, and the whole plan still validates under the preparation's root
 * distribution.
 */
case class PushDownKeyedShuffleExchange(
    ensureRequirements: EnsureRequirements,
    queryContainsSubqueries: Boolean) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED) ||
        (queryContainsSubqueries &&
          !conf.getConf(SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ALLOW_WITH_SUBQUERIES_ENABLED)) ||
        !conf.v2BucketingShuffleEnabled || conf.v2BucketingPartiallyClusteredDistributionEnabled) {
      return plan
    }

    lazy val sharedExchanges: Set[SparkPlan] =
      if (conf.exchangeReuseEnabled) AQEUtils.duplicatedShuffleForms(plan) else Set.empty

    val pushed = plan.transformUp {
      // The layout has to be able to serve as a shuffle's template, as
      // `KeyedShuffleSpec.canCreatePartitioning` asks of one it lays another child out on.
      case keyedExchange @ ShuffleExchangeExec(
          keyed: KeyedPartitioning, _, ENSURE_REQUIREMENTS, _, _) if keyed.canTemplateShuffle =>
        pushDown(keyedExchange, sharedExchanges).getOrElse(keyedExchange)
    }
    if (pushed.fastEquals(plan) ||
        !ValidateRequirements.validateWithOrigins(
          pushed, ensureRequirements.requiredDistribution)) {
      plan
    } else {
      pushed
    }
  }

  /**
   * `keyedExchange`'s child with the hash shuffle below it retargeted to the layout
   * `keyedExchange` reports, or `None` when the walk cannot reach a hash shuffle that can take
   * the layout, or the plan reuses both shuffles. The result reports the layout.
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
        layout: KeyedPartitioning): Option[SparkPlan] = {
      val streamed = if (buildSide == BuildLeft) join.right else join.left
      retarget(streamed, layout).map { child =>
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
        admit: KeyedPartitioning => Boolean = _ => true): Option[SparkPlan] = {
      if (admit(childLayout)) {
        retarget(operator.child, childLayout).map(child => operator.withNewChildren(Seq(child)))
      } else {
        None
      }
    }

    def retarget(plan: SparkPlan, layout: KeyedPartitioning): Option[SparkPlan] = plan match {
      case hashExchange @ ShuffleExchangeExec(hash: HashPartitioning, _, ENSURE_REQUIREMENTS, _, _)
          if hash.numPartitions <= layout.numPartitions &&
            layout.expressions.forall(_.references.subsetOf(hashExchange.child.outputSet)) &&
            !sharedExchanges.contains(keyedExchange.canonicalized) &&
            !sharedExchanges.contains(hashExchange.canonicalized) =>
        // `EnsureRequirements` builds the layout with `KeyedShuffleSpec.createPartitioning`, which
        // leaves it unmarked, so this shuffle routes undeclared rows by hash as the layout claims.
        val retargeted = hashExchange.copy(outputPartitioning = layout)
        // A custom strategy can plan a user repartition right above this shuffle, and its
        // elision records the user layout here; keep it so the check below can refuse.
        retargeted.copyTagsFrom(hashExchange)
        Option.when(PartitioningOrigin.validateNode(retargeted))(retargeted)

      case project: ProjectExec if project.expressions.forall(_.deterministic) =>
        rebind(layout, project).flatMap(retargetChild(project, _))
      case filter: FilterExec if filter.expressions.forall(_.deterministic) =>
        retargetChild(filter, layout)
      case sort: SortExec if !sort.global =>
        retargetChild(sort, layout)
      case generate: GenerateExec if generate.expressions.forall(_.deterministic) =>
        retargetChild(generate, layout)

      case aggregate: BaseAggregateExec
          if !aggregate.isStreaming && aggregate.expressions.forall(_.deterministic) =>
        rebind(layout, aggregate).flatMap(childLayout =>
          retargetChild(aggregate, childLayout, layout =>
            aggregate.requiredChildDistributionExpressions.exists(servesClustering(layout, _))))
      case window: WindowExecBase =>
        retargetChild(window, layout,
          childLayout => servesClustering(childLayout, window.partitionSpec))
      case wgl: WindowGroupLimitExec if wgl.mode == FinalWindowGroupLimit =>
        retargetChild(wgl, layout, childLayout => servesClustering(childLayout, wgl.partitionSpec))

      case join: BroadcastHashJoinExec if join.condition.forall(_.deterministic) =>
        retargetStreamed(join, join.buildSide, layout)
      case join: BroadcastNestedLoopJoinExec if join.condition.forall(_.deterministic) =>
        retargetStreamed(join, join.buildSide, layout)

      case _ => None
    }

    retarget(keyedExchange.child, layout).filter { moved =>
      PartitioningCollection.representativeOf(moved.outputPartitioning)
        .exists(_.semanticEquals(layout))
    }
  }

  /**
   * Whether `layout` can serve an operator clustered on `keys`. Empty keys ask for all tuples in
   * one partition, which a keyed layout does not give.
   */
  private def servesClustering(layout: KeyedPartitioning, keys: Seq[Expression]): Boolean =
    keys.nonEmpty && layout.satisfies(ClusteredDistribution(keys))

  /**
   * `layout` rewritten from `operator`'s output attributes into its child's, or `None` when one of
   * its expressions references an attribute the operator computes, which the child does not have.
   * Only a plain rename is followed (`c1 AS x` becomes `c1`), as in `PushDownLocalSort`.
   */
  private def rebind(
      layout: KeyedPartitioning,
      operator: UnaryExecNode): Option[KeyedPartitioning] = {
    val outputExprs: Seq[Expression] = operator match {
      case project: ProjectExec => project.projectList
      case aggregate: BaseAggregateExec => aggregate.resultExpressions
      case _ => Nil
    }
    val aliases = AttributeMap(outputExprs.collect {
      case a @ Alias(child: AttributeReference, _) => (a.toAttribute, child: Attribute)
    })
    val rebound = if (aliases.isEmpty) {
      layout
    } else {
      layout.copy(expressions = layout.expressions.map(_.transformUp {
        case a: Attribute => aliases.getOrElse(a, a)
      }))
    }
    Option.when(rebound.expressions.forall(
      _.references.subsetOf(operator.child.outputSet)))(rebound)
  }
}
