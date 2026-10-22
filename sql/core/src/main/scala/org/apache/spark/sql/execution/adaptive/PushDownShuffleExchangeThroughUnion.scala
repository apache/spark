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

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, SessionWindow, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Partial => PartialAggregate}
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning, Partitioning, PartitioningCollection}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{
  FilterExec, LeafExecNode, ProjectExec, SortExec, SparkPlan, UnaryExecNode, UnionConfSnapshot,
  UnionExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, ShuffleExchangeExec, ShuffleExchangeLike, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.execution.window.{Partial => PartialWindowGroupLimit, WindowGroupLimitExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Push down a shuffle [[EnsureRequirements]] put above a [[UnionExec]] below the union, so that
 * only the union's children that are not already laid out on the target take one. A local sort
 * above the shuffle, and the operators between it and the union, are rebuilt above the union:
 *
 * {{{
 *   SortMergeJoin                             SortMergeJoin
 *    :- Sort                                   :- Sort
 *    :  +- Exchange                 =>         :  +- Union
 *    :     +- Union                            :     :- HashAggregate
 *    :        :- HashAggregate                 :     :  +- Exchange
 *    :        :  +- Exchange                   :     :     +- Scan
 *    :        :     +- Scan                    :     +- Exchange
 *    :        +- Project                       :        +- Project
 *    :           +- Scan                       :           +- Scan
 *    +- ...                                    +- ...
 * }}}
 *
 * The left child already answers the target, since it shuffles on the join key for its aggregate,
 * so it keeps its layout and only the right child takes a shuffle.
 *
 * The operators between the shuffle and the union are looked through only under
 * `spark.sql.optimizer.pushDownShuffleThroughUnion.lookThroughOperators.enabled`, and only when
 * moving the shuffle past them preserves results: a deterministic project, a local sort, a
 * filter, a partial aggregate and a partial window group limit. The reducing ones are why the
 * gate exists: the child is then shuffled before the reduction, which can send more data than the
 * one shuffle it replaces.
 *
 * Targets are hash layouts reported by the union's children, ranked by how many children offer
 * them, then by descending partition count, then by first occurrence. The count must be at least
 * the smaller of the replaced exchange's count and `spark.sql.shuffle.partitions`. At least one
 * child keeps its layout; each other child gets its own exchange, without changing child order.
 * `spark.sql.optimizer.pushDownShuffleThroughUnion.maxMovedBranches` limits these new exchanges.
 * The rebuilt child must satisfy its consumer's distribution and pair with fixed siblings.
 *
 * Removing the union's exchange can reduce AQE join conversion and skew handling opportunities.
 * Keeping a shuffled join child's layout can also make a skew split invalidate an ancestor's
 * distribution, causing AQE to discard skew optimization for the round. Allowing non-coalescible
 * targets via `spark.sql.optimizer.pushDownShuffleThroughUnion.keepNonCoalescibleBranches.enabled`
 * can prevent coalescing of the consumer's compatible shuffle group.
 *
 * The rule runs only in AQE preparation, after `EnsureRequirements` and before
 * `StampUnionDecisions`. It uses that `EnsureRequirements` instance's root distribution to
 * preserve an elided user repartition's keys and explicit count. Classic preparation can also
 * elide a repartition, but does not retain this root contract.
 *
 * TODO: support push down of a keyed layout.
 */
case class PushDownShuffleExchangeThroughUnion(
    snapshot: UnionConfSnapshot,
    ensureRequirements: EnsureRequirements) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!snapshot.outputPartitioning ||
        !conf.getConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED)) {
      return plan
    }

    val lookThrough =
      conf.getConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_LOOK_THROUGH_OPERATORS_ENABLED)
    val policy = LayoutPolicy(
      keepNonCoalescibleBranches = conf.getConf(
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED),
      coalescing = conf.getConf(SQLConf.COALESCE_PARTITIONS_ENABLED),
      maxMovedBranches =
        conf.getConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_MAX_MOVED_BRANCHES))

    // An exchange the plan reuses elsewhere is worth more than this rewrite: moving it below the
    // union loses the stage the others reuse. See `AQEUtils.duplicatedShuffleForms`. With reuse
    // off every exchange gets its own stage, so there is nothing to protect.
    lazy val sharedExchanges: Set[SparkPlan] =
      if (conf.exchangeReuseEnabled) AQEUtils.duplicatedShuffleForms(plan) else Set.empty

    val pushed = plan.transformUp {
      case operator: SparkPlan =>
        // In child order, and nothing for a child this rule cannot move.
        val matched = operator.children.map(findPushableShuffle(_, lookThrough))
        if (matched.forall(_.isEmpty)) {
          operator
        } else {
          // Only an operator that holds one is asked what its children owe, and only once: see
          // `findPushableShuffle`. Every child has to owe a `ClusteredDistribution`, since the
          // rewrite lines them up on one clustered layout.
          val required = operator.requiredChildDistribution
          if (!required.forall(_.isInstanceOf[ClusteredDistribution])) {
            operator
          } else {
            val requirements = required.map(_.asInstanceOf[ClusteredDistribution])
            pushDownThroughUnion(
              operator,
              matched.zip(requirements),
              coPartitioned = operator.children.length > 1,
              sharedExchanges = sharedExchanges,
              policy = policy).getOrElse(operator)
          }
        }
    }
    if (pushed.fastEquals(plan)) {
      plan
    } else {
      // Each rewrite has already answered its own operator. This is the whole-plan invariant on
      // top of that, and it also holds the root to the user-specified repartition that
      // `EnsureRequirements` was allowed to elide.
      val satisfied = ensureRequirements.requiredDistribution match {
        // A user repartition elided above was asked for on all its keys, so a layout on a subset
        // of them does not stand in for it.
        case Some(required: ClusteredDistribution) =>
          ValidateRequirements.validate(pushed, required.copy(requireAllClusterKeys = true))
        case Some(required) =>
          ValidateRequirements.validate(pushed, required)
        case None =>
          ValidateRequirements.validate(pushed)
      }
      if (satisfied) {
        pushed
      } else {
        plan
      }
    }
  }

  /**
   * One of an operator's children this rule can rewrite: the shuffle it moves and the union that
   * shuffle is moved past.
   */
  private case class PushableChild(shuffle: ShuffleExchangeExec, union: UnionExec)

  /**
   * Layout selection settings, read once per pass: non-coalescible targets, coalescing, and the
   * maximum number of children to shuffle.
   */
  private case class LayoutPolicy(
      keepNonCoalescibleBranches: Boolean,
      coalescing: Boolean,
      maxMovedBranches: Int)

  /**
   * The shape this rule acts on below one of an operator's children: a pushable shuffle and the
   * union it is moved past, with the operators in between looked through to reach it. Only a hash
   * shuffle is pushable, since the layout that replaces it comes from the union's own children.
   *
   * What the child owes its operator is not read here: this is asked of every child of every
   * operator, and `requiredChildDistribution` is not free of side effects: `WindowExecBase` logs
   * from it for an unpartitioned window.
   */
  @tailrec
  private def findPushableShuffle(
      plan: SparkPlan,
      lookThrough: Boolean,
      found: Option[ShuffleExchangeExec] = None): Option[PushableChild] = {
    plan match {
      // A local sort above the shuffle is looked through, since the rebuilt child is sorted back
      // to the same requirement; one below it is an operator in between and needs the gate.
      case SortExec(_, false, child, _) if found.isEmpty || lookThrough =>
        findPushableShuffle(child, lookThrough, found)
      case shuffle @ ShuffleExchangeExec(partitioning, child, ENSURE_REQUIREMENTS, _, _)
          if found.isEmpty && supportPushDown(partitioning) =>
        findPushableShuffle(child, lookThrough, Some(shuffle))
      // Below it: the operators this rule can cross, down to the union the shuffle is moved past.
      case union: UnionExec =>
        found.map(PushableChild(_, union))
      case operator: UnaryExecNode if found.isDefined && canCross(operator, lookThrough) =>
        findPushableShuffle(operator.child, lookThrough, found)
      case _ => None
    }
  }

  /**
   * Rewrites the children of `operator` that `pushable` holds, or returns `None` when any of them
   * has no viable target. All replacements for an operator are accepted or discarded together.
   *
   * `pushable` holds one entry per child, in child order: what can be rewritten about it, and
   * what it owes the operator.
   */
  private def pushDownThroughUnion(
      operator: SparkPlan,
      pushable: Seq[(Option[PushableChild], ClusteredDistribution)],
      coPartitioned: Boolean,
      sharedExchanges: Set[SparkPlan],
      policy: LayoutPolicy): Option[SparkPlan] = {
    val children = operator.children
    val requiredOrderings = operator.requiredChildOrdering

    // Rebuild in child order, against unchanged siblings and already rebuilt children. If any
    // child has no viable target, discard all replacements for this operator.
    val newChildren = new ArrayBuffer[SparkPlan](children.length)
    def fixedSiblings(index: Int): Seq[(SparkPlan, ClusteredDistribution)] =
      children.indices.flatMap { position =>
        val (pushableChild, required) = pushable(position)
        Option.when(position != index && (position < index || pushableChild.isEmpty)) {
          (if (position < index) newChildren(position) else children(position), required)
        }
      }

    val rewritten = children.indices.forall { index =>
      val (pushableChild, required) = pushable(index)
      pushableChild match {
        case Some(child) =>
          rebuildChild(
            child, required, fixedSiblings(index), coPartitioned, sharedExchanges, policy) match {
            case Some(rebuilt) =>
              newChildren += rebuilt
              true
            case None => false
          }
        case None =>
          newChildren += children(index)
          true
      }
    }
    if (!rewritten) {
      return None
    }

    // The ordering is put back once the layout is fixed. A local sort leaves the layout alone,
    // and a child that already answers keeps its own.
    val sorted = newChildren.toSeq.lazyZip(requiredOrderings).map {
      case (child, ordering) => withOrdering(child, ordering)
    }
    Some(operator.withNewChildren(sorted))
  }

  /**
   * `pushable` rebuilt on the best of its union's own layouts that answers both `required`, the
   * distribution the child owes, and `fixed`, the siblings whose layout cannot change any more.
   */
  private def rebuildChild(
      pushable: PushableChild,
      required: ClusteredDistribution,
      fixed: Seq[(SparkPlan, ClusteredDistribution)],
      coPartitioned: Boolean,
      sharedExchanges: Set[SparkPlan],
      policy: LayoutPolicy): Option[SparkPlan] = {
    // The union's subtree would run once per consumer if the exchange is reused elsewhere.
    if (sharedExchanges.nonEmpty &&
        sharedExchanges.contains(pushable.shuffle.canonicalized)) {
      return None
    }

    val layouts = childLayouts(pushable.union, policy)
    // Do not reduce parallelism below both the replaced exchange and the configured default.
    val minPartitions = math.min(
      pushable.shuffle.outputPartitioning.numPartitions, conf.numShufflePartitions)

    // `findPushableShuffle` already checked this chain, including local sorts.
    def rebuild(plan: SparkPlan, target: Partitioning with Expression): Option[SparkPlan] =
      plan match {
        case union: UnionExec => rebuildUnion(union, layouts, target)
        case operator: UnaryExecNode =>
          rebuild(operator.child, target).map(child => operator.withNewChildren(Seq(child)))
        case other =>
          throw SparkException.internalError(
            s"a shuffle push-down reached ${other.nodeName}, not the union it was found above")
      }

    candidateTargets(layouts, minPartitions).view
      .filter(target => layouts.count(child => !child.contains(target)) <= policy.maxMovedBranches)
      .flatMap(target => rebuild(pushable.shuffle.child, target))
      .find { child =>
        val partitioning = child.outputPartitioning
        partitioning.satisfies(required) &&
          // A co-partitioned pair has to be able to create each other's partitioning: a layout on
          // a subset of the keys is what the co-partition skew guard refuses.
          (!coPartitioned ||
            PartitioningCollection.specsForPairing(partitioning, required)
              .exists(_.canCreatePartitioning)) &&
          ValidateRequirements.satisfiesForPairing(
            child +: fixed.map(_._1), required +: fixed.map(_._2))
      }
  }

  /**
   * The layouts `union` could be rebuilt onto, best first: the one the most children already
   * report wins, since it is the one that shuffles the fewest of them; then the finer of two, for
   * the parallelism the operator above runs with; then the one the earliest child reports.
   */
  private def candidateTargets(
      layouts: Seq[Seq[Partitioning with Expression]],
      minPartitions: Int): Seq[Partitioning with Expression] = {
    layouts.flatten.distinct.filter(_.numPartitions >= minPartitions).zipWithIndex
      .sortBy { case (target, position) =>
        (-layouts.count(_.contains(target)), -target.numPartitions, position)
      }
      .map(_._1)
  }

  /**
   * The layouts each child of `union` reports, in child order, each expressed in the union's
   * output attributes. An eligible child reporting a [[PartitioningCollection]] offers its hash
   * members. A child that offers nothing takes a shuffle whichever layout is picked.
   *
   * They are compared by equality below, which is what `UnionExec` itself compares its children
   * with: `prepareOutputPartitioning` puts every child in the union's attributes. Where equality
   * errs it errs towards shuffling a child it did not recognise.
   */
  private def childLayouts(
      union: UnionExec,
      policy: LayoutPolicy): Seq[Seq[Partitioning with Expression]] = {
    union.prepareOutputPartitioning().zip(union.children).map { case (partitioning, child) =>
      if (keepableLayout(child, policy)) {
        PartitioningCollection.flatten(partitioning).collect {
          case hash: HashPartitioning => hash
        }
      } else {
        Nil
      }
    }
  }

  /**
   * Whether `plan` may offer a target. With coalescing enabled, all sources must be supported
   * shuffles or shuffle query stages unless explicitly allowed. Broadcast joins only depend on
   * streamed sources. Stop at each shuffle boundary: its input sources do not affect whether its
   * output partitions can be coalesced.
   */
  private def keepableLayout(plan: SparkPlan, policy: LayoutPolicy): Boolean = {
    def supportedSources(plan: SparkPlan): Boolean = plan match {
      case shuffle: ShuffleExchangeLike =>
        CoalesceShufflePartitions.supportedShuffleOrigins.contains(shuffle.shuffleOrigin)
      case stage: ShuffleQueryStageExec =>
        CoalesceShufflePartitions.supportedShuffleOrigins.contains(stage.shuffle.shuffleOrigin)
      case join: BroadcastHashJoinExec =>
        supportedSources(if (join.buildSide == BuildLeft) { join.right } else { join.left })
      case join: BroadcastNestedLoopJoinExec =>
        supportedSources(if (join.buildSide == BuildLeft) { join.right } else { join.left })
      case _: LeafExecNode => false
      case other => other.children.forall(supportedSources)
    }

    policy.keepNonCoalescibleBranches || !policy.coalescing || supportedSources(plan)
  }

  /**
   * `union` rebuilt on `target`: eligible children already reporting it are kept; each other child
   * gets an exchange. `None` when the rebuilt union does not report `target` itself,
   * which is stricter than it satisfying the requirement: the operator above was planned against
   * one layout, and only that layout keeps the siblings it was paired with lined up.
   */
  private def rebuildUnion(
      union: UnionExec,
      layouts: Seq[Seq[Partitioning with Expression]],
      target: Partitioning with Expression): Option[SparkPlan] = {
    val newChildren = union.children.zip(layouts).map { case (child, reported) =>
      if (reported.contains(target)) {
        child
      } else {
        ShuffleExchangeExec(rebind(target, union, child), child, ENSURE_REQUIREMENTS)
      }
    }
    // Rebuilt rather than constructed, so the preparation conf and the decision
    // `StampUnionDecisions` may already have stamped travel onto the new node.
    val rebuilt = union.withNewChildren(newChildren)
    Option.when(
      PartitioningCollection.flatten(rebuilt.outputPartitioning).contains(target))(rebuilt)
  }

  /**
   * `target`, which is expressed in `union`'s output attributes, in `child`'s own: a child of a
   * union holds the union's attributes under its own names, at the same positions.
   *
   * The child has to end up on this layout itself, not merely something that answers the same
   * distribution, since `UnionExec` merges its children by equality. Hence the layout is rebound
   * rather than rebuilt from a [[ShuffleSpec]] the way `EnsureRequirements` lays one child out on
   * another's: a spec maps keys by their position in a distribution both children answer, and a
   * union's children have no such distribution. A crossed projection can alias the key the
   * consumer clusters on, leaving it absent from the layout altogether.
   */
  private def rebind(
      target: Partitioning with Expression,
      union: UnionExec,
      child: SparkPlan): Partitioning = {
    val attributes = AttributeMap(union.output.zip(child.output))
    target.transform {
      case attribute: Attribute if attributes.contains(attribute) => attributes(attribute)
    }.asInstanceOf[Partitioning]
  }

  /**
   * Whether moving the shuffle past `operator` preserves results. Only deterministic projects
   * and filters, non-streaming grouped partial aggregates without session windows, and partial
   * window group limits are admitted.
   *
   * The operators in between are all behind `lookThrough`, and the reducing ones are why: the
   * rewrite shuffles a child before the reduction, which can be more data than the shuffle it
   * replaces.
   *   - A project drops no row.
   *   - A partial aggregate holds one buffer per group per input partition, and the aggregate
   *     above the shuffle merges them, whichever way the rows arrived.
   *   - A partial window group limit keeps the top rows per group per input partition, a superset
   *     of the global top rows the final one then takes.
   *   - A filter drops the same rows wherever they sit.
   * A local sort is looked through by `findPushableShuffle` instead, since it re-establishes its
   * ordering within whatever partitions it is handed; that is also why its ordering does not have
   * to be deterministic.
   */
  private def canCross(operator: UnaryExecNode, lookThrough: Boolean): Boolean = operator match {
    case project: ProjectExec =>
      lookThrough && project.projectList.forall(_.deterministic)
    case filter: FilterExec =>
      lookThrough && filter.condition.deterministic
    case aggregate: BaseAggregateExec =>
      lookThrough &&
        !aggregate.isStreaming &&
        aggregate.requiredChildDistributionExpressions.isEmpty &&
        aggregate.groupingExpressions.nonEmpty &&
        !aggregate.groupingExpressions.exists(_.metadata.contains(SessionWindow.marker)) &&
        aggregate.aggregateExpressions.forall(_.mode == PartialAggregate) &&
        aggregate.expressions.forall(_.deterministic)
    case limit: WindowGroupLimitExec =>
      lookThrough && limit.mode == PartialWindowGroupLimit
    case _ => false
  }

  /** Whether a shuffle on this partitioning is one the union's own children could replace. */
  private def supportPushDown(partitioning: Partitioning): Boolean =
    partitioning.isInstanceOf[HashPartitioning]

  /**
   * `child` sorted back to the ordering its operator requires, which is what put the local sort
   * above the shuffle this rule moved, added the way [[EnsureRequirements]] adds it.
   */
  private def withOrdering(child: SparkPlan, requiredOrdering: Seq[SortOrder]): SparkPlan = {
    if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
      child
    } else {
      SortExec(requiredOrdering, global = false, child)
    }
  }
}
