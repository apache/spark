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
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, SessionWindow, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Partial => PartialAggregate}
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning, HashShuffleSpec, Partitioning, PartitioningCollection}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{
  FilterExec, ProjectExec, SortExec, SparkPlan, UnaryExecNode, UnionConfSnapshot,
  UnionExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, Exchange, PartitioningOrigin, REPARTITION_BY_COL, REPARTITION_BY_NUM, ShuffleExchangeExec, ShuffleExchangeLike, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec, ShuffledJoin}
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
 * them when size estimates are unavailable; otherwise estimated bytes kept decide first. The
 * count must be at least the smaller of the replaced exchange's count and the effective initial
 * shuffle partition count (`coalescePartitions.initialPartitionNum` when set). At least one
 * child keeps its layout; others are retargeted or shuffled without changing child order.
 * `spark.sql.optimizer.pushDownShuffleThroughUnion.maxMovedBranches` limits moved children, and
 * `maxUnionChildren` limits all branches whose tails run within each consumer task.
 * The rebuilt child must satisfy its consumer's distribution and pair with fixed siblings.
 *
 * Removing the union's exchange can reduce AQE join conversion and skew handling opportunities.
 * Keeping a shuffled join child's layout can make skew join optimization or local shuffle reads
 * invalidate an ancestor's distribution. AQE can discard the optimization for the round, or add
 * another shuffle when skew optimization is forced. Join inputs and join branches are opt-in.
 * Allowing targets via
 * `spark.sql.optimizer.pushDownShuffleThroughUnion.keepNonCoalescibleBranches.enabled`
 * can prevent coalescing of the consumer's compatible shuffle group. Forced skew optimization
 * can restore a union exchange after moved branches have shuffled, shuffling them a second time.
 *
 * The rule runs only in AQE preparation, after `EnsureRequirements` and before
 * `StampUnionDecisions`. It preserves the root distribution and all user repartition layouts
 * recorded when `EnsureRequirements` elides exchanges, including requirements below the root.
 * These origin checks apply only to this rule's proposals, not to other adaptive optimizations.
 * Cost compensation belongs to SimpleCostEvaluator. Custom evaluators should account for the
 * one-to-many exchange rewrite when this rule is enabled, or they may reject the entire re-plan.
 *
 * TODO: support push down of a keyed layout.
 */
case class PushDownShuffleExchangeThroughUnion(
    snapshot: UnionConfSnapshot,
    ensureRequirements: EnsureRequirements,
    queryContainsSubqueries: Boolean) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!snapshot.outputPartitioning ||
        !conf.pushDownShuffleThroughUnionEnabled ||
        (queryContainsSubqueries &&
          !conf.getConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_WITH_SUBQUERIES_ENABLED))) {
      return plan
    }

    val lookThrough =
      conf.getConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_LOOK_THROUGH_OPERATORS_ENABLED)
    val policy = LayoutPolicy(
      allowNonCoalescible = !conf.getConf(SQLConf.COALESCE_PARTITIONS_ENABLED) || conf.getConf(
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED),
      maxMovedBranches =
        conf.getConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_MAX_MOVED_BRANCHES),
      maxUnionChildren =
        conf.getConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_MAX_UNION_CHILDREN),
      keepJoinBranches =
        conf.getConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_JOIN_BRANCHES_ENABLED),
      allowSubsetTargets = conf.getConf(SQLConf
        .PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_PARTITION_KEYS_SUBSET_OF_CLUSTER_KEYS_ENABLED))
    val allowJoinInputs =
      conf.getConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_JOIN_INPUTS_ENABLED)

    // An exchange the plan reuses elsewhere is worth more than this rewrite: moving it below the
    // union loses the stage the others reuse. See `AQEUtils.duplicatedShuffleForms`. With reuse
    // off every exchange gets its own stage, so there is nothing to protect.
    lazy val sharedExchanges: Set[SparkPlan] =
      if (conf.exchangeReuseEnabled) AQEUtils.duplicatedShuffleForms(plan) else Set.empty

    val pushed = plan.transformUp {
      case join: ShuffledJoin if !allowJoinInputs => join
      case operator: SparkPlan =>
        val matched = operator.children.map { child =>
          findPushableShuffle(child, lookThrough).filter { pushable =>
            pushable.union.children.length <= policy.maxUnionChildren &&
              (sharedExchanges.isEmpty ||
                !sharedExchanges.contains(pushable.shuffle.canonicalized))
          }
        }
        if (matched.forall(_.isEmpty)) {
          operator
        } else {
          // Read requirements only after finding a site; some operators log when asked.
          val required = operator.requiredChildDistribution
          if (!required.forall(_.isInstanceOf[ClusteredDistribution])) {
            operator
          } else {
            val requirements = required.map(_.asInstanceOf[ClusteredDistribution])
            pushDownThroughUnion(
              operator,
              matched.zip(requirements),
              sharedExchanges = sharedExchanges,
              policy = policy.copy(allowNonCoalescible =
                policy.allowNonCoalescible || !allExchangeInputs(operator))).getOrElse(operator)
          }
        }
    }
    val accepted = if (pushed.fastEquals(plan)) {
      plan
    } else {
      // Local checks do not cover ancestors or elided repartitions below the root. Reject the
      // whole proposal if any plan requirement or recorded output layout is lost.
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
      if (satisfied && !pushed.exists(node => !PartitioningOrigin.validateNode(node))) {
        pushed
      } else {
        plan
      }
    }
    // Re-planning creates fresh unions without the cost mark. Once a moved branch is a stage,
    // all children may already share the target, so EnsureRequirements adds no union exchange
    // and this pass has nothing to push down. Check every accepted union, including partially
    // staged ones, for direct requirement or retargeted column shuffles. Restoring the mark keeps
    // an equivalent re-plan from costing one shuffle less and masking a new shuffle elsewhere.
    // Mark only after validation so rejected candidates cannot affect subsequent cost comparisons.
    accepted.transform {
      case union: UnionExec if union.children.exists(hasPushedShuffle) && !union.isPlainUnion =>
        union.setTagValue(AQEUtils.PUSHED_THROUGH_UNION, true)
        union
    }
  }

  /**
   * One of an operator's children this rule can rewrite: the shuffle it moves and the union that
   * shuffle is moved past.
   */
  private case class PushableChild(shuffle: ShuffleExchangeExec, union: UnionExec)

  /**
   * Layout selection settings, read once per pass.
   */
  private case class LayoutPolicy(
      allowNonCoalescible: Boolean,
      maxMovedBranches: Int,
      maxUnionChildren: Int,
      keepJoinBranches: Boolean,
      allowSubsetTargets: Boolean)

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
   * Recognizes the two direct shuffle shapes produced for moved union branches: a requirement
   * shuffle, or a column repartition retargeted away from the effective default partition count.
   * Column retargeting preserves keys in order, so only its count distinguishes it here. Query
   * stages retain these origins and counts. No intermediate operators are followed: adaptive reads
   * are added after preparation. Update this check if push-down changes these shapes or origins.
   */
  private def hasPushedShuffle(plan: SparkPlan): Boolean = plan match {
    case shuffle: ShuffleExchangeLike =>
      shuffle.shuffleOrigin == ENSURE_REQUIREMENTS ||
        (shuffle.shuffleOrigin == REPARTITION_BY_COL &&
          shuffle.outputPartitioning.numPartitions != conf.numShufflePartitions)
    case stage: ShuffleQueryStageExec => hasPushedShuffle(stage.shuffle)
    case _ => false
  }

  /**
   * Rewrites eligible children in order. If a joint rewrite fails, retry individual children
   * against all current siblings, keeping children without a viable target unchanged.
   *
   * `pushable` holds one entry per child, in child order: what can be rewritten about it, and
   * what it owes the operator.
   */
  private def pushDownThroughUnion(
      operator: SparkPlan,
      pushable: Seq[(Option[PushableChild], ClusteredDistribution)],
      sharedExchanges: Set[SparkPlan],
      policy: LayoutPolicy): Option[SparkPlan] = {
    val children = operator.children
    val requiredOrderings = operator.requiredChildOrdering

    val newChildren = new ArrayBuffer[SparkPlan](children.length)
    def rebuildChildren(joint: Boolean): Boolean = {
      newChildren.clear()
      children.indices.forall { index =>
        val (pushableChild, required) = pushable(index)
        val rebuilt = pushableChild.flatMap { child =>
          val fixed = children.indices.flatMap { position =>
            val (sibling, distribution) = pushable(position)
            Option.when(position != index && (!joint || position < index || sibling.isEmpty)) {
              val fixedChild = if (position < index) {
                newChildren(position)
              } else {
                children(position)
              }
              (fixedChild, distribution)
            }
          }
          rebuildChild(child, required, fixed, sharedExchanges, policy)
        }
        if (joint && pushableChild.isDefined && rebuilt.isEmpty) {
          false
        } else {
          newChildren += rebuilt.getOrElse(children(index))
          true
        }
      }
    }

    // Joint selection lets later pushable siblings follow; the fallback fixes every sibling.
    if (!rebuildChildren(joint = true)) {
      rebuildChildren(joint = false)
    }

    // The ordering is put back once the layout is fixed. A local sort leaves the layout alone,
    // and a child that already answers keeps its own.
    val sorted = newChildren.toSeq.lazyZip(requiredOrderings).map {
      case (child, ordering) => withOrdering(child, ordering)
    }
    Some(operator.withNewChildren(sorted))
  }

  /**
   * Rebuild on the best eligible union layout satisfying the consumer and pairing with fixed
   * siblings. Subset keys require both the rule's permission and the co-partition policy, even
   * when a single-input consumer accepts a coarser distribution.
   */
  private def rebuildChild(
      pushable: PushableChild,
      required: ClusteredDistribution,
      fixed: Seq[(SparkPlan, ClusteredDistribution)],
      sharedExchanges: Set[SparkPlan],
      policy: LayoutPolicy): Option[SparkPlan] = {
    val layouts = childLayouts(pushable.union, policy)
    // Do not reduce parallelism below both the replaced exchange and the configured default.
    val minPartitions = math.min(
      pushable.shuffle.outputPartitioning.numPartitions, conf.numShufflePartitions)

    // `findPushableShuffle` already checked this chain, including local sorts.
    def rebuild(plan: SparkPlan, target: Partitioning with Expression): Option[SparkPlan] =
      plan match {
        case union: UnionExec => rebuildUnion(union, layouts, target, sharedExchanges)
        case operator: UnaryExecNode =>
          rebuild(operator.child, target).map { child =>
            // A zero-move union can be structurally equal but must remain isolated for origin tags.
            operator.mapChildrenWithReferenceEquality(_ => child)
          }
        case other =>
          throw SparkException.internalError(
            s"a shuffle push-down reached ${other.nodeName}, not the union it was found above")
      }

    candidateTargets(layouts, pushable.union.children, minPartitions).view
      .filter(target => layouts.count(child => !child.contains(target)) <= policy.maxMovedBranches)
      .flatMap(target => rebuild(pushable.shuffle.child, target))
      .map(child => PartitioningOrigin.inherit(pushable.shuffle, child))
      .find { child =>
        val partitioning = child.outputPartitioning
        partitioning.satisfies(required) &&
          PartitioningCollection.specsForPairing(partitioning, required).exists { spec =>
            spec.canCreatePartitioning && (policy.allowSubsetTargets || (spec match {
              case hash: HashShuffleSpec =>
                required.areAllClusterKeysMatched(hash.partitioning.expressions)
              case _ => false
            }))
          } &&
          ValidateRequirements.satisfiesForPairing(
            child +: fixed.map(_._1), required +: fixed.map(_._2))
      }
  }

  private case class CandidateBenefit(
      offeringChildren: Int,
      keptBytes: BigInt,
      firstOccurrence: Int)

  /**
   * Rank by estimated bytes kept when every child has a usable estimate. Missing estimates
   * trigger count-based ranking rather than treating unknown branches as smaller. Partition
   * count and first occurrence break ties deterministically.
   */
  private def candidateTargets(
      layouts: Seq[Seq[Partitioning with Expression]],
      children: Seq[SparkPlan],
      minPartitions: Int): Seq[Partitioning with Expression] = {
    val sizes = children.map { child =>
      child.logicalLink.map(_.stats.sizeInBytes).filter(_ < conf.defaultSizeInBytes)
    }
    val useSizes = sizes.forall(_.isDefined)
    val benefits = mutable.HashMap.empty[Partitioning with Expression, CandidateBenefit]
    layouts.zip(sizes).foreach { case (reported, size) =>
      reported.filter(_.numPartitions >= minPartitions).distinct.foreach { target =>
        val previous = benefits.getOrElse(target, CandidateBenefit(0, BigInt(0), benefits.size))
        benefits.update(target, previous.copy(
          offeringChildren = previous.offeringChildren + 1,
          keptBytes = previous.keptBytes + size.getOrElse(BigInt(0))))
      }
    }
    benefits.toSeq.sortBy { case (target, benefit) =>
        val keptBytes = if (useSizes) { benefit.keptBytes } else { BigInt(0) }
        (-keptBytes, -benefit.offeringChildren, -target.numPartitions, benefit.firstOccurrence)
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
   * Whether every input path reaches an exchange boundary. Stop at unstaged exchanges as well
   * as query stages: their input scans do not prevent coalescing. A non-exchange leaf already
   * prevents the consumer's compatible group from coalescing, independently of target selection.
   */
  private def allExchangeInputs(plan: SparkPlan): Boolean = plan match {
    case _: Exchange => true
    case _: ExchangeQueryStageExec => true
    case leaf if leaf.children.isEmpty => false
    case other => other.children.forall(allExchangeInputs)
  }

  /**
   * Whether a union child may retain its layout as a target. Check every input path, stopping at
   * shuffle boundaries: requirement and column repartitions are eligible. Fixed-count shuffles
   * and non-shuffle leaves require the non-coalescible allowance; other origins are rejected.
   * Shuffled joins require the join-branch opt-in; exploding joins require the non-coalescible
   * allowance. Broadcast joins inspect only their streamed side. `fromBroadcastHashJoinExec`
   * follows that probe through wrappers and stages so requirement shuffles also require the
   * join-branch opt-in, preserving opportunities for adaptive skew splitting and local reads.
   */
  private def keepableLayout(
      plan: SparkPlan,
      policy: LayoutPolicy,
      fromBroadcastHashJoinExec: Boolean = false): Boolean = plan match {
    case shuffle: ShuffleExchangeLike =>
      shuffle.shuffleOrigin match {
        case ENSURE_REQUIREMENTS => !fromBroadcastHashJoinExec || policy.keepJoinBranches
        case REPARTITION_BY_COL => true
        case REPARTITION_BY_NUM => policy.allowNonCoalescible
        case _ => false
      }
    case stage: ShuffleQueryStageExec =>
      keepableLayout(stage.shuffle, policy, fromBroadcastHashJoinExec)
    case _: ShuffledJoin if !policy.keepJoinBranches => false
    case _: BroadcastNestedLoopJoinExec | _: CartesianProductExec
        if !policy.allowNonCoalescible => false
    case join: BroadcastHashJoinExec =>
      val streamed = if (join.buildSide == BuildLeft) { join.right } else { join.left }
      keepableLayout(streamed, policy, fromBroadcastHashJoinExec = true)
    case join: BroadcastNestedLoopJoinExec =>
      val streamed = if (join.buildSide == BuildLeft) { join.right } else { join.left }
      keepableLayout(streamed, policy, fromBroadcastHashJoinExec)
    case leaf if leaf.children.isEmpty => policy.allowNonCoalescible
    case other => other.children.forall(keepableLayout(_, policy, fromBroadcastHashJoinExec))
  }

  /**
   * `union` rebuilt on `target`: eligible children are kept, unshared movable exchanges are
   * retargeted, and other children get an exchange. `None` if the union does not report `target`,
   * which is stricter than satisfying the consumer's distribution. Retargeting is limited to
   * direct, unshared requirement or column-repartition exchanges. Column repartitions retain their
   * keys in order; a key change or recorded-layout violation keeps the exchange and adds one above.
   * Tag newly added exchanges for cost accounting; retargeting adds no shuffle.
   */
  private def rebuildUnion(
      union: UnionExec,
      layouts: Seq[Seq[Partitioning with Expression]],
      target: Partitioning with Expression,
      sharedExchanges: Set[SparkPlan]): Option[SparkPlan] = {
    def addExchange(partitioning: Partitioning, child: SparkPlan): ShuffleExchangeExec = {
      val exchange = ShuffleExchangeExec(partitioning, child, ENSURE_REQUIREMENTS)
      exchange.setTagValue(AQEUtils.PUSHED_DOWN_SHUFFLE, true)
      exchange
    }
    val newChildren = union.children.zip(layouts).map { case (child, reported) =>
      if (reported.contains(target)) {
        child
      } else {
        val partitioning = rebind(target, union, child)
        child match {
          case shuffle: ShuffleExchangeExec
              if (shuffle.shuffleOrigin == ENSURE_REQUIREMENTS ||
                  (shuffle.shuffleOrigin == REPARTITION_BY_COL &&
                    samePartitionKeys(shuffle.outputPartitioning, partitioning))) &&
                (sharedExchanges.isEmpty || !sharedExchanges.contains(shuffle.canonicalized)) =>
            val retargeted = shuffle.copy(outputPartitioning = partitioning)
            retargeted.copyTagsFrom(shuffle)
            if (PartitioningOrigin.validateNode(retargeted)) {
              retargeted
            } else {
              addExchange(partitioning, child)
            }
          case _ => addExchange(partitioning, child)
        }
      }
    }
    // Origin inheritance can tag a zero-move candidate, so it must not reuse the input union.
    val rebuilt = union.copy(children = newChildren)
    rebuilt.copyTagsFrom(union)
    Option.when(
      PartitioningCollection.flatten(rebuilt.outputPartitioning).contains(target))(rebuilt)
  }

  private def samePartitionKeys(original: Partitioning, target: Partitioning): Boolean = {
    (original, target) match {
      case (from: HashPartitioning, to: HashPartitioning) =>
        from.expressions.corresponds(to.expressions)(_.semanticEquals(_))
      case _ => false
    }
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
   * replaces. These reducers stay above the union; target ranking does not estimate the lost
   * reduction on moved branches.
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
