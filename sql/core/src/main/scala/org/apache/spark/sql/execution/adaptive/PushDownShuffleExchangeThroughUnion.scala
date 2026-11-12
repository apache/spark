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
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{
  FilterExec, ProjectExec, SortExec, SparkPlan, UnaryExecNode, UnionExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, Exchange, PartitioningOrigin, REPARTITION_BY_COL, REPARTITION_BY_NUM, ShuffleExchangeExec, ShuffleExchangeLike, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec, ShuffledJoin}
import org.apache.spark.sql.execution.window.{Partial => PartialWindowGroupLimit, WindowGroupLimitExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Push down a shuffle [[EnsureRequirements]] put above a [[UnionExec]] below the union. Eligible
 * children already reporting the target retain their layouts; other children are retargeted or
 * receive a new shuffle. Local sorts above the shuffle and operators between it and the union
 * are rebuilt above the union:
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
 * `spark.sql.adaptive.pushDownShuffleThroughUnion.lookThroughOperators.enabled`, and only when
 * moving the shuffle past them preserves results: a deterministic project, a local sort, a
 * filter, a partial aggregate and a partial window group limit. The reducing ones are why the
 * gate exists: the child is then shuffled before the reduction, which can send more data than the
 * one shuffle it replaces.
 *
 * Targets are hash layouts reported by the union's children, ranked by how many children offer
 * them when size estimates are unavailable; otherwise estimated bytes kept decide first. The
 * count must be between the smaller and the larger of the replaced exchange's count and the
 * effective initial shuffle partition count (`coalescePartitions.initialPartitionNum` when set);
 * the larger side is scaled by `maxTargetPartitionNumFactor`. A branch that is already a query
 * stage pins the target to the layout it reports, unless its stage wraps an unmarked user
 * repartition: the rewrite could only stack on such a branch, and a re-plan must be able to
 * repeat that. At least one child keeps its layout; others are retargeted or shuffled without
 * changing child order.
 * `spark.sql.adaptive.pushDownShuffleThroughUnion.maxMovedBranches` limits moved children, and
 * `maxUnionChildren` limits all branches whose tails run within each consumer task.
 * The rebuilt child must satisfy its consumer's distribution and pair with fixed siblings.
 *
 * Removing the union's exchange can reduce AQE join conversion and skew handling opportunities.
 * Keeping a shuffled join child's layout can make skew join optimization or local shuffle reads
 * invalidate an ancestor's distribution. AQE can discard the optimization for the round, or add
 * another shuffle when skew optimization is forced. Join inputs and join branches are opt-in.
 * Allowing targets via
 * `spark.sql.adaptive.pushDownShuffleThroughUnion.keepNonCoalescibleBranches.enabled`
 * can prevent coalescing of the consumer's compatible shuffle group. Forced skew optimization
 * can restore a union exchange after moved branches have shuffled, shuffling them a second time.
 *
 * The query-wide subquery gate includes optimizer-injected runtime Bloom filter builds.
 * Allowing subqueries can make pruning builds differ from main-query broadcast builds. With
 * broadcast-only dynamic pruning, failed broadcast reuse removes the pruning filter and can scan
 * all fact partitions; that cost can outweigh the saved build-side shuffle.
 *
 * The rule runs only in AQE preparation, after `EnsureRequirements` and before
 * `StampUnionDecisions`. It preserves the root distribution and all user repartition layouts
 * recorded when `EnsureRequirements` elides exchanges, including requirements below the root.
 * Recording and push-down share the preparation's effective enablement on EnsureRequirements;
 * classic and disabled preparations do not record these origins.
 * A proposal that breaks an ancestor requirement or a recorded layout is retried once on the
 * layout of the exchange being removed, which every ancestor was planned against.
 * Every exchange the rewrite inserts or retargets below a union carries a [[PushedDownShuffle]]
 * mark naming its kind. The cost model excludes inserted exchanges, keeps retargeted ones
 * counted, and charges a union one shuffle when a direct child carries the mark; a union
 * without one costs nothing. A branch served by reuse of an already materialized stage keeps
 * that stage's own exchange: reuse of an unmarked exchange adds no shuffle and is not charged,
 * while a marked one, possibly inserted for another site, still is.
 * Cost compensation belongs to SimpleCostEvaluator. Custom evaluators should account for the
 * one-to-many exchange rewrite when this rule is enabled, or they may reject the entire re-plan.
 *
 * TODO: support push down of a keyed layout.
 */
case class PushDownShuffleExchangeThroughUnion(
    ensureRequirements: EnsureRequirements) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!ensureRequirements.pushDownShuffleThroughUnionEnabled) {
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
      maxTargetPartitionNumFactor = conf.getConf(
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_MAX_TARGET_PARTITION_NUM_FACTOR),
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

    // Whether every adopted target is the layout of the exchange this rewrite removes. The retry
    // cannot help while this holds.
    var usedRemovedLayout = true
    // Whether an earlier site in this pass has rewritten the plan. Until then its layouts are the
    // ones preparation produced and can be read without checking.
    var hasPushed = false
    def rewrite(preferRemovedLayout: Boolean): SparkPlan = {
      hasPushed = false
      plan.transformUp {
        case join: ShuffledJoin if !allowJoinInputs => join
        case operator: SparkPlan =>
          val matched = operator.children.map { child =>
            findPushableShuffle(child, lookThrough, policy)
          }
          if (matched.forall(_.isEmpty)) {
            operator
          } else {
            // Read requirements only after finding a site; some operators log when asked.
            val required = operator.requiredChildDistribution
            if (!required.forall(_.isInstanceOf[ClusteredDistribution])) {
              operator
            } else if (hasPushed && operator.children.exists(child =>
                !ValidateRequirements.validate(child))) {
              // An earlier site in this pass can leave a child invalid; its layout cannot be read.
              operator
            } else {
              val requirements = required.map(_.asInstanceOf[ClusteredDistribution])
              val (candidate, operatorUsedRemovedLayout) = pushDownThroughUnion(
                operator,
                matched.zip(requirements),
                sharedExchangesFunc = sharedExchanges,
                policy = policy.copy(
                  allowNonCoalescible =
                    policy.allowNonCoalescible || !allExchangeInputs(operator),
                  preferRemovedLayout = preferRemovedLayout))
              usedRemovedLayout = usedRemovedLayout && operatorUsedRemovedLayout
              hasPushed = hasPushed || (candidate ne operator)
              candidate
            }
          }
      }
    }

    def accepted(candidate: SparkPlan): Boolean = {
      // Local checks do not cover ancestors or elided repartitions below the root. Reject the
      // whole proposal if any plan requirement or recorded output layout is lost.
      val satisfied = ensureRequirements.requiredDistribution match {
        // A user repartition elided above was asked for on all its keys, so a layout on a subset
        // of them does not stand in for it.
        case Some(required: ClusteredDistribution) =>
          ValidateRequirements.validate(candidate, required.copy(requireAllClusterKeys = true))
        case Some(required) =>
          ValidateRequirements.validate(candidate, required)
        case None =>
          ValidateRequirements.validate(candidate)
      }
      satisfied && !candidate.exists(node => !PartitioningOrigin.validateNode(node))
    }

    val pushed = rewrite(preferRemovedLayout = false)
    if (pushed.fastEquals(plan)) {
      plan
    } else if (accepted(pushed)) {
      pushed
    } else if (usedRemovedLayout) {
      // Every site already used the layout of the exchange this rewrite removes, so the retry
      // would rebuild the same proposal and fail again.
      plan
    } else {
      // A target can satisfy the consumer and the fixed siblings yet break an ancestor, such as
      // a join that pairs this branch with a different partition count. Retry once on the removed
      // exchange's own layout, which every ancestor was planned against.
      val retried = rewrite(preferRemovedLayout = true)
      if (retried.fastEquals(plan)) {
        plan
      } else if (accepted(retried)) {
        retried
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
   * Layout selection settings, read once per pass.
   */
  private case class LayoutPolicy(
      allowNonCoalescible: Boolean,
      maxMovedBranches: Int,
      maxUnionChildren: Int,
      maxTargetPartitionNumFactor: Double,
      keepJoinBranches: Boolean,
      allowSubsetTargets: Boolean,
      preferRemovedLayout: Boolean = false)

  /** Lazily discover and rank layouts once per site for both joint and fallback attempts. */
  private class PreparedChild(val pushable: PushableChild, policy: LayoutPolicy) {
    /**
     * Eligible child layouts in child order, expressed in the union's output attributes.
     * Collections offer their hash members; a child offering nothing takes a shuffle.
     * Equality follows UnionExec's comparison after prepareOutputPartitioning rebinding.
     */
    lazy val layouts: Seq[Seq[Partitioning with Expression]] = {
      val union = pushable.union
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

    lazy val targets: Seq[Partitioning with Expression] = {
      val minPartitions = math.min(removedLayout.numPartitions, conf.numShufflePartitions)
      val maxPartitions = math.ceil(
        math.max(removedLayout.numPartitions, conf.numShufflePartitions) *
          policy.maxTargetPartitionNumFactor).toInt
      val candidates = candidateTargets(
        layouts, pushable.union.children, minPartitions, maxPartitions, stagedIndices)
      val limited = if (policy.preferRemovedLayout) {
        // The whole-plan check failed. Retry on the layout of the exchange this rewrite removes:
        // every ancestor was planned against it. Keys that a look-through operator renames match
        // nothing here, so that site stays unchanged.
        candidates.filter(_.semanticEquals(removedLayout))
      } else {
        candidates
      }
      limited.filter { target =>
        layouts.count(child => !child.contains(target)) <= policy.maxMovedBranches
      }
    }

    /**
     * The layout of the exchange this rewrite removes. `findPushableShuffle` only admits a hash
     * shuffle, and every ancestor was planned against this layout, so a failed whole-plan check
     * can fall back to it.
     */
    lazy val removedLayout: Partitioning with Expression =
      pushable.shuffle.outputPartitioning.asInstanceOf[HashPartitioning]

    /**
     * Indices of branches that are already shuffle query stages and pin the target: their
     * exchange carries a rewrite mark, or `EnsureRequirements` added it. Such a branch carries
     * the shuffle work that laid it out, so laying it out again adds a shuffle on top of
     * materialized data, and a target is viable only when every one of these branches already
     * reports it. An unmarked user repartition is left out: the rewrite could not retarget it and
     * stacked an exchange instead, which a re-plan has to be able to repeat. Non-staged branches
     * do not constrain the target: one that reports it is kept, and one that does not is laid out
     * as usual.
     */
    private lazy val stagedIndices: Set[Int] = {
      val children = pushable.union.children
      children.indices.filter { index =>
        children(index) match {
          case stage: ShuffleQueryStageExec =>
            val shuffle = stage.shuffle
            PushDownShuffleExchangeThroughUnion.isMarked(shuffle) ||
              shuffle.shuffleOrigin == ENSURE_REQUIREMENTS
          case _ => false
        }
      }.toSet
    }
  }

  /**
   * The shape this rule acts on below one of an operator's children: a pushable shuffle and the
   * union it is moved past, with the operators in between looked through to reach it. Only a hash
   * shuffle is pushable, since the layout that replaces it comes from the union's own children,
   * and a union with more children than `maxUnionChildren` is not a site.
   *
   * What the child owes its operator is not read here: this is asked of every child of every
   * operator, and `requiredChildDistribution` is not free of side effects: `WindowExecBase` logs
   * from it for an unpartitioned window.
   */
  @tailrec
  private def findPushableShuffle(
      plan: SparkPlan,
      lookThrough: Boolean,
      policy: LayoutPolicy,
      found: Option[ShuffleExchangeExec] = None): Option[PushableChild] = {
    plan match {
      // A local sort above the shuffle is looked through, since the rebuilt child is sorted back
      // to the same requirement; one below it is an operator in between and needs the gate.
      case SortExec(_, false, child, _) if found.isEmpty || lookThrough =>
        findPushableShuffle(child, lookThrough, policy, found)
      case shuffle @ ShuffleExchangeExec(_: HashPartitioning, child, ENSURE_REQUIREMENTS, _, _)
          if found.isEmpty =>
        findPushableShuffle(child, lookThrough, policy, Some(shuffle))
      // A union is a site only when the shuffle below it was found and it is eligible for one.
      // AttributeMap keeps the last duplicate exprId, while runtime binding uses the first;
      // positional rebinding is ambiguous for those unions, so retain their exchange. A union
      // with too many children is not a site either.
      case union: UnionExec if found.isEmpty || union.childOutputRepeatsExprId ||
          union.children.length > policy.maxUnionChildren =>
        None
      // Below it: the operators this rule can cross, down to the union the shuffle is moved past.
      case union: UnionExec =>
        found.map(PushableChild(_, union))
      case operator: UnaryExecNode if found.isDefined && lookThrough && canCross(operator) =>
        findPushableShuffle(operator.child, lookThrough, policy, found)
      case _ => None
    }
  }

  /**
   * Rewrites eligible children in order. If a joint rewrite fails, retry individual children
   * against all current siblings, keeping children without a viable target unchanged.
   *
   * Returns the rewritten operator and whether every site used the layout of the exchange this
   * rewrite removes; a failed whole-plan check uses the latter to decide whether a retry on that
   * layout can produce a different proposal.
   *
   * `pushable` holds one entry per child, in child order: what can be rewritten about it, and
   * what it owes the operator.
   */
  private def pushDownThroughUnion(
      operator: SparkPlan,
      pushable: Seq[(Option[PushableChild], ClusteredDistribution)],
      sharedExchangesFunc: => Set[SparkPlan],
      policy: LayoutPolicy): (SparkPlan, Boolean) = {
    val children = operator.children
    val requiredOrderings = operator.requiredChildOrdering

    // Discover layouts and targets before walking the whole plan for shared exchanges, so a
    // site with no viable target is rejected without that walk.
    val prepared = pushable.map { case (site, _) =>
      site.map(child => new PreparedChild(child, policy))
    }
    if (!prepared.exists(_.exists(_.targets.nonEmpty))) {
      return (operator, true)
    }
    val sharedExchanges = sharedExchangesFunc
    val movableSites = pushable.zip(prepared).map { case ((site, required), child) =>
      val movable = site.exists { pushableChild =>
        sharedExchanges.isEmpty || !sharedExchanges.contains(pushableChild.shuffle.canonicalized)
      }
      (if (movable) child else None, required)
    }

    val newChildren = new ArrayBuffer[SparkPlan](children.length)
    // Whether every target adopted by the attempt whose result is used is the layout of the
    // exchange this rewrite removes. The retry cannot help while this holds.
    var usedRemovedLayout = true
    def rebuildChildren(joint: Boolean): Boolean = {
      // The joint attempt may have adopted another layout before failing on a sibling; only the
      // attempt whose children are used counts.
      usedRemovedLayout = true
      newChildren.clear()
      children.indices.forall { index =>
        val (pushableChild, required) = movableSites(index)
        val rebuilt = pushableChild.flatMap { child =>
          val fixed = children.indices.flatMap { position =>
            val (sibling, distribution) = movableSites(position)
            Option.when(position != index && (!joint || position < index || sibling.isEmpty)) {
              val fixedChild = if (position < index) {
                newChildren(position)
              } else {
                children(position)
              }
              (fixedChild, distribution)
            }
          }
          rebuildChild(child, required, fixed, sharedExchanges, policy).map {
            case (candidate, usesRemovedLayout) =>
              usedRemovedLayout = usedRemovedLayout && usesRemovedLayout
              candidate
          }
        }
        if (joint && pushableChild.isDefined && rebuilt.isEmpty) {
          false
        } else {
          newChildren += rebuilt.getOrElse(children(index))
          true
        }
      }
    }

    // Joint selection lets later pushable siblings follow; the fallback fixes every sibling. A
    // single pushable child makes both attempts identical, so a failed joint attempt stays as is.
    if (!rebuildChildren(joint = true)) {
      if (movableSites.count(_._1.isDefined) <= 1) {
        return (operator, usedRemovedLayout)
      }
      rebuildChildren(joint = false)
    }

    // The ordering is put back once the layout is fixed. A local sort leaves the layout alone,
    // and a child that already answers keeps its own.
    val sorted = newChildren.toSeq.lazyZip(requiredOrderings).map {
      case (child, ordering) => withOrdering(child, ordering)
    }
    (operator.withNewChildren(sorted), usedRemovedLayout)
  }

  /**
   * Rebuild on the best eligible union layout satisfying the consumer and pairing with fixed
   * siblings. Subset keys require both the rule's permission and the co-partition policy, even
   * when a single-input consumer accepts a coarser distribution. Returns the rebuilt child and
   * whether its layout is the one the rewrite removes.
   */
  private def rebuildChild(
      prepared: PreparedChild,
      required: ClusteredDistribution,
      fixed: Seq[(SparkPlan, ClusteredDistribution)],
      sharedExchanges: Set[SparkPlan],
      policy: LayoutPolicy): Option[(SparkPlan, Boolean)] = {
    val pushable = prepared.pushable
    val layouts = prepared.layouts

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

    prepared.targets.view
      .flatMap { target =>
        rebuild(pushable.shuffle.child, target).map(child => (target, child))
      }
      .map { case (target, child) =>
        (target, PartitioningOrigin.inherit(pushable.shuffle, child))
      }
      .find { case (_, child) =>
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
      .map { case (target, child) =>
        (child, target.semanticEquals(prepared.removedLayout))
      }
  }

  private case class CandidateBenefit(
      offeredBy: Set[Int],
      keptBytes: BigInt,
      firstOccurrence: Int)

  /**
   * Rank by estimated bytes kept when every child has a usable estimate. Missing or zero estimates
   * trigger count-based ranking rather than treating unknown branches as smaller. Only targets
   * within the partition count bounds are eligible. Targets that a staged child does not offer are
   * dropped, so no materialized branch is laid out twice. Partition count and first occurrence
   * break ties deterministically.
   */
  private def candidateTargets(
      layouts: Seq[Seq[Partitioning with Expression]],
      children: Seq[SparkPlan],
      minPartitions: Int,
      maxPartitions: Int,
      stagedIndices: Set[Int]): Seq[Partitioning with Expression] = {
    val sizes = children.map { child =>
      child.logicalLink.map(_.stats.sizeInBytes).filter { size =>
        size > 0 && size < conf.defaultSizeInBytes
      }
    }
    val useSizes = sizes.forall(_.isDefined)
    val benefits = mutable.HashMap.empty[Partitioning with Expression, CandidateBenefit]
    layouts.zip(sizes).zipWithIndex.foreach { case ((reported, size), index) =>
      reported.filter { target =>
        target.numPartitions >= minPartitions && target.numPartitions <= maxPartitions
      }.distinct.foreach { target =>
        val previous = benefits.getOrElse(
          target, CandidateBenefit(Set.empty, BigInt(0), benefits.size))
        benefits.update(target, previous.copy(
          offeredBy = previous.offeredBy + index,
          keptBytes = previous.keptBytes + size.getOrElse(BigInt(0))))
      }
    }
    benefits.toSeq
      .filter { case (_, benefit) => stagedIndices.subsetOf(benefit.offeredBy) }
      .sortBy { case (target, benefit) =>
        val keptBytes = if (useSizes) { benefit.keptBytes } else { BigInt(0) }
        (-keptBytes, -benefit.offeredBy.size, -target.numPartitions, benefit.firstOccurrence)
      }
      .map(_._1)
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
   * allowance. Unions with duplicate child output identities cannot supply a kept layout, even
   * through wrappers, because their positional mapping is ambiguous.
   * Broadcast joins inspect only their streamed side. `fromBroadcastHashJoinExec` follows that
   * probe through wrappers and stages so requirement shuffles also require the join-branch opt-in,
   * preserving opportunities for adaptive skew splitting and local reads.
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
    case union: UnionExec if union.childOutputRepeatsExprId => false
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
   * Tag added exchanges as free and retargeted ones as counted for cost accounting; retargeting
   * adds no shuffle.
   */
  private def rebuildUnion(
      union: UnionExec,
      layouts: Seq[Seq[Partitioning with Expression]],
      target: Partitioning with Expression,
      sharedExchanges: Set[SparkPlan]): Option[SparkPlan] = {
    def addExchange(partitioning: Partitioning, child: SparkPlan): ShuffleExchangeExec = {
      val exchange = ShuffleExchangeExec(partitioning, child, ENSURE_REQUIREMENTS)
      PushDownShuffleExchangeThroughUnion.mark(exchange, PushedShuffle)
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
            PushDownShuffleExchangeThroughUnion.mark(retargeted, RetargetedShuffle)
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
  private def canCross(operator: UnaryExecNode): Boolean = operator match {
    case project: ProjectExec =>
      project.projectList.forall(_.deterministic)
    case filter: FilterExec =>
      filter.condition.deterministic
    case aggregate: BaseAggregateExec =>
      !aggregate.isStreaming &&
        aggregate.requiredChildDistributionExpressions.isEmpty &&
        aggregate.groupingExpressions.nonEmpty &&
        !aggregate.groupingExpressions.exists(_.metadata.contains(SessionWindow.marker)) &&
        aggregate.aggregateExpressions.forall(_.mode == PartialAggregate) &&
        aggregate.expressions.forall(_.deterministic)
    case limit: WindowGroupLimitExec =>
      limit.mode == PartialWindowGroupLimit
    case _ => false
  }

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

/**
 * How a shuffle below a union came to be there, recorded by the push-down rewrite. One mark per
 * kind, so the cost model can tell the exchanges the rewrite created or retargeted apart.
 */
private[adaptive] sealed trait PushedDownShuffle

/** Inserted below a union by the rewrite, replacing the union's removed exchange. */
private[adaptive] case object PushedShuffle extends PushedDownShuffle

/** Retargeted below a union by the rewrite; the exchange existed before the rewrite. */
private[adaptive] case object RetargetedShuffle extends PushedDownShuffle

object PushDownShuffleExchangeThroughUnion {
  private val PUSHED_DOWN_SHUFFLE = TreeNodeTag[PushedDownShuffle]("unionPushedDownShuffle")

  private[adaptive] def mark(plan: SparkPlan, kind: PushedDownShuffle): Unit =
    plan.setTagValue(PUSHED_DOWN_SHUFFLE, kind)

  /**
   * Whether this node carries either kind of rewrite mark.
   */
  private[adaptive] def isMarked(plan: SparkPlan): Boolean =
    plan.getTagValue(PUSHED_DOWN_SHUFFLE).isDefined

  /**
   * Whether this node is a shuffle inserted below a union in place of its removed exchange. Custom
   * cost evaluators can distinguish these exchanges from ordinary shuffles; an exchange only
   * retargeted by the rule carries [[RetargetedShuffle]] and still counts.
   */
  def isPushedDownShuffle(plan: SparkPlan): Boolean =
    plan.getTagValue(PUSHED_DOWN_SHUFFLE).contains(PushedShuffle)

  /**
   * Whether this union counts one shuffle in the push-down cost model: a direct child is an
   * exchange the rewrite marked. Staging keeps the mark, since the stage wraps that exchange.
   * A branch served by reuse of an already materialized stage keeps that stage's own exchange:
   * reuse of an unmarked one adds no shuffle and is not counted, while a marked one, possibly
   * inserted for another site, still is. Pruning the only qualifying child removes the
   * contribution.
   * Costs are compared before read optimization, which runs inside exchange or final result stages.
   */
  def hasPushedShuffle(union: UnionExec): Boolean = {
    def counted(plan: SparkPlan): Boolean = plan match {
      case shuffle: ShuffleExchangeLike => isMarked(shuffle)
      case stage: ShuffleQueryStageExec => counted(stage.shuffle)
      case _ => false
    }
    union.children.exists(counted)
  }
}
