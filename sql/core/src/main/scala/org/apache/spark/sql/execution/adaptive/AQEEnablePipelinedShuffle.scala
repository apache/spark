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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{BinaryExecNode, CoalesceExec, CollectLimitExec, CollectTailExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.exchange.{PipelinedShuffleEligibility, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.ShuffledJoin

/**
 * Opt-in (SPARK-57399). Flips eligible [[ShuffleExchangeExec]]
 * nodes to `pipelined = true` under AQE, the adaptive counterpart of the non-AQE
 * `EnablePipelinedShuffle` preparation rule (which is a no-op once the plan is wrapped in
 * `AdaptiveSparkPlanExec`). Runs in `AdaptiveSparkPlanExec.queryStagePreparationRules`, so
 * it is re-applied on every replanning round; the decision is deterministic on plan shape,
 * and already-flipped exchanges are left alone.
 *
 * Placement policy. A flipped exchange has no
 * map output statistics (it never materializes as a query stage: the DAGScheduler
 * gang-runs it inline with its consumer in the final job -- see the pipelined case in
 * `AdaptiveSparkPlanExec.createNonResultQueryStages`), so only exchanges whose statistics
 * no AQE decision consumes are flipped:
 *
 *   - "free" candidate: the path from the candidate to the plan root crosses no
 *     stats-sensitive node ([[BinaryExecNode]], another [[ShuffleExchangeExec]], or a
 *     query stage). Its own coalescing/skew handling is given up; nothing above needed its
 *     stats.
 *   - "join-paired" candidate: the immediate shuffle inputs of a [[ShuffledJoin]] whose
 *     path to the root is otherwise free, flipped only as a symmetric pair (an asymmetric
 *     flip would leave one side participating in AQE coalesce/skew and the other fixed).
 *   - everything else stays regular and materializes as usual -- those stages form the
 *     fully-materialized prefix the scheduler's mixed-job shape requires.
 *
 * A pipelined exchange supports every
 * partitioning, so a SinglePartition exchange in a free position is simply a candidate
 * itself. The walk stops below a flipped candidate: exchanges underneath stay regular and
 * keep full AQE treatment. Candidates whose canonicalized form occurs more than once in
 * the plan (including inside materialized stages and subqueries) are skipped: flipping
 * them would trade AQE's stage reuse for duplicate recomputation, and a pipelined producer
 * cannot be consumed twice.
 */
case class AQEEnablePipelinedShuffle() extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    // Shared environment gate (opt-in flag, single-executor local mode, channel manager active),
    // identical to the non-AQE rule's -- see PipelinedShuffleEligibility for why it is a
    // correctness gate that must not drift between the two rules.
    if (!PipelinedShuffleEligibility.enabled(plan, conf)) return plan

    flipEligibleExchanges(plan)
  }

  /**
   * The plan-shape core of the rule, factored out of [[apply]]'s environment guards (opt-in flag,
   * local mode, channel manager) so it can be unit-tested on a hand-built plan directly. Collects
   * the eligible exchanges and returns the plan with each flipped to `pipelined = true`.
   */
  private[adaptive] def flipEligibleExchanges(plan: SparkPlan): SparkPlan = {
    val shared = if (conf.exchangeReuseEnabled) duplicatedShuffleForms(plan) else Set.empty[Any]
    // Collect the exchanges to flip BY IDENTITY (SparkPlan.id, unique per instance), not by the
    // node itself: TreeNode overrides hashCode but not equals, so a HashSet[ShuffleExchangeExec]
    // matches structurally, and the transformDown below would then flip EVERY exchange
    // structurally equal to a collected one -- including a twin the collector deliberately left
    // regular on a blocked path. That twin, if it sits below a regular boundary, makes
    // classifyJobShuffleShape reject the whole job. Keying on the instance id flips exactly the
    // nodes the collector chose, regardless of spark.sql.exchange.reuse (duplicatedShuffleForms,
    // the only other guard, is empty when reuse is off). transformDown matches each ORIGINAL node
    // before rebuilding it, so its id is the same instance id the collector recorded.
    val toFlip = mutable.HashSet.empty[Int]
    collectCandidates(plan, blocked = false, shared, toFlip)
    if (toFlip.isEmpty) return plan

    // transformDown, NOT transformUp: candidates can be nested (a SinglePartition candidate
    // above a hash candidate). transformUp rebuilds children first, so by the time it
    // reaches the upper candidate that node is a NEW instance whose (already flipped) child
    // no longer matches the collected original structurally, and the upper flip is silently
    // dropped -- leaving a regular exchange above a pipelined one, which the scheduler then
    // rejects. transformDown hands each candidate to the pattern before its subtree is
    // rebuilt, so both nested flips apply.
    plan.transformDown {
      case s: ShuffleExchangeExec if toFlip.contains(s.id) => s.copy(pipelined = true)
    }
  }

  private def isCandidate(s: ShuffleExchangeExec, shared: Set[Any]): Boolean =
    !s.pipelined && !shared.contains(s.canonicalized)

  /**
   * Top-down walk collecting exchanges to flip. `blocked` is true once the path from the
   * root has crossed a stats-sensitive node.
   */
  private def collectCandidates(
      plan: SparkPlan,
      blocked: Boolean,
      shared: Set[Any],
      out: mutable.HashSet[Int]): Unit = plan match {
    case s: ShuffleExchangeExec =>
      val flipped = !blocked && isCandidate(s, shared)
      if (flipped) {
        out += s.id
      }
      // A flipped SinglePartition exchange keeps the walk going: AQE makes no decision at
      // it (it cannot be coalesced or skew-split), so free candidates BELOW it flip too,
      // forming a pipelined chain: all exchanges in such a chain must flip together (a
      // SinglePartition exchange cannot be coalesced or skew-split, so AQE makes no decision
      // at it and free candidates below it flip too). Below any OTHER exchange (flipped or
      // not) the walk stops: what is underneath either materializes as the prefix or feeds
      // a regular exchange whose stats AQE uses, and keeps full AQE treatment either way.
      if (flipped &&
          s.outputPartitioning == org.apache.spark.sql.catalyst.plans.physical.SinglePartition) {
        collectCandidates(s.child, blocked = false, shared, out)
      }

    case _: QueryStageExec => // already materialized; a leaf here

    case j: ShuffledJoin if !blocked =>
      // Flip the join's immediate shuffle inputs only as a symmetric pair.
      val leftCandidate = immediateShuffleInput(j.left, shared)
      val rightCandidate = immediateShuffleInput(j.right, shared)
      (leftCandidate, rightCandidate) match {
        case (Some(l), Some(r)) =>
          out += l.id
          out += r.id
        case _ => // asymmetric (a broadcast side, a materialized stage, no clean input): skip
      }
      // Anything deeper is below a join input; blocked either way.

    case p if isStatsSensitive(p) =>
      p.children.foreach(collectCandidates(_, blocked = true, shared, out))

    case p =>
      p.children.foreach(collectCandidates(_, blocked, shared, out))
  }

  /**
   * The single eligible [[ShuffleExchangeExec]] at the top of one join input, looking
   * through unary non-stats-sensitive forwarders. None if the input is anything else.
   */
  private def immediateShuffleInput(
      plan: SparkPlan,
      shared: Set[Any]): Option[ShuffleExchangeExec] = plan match {
    case s: ShuffleExchangeExec => Some(s).filter(isCandidate(_, shared))
    case _: QueryStageExec => None
    case p if isStatsSensitive(p) => None
    case p if p.children.size == 1 => immediateShuffleInput(p.children.head, shared)
    case _ => None
  }

  /**
   * Nodes below which a candidate exchange must NOT be flipped. Reasons a node lands here:
   *   - AQE consumes map output statistics from the stages below it ([[BinaryExecNode]], another
   *     [[ShuffleExchangeExec]]); flipping below it would give up stats a decision needs.
   *   - [[CoalesceExec]] reads its child shuffle multi-partition-per-task (a `CoalescedRDD` over
   *     the `ShuffledRowRDD`), which the channel transport cannot serve; the shuffle it reads
   *     must stay regular.
   *   - the limit operators [[CollectLimitExec]] / [[CollectTailExec]] /
   *     [[TakeOrderedAndProjectExec]] each build a hidden regular (`pipelined = false`) shuffle
   *     inside `doExecute` (via `prepareShuffleDependency`) that no plan walk can see; a flipped
   *     exchange below one of them would sit under that unmaterialized regular boundary and the
   *     job would hard-fail at submission (pipelined-below-regular).
   * Blocking here keeps the shuffle below -- and everything deeper -- regular, so no pipelined
   * exchange ends up below the operator's regular boundary. See the non-AQE
   * `EnablePipelinedShuffle` for the full rationale on each operator.
   */
  private def isStatsSensitive(plan: SparkPlan): Boolean = plan match {
    case _: BinaryExecNode => true
    case _: ShuffleExchangeExec => true
    case _: CoalesceExec => true
    case _: CollectLimitExec | _: CollectTailExec | _: TakeOrderedAndProjectExec => true
    case _ => false
  }

  /**
   * Canonicalized forms of shuffle exchanges occurring more than once across the plan,
   * materialized stages, and subquery plans -- flipping one of these loses stage reuse.
   *
   * Note: `ShuffleExchangeExec.pipelined` is a plain case-class field with no doCanonicalize
   * override, so it PARTICIPATES in the canonical form. An already-flipped (pipelined = true)
   * exchange and a structurally identical unflipped twin therefore canonicalize DIFFERENTLY and
   * would not be paired here. The counts are taken over the CURRENT plan on each replanning
   * round (and this rule does re-run in later rounds on a plan that may already contain an
   * earlier round's flip -- it is NOT a before-any-flip pass). That is still safe: a flipped and
   * an unflipped structural twin cannot coexist as a real duplicate, because reuse collapses
   * twins (ReuseExchangeAndSubquery / ShuffleQueryStageExec) before this rule runs, and if one
   * somehow slipped through, the DAGScheduler's fan-out check rejects a multi-consumer pipelined
   * producer rather than producing wrong results. So missing such a pair here is fail-safe.
   */
  private def duplicatedShuffleForms(plan: SparkPlan): Set[Any] = {
    val counts = mutable.HashMap.empty[Any, Int]
    def inc(s: ShuffleExchangeExec): Unit = {
      val c = s.canonicalized
      counts.update(c, counts.getOrElse(c, 0) + 1)
    }
    def visit(p: SparkPlan): Unit = p.foreach {
      case s: ShuffleExchangeExec => inc(s)
      case q: ShuffleQueryStageExec =>
        q.plan match {
          case s: ShuffleExchangeExec => inc(s)
          case r: ReusedExchangeExec =>
            r.child match {
              case s: ShuffleExchangeExec => inc(s)
              case _ =>
            }
          case _ =>
        }
      case _ =>
    }
    visit(plan)
    plan.subqueriesAll.foreach(visit)
    counts.filter(_._2 > 1).keys.toSet
  }
}
