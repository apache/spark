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
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.ShuffledJoin

/**
 * WIP / opt-in (SPARK-57399 local-repartition v2). Flips eligible [[ShuffleExchangeExec]]
 * nodes to `pipelined = true` under AQE, the adaptive counterpart of the non-AQE
 * `EnablePipelinedShuffle` preparation rule (which is a no-op once the plan is wrapped in
 * `AdaptiveSparkPlanExec`). Runs in `AdaptiveSparkPlanExec.queryStagePreparationRules`, so
 * it is re-applied on every replanning round; the decision is deterministic on plan shape,
 * and already-flipped exchanges are left alone.
 *
 * Placement adapts v1's `AQEReplaceWithLocalRepartition` policy. A flipped exchange has no
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
 * Unlike v1 (whose operator replaced hash exchanges only, leaving SinglePartition
 * exchanges as transparent regular walls), a pipelined exchange supports every
 * partitioning, so a SinglePartition exchange in a free position is simply a candidate
 * itself. The walk stops below a flipped candidate: exchanges underneath stay regular and
 * keep full AQE treatment. Candidates whose canonicalized form occurs more than once in
 * the plan (including inside materialized stages and subqueries) are skipped: flipping
 * them would trade AQE's stage reuse for duplicate recomputation, and a pipelined producer
 * cannot be consumed twice.
 */
case class AQEEnablePipelinedShuffle() extends Rule[SparkPlan] {

  private val confKey = "spark.sql.pipelinedShuffle.enabled"

  override def apply(plan: SparkPlan): SparkPlan = {
    if (conf.getConfString(confKey, "false").toBoolean != true) return plan
    // Single-executor only, like v1's AQE rule: the validated transport (the in-process
    // channel manager) requires producer and consumer in one JVM.
    if (plan.session == null || !plan.session.sparkContext.isLocal) return plan

    val shared = if (conf.exchangeReuseEnabled) duplicatedShuffleForms(plan) else Set.empty[Any]
    // v1's AQE output cap, adapted: never flip an exchange whose partition count exceeds the
    // local task-concurrency limit -- the gang's demand would exceed the slots and admission
    // would FAIL the query where the rule must simply not accelerate it. Per-candidate here
    // (unlike the non-AQE all-or-nothing): a skipped candidate just materializes as a regular
    // stage below or above the flipped tail, which the prefix shape supports.
    val cap = plan.session.sparkContext.defaultParallelism
    val toFlip = mutable.HashSet.empty[ShuffleExchangeExec]
    collectCandidates(plan, blocked = false, shared, cap, toFlip)
    if (toFlip.isEmpty) return plan

    // transformDown, NOT transformUp: candidates can be nested (a SinglePartition candidate
    // above a hash candidate). transformUp rebuilds children first, so by the time it
    // reaches the upper candidate that node is a NEW instance whose (already flipped) child
    // no longer matches the collected original structurally, and the upper flip is silently
    // dropped -- leaving a regular exchange above a pipelined one, which the scheduler then
    // rejects. transformDown hands each candidate to the pattern before its subtree is
    // rebuilt, so both nested flips apply.
    plan.transformDown {
      case s: ShuffleExchangeExec if toFlip.contains(s) => s.copy(pipelined = true)
    }
  }

  private def isCandidate(s: ShuffleExchangeExec, shared: Set[Any], cap: Int): Boolean =
    !s.pipelined && !shared.contains(s.canonicalized) && {
      val n = s.outputPartitioning.numPartitions
      n > 0 && n <= cap
    }

  /**
   * Top-down walk collecting exchanges to flip. `blocked` is true once the path from the
   * root has crossed a stats-sensitive node.
   */
  private def collectCandidates(
      plan: SparkPlan,
      blocked: Boolean,
      shared: Set[Any],
      cap: Int,
      out: mutable.HashSet[ShuffleExchangeExec]): Unit = plan match {
    case s: ShuffleExchangeExec =>
      val flipped = !blocked && isCandidate(s, shared, cap)
      if (flipped) {
        out += s
      }
      // A flipped SinglePartition exchange keeps the walk going: AQE makes no decision at
      // it (it cannot be coalesced or skew-split), so free candidates BELOW it flip too,
      // forming a pipelined chain -- v1's "transparent SinglePartition" lesson (its AQE-on
      // prototype numbers went from baseline-equal to 2.4-2.7x on exactly this), adapted
      // to v2's all-pipelined constraint: where v1 could leave the single exchange regular
      // and replace only the hash below, v2 must flip the whole chain. Below any OTHER
      // exchange (flipped or not) the walk stops: what is underneath either materializes
      // as the prefix or feeds a regular exchange whose stats AQE uses, and keeps full AQE
      // treatment either way.
      if (flipped &&
          s.outputPartitioning == org.apache.spark.sql.catalyst.plans.physical.SinglePartition) {
        collectCandidates(s.child, blocked = false, shared, cap, out)
      }

    case _: QueryStageExec => // already materialized; a leaf here

    case j: ShuffledJoin if !blocked =>
      // Flip the join's immediate shuffle inputs only as a symmetric pair.
      val leftCandidate = immediateShuffleInput(j.left, shared, cap)
      val rightCandidate = immediateShuffleInput(j.right, shared, cap)
      (leftCandidate, rightCandidate) match {
        case (Some(l), Some(r)) =>
          out += l
          out += r
        case _ => // asymmetric (a broadcast side, a materialized stage, no clean input): skip
      }
      // Anything deeper is below a join input; blocked either way.

    case p if isStatsSensitive(p) =>
      p.children.foreach(collectCandidates(_, blocked = true, shared, cap, out))

    case p =>
      p.children.foreach(collectCandidates(_, blocked, shared, cap, out))
  }

  /**
   * The single eligible [[ShuffleExchangeExec]] at the top of one join input, looking
   * through unary non-stats-sensitive forwarders. None if the input is anything else.
   */
  private def immediateShuffleInput(
      plan: SparkPlan,
      shared: Set[Any],
      cap: Int): Option[ShuffleExchangeExec] = plan match {
    case s: ShuffleExchangeExec => Some(s).filter(isCandidate(_, shared, cap))
    case _: QueryStageExec => None
    case p if isStatsSensitive(p) => None
    case p if p.children.size == 1 => immediateShuffleInput(p.children.head, shared, cap)
    case _ => None
  }

  /** Nodes below which AQE typically consumes map output statistics from stages. */
  private def isStatsSensitive(plan: SparkPlan): Boolean = plan match {
    case _: BinaryExecNode => true
    case _: ShuffleExchangeExec => true
    case _ => false
  }

  /**
   * Canonicalized forms of shuffle exchanges occurring more than once across the plan,
   * materialized stages, and subquery plans -- flipping one of these loses stage reuse.
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
