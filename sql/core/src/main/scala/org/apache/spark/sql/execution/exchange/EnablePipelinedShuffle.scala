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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CollectLimitExec, CollectTailExec, CoalesceExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.joins.CartesianProductExec

/**
 * Opt-in (SPARK-57399). Rewrites EVERY [[ShuffleExchangeExec]] in a
 * batch physical plan to `pipelined = true`, so each shuffle is served by the in-process
 * pipelined channel manager and the concurrent-stage scheduler runs the map and reduce stages
 * together. This is the minimal SQL entry point that lets a batch query exercise the
 * pipelined channel execution path; a production version would be a targeted,
 * cost/shape-aware replacement rather than a blanket rewrite.
 *
 * Enabled only when `spark.sql.pipelinedShuffle.enabled=true`. It runs in the non-AQE
 * `preparations` list, so it also requires AQE to be off (under AQE the plan is hidden behind
 * an opaque `AdaptiveSparkPlanExec` leaf and this rule sees no exchanges).
 *
 * Rewriting ALL shuffles (not just hash-partitioning ones) keeps the job all-pipelined, which
 * the DAGScheduler requires: a mix of pipelined and regular shuffles in one job is rejected.
 * SinglePartition and RangePartitioning exchanges pipeline fine -- the channel transport only
 * routes by `partitioner.getPartition(key)` and does not care which partitioning produced the
 * id (SinglePartition is the numPartitions == 1 degenerate case).
 *
 * These shapes make the rule leave the whole plan regular:
 *   - reuse: a pipelined producer with more than one consumer (fan-out) is rejected, so if any
 *     exchange in the plan is reused the rule bails out.
 *   - an UNSUPPORTED CONSUMER reading a shuffle (see [[readsShuffleThroughUnsupportedConsumer]]):
 *     an operator that would drain a shuffle in a way the channel transport cannot serve, or that
 *     builds its own hidden regular shuffle. If such an operator sits above any shuffle the rule
 *     leaves the WHOLE plan regular (leaving only that one exchange regular would put a pipelined
 *     exchange below a regular boundary, which the scheduler rejects -- so it is all-or-nothing).
 *     The query runs correctly, just not pipelined. The unsupported consumers are:
 *       - `CoalesceExec` (user `.coalesce(n)`): its `CoalescedRDD` makes ONE reduce task drain
 *         SEVERAL reduce partitions sequentially. The single-threaded writer parks on a full
 *         bounded queue filling a later partition before emitting an earlier one's markers, so a
 *         reader draining partitions in order deadlocks the writer with no timeout escape;
 *         `coalesce`'s narrow-merge contract also cannot be honored by re-hashing to `n`.
 *       - `CartesianProductExec`: its `UnsafeCartesianRDD` reads each left (child) partition once
 *         per right partition, so N reduce tasks mint N readers on the SAME rendezvous queue for
 *         one `(shuffleId, epoch, pid)` -- rows and end-of-stream markers split
 *         nondeterministically (wrong results), a reader short of `numMaps` markers hangs, and
 *         the first to finish abandons the queue and discards the others' data. The fan-out check
 *         does not catch it (one consumer RDD, computed many times), nor the width-1 require.
 *       - `CollectLimitExec` / `CollectTailExec` / `TakeOrderedAndProjectExec`: each builds a
 *         hidden regular (`pipelined = false`) shuffle inside `doExecute` via
 *         `prepareShuffleDependency`, invisible to this plan walk. A flipped exchange below one of
 *         them would sit under that unmaterialized regular boundary and the job would hard-fail at
 *         submission (`classifyJobShuffleShape`'s pipelined-below-regular rejection). (`.collect()`
 *         on a limit takes `executeTake` and never hits `doExecute`; `.write` / `.toLocalIterator`
 *         / a non-root position do.)
 */
case class EnablePipelinedShuffle() extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    // Shared environment gate (opt-in flag, single-executor local mode, channel manager active),
    // identical to the AQE rule's -- see PipelinedShuffleEligibility for why it is a correctness
    // gate. It also requires AQE off implicitly: under AQE this rule sees no exchanges.
    if (!PipelinedShuffleEligibility.enabled(plan, conf)) return plan

    val shuffles = plan.collect { case s: ShuffleExchangeExec => s }
    if (shuffles.isEmpty) return plan

    // A reused exchange has more than one consumer; a pipelined producer cannot fan out, so
    // leave the whole plan regular rather than produce a rejected job. Check subquery plans
    // too (plan.exists walks the operator tree only): today no SQL shape can place a reused
    // PIPELINED exchange there -- same-tree reuse is caught here, main-vs-subquery reuse
    // never fires because the subquery's own preparation pass (PlanSubqueries ->
    // prepareExecutedPlan, which includes this rule) flips its exchanges pipelined BEFORE
    // the outer ReuseExchangeAndSubquery compares canonical forms, and subquery-vs-subquery
    // duplication is collapsed by MergeScalarSubqueries / subquery reuse first -- but the
    // second mechanism is an accident of rule ordering and the third is optimizer behavior,
    // so this gate does not rely on either.
    if (plan.collectWithSubqueries { case r: ReusedExchangeExec => r }.nonEmpty) {
      // Not a warning: this is a normal, expected fallback (reuse is routine optimizer output,
      // e.g. self-joins), the query still runs correctly as a regular shuffle, and the user has
      // nothing to act on. Log at DEBUG as diagnostic ("why this query did not go pipelined")
      // rather than WARN, which would fire on every reuse-bearing query and read as a fault.
      logDebug("EnablePipelinedShuffle: plan has a reused exchange; leaving it regular to " +
        "avoid a fan-out pipelined job.")
      return plan
    }

    // An operator that would read a shuffle in a way the channel transport cannot serve, or that
    // builds its own hidden regular shuffle, forces the whole plan regular (see class doc). Like
    // the reuse fallback this is a normal, expected outcome, so log at DEBUG rather than WARN.
    if (readsShuffleThroughUnsupportedConsumer(plan)) {
      logDebug("EnablePipelinedShuffle: a shuffle is read through an operator the channel " +
        "transport cannot serve (coalesce / cartesian product / a limit operator that builds a " +
        "hidden shuffle); leaving the plan regular.")
      return plan
    }

    plan.transformUp {
      case s: ShuffleExchangeExec if !s.pipelined => s.copy(pipelined = true)
    }
  }

  /**
   * True if any [[ShuffleExchangeExec]] in `plan` is read by an operator the channel transport
   * cannot serve. The unsupported operators (see class doc for why each is fatal) are
   * `CoalesceExec`, `CartesianProductExec`, and the limit operators `CollectLimitExec` /
   * `CollectTailExec` / `TakeOrderedAndProjectExec`. For each such operator anywhere in the plan,
   * check whether a shuffle is reachable below it.
   *
   * The reachability walk descends through EVERY child of a non-exchange node -- not only unary
   * children -- so a shuffle behind a `UnionExec`/join (a `BinaryExecNode`) beneath the operator
   * is still found. It stops at the FIRST [[ShuffleExchangeExec]] on each path: a shuffle deeper
   * than that first one is not read by this operator (the intervening exchange's own reader reads
   * one reduce partition per task), so it is not this operator's concern.
   */
  private def readsShuffleThroughUnsupportedConsumer(plan: SparkPlan): Boolean = {
    def reachesShuffle(p: SparkPlan): Boolean = p match {
      case _: ShuffleExchangeExec => true
      case other => other.children.exists(reachesShuffle)
    }
    def isUnsupportedConsumer(p: SparkPlan): Boolean = p match {
      case _: CoalesceExec | _: CartesianProductExec |
          _: CollectLimitExec | _: CollectTailExec | _: TakeOrderedAndProjectExec => true
      case _ => false
    }
    plan.exists {
      case p if isUnsupportedConsumer(p) => p.children.exists(reachesShuffle)
      case _ => false
    }
  }
}
