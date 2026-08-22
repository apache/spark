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

import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CoalesceExec, SparkPlan, UnaryExecNode}

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
 * Two shapes make the rule leave the whole plan regular:
 *   - reuse: a pipelined producer with more than one consumer (fan-out) is rejected, so if any
 *     exchange in the plan is reused the rule bails out.
 *   - coalesce over a shuffle: a `CoalesceExec` (user `.coalesce(n)`, a narrow no-shuffle
 *     partition reduction) reading from a shuffle makes ONE reduce task drain SEVERAL reduce
 *     partitions sequentially (a core `CoalescedRDD` over the `ShuffledRowRDD`). The channel
 *     transport cannot serve that -- the map-side writer interleaves all partitions on one
 *     thread and parks on a full bounded queue, so a reader draining partition `start` to
 *     completion before touching `start + 1` deadlocks with the parked writer, and there is no
 *     "combine adjacent partitions" narrow read the transport could substitute without giving
 *     up the bounded-queue backpressure the transport relies on. `coalesce`'s API contract is a
 *     narrow dependency that merges adjacent partitions, so we cannot honor it by re-hashing to
 *     `n` partitions either. So if any shuffle in the plan is read by a coalesce (directly or
 *     through a narrow chain) the rule leaves the whole plan regular; the query runs correctly,
 *     just not pipelined. (Leaving only that one exchange regular would put a pipelined exchange
 *     below a regular boundary, which the scheduler rejects -- so it must be all-or-nothing.)
 */
case class EnablePipelinedShuffle() extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.pipelinedShuffleEnabled) return plan
    // Single-executor only: the validated transport (the in-process channel
    // manager) requires producer and consumer in one JVM. (The pipelined machinery itself is
    // not local-only -- the RPC streaming transport is cross-executor -- but batch queries
    // over it are unexplored territory, so the rule stays conservative.)
    if (plan.session == null || !plan.session.sparkContext.isLocal) return plan

    // The SQL flag alone does not pick a transport: the pipelined manager is set separately by
    // spark.shuffle.manager.incremental, and DEFAULTS to the RPC StreamingShuffleManager. Only
    // the in-process channel manager is validated for batch queries in local mode; flipping to
    // pipelined while the incremental manager is still the RPC streaming one would route these
    // exchanges to an untested transport (and, because that manager reports
    // requiresDetachedRecords = false, also skip the row copy). So require the channel manager
    // to be active; otherwise leave the plan regular, mirroring the reuse fallback below.
    if (!SparkEnv.get.pipelinedShuffleManager.isInstanceOf[PipelinedChannelShuffleManager]) {
      logDebug("EnablePipelinedShuffle: spark.sql.pipelinedShuffle.enabled is on but the " +
        "incremental shuffle manager is not the in-process channel manager " +
        "(spark.shuffle.manager.incremental); leaving the plan regular.")
      return plan
    }

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

    // A CoalesceExec reading from a shuffle would make one reduce task drain several reduce
    // partitions sequentially, which the channel transport cannot serve (see class doc). Leave
    // the whole plan regular -- like the reuse fallback, this is a normal, expected outcome the
    // user has nothing to act on, so log at DEBUG rather than WARN.
    if (readsShuffleByCoalesce(plan)) {
      logDebug("EnablePipelinedShuffle: a coalesce reads from a shuffle; leaving the plan " +
        "regular to avoid a coalesced multi-partition read the channel transport cannot serve.")
      return plan
    }

    plan.transformUp {
      case s: ShuffleExchangeExec if !s.pipelined => s.copy(pipelined = true)
    }
  }

  /**
   * True if any [[ShuffleExchangeExec]] in `plan` is read by a [[CoalesceExec]] above it through
   * a chain of only narrow (unary, non-exchange) operators. Such a shuffle would be drained
   * multi-partition-per-task by a `CoalescedRDD` and cannot be pipelined (see class doc). A
   * shuffle underneath that shuffle is NOT affected: its own reader reads one reduce partition
   * per task, so the walk from a coalesce stops at the FIRST shuffle it reaches.
   */
  private def readsShuffleByCoalesce(plan: SparkPlan): Boolean = {
    // Does a narrow chain from `p` reach a ShuffleExchangeExec before any other exchange?
    def reachesShuffle(p: SparkPlan): Boolean = p match {
      case _: ShuffleExchangeExec => true
      case u: UnaryExecNode => reachesShuffle(u.child)
      case _ => false
    }
    plan.exists {
      case c: CoalesceExec => reachesShuffle(c.child)
      case _ => false
    }
  }
}
