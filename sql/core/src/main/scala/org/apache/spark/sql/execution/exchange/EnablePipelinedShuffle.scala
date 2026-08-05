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

import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * WIP / opt-in (SPARK-57399 local-repartition v2). Rewrites every hash-partitioning
 * [[ShuffleExchangeExec]] in a batch physical plan to `pipelined = true`, so its shuffle is
 * served by the in-process pipelined channel manager and the concurrent-stage scheduler runs
 * the map and reduce stages together. This is the minimal SQL entry point that lets a batch
 * query exercise the v2 execution path; a production version would be a targeted,
 * cost/shape-aware replacement rather than a blanket rewrite.
 *
 * Enabled only when `spark.sql.pipelinedShuffle.enabled=true`. It runs in the non-AQE
 * `preparations` list, so it also requires AQE to be off (under AQE the plan is hidden behind
 * an opaque `AdaptiveSparkPlanExec` leaf and this rule sees no exchanges).
 *
 * Conservative all-or-nothing gate to avoid the DAGScheduler's job-shape rejections:
 *   - Rewrites only when EVERY `ShuffleExchangeExec` in the plan is `HashPartitioning`. A
 *     `SinglePartition`/`RangePartitioning` exchange produces a regular `ShuffleDependency`;
 *     mixing regular and pipelined shuffles in one job is rejected, so if any such exchange
 *     is present the rule leaves the whole plan alone. (Broadcasts are not shuffles and are
 *     fine.)
 *   - Skips when any exchange is reused (a `ReusedExchangeExec` present), since a pipelined
 *     producer with more than one consumer (fan-out) is rejected.
 */
case class EnablePipelinedShuffle() extends Rule[SparkPlan] {

  private val confKey = "spark.sql.pipelinedShuffle.enabled"

  override def apply(plan: SparkPlan): SparkPlan = {
    if (conf.getConfString(confKey, "false").toBoolean != true) return plan

    val shuffles = plan.collect { case s: ShuffleExchangeExec => s }
    if (shuffles.isEmpty) return plan

    // All shuffles must be hash-partitioning; otherwise a regular shuffle would coexist with
    // the pipelined ones (mixed job -> rejected).
    val allHash = shuffles.forall(_.outputPartitioning.isInstanceOf[HashPartitioning])
    // Any reuse means a shuffle has more than one consumer (fan-out -> rejected).
    val hasReuse = plan.exists(_.isInstanceOf[ReusedExchangeExec])

    if (!allHash || hasReuse) {
      logWarning("EnablePipelinedShuffle: plan has a non-hash or reused shuffle; leaving it " +
        "regular to avoid a mixed/fan-out pipelined job.")
      return plan
    }

    plan.transformUp {
      case s: ShuffleExchangeExec if !s.pipelined => s.copy(pipelined = true)
    }
  }
}
