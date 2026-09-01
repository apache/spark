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
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * Shared environment gate for the two pipelined-shuffle enabling rules
 * ([[EnablePipelinedShuffle]] non-AQE and `AQEEnablePipelinedShuffle` under AQE). This is a
 * CORRECTNESS gate, not cosmetics: flipping an exchange to pipelined while the incremental manager
 * is still the RPC streaming one would route it to an untested transport that reports
 * `requiresDetachedRecords = false`, so the SQL layer would skip the row copy and silently corrupt
 * rows shared across the writer/reader threads. Both rules must apply the identical gate, so it
 * lives here rather than being copy-pasted into each `apply` (where the two could drift and split
 * AQE vs non-AQE behavior). Each rule keeps only its own plan-shape logic.
 */
private[sql] object PipelinedShuffleEligibility extends Logging {

  /**
   * Whether the pipelined channel transport may be used for `plan` at all, independent of plan
   * shape. Requires: the opt-in flag on; single-executor local mode (the in-process channel
   * transport needs producer and consumer in one JVM); and the configured incremental manager
   * actually being the in-process channel manager. Returns false (with a DEBUG diagnostic) when
   * any gate fails, so the caller leaves the plan regular.
   */
  def enabled(plan: SparkPlan, conf: SQLConf): Boolean = {
    if (!conf.localPipelinedShuffleEnabled) {
      return false
    }
    if (plan.session == null || !plan.session.sparkContext.isLocal) {
      return false
    }
    if (!SparkEnv.get.pipelinedShuffleManager.isInstanceOf[PipelinedChannelShuffleManager]) {
      logDebug("Pipelined shuffle: spark.sql.shuffle.localPipelined.enabled is on but the " +
        "incremental shuffle manager is not the in-process channel manager " +
        "(spark.shuffle.manager.incremental); leaving the plan regular.")
      return false
    }
    true
  }
}
