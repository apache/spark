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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
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

  // The flag/manager mismatch is a start-up misconfiguration, so warn once per JVM rather than on
  // every query planned in the session.
  private val mismatchWarned = new AtomicBoolean(false)

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
    // Batch only. `IncrementalExecution.preparations` inherits QueryExecution's list, so without
    // this gate a streaming plan would be rewritten here: every micro-batch exchange (the
    // state-store shuffles, the static side of a stream-static join) would be flipped to pipelined
    // BEFORE `MarkPipelinedShuffleForRealTimeMode` runs. That contradicts what the Real-Time Mode
    // rule deliberately does -- it leaves the static side regular, because pulling it into the gang
    // would demand slots for stages that must instead finish first, failing admission. Streaming
    // marks its own pipelined boundaries; this opt-in batch path must not pre-empt that decision.
    // (`logicalLink.exists(_.isStreaming)` is the same signal InsertAdaptiveSparkPlan uses to keep
    // AQE off streaming plans.)
    if (plan.exists(_.logicalLink.exists(_.isStreaming))) {
      logDebug("Pipelined shuffle: the plan is a streaming plan; leaving it to the streaming " +
        "engine's own pipelined-shuffle marking.")
      return false
    }
    if (!SparkEnv.get.pipelinedShuffleManager.isInstanceOf[PipelinedChannelShuffleManager]) {
      // WARN, not DEBUG, and once per JVM: the user asked for this feature and is silently not
      // getting it, which no plan or metric reveals. The flag alone cannot select the transport --
      // the manager is a separate, start-up-only config -- so the mismatch is a misconfiguration
      // the user has to act on, unlike the plan-shape fallbacks (reuse, coalesce) which are normal
      // outcomes and stay at DEBUG.
      if (mismatchWarned.compareAndSet(false, true)) {
        logWarning(s"${SQLConf.LOCAL_PIPELINED_SHUFFLE_ENABLED.key} is enabled but " +
          s"${config.SHUFFLE_MANAGER_INCREMENTAL.key} is not the in-process channel manager, so " +
          s"no shuffle will be pipelined. Set it to " +
          s"${classOf[PipelinedChannelShuffleManager].getName} to enable the feature, or unset " +
          s"the SQL flag to silence this warning.")
      }
      return false
    }
    true
  }
}
