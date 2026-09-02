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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

/**
 * Builds the feature-on [[SparkSession]] the pipelined-shuffle suites need (SPARK-57399).
 *
 * The feature is gated on session configs that must be present when the session is CREATED (the
 * shuffle manager is instantiated once per SparkContext), so these suites cannot use
 * `SharedSparkSession` + `withSQLConf`; each builds its own session instead. This trait is that
 * one builder, shared so the gate configs cannot drift between suites.
 */
trait PipelinedShuffleTestSession {

  /**
   * Runs `body` against a fresh session with the in-process channel shuffle enabled.
   *
   * `aqe` picks the rule under test: the non-AQE `EnablePipelinedShuffle` preparation rule (false)
   * or the adaptive `AQEEnablePipelinedShuffle` (true).
   */
  protected def withPipelinedSession(
      appName: String,
      aqe: Boolean)(body: SparkSession => Unit): Unit = {
    // sql/core suites share a JVM. If an earlier suite left an active/default SparkSession behind,
    // getOrCreate() below would return THAT session and silently ignore every .config() here, so
    // no exchange would be flipped and the assertions would fail pointing nowhere near the cause.
    // Stop and clear any pre-existing session first so this harness gets a fresh one.
    //
    // Stopping a session this trait did not create is deliberate: these suites need the configs
    // above to take effect at session creation, which is impossible while another session owns the
    // JVM's SparkContext. Suites that share a session (SharedSparkSession) recreate it on demand,
    // so the one being stopped here is replaced rather than lost.
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val spark = SparkSession.builder()
      // High task-concurrency cap so the pipelined group's whole-group slot demand (the sum of
      // every concurrent stage's partitions) is admitted. These are correctness harnesses, not perf
      // ones: on a smaller physical machine these logical slots oversubscribe the cores, which is
      // fine for verifying results but meaningless for timing.
      .master("local[16]")
      .appName(appName)
      .config("spark.shuffle.manager.incremental",
        "org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager")
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, aqe.toString)
      .config(SQLConf.LOCAL_PIPELINED_SHUFFLE_ENABLED.key, "true")
      .config("spark.speculation", "false")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "4")
      .getOrCreate()
    try {
      body(spark)
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}
