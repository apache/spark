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

package org.apache.spark.sql.execution.streaming

import java.util.UUID

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.util.Clock

/**
 *  Holds the mutable state and metrics for a single batch for streaming query.
 */
abstract class StreamExecutionContext(
    val id: UUID,
    runId: UUID,
    name: String,
    triggerClock: Clock,
    sources: Seq[SparkDataStream],
    sink: Table,
    progressReporter: ProgressReporter,
    var batchId: Long,
    sparkSession: SparkSession)
  extends ProgressContext(id, runId, name, triggerClock, sources, sink, progressReporter) {

  /** Metadata associated with the offset seq of a batch in the query. */
  @volatile
  var offsetSeqMetadata: OffsetSeqMetadata = OffsetSeqMetadata(
    batchWatermarkMs = 0, batchTimestampMs = 0, sparkSession.conf)

  /** Holds the most recent input data for each source. */
  var newData: Map[SparkDataStream, LogicalPlan] = _

  /**
   * Stores the start offset for this batch.
   * Only the scheduler thread should modify this field, and only in atomic steps.
   * Other threads should make a shallow copy if they are going to access this field more than
   * once, since the field's value may change at any time.
   */
  @volatile
  var startOffsets = new StreamProgress

  /**
   * Stores the end offsets for this batch.
   * Only the scheduler thread should modify this field, and only in atomic steps.
   * Other threads should make a shallow copy if they are going to access this field more than
   * once, since the field's value may change at any time.
   */
  @volatile
  var endOffsets = new StreamProgress

  /**
   * Tracks the latest offsets for each input source.
   * Only the scheduler thread should modify this field, and only in atomic steps.
   * Other threads should make a shallow copy if they are going to access this field more than
   * once, since the field's value may change at any time.
   */
  @volatile
  var latestOffsets = new StreamProgress

  @volatile var executionPlan: IncrementalExecution = _

  // Called at the end of the execution.
  def onExecutionComplete(): Unit = {}

  // Called at time when execution fails.
  def onExecutionFailure(): Unit = {}
}

/**
 * Holds the all mutable state and metrics for a epoch when using continuous execution mode
 */
class ContinuousExecutionContext(
    id: UUID,
    runId: UUID,
    name: String,
    triggerClock: Clock,
    sources: Seq[SparkDataStream],
    sink: Table,
    progressReporter: ProgressReporter,
    epochId: Long,
    sparkSession: SparkSession)
  extends StreamExecutionContext(
    id,
    runId,
    name,
    triggerClock,
    sources,
    sink,
    progressReporter,
    epochId,
    sparkSession)

/**
 * Holds the all the mutable state and processing metrics for a single micro-batch
 * when using micro batch execution mode.
 *
 * @param _batchId the id of this batch
 * @param previousContext the execution context of the previous micro-batch
 */
class MicroBatchExecutionContext(
    id: UUID,
    runId: UUID,
    name: String,
    triggerClock: Clock,
    sources: Seq[SparkDataStream],
    sink: Table,
    progressReporter: ProgressReporter,
    var _batchId: Long,
    sparkSession: SparkSession,
    var previousContext: Option[MicroBatchExecutionContext])
  extends StreamExecutionContext(
    id,
    runId,
    name,
    triggerClock,
    sources,
    sink,
    progressReporter,
    _batchId,
    sparkSession) with Logging {

  /**
   * Signifies whether current batch (i.e. for the batch `currentBatchId`) has been constructed
   * (i.e. written to the offsetLog) and is ready for execution.
   */
  var isCurrentBatchConstructed = false

  // copying some of the state from the previous batch
  previousContext.foreach { ctx =>
    {
      // the start offsets are the end offsets for the previous batch
      startOffsets = ctx.endOffsets

      // needed for sources that support admission control as the start offset needs
      // to be provided
      endOffsets = ctx.endOffsets

      latestOffsets = ctx.latestOffsets

      // need to carry this over from previous batch since this gets set once and remains
      // the same value for the rest of the run
      metricWarningLogged = ctx.metricWarningLogged

      // need to carry this over to track to know when the previous batch started
      currentTriggerStartTimestamp = ctx.currentTriggerStartTimestamp

      // needed to carry over to new batch because api accessing this value does not expect
      // it to be null even if its the old plan. For constructing the progress on idle trigger
      // no longer relies on executionPlan - we use carryOverExecStatsOnLatestExecutedBatch().
      executionPlan = ctx.executionPlan

      // needs to be carried over to new batch to output metrics for sink
      // even when no data is processed.
      sinkCommitProgress = ctx.sinkCommitProgress

      // needed for test org.apache.spark.sql.streaming.EventTimeWatermarkSuite
      // - recovery from Spark ver 2.3.1 commit log without commit metadata (SPARK-24699)
      offsetSeqMetadata = ctx.offsetSeqMetadata
    }
  }

  def getNextContext(): MicroBatchExecutionContext = {
    new MicroBatchExecutionContext(
      id,
      runId,
      name,
      triggerClock,
      sources,
      sink,
      progressReporter,
      batchId + 1,
      sparkSession,
      Some(this))
  }

  override def startTrigger(): Unit = {
    super.startTrigger()
    currentStatus = currentStatus.copy(isTriggerActive = true)
  }

  override def onExecutionComplete(): Unit = {
    // Release the ref to avoid infinite chain.
    previousContext = None
    super.onExecutionComplete()
  }

  override def onExecutionFailure(): Unit = {
    // Release the ref to avoid infinite chain.
    previousContext = None
    super.onExecutionFailure()
  }

  override def toString: String = s"MicroBatchExecutionContext(batchId=$batchId," +
    s" isCurrentBatchConstructed=$isCurrentBatchConstructed," +
    s" offsetSeqMetadata=$offsetSeqMetadata," +
    s" sinkCommitProgress=$sinkCommitProgress," +
    s" endOffsets$endOffsets," +
    s" startOffsets=$startOffsets," +
    s" latestOffsets=$latestOffsets)," +
    s" executionPlan=${executionPlan}," +
    s" currentStatus: ${currentStatus}"

  def carryOverExecStatsOnLatestExecutedBatch(): Unit = {
    execStatsOnLatestExecutedBatch = previousContext.flatMap(_.execStatsOnLatestExecutedBatch)
  }

  def getStartTime(): Long = {
    currentTriggerStartTimestamp
  }
}

case class MicroBatchExecutionResult(isActive: Boolean, didExecute: Boolean)

case class MicroBatchExecutionFailed() extends RuntimeException
