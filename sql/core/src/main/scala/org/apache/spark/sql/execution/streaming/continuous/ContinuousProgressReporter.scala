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

package org.apache.spark.sql.execution.streaming.continuous

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.execution.streaming.{BaseStreamingSource, ProgressReporter, StreamProgress}
import org.apache.spark.sql.streaming.{SinkProgress, SourceProgress, StreamingQueryProgress}

trait ContinuousProgressReporter extends ProgressReporter {

  protected def epochEndpoint: RpcEndpointRef

  private var earliestEpochId: Long = -1

  private val currentDurationsMs =
    new mutable.HashMap[Long, (Long, mutable.HashMap[String, Long])]()
  private val recordDurationMs = new mutable.HashMap[String, Long]()

  private val currentTriggerStartOffsets: mutable.HashMap[Long, Map[BaseStreamingSource, String]] =
    new mutable.HashMap[Long, Map[BaseStreamingSource, String]]()
  private val currentTriggerEndOffsets: mutable.HashMap[Long, Map[BaseStreamingSource, String]] =
    new mutable.HashMap[Long, Map[BaseStreamingSource, String]]()

  // TODO: Restore this from the checkpoint when possible.
  private var lastTriggerStartTimestamp = -1L

  // Local timestamps and counters.
  private var currentTriggerStartTimestamp = -1L

  private val noDataProgressEventInterval =
    sparkSession.sessionState.conf.streamingNoDataProgressEventInterval

  // The timestamp we report an event that has no input data
  private var lastNoDataProgressEventTime = Long.MinValue

  /** Begins recording statistics about query progress for a given trigger. */
  override protected def startTrigger(): Unit = {
    logDebug("Starting Trigger Calculation")
    if (earliestEpochId == -1) {
      earliestEpochId = currentBatchId
    }
    checkQueueBoundaries()
    lastTriggerStartTimestamp = currentTriggerStartTimestamp
    currentTriggerStartTimestamp = triggerClock.getTimeMillis()
    currentDurationsMs.put(currentBatchId,
      (currentTriggerStartTimestamp, new mutable.HashMap[String, Long]()))
  }

  /** Finalizes the query progress and adds it to list of recent status updates. */
  protected def finishTrigger(hasNewData: Boolean, epochId: Long, epochStats: EpochStats): Unit = {
    assert(currentTriggerStartOffsets.get(epochId).isDefined
      && currentTriggerEndOffsets.get(epochId).isDefined
      && currentDurationsMs.get(epochId).isDefined)
    val currentTriggerStartTimestamp = currentDurationsMs(epochId)._1
    val currentTriggerEndTimestamp = triggerClock.getTimeMillis()

    val executionStats = extractExecutionStats(hasNewData, Some(epochStats))
    val processingTimeSec =
      (currentTriggerEndTimestamp - currentTriggerStartTimestamp).toDouble / MILLIS_PER_SECOND

    val inputTimeSec = if (lastTriggerStartTimestamp >= 0) {
      (currentTriggerStartTimestamp - lastTriggerStartTimestamp).toDouble / MILLIS_PER_SECOND
    } else {
      Double.NaN
    }
    logDebug(s"Execution stats: $executionStats")

    val sourceProgress = sources.distinct.map { source =>
      val numRecords = executionStats.inputRows.getOrElse(source, 0L)
      new SourceProgress(
        description = source.toString,
        startOffset = currentTriggerStartOffsets(epochId).get(source).orNull,
        endOffset = currentTriggerEndOffsets(epochId).get(source).orNull,
        numInputRows = numRecords,
        inputRowsPerSecond = numRecords / inputTimeSec,
        processedRowsPerSecond = numRecords / processingTimeSec
      )
    }

    val sinkProgress = SinkProgress(
      sink.toString,
      sinkCommitProgress.map(_.numOutputRows))

    val newProgress = new StreamingQueryProgress(
      id = id,
      runId = runId,
      name = name,
      timestamp = formatTimestamp(currentTriggerStartTimestamp),
      batchId = epochId,
      durationMs = new java.util.HashMap(
        (currentDurationsMs(epochId)._2 ++ recordDurationMs).toMap.mapValues(long2Long).asJava),
      eventTime = new java.util.HashMap(executionStats.eventTimeStats.asJava),
      stateOperators = executionStats.stateOperators.toArray,
      sources = sourceProgress.toArray,
      sink = sinkProgress)

    if (hasNewData) {
      // Reset noDataEventTimestamp if we processed any data
      lastNoDataProgressEventTime = Long.MinValue
      updateProgress(newProgress)
    } else {
      val now = triggerClock.getTimeMillis()
      if (now - noDataProgressEventInterval >= lastNoDataProgressEventTime) {
        lastNoDataProgressEventTime = now
        updateProgress(newProgress)
      }
    }

    currentStatus = currentStatus.copy(isTriggerActive = false)
  }

  protected def extractSourceToNumInputRows(t: Option[Any] = None)
      : Map[BaseStreamingSource, Long] = {
    require(t.isDefined && t.get.isInstanceOf[EpochStats])
    Map(sources(0) -> t.get.asInstanceOf[EpochStats].inputRows)
  }

  /**
   * Record the offsets range this trigger will process. Call this before updating
   * `committedOffsets` in `StreamExecution` to make sure that the correct range is recorded.
   */
  override protected def recordTriggerOffsets(
      from: StreamProgress,
      to: StreamProgress,
      epochId: Long): Unit = {
    checkQueueBoundaries()
    currentTriggerStartOffsets.put(epochId, from.mapValues(_.json))
    currentTriggerEndOffsets.put(epochId, to.mapValues(_.json))
  }

  /** Records the duration of running `body` for the next query progress update. */
  def reportTimeTaken[T](triggerDetailKey: String, epochId: Long)(body: => T): T = {
    checkQueueBoundaries()
    val durations = currentDurationsMs.getOrElseUpdate(epochId,
      (triggerClock.getTimeMillis(), new mutable.HashMap[String, Long]()))
    addOrUpdateTime(triggerDetailKey, durations._2)(body)
  }

  /**
   * Records the duration of running `body` for once time, which will not be cleared
   * when start a new trigger.
   */
  protected def recordTimeTaken[T](triggerDetailKey: String)(body: => T): T = {
    addOrUpdateTime(triggerDetailKey, recordDurationMs)(body)
  }

  private def checkQueueBoundaries(): Unit = {
    if (currentDurationsMs.size > numProgressRetention) {
      currentDurationsMs.remove(earliestEpochId)
    }

    if (currentTriggerStartOffsets.size > numProgressRetention) {
      currentTriggerStartOffsets.remove(earliestEpochId)
    }

    if (currentTriggerEndOffsets.size > numProgressRetention) {
      currentTriggerEndOffsets.remove(earliestEpochId)
    }

    earliestEpochId += 1
  }
}
