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

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone, UUID}

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.apache.spark.util.Clock

/**
 * Responsible for continually reporting statistics about the amount of data processed as well
 * as latency for a streaming query.  This trait is designed to be mixed into the
 * [[StreamExecution]], who is responsible for calling `startTrigger` and `finishTrigger`
 * at the appropriate times. Additionally, the status can updated with `updateStatusMessage` to
 * allow reporting on the streams current state (i.e. "Fetching more data").
 */
trait ProgressReporter extends Logging {

  case class ExecutionStats(
    inputRows: Map[Source, Long],
    stateOperators: Seq[StateOperatorProgress],
    eventTimeStats: Map[String, String])

  // Internal state of the stream, required for computing metrics.
  protected def id: UUID
  protected def runId: UUID
  protected def name: String
  protected def triggerClock: Clock
  protected def logicalPlan: LogicalPlan
  protected def lastExecution: QueryExecution
  protected def newData: Map[Source, DataFrame]
  protected def availableOffsets: StreamProgress
  protected def committedOffsets: StreamProgress
  protected def sources: Seq[Source]
  protected def sink: Sink
  protected def offsetSeqMetadata: OffsetSeqMetadata
  protected def currentBatchId: Long
  protected def sparkSession: SparkSession
  protected def postEvent(event: StreamingQueryListener.Event): Unit

  // Local timestamps and counters.
  private var currentTriggerStartTimestamp = -1L
  private var currentTriggerEndTimestamp = -1L
  // TODO: Restore this from the checkpoint when possible.
  private var lastTriggerStartTimestamp = -1L
  private val currentDurationsMs = new mutable.HashMap[String, Long]()

  /** Flag that signals whether any error with input metrics have already been logged */
  private var metricWarningLogged: Boolean = false

  /** Holds the most recent query progress updates.  Accesses must lock on the queue itself. */
  private val progressBuffer = new mutable.Queue[StreamingQueryProgress]()

  private val noDataProgressEventInterval =
    sparkSession.sessionState.conf.streamingNoDataProgressEventInterval

  // The timestamp we report an event that has no input data
  private var lastNoDataProgressEventTime = Long.MinValue

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

  @volatile
  protected var currentStatus: StreamingQueryStatus = {
    new StreamingQueryStatus(
      message = "Initializing StreamExecution",
      isDataAvailable = false,
      isTriggerActive = false)
  }

  /** Returns the current status of the query. */
  def status: StreamingQueryStatus = currentStatus

  /** Returns an array containing the most recent query progress updates. */
  def recentProgress: Array[StreamingQueryProgress] = progressBuffer.synchronized {
    progressBuffer.toArray
  }

  /** Returns the most recent query progress update or null if there were no progress updates. */
  def lastProgress: StreamingQueryProgress = progressBuffer.synchronized {
    progressBuffer.lastOption.orNull
  }

  /** Begins recording statistics about query progress for a given trigger. */
  protected def startTrigger(): Unit = {
    logDebug("Starting Trigger Calculation")
    lastTriggerStartTimestamp = currentTriggerStartTimestamp
    currentTriggerStartTimestamp = triggerClock.getTimeMillis()
    currentStatus = currentStatus.copy(isTriggerActive = true)
    currentDurationsMs.clear()
  }

  private def updateProgress(newProgress: StreamingQueryProgress): Unit = {
    progressBuffer.synchronized {
      progressBuffer += newProgress
      while (progressBuffer.length >= sparkSession.sqlContext.conf.streamingProgressRetention) {
        progressBuffer.dequeue()
      }
    }
    postEvent(new QueryProgressEvent(newProgress))
    logInfo(s"Streaming query made progress: $newProgress")
  }

  /** Finalizes the query progress and adds it to list of recent status updates. */
  protected def finishTrigger(hasNewData: Boolean): Unit = {
    currentTriggerEndTimestamp = triggerClock.getTimeMillis()

    val executionStats = extractExecutionStats(hasNewData)
    val processingTimeSec =
      (currentTriggerEndTimestamp - currentTriggerStartTimestamp).toDouble / 1000

    val inputTimeSec = if (lastTriggerStartTimestamp >= 0) {
      (currentTriggerStartTimestamp - lastTriggerStartTimestamp).toDouble / 1000
    } else {
      Double.NaN
    }
    logDebug(s"Execution stats: $executionStats")

    val sourceProgress = sources.map { source =>
      val numRecords = executionStats.inputRows.getOrElse(source, 0L)
      new SourceProgress(
        description = source.toString,
        startOffset = committedOffsets.get(source).map(_.json).orNull,
        endOffset = availableOffsets.get(source).map(_.json).orNull,
        numInputRows = numRecords,
        inputRowsPerSecond = numRecords / inputTimeSec,
        processedRowsPerSecond = numRecords / processingTimeSec
      )
    }
    val sinkProgress = new SinkProgress(sink.toString)

    val newProgress = new StreamingQueryProgress(
      id = id,
      runId = runId,
      name = name,
      timestamp = formatTimestamp(currentTriggerStartTimestamp),
      batchId = currentBatchId,
      durationMs = new java.util.HashMap(currentDurationsMs.toMap.mapValues(long2Long).asJava),
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

  /** Extract statistics about stateful operators from the executed query plan. */
  private def extractStateOperatorMetrics(hasNewData: Boolean): Seq[StateOperatorProgress] = {
    if (lastExecution == null) return Nil
    // lastExecution could belong to one of the previous triggers if `!hasNewData`.
    // Walking the plan again should be inexpensive.
    val stateNodes = lastExecution.executedPlan.collect {
      case p if p.isInstanceOf[StateStoreSaveExec] => p
    }
    stateNodes.map { node =>
      val numRowsUpdated = if (hasNewData) {
        node.metrics.get("numUpdatedStateRows").map(_.value).getOrElse(0L)
      } else {
        0L
      }
      new StateOperatorProgress(
        numRowsTotal = node.metrics.get("numTotalStateRows").map(_.value).getOrElse(0L),
        numRowsUpdated = numRowsUpdated)
    }
  }

  /** Extracts statistics from the most recent query execution. */
  private def extractExecutionStats(hasNewData: Boolean): ExecutionStats = {
    val hasEventTime = logicalPlan.collect { case e: EventTimeWatermark => e }.nonEmpty
    val watermarkTimestamp =
      if (hasEventTime) Map("watermark" -> formatTimestamp(offsetSeqMetadata.batchWatermarkMs))
      else Map.empty[String, String]

    // SPARK-19378: Still report metrics even though no data was processed while reporting progress.
    val stateOperators = extractStateOperatorMetrics(hasNewData)

    if (!hasNewData) {
      return ExecutionStats(Map.empty, stateOperators, watermarkTimestamp)
    }

    // We want to associate execution plan leaves to sources that generate them, so that we match
    // the their metrics (e.g. numOutputRows) to the sources. To do this we do the following.
    // Consider the translation from the streaming logical plan to the final executed plan.
    //
    //  streaming logical plan (with sources) <==> trigger's logical plan <==> executed plan
    //
    // 1. We keep track of streaming sources associated with each leaf in the trigger's logical plan
    //    - Each logical plan leaf will be associated with a single streaming source.
    //    - There can be multiple logical plan leaves associated with a streaming source.
    //    - There can be leaves not associated with any streaming source, because they were
    //      generated from a batch source (e.g. stream-batch joins)
    //
    // 2. Assuming that the executed plan has same number of leaves in the same order as that of
    //    the trigger logical plan, we associate executed plan leaves with corresponding
    //    streaming sources.
    //
    // 3. For each source, we sum the metrics of the associated execution plan leaves.
    //
    val logicalPlanLeafToSource = newData.flatMap { case (source, df) =>
      df.logicalPlan.collectLeaves().map { leaf => leaf -> source }
    }
    val allLogicalPlanLeaves = lastExecution.logical.collectLeaves() // includes non-streaming
    val allExecPlanLeaves = lastExecution.executedPlan.collectLeaves()
    val numInputRows: Map[Source, Long] =
      if (allLogicalPlanLeaves.size == allExecPlanLeaves.size) {
        val execLeafToSource = allLogicalPlanLeaves.zip(allExecPlanLeaves).flatMap {
          case (lp, ep) => logicalPlanLeafToSource.get(lp).map { source => ep -> source }
        }
        val sourceToNumInputRows = execLeafToSource.map { case (execLeaf, source) =>
          val numRows = execLeaf.metrics.get("numOutputRows").map(_.value).getOrElse(0L)
          source -> numRows
        }
        sourceToNumInputRows.groupBy(_._1).mapValues(_.map(_._2).sum) // sum up rows for each source
      } else {
        if (!metricWarningLogged) {
          def toString[T](seq: Seq[T]): String = s"(size = ${seq.size}), ${seq.mkString(", ")}"
          logWarning(
            "Could not report metrics as number leaves in trigger logical plan did not match that" +
                s" of the execution plan:\n" +
                s"logical plan leaves: ${toString(allLogicalPlanLeaves)}\n" +
                s"execution plan leaves: ${toString(allExecPlanLeaves)}\n")
          metricWarningLogged = true
        }
        Map.empty
      }

    val eventTimeStats = lastExecution.executedPlan.collect {
      case e: EventTimeWatermarkExec if e.eventTimeStats.value.count > 0 =>
        val stats = e.eventTimeStats.value
        Map(
          "max" -> stats.max,
          "min" -> stats.min,
          "avg" -> stats.avg).mapValues(formatTimestamp)
    }.headOption.getOrElse(Map.empty) ++ watermarkTimestamp

    ExecutionStats(numInputRows, stateOperators, eventTimeStats)
  }

  /** Records the duration of running `body` for the next query progress update. */
  protected def reportTimeTaken[T](triggerDetailKey: String)(body: => T): T = {
    val startTime = triggerClock.getTimeMillis()
    val result = body
    val endTime = triggerClock.getTimeMillis()
    val timeTaken = math.max(endTime - startTime, 0)

    val previousTime = currentDurationsMs.getOrElse(triggerDetailKey, 0L)
    currentDurationsMs.put(triggerDetailKey, previousTime + timeTaken)
    logDebug(s"$triggerDetailKey took $timeTaken ms")
    result
  }

  private def formatTimestamp(millis: Long): String = {
    timestampFormat.format(new Date(millis))
  }

  /** Updates the message returned in `status`. */
  protected def updateStatusMessage(message: String): Unit = {
    currentStatus = currentStatus.copy(message = message)
  }
}
