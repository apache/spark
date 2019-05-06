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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.execution.datasources.v2.{MicroBatchScanExec, StreamingDataSourceV2Relation}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.streaming.{SinkProgress, SourceProgress, StreamingQueryProgress}

trait MicroBatchProgressReporter extends ProgressReporter {

  private val currentDurationsMs = new mutable.HashMap[String, Long]()
  private val recordDurationMs = new mutable.HashMap[String, Long]()

  private var currentTriggerStartOffsets: Map[BaseStreamingSource, String] = _
  private var currentTriggerEndOffsets: Map[BaseStreamingSource, String] = _

  // TODO: Restore this from the checkpoint when possible.
  private var lastTriggerStartTimestamp = -1L

  // Local timestamps and counters.
  private var currentTriggerStartTimestamp = -1L
  private var currentTriggerEndTimestamp = -1L

  private val noDataProgressEventInterval =
    sparkSession.sessionState.conf.streamingNoDataProgressEventInterval

  // The timestamp we report an event that has no input data
  private var lastNoDataProgressEventTime = Long.MinValue

  /** Begins recording statistics about query progress for a given trigger. */
  override protected def startTrigger(): Unit = {
    logDebug("Starting Trigger Calculation")
    lastTriggerStartTimestamp = currentTriggerStartTimestamp
    currentTriggerStartTimestamp = triggerClock.getTimeMillis()
    currentTriggerStartOffsets = null
    currentTriggerEndOffsets = null
    currentDurationsMs.clear()
  }

  /** Finalizes the query progress and adds it to list of recent status updates. */
  protected def finishTrigger(hasNewData: Boolean): Unit = {
    assert(currentTriggerStartOffsets != null && currentTriggerEndOffsets != null)
    currentTriggerEndTimestamp = triggerClock.getTimeMillis()

    val executionStats = extractExecutionStats(hasNewData)
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
        startOffset = currentTriggerStartOffsets.get(source).orNull,
        endOffset = currentTriggerEndOffsets.get(source).orNull,
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
      batchId = currentBatchId,
      durationMs = new java.util.HashMap(
        (currentDurationsMs ++ recordDurationMs).toMap.mapValues(long2Long).asJava),
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

  /** Extract number of input sources for each streaming source in plan */
  protected def extractSourceToNumInputRows(t: Option[Any] = None)
      : Map[BaseStreamingSource, Long] = {

    def sumRows(tuples: Seq[(BaseStreamingSource, Long)]): Map[BaseStreamingSource, Long] = {
      tuples.groupBy(_._1).mapValues(_.map(_._2).sum) // sum up rows for each source
    }

    val onlyDataSourceV2Sources = {
      // Check whether the streaming query's logical plan has only V2 micro-batch data sources
      val allStreamingLeaves = logicalPlan.collect {
        case s: StreamingDataSourceV2Relation =>
          s.stream.isInstanceOf[MicroBatchStream] || s.stream.isInstanceOf[ContinuousStream]
        case _: StreamingExecutionRelation => false
      }
      allStreamingLeaves.forall(_ == true)
    }

    if (onlyDataSourceV2Sources) {
      // It's possible that multiple DataSourceV2ScanExec instances may refer to the same source
      // (can happen with self-unions or self-joins). This means the source is scanned multiple
      // times in the query, we should count the numRows for each scan.
      val sourceToInputRowsTuples = lastExecution.executedPlan.collect {
        case s: MicroBatchScanExec =>
          val numRows = s.metrics.get("numOutputRows").map(_.value).getOrElse(0L)
          val source = s.stream.asInstanceOf[BaseStreamingSource]
          source -> numRows
      }
      logDebug("Source -> # input rows\n\t" + sourceToInputRowsTuples.mkString("\n\t"))
      sumRows(sourceToInputRowsTuples)
    } else {

      // Since V1 source do not generate execution plan leaves that directly link with source that
      // generated it, we can only do a best-effort association between execution plan leaves to the
      // sources. This is known to fail in a few cases, see SPARK-24050.
      //
      // We want to associate execution plan leaves to sources that generate them, so that we match
      // the their metrics (e.g. numOutputRows) to the sources. To do this we do the following.
      // Consider the translation from the streaming logical plan to the final executed plan.
      //
      // streaming logical plan (with sources) <==> trigger's logical plan <==> executed plan
      //
      // 1. We keep track of streaming sources associated with each leaf in trigger's logical plan
      //  - Each logical plan leaf will be associated with a single streaming source.
      //  - There can be multiple logical plan leaves associated with a streaming source.
      //  - There can be leaves not associated with any streaming source, because they were
      //      generated from a batch source (e.g. stream-batch joins)
      //
      // 2. Assuming that the executed plan has same number of leaves in the same order as that of
      //    the trigger logical plan, we associate executed plan leaves with corresponding
      //    streaming sources.
      //
      // 3. For each source, we sum the metrics of the associated execution plan leaves.
      //
      val logicalPlanLeafToSource = newData.flatMap { case (source, logicalPlan) =>
        logicalPlan.collectLeaves().map { leaf => leaf -> source }
      }
      val allLogicalPlanLeaves = lastExecution.logical.collectLeaves() // includes non-streaming
      val allExecPlanLeaves = lastExecution.executedPlan.collectLeaves()
      if (allLogicalPlanLeaves.size == allExecPlanLeaves.size) {
        val execLeafToSource = allLogicalPlanLeaves.zip(allExecPlanLeaves).flatMap {
          case (lp, ep) => logicalPlanLeafToSource.get(lp).map { source => ep -> source }
        }
        val sourceToInputRowsTuples = execLeafToSource.map { case (execLeaf, source) =>
          val numRows = execLeaf.metrics.get("numOutputRows").map(_.value).getOrElse(0L)
          source -> numRows
        }
        sumRows(sourceToInputRowsTuples)
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
    }
  }

  /**
   * Record the offsets range this trigger will process. Call this before updating
   * `committedOffsets` in `StreamExecution` to make sure that the correct range is recorded.
   */
  override protected def recordTriggerOffsets(
      from: StreamProgress,
      to: StreamProgress,
      epochId: Long = currentBatchId): Unit = {
    currentTriggerStartOffsets = from.mapValues(_.json)
    currentTriggerEndOffsets = to.mapValues(_.json)
  }

  /** Records the duration of running `body` for the next query progress update. */
  override protected def reportTimeTaken[T](
      triggerDetailKey: String,
      epochId: Long = currentBatchId)(body: => T): T = {
    addOrUpdateTime(triggerDetailKey, currentDurationsMs)(body)
  }

  /**
   * Records the duration of running `body` which will not be cleared when start a new trigger.
   */
  override protected def recordTimeTaken[T](triggerDetailKey: String)(body: => T): T = {
    addOrUpdateTime(triggerDetailKey, recordDurationMs)(body)
  }
}
