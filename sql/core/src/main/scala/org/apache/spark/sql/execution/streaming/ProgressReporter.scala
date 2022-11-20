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
import java.util.{Date, Optional, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.optimizer.InlineCTE
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan, WithCTE}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MILLIS_PER_SECOND
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, ReportsSinkMetrics, ReportsSourceMetrics, SparkDataStream}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.{MicroBatchScanExec, StreamingDataSourceV2Relation, StreamWriterCommitProgress}
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
    inputRows: Map[SparkDataStream, Long],
    stateOperators: Seq[StateOperatorProgress],
    eventTimeStats: Map[String, String])

  // Internal state of the stream, required for computing metrics.
  protected def id: UUID
  protected def runId: UUID
  protected def name: String
  protected def triggerClock: Clock
  protected def logicalPlan: LogicalPlan
  protected def lastExecution: QueryExecution
  protected def newData: Map[SparkDataStream, LogicalPlan]
  protected def sinkCommitProgress: Option[StreamWriterCommitProgress]
  protected def sources: Seq[SparkDataStream]
  protected def sink: Table
  protected def offsetSeqMetadata: OffsetSeqMetadata
  protected def currentBatchId: Long
  protected def sparkSession: SparkSession
  protected def postEvent(event: StreamingQueryListener.Event): Unit

  // Local timestamps and counters.
  private var currentTriggerStartTimestamp = -1L
  private var currentTriggerEndTimestamp = -1L
  private var currentTriggerStartOffsets: Map[SparkDataStream, String] = _
  private var currentTriggerEndOffsets: Map[SparkDataStream, String] = _
  private var currentTriggerLatestOffsets: Map[SparkDataStream, String] = _

  // TODO: Restore this from the checkpoint when possible.
  private var lastTriggerStartTimestamp = -1L

  private val currentDurationsMs = new mutable.HashMap[String, Long]()

  /** Flag that signals whether any error with input metrics have already been logged */
  private var metricWarningLogged: Boolean = false

  /** Holds the most recent query progress updates.  Accesses must lock on the queue itself. */
  private val progressBuffer = new mutable.Queue[StreamingQueryProgress]()

  private val noDataProgressEventInterval =
    sparkSession.sessionState.conf.streamingNoDataProgressEventInterval

  // The timestamp we report an event that has not executed anything
  private var lastNoExecutionProgressEventTime = Long.MinValue

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(DateTimeUtils.getTimeZone("UTC"))

  @volatile
  protected var currentStatus: StreamingQueryStatus = {
    new StreamingQueryStatus(
      message = "Initializing StreamExecution",
      isDataAvailable = false,
      isTriggerActive = false)
  }

  private var latestStreamProgress: StreamProgress = _

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
    currentTriggerStartOffsets = null
    currentTriggerEndOffsets = null
    currentTriggerLatestOffsets = null
    currentDurationsMs.clear()
  }

  /**
   * Record the offsets range this trigger will process. Call this before updating
   * `committedOffsets` in `StreamExecution` to make sure that the correct range is recorded.
   */
  protected def recordTriggerOffsets(
      from: StreamProgress,
      to: StreamProgress,
      latest: StreamProgress): Unit = {
    currentTriggerStartOffsets = from.mapValues(_.json).toMap
    currentTriggerEndOffsets = to.mapValues(_.json).toMap
    currentTriggerLatestOffsets = latest.mapValues(_.json).toMap
    latestStreamProgress = to
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

  /**
   * Finalizes the query progress and adds it to list of recent status updates.
   *
   * @param hasNewData Whether the sources of this stream had new data for this trigger.
   * @param hasExecuted Whether any batch was executed during this trigger. Streaming queries that
   *                    perform stateful aggregations with timeouts can still run batches even
   *                    though the sources don't have any new data.
   */
  protected def finishTrigger(hasNewData: Boolean, hasExecuted: Boolean): Unit = {
    assert(currentTriggerStartOffsets != null && currentTriggerEndOffsets != null &&
      currentTriggerLatestOffsets != null)
    currentTriggerEndTimestamp = triggerClock.getTimeMillis()

    val executionStats = extractExecutionStats(hasNewData, hasExecuted)
    val processingTimeMills = currentTriggerEndTimestamp - currentTriggerStartTimestamp
    val processingTimeSec = Math.max(1L, processingTimeMills).toDouble / MILLIS_PER_SECOND

    val inputTimeSec = if (lastTriggerStartTimestamp >= 0) {
      (currentTriggerStartTimestamp - lastTriggerStartTimestamp).toDouble / MILLIS_PER_SECOND
    } else {
      Double.PositiveInfinity
    }
    logDebug(s"Execution stats: $executionStats")

    val sourceProgress = sources.distinct.map { source =>
      val numRecords = executionStats.inputRows.getOrElse(source, 0L)
      val sourceMetrics = source match {
        case withMetrics: ReportsSourceMetrics =>
          withMetrics.metrics(Optional.ofNullable(latestStreamProgress.get(source).orNull))
        case _ => Map[String, String]().asJava
      }
      new SourceProgress(
        description = source.toString,
        startOffset = currentTriggerStartOffsets.get(source).orNull,
        endOffset = currentTriggerEndOffsets.get(source).orNull,
        latestOffset = currentTriggerLatestOffsets.get(source).orNull,
        numInputRows = numRecords,
        inputRowsPerSecond = numRecords / inputTimeSec,
        processedRowsPerSecond = numRecords / processingTimeSec,
        metrics = sourceMetrics
      )
    }

    val sinkOutput = if (hasExecuted) {
      sinkCommitProgress.map(_.numOutputRows)
    } else {
      sinkCommitProgress.map(_ => 0L)
    }

    val sinkMetrics = sink match {
      case withMetrics: ReportsSinkMetrics =>
        withMetrics.metrics()
      case _ => Map[String, String]().asJava
    }

    val sinkProgress = SinkProgress(
      sink.toString, sinkOutput, sinkMetrics)

    val observedMetrics = extractObservedMetrics(hasNewData, lastExecution)

    val newProgress = new StreamingQueryProgress(
      id = id,
      runId = runId,
      name = name,
      timestamp = formatTimestamp(currentTriggerStartTimestamp),
      batchId = currentBatchId,
      batchDuration = processingTimeMills,
      durationMs =
        new java.util.HashMap(currentDurationsMs.toMap.mapValues(long2Long).toMap.asJava),
      eventTime = new java.util.HashMap(executionStats.eventTimeStats.asJava),
      stateOperators = executionStats.stateOperators.toArray,
      sources = sourceProgress.toArray,
      sink = sinkProgress,
      observedMetrics = new java.util.HashMap(observedMetrics.asJava))

    if (hasExecuted) {
      // Reset noDataEventTimestamp if we processed any data
      lastNoExecutionProgressEventTime = triggerClock.getTimeMillis()
      updateProgress(newProgress)
    } else {
      val now = triggerClock.getTimeMillis()
      if (now - noDataProgressEventInterval >= lastNoExecutionProgressEventTime) {
        lastNoExecutionProgressEventTime = now
        updateProgress(newProgress)
      }
    }

    currentStatus = currentStatus.copy(isTriggerActive = false)
  }

  /** Extract statistics about stateful operators from the executed query plan. */
  private def extractStateOperatorMetrics(hasExecuted: Boolean): Seq[StateOperatorProgress] = {
    if (lastExecution == null) return Nil
    // lastExecution could belong to one of the previous triggers if `!hasExecuted`.
    // Walking the plan again should be inexpensive.
    lastExecution.executedPlan.collect {
      case p if p.isInstanceOf[StateStoreWriter] =>
        val progress = p.asInstanceOf[StateStoreWriter].getProgress()
        if (hasExecuted) {
          progress
        } else {
          progress.copy(newNumRowsUpdated = 0, newNumRowsDroppedByWatermark = 0)
        }
    }
  }

  /** Extracts statistics from the most recent query execution. */
  private def extractExecutionStats(hasNewData: Boolean, hasExecuted: Boolean): ExecutionStats = {
    val hasEventTime = logicalPlan.collect { case e: EventTimeWatermark => e }.nonEmpty
    val watermarkTimestamp =
      if (hasEventTime) Map("watermark" -> formatTimestamp(offsetSeqMetadata.batchWatermarkMs))
      else Map.empty[String, String]

    // SPARK-19378: Still report metrics even though no data was processed while reporting progress.
    val stateOperators = extractStateOperatorMetrics(hasExecuted)

    if (!hasNewData) {
      return ExecutionStats(Map.empty, stateOperators, watermarkTimestamp)
    }

    val numInputRows = extractSourceToNumInputRows()

    val eventTimeStats = lastExecution.executedPlan.collect {
      case e: EventTimeWatermarkExec if e.eventTimeStats.value.count > 0 =>
        val stats = e.eventTimeStats.value
        Map(
          "max" -> stats.max,
          "min" -> stats.min,
          "avg" -> stats.avg.toLong).mapValues(formatTimestamp)
    }.headOption.getOrElse(Map.empty) ++ watermarkTimestamp

    ExecutionStats(numInputRows, stateOperators, eventTimeStats.toMap)
  }

  /** Extract number of input sources for each streaming source in plan */
  private def extractSourceToNumInputRows(): Map[SparkDataStream, Long] = {

    def sumRows(tuples: Seq[(SparkDataStream, Long)]): Map[SparkDataStream, Long] = {
      tuples.groupBy(_._1).mapValues(_.map(_._2).sum).toMap // sum up rows for each source
    }

    def unrollCTE(plan: LogicalPlan): LogicalPlan = {
      val containsCTE = plan.exists {
        case _: WithCTE => true
        case _ => false
      }

      if (containsCTE) {
        InlineCTE(alwaysInline = true).apply(plan)
      } else {
        plan
      }
    }

    val onlyDataSourceV2Sources = {
      // Check whether the streaming query's logical plan has only V2 micro-batch data sources
      val allStreamingLeaves = logicalPlan.collect {
        case s: StreamingDataSourceV2Relation => s.stream.isInstanceOf[MicroBatchStream]
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
          val source = s.stream
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

      // SPARK-41198: CTE is inlined in optimization phase, which ends up with having different
      // number of leaf nodes between (analyzed) logical plan and executed plan. Here we apply
      // inlining CTE against logical plan manually if there is a CTE node.
      val finalLogicalPlan = unrollCTE(lastExecution.logical)

      val allLogicalPlanLeaves = finalLogicalPlan.collectLeaves() // includes non-streaming
      val allExecPlanLeaves = lastExecution.executedPlan.collectLeaves()
      if (allLogicalPlanLeaves.size == allExecPlanLeaves.size) {
        val execLeafToSource = allLogicalPlanLeaves.zip(allExecPlanLeaves).flatMap {
          case (_, ep: MicroBatchScanExec) =>
            // SPARK-41199: `logicalPlanLeafToSource` contains OffsetHolder instance for DSv2
            // streaming source, hence we cannot lookup the actual source from the map.
            // The physical node for DSv2 streaming source contains the information of the source
            // by itself, so leverage it.
            Some(ep -> ep.stream)
          case (lp, ep) =>
            logicalPlanLeafToSource.get(lp).map { source => ep -> source }
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

  /** Extracts observed metrics from the most recent query execution. */
  private def extractObservedMetrics(
      hasNewData: Boolean,
      lastExecution: QueryExecution): Map[String, Row] = {
    if (!hasNewData || lastExecution == null) {
      return Map.empty
    }
    lastExecution.observedMetrics
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

  protected def formatTimestamp(millis: Long): String = {
    timestampFormat.format(new Date(millis))
  }

  /** Updates the message returned in `status`. */
  protected def updateStatusMessage(message: String): Unit = {
    currentStatus = currentStatus.copy(message = message)
  }
}
