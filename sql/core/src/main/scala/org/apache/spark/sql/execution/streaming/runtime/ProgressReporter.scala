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

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.{Optional, UUID}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.optimizer.InlineCTE
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan, WithCTE}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MILLIS_PER_SECOND
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, ReportsSinkMetrics, ReportsSourceMetrics, SparkDataStream}
import org.apache.spark.sql.execution.{QueryExecution, StreamSourceAwareSparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{MicroBatchScanExec, StreamingDataSourceV2ScanRelation, StreamWriterCommitProgress}
import org.apache.spark.sql.execution.streaming.state.StateStoreCoordinatorRef
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryIdleEvent, QueryProgressEvent}
import org.apache.spark.util.{Clock, Utils}

/**
 * Responsible for continually reporting statistics about the amount of data processed as well
 * as latency for a streaming query.  This class is designed to hold information about
 * a streaming query and contains methods that can be used on a streaming query,
 * such as get the most recent progress of the query.
 */
class ProgressReporter(
    private val sparkSession: SparkSession,
    private val triggerClock: Clock,
    val logicalPlan: () => LogicalPlan)
  extends Logging {

  // The timestamp we report an event that has not executed anything
  var lastNoExecutionProgressEventTime = Long.MinValue

  /** Holds the most recent query progress updates.  Accesses must lock on the queue itself. */
  private val progressBuffer = new mutable.Queue[StreamingQueryProgress]()

  val noDataProgressEventInterval: Long =
    sparkSession.sessionState.conf.streamingNoDataProgressEventInterval

  val coordinatorReportSnapshotUploadLag: Boolean =
    sparkSession.sessionState.conf.stateStoreCoordinatorReportSnapshotUploadLag

  val stateStoreCoordinator: StateStoreCoordinatorRef =
    sparkSession.sessionState.streamingQueryManager.stateStoreCoordinator

  private val timestampFormat =
    DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
      .withZone(DateTimeUtils.getZoneId("UTC"))

  /** Returns an array containing the most recent query progress updates. */
  def recentProgress: Array[StreamingQueryProgress] = progressBuffer.synchronized {
    progressBuffer.toArray
  }

  /** Returns the most recent query progress update or null if there were no progress updates. */
  def lastProgress: StreamingQueryProgress = progressBuffer.synchronized {
    progressBuffer.lastOption.orNull
  }

  def updateProgress(newProgress: StreamingQueryProgress): Unit = {
    // Reset noDataEventTimestamp if we processed any data
    lastNoExecutionProgressEventTime = triggerClock.getTimeMillis()

    addNewProgress(newProgress)
    postEvent(new QueryProgressEvent(newProgress))
    logInfo(
      log"Streaming query made progress: ${MDC(LogKeys.STREAMING_QUERY_PROGRESS, newProgress)}")
  }

  private def addNewProgress(newProgress: StreamingQueryProgress): Unit = {
    progressBuffer.synchronized {
      progressBuffer += newProgress
      while (progressBuffer.length >= sparkSession.sessionState.conf.streamingProgressRetention) {
        progressBuffer.dequeue()
      }
    }
  }

  def updateIdleness(
      id: UUID,
      runId: UUID,
      currentTriggerStartTimestamp: Long,
      newProgress: StreamingQueryProgress): Unit = {
    val now = triggerClock.getTimeMillis()
    if (now - noDataProgressEventInterval >= lastNoExecutionProgressEventTime) {
      addNewProgress(newProgress)
      if (lastNoExecutionProgressEventTime > Long.MinValue) {
        postEvent(new QueryIdleEvent(id, runId, formatTimestamp(currentTriggerStartTimestamp)))
        logInfo(log"Streaming query has been idle and waiting for new data more than " +
          log"${MDC(LogKeys.TIME_UNITS, noDataProgressEventInterval)} ms.")
      }

      lastNoExecutionProgressEventTime = now
    }
  }

  private def postEvent(event: StreamingQueryListener.Event): Unit = {
    sparkSession.streams.postListenerEvent(event)
  }

  def formatTimestamp(millis: Long): String = {
    Instant.ofEpochMilli(millis)
      .atZone(ZoneId.of("Z")).format(timestampFormat)
  }
}

/**
 * This class holds variables and methods that are used track metrics and progress
 * during the execution lifecycle of a batch that is being processed by the streaming query
 */
abstract class ProgressContext(
    id: UUID,
    runId: UUID,
    name: String,
    triggerClock: Clock,
    sources: Seq[SparkDataStream],
    sink: Table,
    progressReporter: ProgressReporter)
  extends Logging {

  import ProgressContext._

  // offset metadata for this batch
  protected def offsetSeqMetadata: OffsetSeqMetadata

  // the most recent input data for each source.
  protected def newData: Map[SparkDataStream, LogicalPlan]

  /** Flag that signals whether any error with input metrics have already been logged */
  protected var metricWarningLogged: Boolean = false

  @volatile
  var sinkCommitProgress: Option[StreamWriterCommitProgress] = None

  // Local timestamps and counters.
  protected var currentTriggerStartTimestamp = -1L
  private var currentTriggerEndTimestamp = -1L
  private var currentTriggerStartOffsets: Map[SparkDataStream, String] = _
  private var currentTriggerEndOffsets: Map[SparkDataStream, String] = _
  private var currentTriggerLatestOffsets: Map[SparkDataStream, String] = _

  // TODO: Restore this from the checkpoint when possible.
  protected var lastTriggerStartTimestamp = -1L

  private val currentDurationsMs = new mutable.HashMap[String, Long]()

  // This field tracks the execution stats being calculated during reporting metrics for the
  // latest executed batch. We track the value to construct the progress for idle trigger which
  // doesn't execute a batch. Since an idle trigger doesn't execute a batch, it has no idea about
  // the snapshot of the query status, hence it has to refer to the latest executed batch.
  @volatile protected var execStatsOnLatestExecutedBatch: Option[ExecutionStats] = None

  @volatile
  var currentStatus: StreamingQueryStatus = {
    new StreamingQueryStatus(
      message = "Initializing StreamExecution",
      isDataAvailable = false,
      isTriggerActive = false
    )
  }

  private var latestStreamProgress: StreamProgress = _

  /** Records the duration of running `body` for the next query progress update. */
  def reportTimeTaken[T](triggerDetailKey: String)(body: => T): T = {
    val startTime = triggerClock.getTimeMillis()
    val result = body
    val endTime = triggerClock.getTimeMillis()
    val timeTaken = math.max(endTime - startTime, 0)

    reportTimeTaken(triggerDetailKey, timeTaken)
    result
  }

  /**
   * Reports an input duration for a particular detail key in the next query progress
   * update. Can be used directly instead of reportTimeTaken(key)(body) when the duration
   * is measured asynchronously.
   */
  def reportTimeTaken(triggerDetailKey: String, timeTakenMs: Long): Unit = {
    val previousTime = currentDurationsMs.getOrElse(triggerDetailKey, 0L)
    currentDurationsMs.put(triggerDetailKey, previousTime + timeTakenMs)
    logDebug(s"$triggerDetailKey took $timeTakenMs ms")
  }

  /** Updates the message returned in `status`. */
  def updateStatusMessage(message: String): Unit = {
    currentStatus = currentStatus.copy(message = message)
  }

  /** Begins recording statistics about query progress for a given trigger. */
  def startTrigger(): Unit = {
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
  def recordTriggerOffsets(
      from: StreamProgress,
      to: StreamProgress,
      latest: StreamProgress): Unit = {
    currentTriggerStartOffsets = from.transform((_, v) => v.json)
    currentTriggerEndOffsets = to.transform((_, v) => v.json)
    currentTriggerLatestOffsets = latest.transform((_, v) => v.json)
    latestStreamProgress = to
    currentTriggerLatestOffsets = latest.transform((_, v) => v.json)
  }

  /** Finalizes the trigger which did not execute a batch. */
  def finishNoExecutionTrigger(lastExecutedEpochId: Long): Unit = {
    currentTriggerEndTimestamp = triggerClock.getTimeMillis()
    val processingTimeMills = currentTriggerEndTimestamp - currentTriggerStartTimestamp

    val execStatsOnNoExecution = execStatsOnLatestExecutedBatch.map(resetExecStatsForNoExecution)

    val newProgress = constructNewProgress(processingTimeMills, lastExecutedEpochId,
      execStatsOnNoExecution, Map.empty[String, Row])

    progressReporter.updateIdleness(id, runId, currentTriggerStartTimestamp, newProgress)

    warnIfFinishTriggerTakesTooLong(currentTriggerEndTimestamp, processingTimeMills)

    currentStatus = currentStatus.copy(isTriggerActive = false)
  }

  /**
   * Retrieve a measured duration
   */
  def getDuration(key: String): Option[Long] = {
    currentDurationsMs.get(key)
  }

  /**
   * Finalizes the query progress and adds it to list of recent status updates.
   *
   * @param hasNewData Whether the sources of this stream had new data for this trigger.
   */
  def finishTrigger(
      hasNewData: Boolean,
      sourceToNumInputRowsMap: Map[SparkDataStream, Long],
      lastExecution: IncrementalExecution,
      lastEpochId: Long): Unit = {
    assert(
      currentTriggerStartOffsets != null && currentTriggerEndOffsets != null &&
        currentTriggerLatestOffsets != null
    )
    currentTriggerEndTimestamp = triggerClock.getTimeMillis()
    val processingTimeMills = currentTriggerEndTimestamp - currentTriggerStartTimestamp
    assert(lastExecution != null, "executed batch should provide the information for execution.")
    val execStats = extractExecutionStats(hasNewData, sourceToNumInputRowsMap, lastExecution)
    logDebug(s"Execution stats: $execStats")

    val observedMetrics = extractObservedMetrics(lastExecution)
    val newProgress = constructNewProgress(processingTimeMills, lastEpochId, Some(execStats),
      observedMetrics)

    progressReporter.lastNoExecutionProgressEventTime = triggerClock.getTimeMillis()
    progressReporter.updateProgress(newProgress)

    // Ask the state store coordinator to log all lagging state stores
    if (progressReporter.coordinatorReportSnapshotUploadLag) {
      val latestVersion = lastEpochId + 1
      progressReporter.stateStoreCoordinator
        .logLaggingStateStores(
          lastExecution.runId,
          latestVersion,
          lastExecution.isTerminatingTrigger
        )
    }

    // Update the value since this trigger executes a batch successfully.
    this.execStatsOnLatestExecutedBatch = Some(execStats)

    warnIfFinishTriggerTakesTooLong(currentTriggerEndTimestamp, processingTimeMills)

    currentStatus = currentStatus.copy(isTriggerActive = false)
  }

  private def constructNewProgress(
      processingTimeMills: Long,
      batchId: Long,
      execStats: Option[ExecutionStats],
      observedMetrics: Map[String, Row]): StreamingQueryProgress = {
    val processingTimeSec = Math.max(1L, processingTimeMills).toDouble / MILLIS_PER_SECOND

    val inputTimeSec = if (lastTriggerStartTimestamp >= 0) {
      (currentTriggerStartTimestamp - lastTriggerStartTimestamp).toDouble / MILLIS_PER_SECOND
    } else {
      Double.PositiveInfinity
    }
    val sourceProgress = extractSourceProgress(execStats, inputTimeSec, processingTimeSec)
    val sinkProgress = extractSinkProgress(execStats)

    val eventTime = execStats.map { stats =>
      stats.eventTimeStats.asJava
    }.getOrElse(new java.util.HashMap)

    val stateOperators = execStats.map { stats =>
      stats.stateOperators.toArray
    }.getOrElse(Array[StateOperatorProgress]())

    new StreamingQueryProgress(
      id = id,
      runId = runId,
      name = name,
      timestamp = progressReporter.formatTimestamp(currentTriggerStartTimestamp),
      batchId = batchId,
      batchDuration = processingTimeMills,
      durationMs =
        new java.util.HashMap(currentDurationsMs.toMap.transform((_, v) => long2Long(v)).asJava),
      eventTime = new java.util.HashMap(eventTime),
      stateOperators = stateOperators,
      sources = sourceProgress.toArray,
      sink = sinkProgress,
      observedMetrics = new java.util.HashMap(observedMetrics.asJava))
  }

  private def extractSourceProgress(
      execStats: Option[ExecutionStats],
      inputTimeSec: Double,
      processingTimeSec: Double): Seq[SourceProgress] = {
    sources.distinct.map { source =>
      val (result, duration) = Utils.timeTakenMs {
        val numRecords = execStats.flatMap(_.inputRows.get(source)).getOrElse(0L)
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
      logInfo(log"Extracting source progress metrics for source=" +
        log"${MDC(LogKeys.SOURCE, source.toString)} " +
        log"took duration_ms=${MDC(LogKeys.DURATION, duration)}")
      result
    }
  }

  private def extractSinkProgress(execStats: Option[ExecutionStats]): SinkProgress = {
    val (result, duration) = Utils.timeTakenMs {
      val sinkOutput = execStats.flatMap(_.outputRows)
      val sinkMetrics = sink match {
        case withMetrics: ReportsSinkMetrics => withMetrics.metrics()
        case _ => Map[String, String]().asJava
      }

      SinkProgress(sink.toString, sinkOutput, sinkMetrics)
    }
    logInfo(log"Extracting sink progress metrics for sink=${MDC(LogKeys.SINK, sink.toString)} " +
      log"took duration_ms=${MDC(LogKeys.DURATION, duration)}")
    result
  }

  /**
   * Override of finishTrigger to extract the map from IncrementalExecution.
   */
  def finishTrigger(
      hasNewData: Boolean,
      lastExecution: IncrementalExecution,
      lastEpoch: Long): Unit = {
    val map: Map[SparkDataStream, Long] =
      if (hasNewData) extractSourceToNumInputRows(lastExecution) else Map.empty
    finishTrigger(hasNewData, map, lastExecution, lastEpoch)
  }

  private def warnIfFinishTriggerTakesTooLong(
      triggerEndTimestamp: Long,
      processingTimeMills: Long): Unit = {
    // Log a warning message if finishTrigger step takes more time than processing the batch and
    // also longer than min threshold (1 minute).
    val finishTriggerDurationMillis = triggerClock.getTimeMillis() - triggerEndTimestamp
    val thresholdForLoggingMillis = 60 * 1000
    if (finishTriggerDurationMillis > math.max(thresholdForLoggingMillis, processingTimeMills)) {
      logWarning(log"Query progress update takes longer than batch processing time. Progress " +
        log"update takes ${MDC(LogKeys.FINISH_TRIGGER_DURATION, finishTriggerDurationMillis)} " +
        log"milliseconds. Batch processing takes " +
        log"${MDC(LogKeys.PROCESSING_TIME, processingTimeMills)} milliseconds")
    }
  }

  private def extractSourceToNumInputRows(
      lastExecution: IncrementalExecution): Map[SparkDataStream, Long] = {

    def sumRows(tuples: Seq[(SparkDataStream, Long)]): Map[SparkDataStream, Long] = {
      tuples.groupBy(_._1).transform((_, v) => v.map(_._2).sum) // sum up rows for each source
    }

    val sources = newData.keys.toSet

    val sourceToInputRowsTuples = lastExecution.executedPlan
      .collect {
        case node: StreamSourceAwareSparkPlan if node.getStream.isDefined =>
          val numRows = node.metrics.get("numOutputRows").map(_.value).getOrElse(0L)
          node.getStream.get -> numRows
      }

    val capturedSources = sourceToInputRowsTuples.map(_._1).toSet

    if (sources == capturedSources) {
      logDebug("Source -> # input rows\n\t" + sourceToInputRowsTuples.mkString("\n\t"))
      sumRows(sourceToInputRowsTuples)
    } else {
      // Falling back to the legacy approach to avoid any regression.
      val inputRows = legacyExtractSourceToNumInputRows(lastExecution)
      // If the legacy approach fails to extract the input rows, we just pick the new approach
      // as it is more likely that the source nodes have been pruned in valid reason.
      if (inputRows.isEmpty) {
        sumRows(sourceToInputRowsTuples)
      } else {
        inputRows
      }
    }
  }

  /** Extract number of input sources for each streaming source in plan */
  private def legacyExtractSourceToNumInputRows(
      lastExecution: IncrementalExecution): Map[SparkDataStream, Long] = {

    def sumRows(tuples: Seq[(SparkDataStream, Long)]): Map[SparkDataStream, Long] = {
      tuples.groupBy(_._1).transform((_, v) => v.map(_._2).sum) // sum up rows for each source
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
      val allStreamingLeaves = progressReporter.logicalPlan().collect {
        case s: StreamingDataSourceV2ScanRelation => s.stream.isInstanceOf[MicroBatchStream]
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

          logWarning(log"Could not report metrics as number leaves in trigger logical plan did " +
            log"not match that of the execution plan:\nlogical plan leaves: " +
            log"${MDC(LogKeys.LOGICAL_PLAN_LEAVES, toString(allLogicalPlanLeaves))}\nexecution " +
            log"plan leaves: ${MDC(LogKeys.EXECUTION_PLAN_LEAVES, toString(allExecPlanLeaves))}\n")
          metricWarningLogged = true
        }
        Map.empty
      }
    }
  }

  /** Extract statistics about stateful operators from the executed query plan. */
  private def extractStateOperatorMetrics(
      lastExecution: IncrementalExecution): Seq[StateOperatorProgress] = {
    assert(lastExecution != null, "lastExecution is not available")
    lastExecution.executedPlan.collect {
      case p if p.isInstanceOf[StateStoreWriter] =>
        p.asInstanceOf[StateStoreWriter].getProgress()
    }
  }

  /** Extracts statistics from the most recent query execution. */
  private def extractExecutionStats(
      hasNewData: Boolean,
      sourceToNumInputRows: Map[SparkDataStream, Long],
      lastExecution: IncrementalExecution): ExecutionStats = {
    val hasEventTime = progressReporter.logicalPlan().collectFirst {
      case e: EventTimeWatermark => e
    }.nonEmpty

    val watermarkTimestamp =
      if (hasEventTime) {
        Map("watermark" -> progressReporter.formatTimestamp(offsetSeqMetadata.batchWatermarkMs))
      } else Map.empty[String, String]

    // SPARK-19378: Still report metrics even though no data was processed while reporting progress.
    val stateOperators = extractStateOperatorMetrics(lastExecution)

    val sinkOutput = sinkCommitProgress.map(_.numOutputRows)

    if (!hasNewData) {
      return ExecutionStats(Map.empty, stateOperators, watermarkTimestamp, sinkOutput)
    }

    val eventTimeStats = lastExecution.executedPlan
      .collect {
        case e: EventTimeWatermarkExec if e.eventTimeStats.value.count > 0 =>
          val stats = e.eventTimeStats.value
          Map(
            "max" -> stats.max,
            "min" -> stats.min,
            "avg" -> stats.avg.toLong).transform((_, v) => progressReporter.formatTimestamp(v))
      }.headOption.getOrElse(Map.empty) ++ watermarkTimestamp

    ExecutionStats(sourceToNumInputRows, stateOperators, eventTimeStats.toMap, sinkOutput)
  }

  /**
   * Reset values in the execution stats to removes the values which are specific to the batch.
   * New execution stats will only retain the values as a snapshot of the query status.
   * (E.g. for stateful operators, numRowsTotal is a snapshot of the status, whereas
   * numRowsUpdated is bound to the batch.)
   * TODO: We do not seem to clear up all values in StateOperatorProgress which are bound to the
   * batch. Fix this.
   */
  private def resetExecStatsForNoExecution(originExecStats: ExecutionStats): ExecutionStats = {
    val newStatefulOperators = originExecStats.stateOperators.map { so =>
      so.copy(newNumRowsUpdated = 0, newNumRowsDroppedByWatermark = 0)
    }
    val newEventTimeStats = if (originExecStats.eventTimeStats.contains("watermark")) {
      Map("watermark" -> progressReporter.formatTimestamp(offsetSeqMetadata.batchWatermarkMs))
    } else {
      Map.empty[String, String]
    }
    val newOutputRows = originExecStats.outputRows.map(_ => 0L)
    originExecStats.copy(
      inputRows = Map.empty[SparkDataStream, Long],
      stateOperators = newStatefulOperators,
      eventTimeStats = newEventTimeStats,
      outputRows = newOutputRows)
  }

  /** Extracts observed metrics from the most recent query execution. */
  private def extractObservedMetrics(
      lastExecution: QueryExecution): Map[String, Row] = {
    if (lastExecution == null) {
      return Map.empty
    }
    lastExecution.observedMetrics
  }
}

object ProgressContext {
  case class ExecutionStats(
    inputRows: Map[SparkDataStream, Long],
    stateOperators: Seq[StateOperatorProgress],
    eventTimeStats: Map[String, String],
    outputRows: Option[Long])
}
