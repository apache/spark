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

import scala.collection.mutable.{ArrayBuffer, Map => MutableMap}

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, CurrentBatchTimestamp, CurrentDate, CurrentTimestamp}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, Trigger}
import org.apache.spark.util.{Clock, Utils}

class MicroBatchExecution(
    sparkSession: SparkSession,
    name: String,
    checkpointRoot: String,
    analyzedPlan: LogicalPlan,
    sink: Sink,
    trigger: Trigger,
    triggerClock: Clock,
    outputMode: OutputMode,
    deleteCheckpointOnStop: Boolean)
  extends StreamExecution(
    sparkSession, name, checkpointRoot, analyzedPlan, sink,
    trigger, triggerClock, outputMode, deleteCheckpointOnStop) {

  private val triggerExecutor = trigger match {
    case t: ProcessingTime => ProcessingTimeExecutor(t, triggerClock)
    case OneTimeTrigger => OneTimeExecutor()
    case _ => throw new IllegalStateException(s"Unknown type of trigger: $trigger")
  }

  override lazy val logicalPlan: LogicalPlan = {
    assert(queryExecutionThread eq Thread.currentThread,
      "logicalPlan must be initialized in QueryExecutionThread " +
        s"but the current thread was ${Thread.currentThread}")
    var nextSourceId = 0L
    val toExecutionRelationMap = MutableMap[StreamingRelation, StreamingExecutionRelation]()
    val _logicalPlan = analyzedPlan.transform {
      case streamingRelation@StreamingRelation(dataSource, _, output) =>
        toExecutionRelationMap.getOrElseUpdate(streamingRelation, {
          // Materialize source to avoid creating it in every batch
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          val source = dataSource.createSource(metadataPath)
          nextSourceId += 1
          // We still need to use the previous `output` instead of `source.schema` as attributes in
          // "df.logicalPlan" has already used attributes of the previous `output`.
          StreamingExecutionRelation(source, output)(sparkSession)
        })
    }
    sources = _logicalPlan.collect { case s: StreamingExecutionRelation => s.source }
    uniqueSources = sources.distinct
    _logicalPlan
  }

  /**
   * Repeatedly attempts to run batches as data arrives.
   */
  protected def runActivatedStream(sparkSessionForStream: SparkSession): Unit = {
    triggerExecutor.execute(() => {
      startTrigger()

      if (isActive) {
        reportTimeTaken("triggerExecution") {
          if (currentBatchId < 0) {
            // We'll do this initialization only once
            populateStartOffsets(sparkSessionForStream)
            sparkSession.sparkContext.setJobDescription(getBatchDescriptionString)
            logDebug(s"Stream running from $committedOffsets to $availableOffsets")
          } else {
            constructNextBatch()
          }
          if (dataAvailable) {
            currentStatus = currentStatus.copy(isDataAvailable = true)
            updateStatusMessage("Processing new data")
            runBatch(sparkSessionForStream)
          }
        }
        // Report trigger as finished and construct progress object.
        finishTrigger(dataAvailable)
        if (dataAvailable) {
          // Update committed offsets.
          commitLog.add(currentBatchId)
          committedOffsets ++= availableOffsets
          logDebug(s"batch ${currentBatchId} committed")
          // We'll increase currentBatchId after we complete processing current batch's data
          currentBatchId += 1
          sparkSession.sparkContext.setJobDescription(getBatchDescriptionString)
        } else {
          currentStatus = currentStatus.copy(isDataAvailable = false)
          updateStatusMessage("Waiting for data to arrive")
          Thread.sleep(pollingDelayMs)
        }
      }
      updateStatusMessage("Waiting for next trigger")
      isActive
    })
  }

  /**
   * Populate the start offsets to start the execution at the current offsets stored in the sink
   * (i.e. avoid reprocessing data that we have already processed). This function must be called
   * before any processing occurs and will populate the following fields:
   *  - currentBatchId
   *  - committedOffsets
   *  - availableOffsets
   *  The basic structure of this method is as follows:
   *
   *  Identify (from the offset log) the offsets used to run the last batch
   *  IF last batch exists THEN
   *    Set the next batch to be executed as the last recovered batch
   *    Check the commit log to see which batch was committed last
   *    IF the last batch was committed THEN
   *      Call getBatch using the last batch start and end offsets
   *      // ^^^^ above line is needed since some sources assume last batch always re-executes
   *      Setup for a new batch i.e., start = last batch end, and identify new end
   *    DONE
   *  ELSE
   *    Identify a brand new batch
   *  DONE
   */
  private def populateStartOffsets(sparkSessionToRunBatches: SparkSession): Unit = {
    offsetLog.getLatest() match {
      case Some((latestBatchId, nextOffsets)) =>
        /* First assume that we are re-executing the latest known batch
         * in the offset log */
        currentBatchId = latestBatchId
        availableOffsets = nextOffsets.toStreamProgress(sources)
        /* Initialize committed offsets to a committed batch, which at this
         * is the second latest batch id in the offset log. */
        if (latestBatchId != 0) {
          val secondLatestBatchId = offsetLog.get(latestBatchId - 1).getOrElse {
            throw new IllegalStateException(s"batch ${latestBatchId - 1} doesn't exist")
          }
          committedOffsets = secondLatestBatchId.toStreamProgress(sources)
        }

        // update offset metadata
        nextOffsets.metadata.foreach { metadata =>
          OffsetSeqMetadata.setSessionConf(metadata, sparkSessionToRunBatches.conf)
          offsetSeqMetadata = OffsetSeqMetadata(
            metadata.batchWatermarkMs, metadata.batchTimestampMs, sparkSessionToRunBatches.conf)
        }

        /* identify the current batch id: if commit log indicates we successfully processed the
         * latest batch id in the offset log, then we can safely move to the next batch
         * i.e., committedBatchId + 1 */
        commitLog.getLatest() match {
          case Some((latestCommittedBatchId, _)) =>
            if (latestBatchId == latestCommittedBatchId) {
              /* The last batch was successfully committed, so we can safely process a
               * new next batch but first:
               * Make a call to getBatch using the offsets from previous batch.
               * because certain sources (e.g., KafkaSource) assume on restart the last
               * batch will be executed before getOffset is called again. */
              availableOffsets.foreach { ao: (Source, Offset) =>
                val (source, end) = ao
                if (committedOffsets.get(source).map(_ != end).getOrElse(true)) {
                  val start = committedOffsets.get(source)
                  source.getBatch(start, end)
                }
              }
              currentBatchId = latestCommittedBatchId + 1
              committedOffsets ++= availableOffsets
              // Construct a new batch be recomputing availableOffsets
              constructNextBatch()
            } else if (latestCommittedBatchId < latestBatchId - 1) {
              logWarning(s"Batch completion log latest batch id is " +
                s"${latestCommittedBatchId}, which is not trailing " +
                s"batchid $latestBatchId by one")
            }
          case None => logInfo("no commit log present")
        }
        logDebug(s"Resuming at batch $currentBatchId with committed offsets " +
          s"$committedOffsets and available offsets $availableOffsets")
      case None => // We are starting this stream for the first time.
        logInfo(s"Starting new streaming query.")
        currentBatchId = 0
        constructNextBatch()
    }
  }

  /**
   * Returns true if there is any new data available to be processed.
   */
  private def dataAvailable: Boolean = {
    availableOffsets.exists {
      case (source, available) =>
        committedOffsets
          .get(source)
          .map(committed => committed != available)
          .getOrElse(true)
    }
  }

  /**
   * Queries all of the sources to see if any new data is available. When there is new data the
   * batchId counter is incremented and a new log entry is written with the newest offsets.
   */
  private def constructNextBatch(): Unit = {
    // Check to see what new data is available.
    val hasNewData = {
      awaitProgressLock.lock()
      try {
        val latestOffsets: Map[Source, Option[Offset]] = uniqueSources.map { s =>
          updateStatusMessage(s"Getting offsets from $s")
          reportTimeTaken("getOffset") {
            (s, s.getOffset)
          }
        }.toMap
        availableOffsets ++= latestOffsets.filter { case (s, o) => o.nonEmpty }.mapValues(_.get)

        if (dataAvailable) {
          true
        } else {
          noNewData = true
          false
        }
      } finally {
        awaitProgressLock.unlock()
      }
    }
    if (hasNewData) {
      var batchWatermarkMs = offsetSeqMetadata.batchWatermarkMs
      // Update the eventTime watermarks if we find any in the plan.
      if (lastExecution != null) {
        lastExecution.executedPlan.collect {
          case e: EventTimeWatermarkExec => e
        }.zipWithIndex.foreach {
          case (e, index) if e.eventTimeStats.value.count > 0 =>
            logDebug(s"Observed event time stats $index: ${e.eventTimeStats.value}")
            val newWatermarkMs = e.eventTimeStats.value.max - e.delayMs
            val prevWatermarkMs = watermarkMsMap.get(index)
            if (prevWatermarkMs.isEmpty || newWatermarkMs > prevWatermarkMs.get) {
              watermarkMsMap.put(index, newWatermarkMs)
            }

          // Populate 0 if we haven't seen any data yet for this watermark node.
          case (_, index) =>
            if (!watermarkMsMap.isDefinedAt(index)) {
              watermarkMsMap.put(index, 0)
            }
        }

        // Update the global watermark to the minimum of all watermark nodes.
        // This is the safest option, because only the global watermark is fault-tolerant. Making
        // it the minimum of all individual watermarks guarantees it will never advance past where
        // any individual watermark operator would be if it were in a plan by itself.
        if(!watermarkMsMap.isEmpty) {
          val newWatermarkMs = watermarkMsMap.minBy(_._2)._2
          if (newWatermarkMs > batchWatermarkMs) {
            logInfo(s"Updating eventTime watermark to: $newWatermarkMs ms")
            batchWatermarkMs = newWatermarkMs
          } else {
            logDebug(
              s"Event time didn't move: $newWatermarkMs < " +
                s"$batchWatermarkMs")
          }
        }
      }
      offsetSeqMetadata = offsetSeqMetadata.copy(
        batchWatermarkMs = batchWatermarkMs,
        batchTimestampMs = triggerClock.getTimeMillis()) // Current batch timestamp in milliseconds

      updateStatusMessage("Writing offsets to log")
      reportTimeTaken("walCommit") {
        assert(offsetLog.add(
          currentBatchId,
          availableOffsets.toOffsetSeq(sources, offsetSeqMetadata)),
          s"Concurrent update to the log. Multiple streaming jobs detected for $currentBatchId")
        logInfo(s"Committed offsets for batch $currentBatchId. " +
          s"Metadata ${offsetSeqMetadata.toString}")

        // NOTE: The following code is correct because runStream() processes exactly one
        // batch at a time. If we add pipeline parallelism (multiple batches in flight at
        // the same time), this cleanup logic will need to change.

        // Now that we've updated the scheduler's persistent checkpoint, it is safe for the
        // sources to discard data from the previous batch.
        if (currentBatchId != 0) {
          val prevBatchOff = offsetLog.get(currentBatchId - 1)
          if (prevBatchOff.isDefined) {
            prevBatchOff.get.toStreamProgress(sources).foreach {
              case (src, off) => src.commit(off)
            }
          } else {
            throw new IllegalStateException(s"batch $currentBatchId doesn't exist")
          }
        }

        // It is now safe to discard the metadata beyond the minimum number to retain.
        // Note that purge is exclusive, i.e. it purges everything before the target ID.
        if (minLogEntriesToMaintain < currentBatchId) {
          offsetLog.purge(currentBatchId - minLogEntriesToMaintain)
          commitLog.purge(currentBatchId - minLogEntriesToMaintain)
        }
      }
    } else {
      awaitProgressLock.lock()
      try {
        // Wake up any threads that are waiting for the stream to progress.
        awaitProgressLockCondition.signalAll()
      } finally {
        awaitProgressLock.unlock()
      }
    }
  }

  /**
   * Processes any data available between `availableOffsets` and `committedOffsets`.
   * @param sparkSessionToRunBatch Isolated [[SparkSession]] to run this batch with.
   */
  private def runBatch(sparkSessionToRunBatch: SparkSession): Unit = {
    // Request unprocessed data from all sources.
    newData = reportTimeTaken("getBatch") {
      availableOffsets.flatMap {
        case (source, available)
          if committedOffsets.get(source).map(_ != available).getOrElse(true) =>
          val current = committedOffsets.get(source)
          val batch = source.getBatch(current, available)
          assert(batch.isStreaming,
            s"DataFrame returned by getBatch from $source did not have isStreaming=true\n" +
              s"${batch.queryExecution.logical}")
          logDebug(s"Retrieving data from $source: $current -> $available")
          Some(source -> batch)
        case _ => None
      }
    }

    // A list of attributes that will need to be updated.
    val replacements = new ArrayBuffer[(Attribute, Attribute)]
    // Replace sources in the logical plan with data that has arrived since the last batch.
    val withNewSources = logicalPlan transform {
      case StreamingExecutionRelation(source, output) =>
        newData.get(source).map { data =>
          val newPlan = data.logicalPlan
          assert(output.size == newPlan.output.size,
            s"Invalid batch: ${Utils.truncatedString(output, ",")} != " +
              s"${Utils.truncatedString(newPlan.output, ",")}")
          replacements ++= output.zip(newPlan.output)
          newPlan
        }.getOrElse {
          LocalRelation(output, isStreaming = true)
        }
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    val replacementMap = AttributeMap(replacements)
    val triggerLogicalPlan = withNewSources transformAllExpressions {
      case a: Attribute if replacementMap.contains(a) =>
        replacementMap(a).withMetadata(a.metadata)
      case ct: CurrentTimestamp =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          ct.dataType)
      case cd: CurrentDate =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          cd.dataType, cd.timeZoneId)
    }

    reportTimeTaken("queryPlanning") {
      lastExecution = new IncrementalExecution(
        sparkSessionToRunBatch,
        triggerLogicalPlan,
        outputMode,
        checkpointFile("state"),
        runId,
        currentBatchId,
        offsetSeqMetadata)
      lastExecution.executedPlan // Force the lazy generation of execution plan
    }

    val nextBatch =
      new Dataset(sparkSessionToRunBatch, lastExecution, RowEncoder(lastExecution.analyzed.schema))

    reportTimeTaken("addBatch") {
      SQLExecution.withNewExecutionId(sparkSessionToRunBatch, lastExecution) {
        sink.addBatch(currentBatchId, nextBatch)
      }
    }

    awaitProgressLock.lock()
    try {
      // Wake up any threads that are waiting for the stream to progress.
      awaitProgressLockCondition.signalAll()
    } finally {
      awaitProgressLock.unlock()
    }
  }
}
