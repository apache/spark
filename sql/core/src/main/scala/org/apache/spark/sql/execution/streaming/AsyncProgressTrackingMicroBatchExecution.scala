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

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.internal.LogKeys.{BATCH_ID, PRETTY_ID_STRING}
import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.util.{Clock, ThreadUtils}

/**
 * Class to execute micro-batches when async progress tracking is enabled
 */
class AsyncProgressTrackingMicroBatchExecution(
    sparkSession: SparkSession,
    trigger: Trigger,
    triggerClock: Clock,
    extraOptions: Map[String, String],
    plan: WriteToStream)
  extends MicroBatchExecution(sparkSession, trigger, triggerClock, extraOptions, plan) {

  import AsyncProgressTrackingMicroBatchExecution._

  protected val asyncProgressTrackingCheckpointingIntervalMs: Long
  = getAsyncProgressTrackingCheckpointingIntervalMs(extraOptions)

  // Offsets that are ready to be committed by the source.
  // This is needed so that we can call source commit in the same thread as micro-batch execution
  // to be thread safe
  private val sourceCommitQueue = new ConcurrentLinkedQueue[OffsetSeq]()

  // to cache the batch id of the last batch written to storage
  private val lastBatchPersistedToDurableStorage = new AtomicLong(-1)

  // used to check during the first batch if the pipeline is stateful
  private var isFirstBatch: Boolean = true

  // thread pool is only one thread because we want offset
  // writes to execute in order in a serialized fashion
  protected val asyncWritesExecutorService
  = ThreadUtils.newDaemonSingleThreadExecutorWithRejectedExecutionHandler(
    "async-log-write",
    2, // one for offset commit and one for completion commit
    new RejectedExecutionHandler() {
      override def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor): Unit = {
        try {
          if (!executor.isShutdown) {
            val start = System.currentTimeMillis()
            executor.getQueue.put(r)
            logDebug(
              s"Async write paused execution for " +
                s"${System.currentTimeMillis() - start} due to task queue being full."
            )
          }
        } catch {
          case e: InterruptedException =>
            Thread.currentThread.interrupt()
            throw new RejectedExecutionException("Producer interrupted", e)
          case e: Throwable =>
            logError("Encountered error in async write executor service", e)
            errorNotifier.markError(e)
        }
      }
    })

  override val offsetLog = new AsyncOffsetSeqLog(
    sparkSession,
    checkpointFile("offsets"),
    asyncWritesExecutorService,
    asyncProgressTrackingCheckpointingIntervalMs,
    clock = triggerClock
  )

  override val commitLog =
    new AsyncCommitLog(sparkSession, checkpointFile("commits"), asyncWritesExecutorService)

  // perform quick validation to fail faster
  validateAndGetTrigger()

  override def validateOffsetLogAndGetPrevOffset(latestBatchId: Long): Option[OffsetSeq] = {
    /* Initialize committed offsets to a committed batch, which at this
     * is the second latest batch id in the offset log.
     * The offset log may not be contiguous */
    val prevBatchId = offsetLog.getPrevBatchFromStorage(latestBatchId)
    if (latestBatchId != 0 && prevBatchId.isDefined) {
      Some(offsetLog.get(prevBatchId.get).getOrElse({
        throw new IllegalStateException(s"Offset metadata for batch ${prevBatchId}" +
          s" cannot be found.  This should not happen.")
      }))
    } else {
      None
    }
  }

  override def markMicroBatchExecutionStart(execCtx: MicroBatchExecutionContext): Unit = {
    // check if streaming query is stateful
    checkNotStatefulStreamingQuery
  }

  override def cleanUpLastExecutedMicroBatch(execCtx: MicroBatchExecutionContext): Unit = {
    // this is a no op for async progress tracking since we only want to commit sources only
    // after the offset WAL commit has be successfully written
  }

  /**
   * Should not call super method as we need to do something completely different
   * in this method for async progress tracking
   */
  override def markMicroBatchStart(execCtx: MicroBatchExecutionContext): Unit = {
    // Because we are using a thread pool with only one thread, async writes to the offset log
    // are still written in a serial / in order fashion
    offsetLog
      .addAsync(execCtx.batchId, execCtx.endOffsets.toOffsetSeq(sources, execCtx.offsetSeqMetadata))
      .thenAccept(tuple => {
        val (batchId, persistedToDurableStorage) = tuple
        if (persistedToDurableStorage) {
          // batch id cache not initialized
          if (lastBatchPersistedToDurableStorage.get == -1) {
            lastBatchPersistedToDurableStorage.set(
              offsetLog.getPrevBatchFromStorage(batchId).getOrElse(-1))
          }

          if (batchId != 0 && lastBatchPersistedToDurableStorage.get != -1) {
            // sanity check to make sure batch ids are monotonically increasing
            assert(lastBatchPersistedToDurableStorage.get < batchId)
            val prevBatchOff = offsetLog.get(lastBatchPersistedToDurableStorage.get())
            if (prevBatchOff.isDefined) {
              // Offset is ready to be committed by the source. Add to queue
              sourceCommitQueue.add(prevBatchOff.get)
            } else {
              throw new IllegalStateException(
                s"Failed to commit processed data in the source because batch " +
                  s"${lastBatchPersistedToDurableStorage.get()} doesn't exist in the offset log." +
                  s"  This should not happen.")
            }
          }
          lastBatchPersistedToDurableStorage.set(batchId)
        }
      })
      .exceptionally((th: Throwable) => {
        logError(log"Encountered error while performing async offset write for batch " +
          log"${MDC(BATCH_ID, execCtx.batchId)}", th)
        errorNotifier.markError(th)
        return
      })

    // check if there are offsets that are ready to be committed by the source
    var offset = sourceCommitQueue.poll()
    while (offset != null) {
      commitSources(offset)
      offset = sourceCommitQueue.poll()
    }
  }

  override def markMicroBatchEnd(execCtx: MicroBatchExecutionContext): Unit = {
    watermarkTracker.updateWatermark(execCtx.executionPlan.executedPlan)
    execCtx.reportTimeTaken("commitOffsets") {
      // check if current batch there is a async write for the offset log is issued for this batch
      // if so, we should do the same for commit log.  However, if this is the first batch executed
      // in this run we should always persist to the commit log.  There can be situations in which
      // the offset log has more entries than the commit log and on restart we need to make sure
      // we write the missing entries to the commit log.  For example if the offset log is 0, 2, 5
      // and the commit log is 0, 2.  On restart we will re-process the data from batch 3 -> 5.
      // Batch 5 is already part of the offset log but we still need to write the entry to
      // the commit log
      if (offsetLog.getAsyncOffsetWrite(execCtx.batchId).nonEmpty
        || isFirstBatch) {
        isFirstBatch = false

        commitLog
          .addAsync(execCtx.batchId, CommitMetadata(watermarkTracker.currentWatermark))
          .exceptionally((th: Throwable) => {
            logError(log"Got exception during async write to commit log for batch " +
              log"${MDC(BATCH_ID, execCtx.batchId)}", th)
            errorNotifier.markError(th)
            return
          })
      } else {
        if (!commitLog.addInMemory(
          execCtx.batchId, CommitMetadata(watermarkTracker.currentWatermark))) {
          throw QueryExecutionErrors.concurrentStreamLogUpdate(execCtx.batchId)
        }
      }
      offsetLog.removeAsyncOffsetWrite(execCtx.batchId)
    }
    committedOffsets ++= execCtx.endOffsets
  }

  // need to look at the number of files on disk
  override def purge(threshold: Long): Unit = {
    while (offsetLog.writtenToDurableStorage.size() > minLogEntriesToMaintain) {
      offsetLog.writtenToDurableStorage.poll()
    }
    offsetLog.purge(offsetLog.writtenToDurableStorage.peek())

    while (commitLog.writtenToDurableStorage.size() > minLogEntriesToMaintain) {
      commitLog.writtenToDurableStorage.poll()
    }
    commitLog.purge(commitLog.writtenToDurableStorage.peek())
  }

  override def cleanup(): Unit = {
    super.cleanup()

    ThreadUtils.shutdown(asyncWritesExecutorService)
    logInfo(log"Async progress tracking executor pool for query " +
      log"${MDC(PRETTY_ID_STRING, prettyIdString)} has been shutdown")
  }

  // used for testing
  def areWritesPendingOrInProgress(): Boolean = {
    asyncWritesExecutorService.getQueue.size() > 0 || asyncWritesExecutorService.getActiveCount > 0
  }

  override protected def getTrigger(): TriggerExecutor = validateAndGetTrigger()

  private def validateAndGetTrigger(): TriggerExecutor = {
    // validate that the pipeline is using a supported sink
    if (!extraOptions
      .getOrElse(
        ASYNC_PROGRESS_TRACKING_OVERRIDE_SINK_SUPPORT_CHECK, "false")
      .toBoolean) {
      try {
        plan.sink.name() match {
          case "noop-table" =>
          case "console" =>
          case "MemorySink" =>
          case "KafkaTable" =>
          case _ =>
            throw new IllegalArgumentException(
              s"Sink ${plan.sink.name()}" +
                s" does not support async progress tracking"
            )
        }
      } catch {
        case e: IllegalStateException =>
          // sink does not implement name() method
          if (e.getMessage.equals("should not be called.")) {
            throw new IllegalArgumentException(
              s"Sink ${plan.sink}" +
                s" does not support async progress tracking"
            )
          } else {
            throw e
          }
      }
    }

    trigger match {
      case t: ProcessingTimeTrigger => ProcessingTimeExecutor(t, triggerClock)
      case OneTimeTrigger =>
        throw new IllegalArgumentException(
          "Async progress tracking cannot be used with Once trigger")
      case AvailableNowTrigger =>
        throw new IllegalArgumentException(
          "Async progress tracking cannot be used with AvailableNow trigger"
        )
      case _ => throw new IllegalStateException(s"Unknown type of trigger: $trigger")
    }
  }

  private def checkNotStatefulStreamingQuery: Unit = {
    if (isFirstBatch) {
      lastExecution.executedPlan.collect {
        case p if p.isInstanceOf[StateStoreWriter] =>
          throw new IllegalArgumentException(
            "Stateful streaming queries does not support async progress tracking at this moment."
          )
      }
    }
  }
}

object AsyncProgressTrackingMicroBatchExecution {
  val ASYNC_PROGRESS_TRACKING_ENABLED = "asyncProgressTrackingEnabled"
  val ASYNC_PROGRESS_TRACKING_CHECKPOINTING_INTERVAL_MS =
    "asyncProgressTrackingCheckpointIntervalMs"

  // for testing purposes
  val ASYNC_PROGRESS_TRACKING_OVERRIDE_SINK_SUPPORT_CHECK =
    "_asyncProgressTrackingOverrideSinkSupportCheck"

  private def getAsyncProgressTrackingCheckpointingIntervalMs(
      extraOptions: Map[String, String]): Long = {
    extraOptions
      .getOrElse(
        ASYNC_PROGRESS_TRACKING_CHECKPOINTING_INTERVAL_MS,
        "1000"
      )
      .toLong
  }
}
