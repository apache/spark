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

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

import scala.collection.mutable.{Map => MutableMap}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{CurrentDate, CurrentTimestampLike, LocalTimestamp}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.streaming.{StreamingRelationV2, WriteToStream}
import org.apache.spark.sql.catalyst.trees.TreePattern.CURRENT_LIKE
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.distributions.UnspecifiedDistribution
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, PartitionOffset, ReadLimit}
import org.apache.spark.sql.connector.write.{RequiresDistributionAndOrdering, Write}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.v2.{StreamingDataSourceV2Relation, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Clock

class ContinuousExecution(
    sparkSession: SparkSession,
    trigger: Trigger,
    triggerClock: Clock,
    extraOptions: Map[String, String],
    plan: WriteToStream)
  extends StreamExecution(
    sparkSession, plan.name, plan.resolvedCheckpointLocation, plan.inputQuery, plan.sink,
    trigger, triggerClock, plan.outputMode, plan.deleteCheckpointOnStop) {

  // needs to be a lazy val because some of the parameters will not be initialized yet
  private lazy val latestExecutionContext: StreamExecutionContext = new ContinuousExecutionContext(
    id, runId, name, triggerClock, sources, sink, progressReporter, -1, sparkSession)

  override def getLatestExecutionContext(): StreamExecutionContext = {
    latestExecutionContext
  }

  @volatile protected var sources: Seq[ContinuousStream] = Seq()

  // For use only in test harnesses.
  private[sql] var currentEpochCoordinatorId: String = _

  // Throwable that caused the execution to fail
  private val failure: AtomicReference[Throwable] = new AtomicReference[Throwable](null)

  override val logicalPlan: WriteToContinuousDataSource = {
    val v2ToRelationMap = MutableMap[StreamingRelationV2, StreamingDataSourceV2ScanRelation]()
    var nextSourceId = 0
    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
    val _logicalPlan = analyzedPlan.transform {
      case s @ StreamingRelationV2(ds, sourceName, table: SupportsRead, options, output,
        catalog, identifier, _) =>
        val dsStr = if (ds.nonEmpty) s"[${ds.get}]" else ""
        if (!table.supports(TableCapability.CONTINUOUS_READ)) {
          throw QueryExecutionErrors.continuousProcessingUnsupportedByDataSourceError(sourceName)
        }

        v2ToRelationMap.getOrElseUpdate(s, {
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          nextSourceId += 1
          logInfo(log"Reading table [${MDC(STREAMING_TABLE, table)}] " +
            log"from DataSourceV2 named '${MDC(STREAMING_DATA_SOURCE_NAME, sourceName)}' " +
            log"${MDC(STREAMING_DATA_SOURCE_DESCRIPTION, dsStr)}")
          // TODO: operator pushdown.
          val scan = table.newScanBuilder(options).build()
          val stream = scan.toContinuousStream(metadataPath)
          val relation = StreamingDataSourceV2Relation(
              table, output, catalog, identifier, options, metadataPath)
          StreamingDataSourceV2ScanRelation(relation, scan, output, stream)
        })
    }

    sources = _logicalPlan.collect {
      case r: StreamingDataSourceV2ScanRelation => r.stream.asInstanceOf[ContinuousStream]
    }
    uniqueSources = sources.distinct.map(s => s -> ReadLimit.allAvailable()).toMap

    // TODO (SPARK-27484): we should add the writing node before the plan is analyzed.
    val write = createWrite(plan.sink.asInstanceOf[SupportsWrite], extraOptions, _logicalPlan)

    if (hasDistributionRequirements(write) || hasOrderingRequirements(write)) {
      throw QueryCompilationErrors.writeDistributionAndOrderingNotSupportedInContinuousExecution()
    }

    val streamingWrite = write.toStreaming
    val customMetrics = write.supportedCustomMetrics.toImmutableArraySeq
    WriteToContinuousDataSource(streamingWrite, _logicalPlan, customMetrics)
  }

  private def hasDistributionRequirements(write: Write): Boolean = write match {
    case w: RequiresDistributionAndOrdering if w.requiredNumPartitions == 0 =>
      w.requiredDistribution match {
        case _: UnspecifiedDistribution =>
          false
        case _ =>
          true
      }
    case _ =>
      false
  }

  private def hasOrderingRequirements(write: Write): Boolean = write match {
    case w: RequiresDistributionAndOrdering if w.requiredOrdering.nonEmpty =>
      true
    case _ =>
      false
  }

  private val triggerExecutor = trigger match {
    case ContinuousTrigger(t) => ProcessingTimeExecutor(ProcessingTimeTrigger(t), triggerClock)
    case _ => throw new IllegalStateException(s"Unsupported type of trigger: $trigger")
  }

  override protected def runActivatedStream(sparkSessionForStream: SparkSession): Unit = {
    val stateUpdate = new UnaryOperator[State] {
      override def apply(s: State) = s match {
        // If we ended the query to reconfigure, reset the state to active.
        case RECONFIGURING => ACTIVE
        case _ => s
      }
    }

    do {
      runContinuous(sparkSessionForStream)
    } while (state.updateAndGet(stateUpdate) == ACTIVE)

    stopSources()
  }

  /**
   * Populate the start offsets to start the execution at the current offsets stored in the sink
   * (i.e. avoid reprocessing data that we have already processed). This function must be called
   * before any processing occurs and will populate the following fields:
   *  - currentBatchId
   *  - committedOffsets
   *  The basic structure of this method is as follows:
   *
   *  Identify (from the commit log) the latest epoch that has committed
   *  IF last epoch exists THEN
   *    Get end offsets for the epoch
   *    Set those offsets as the current commit progress
   *    Set the next epoch ID as the last + 1
   *    Return the end offsets of the last epoch as start for the next one
   *    DONE
   *  ELSE
   *    Start a new query log
   *  DONE
   */
  private def getStartOffsets(): OffsetSeq = {
    val execCtx = latestExecutionContext.asInstanceOf[ContinuousExecutionContext]
    // Note that this will need a slight modification for exactly once. If ending offsets were
    // reported but not committed for any epochs, we must replay exactly to those offsets.
    // For at least once, we can just ignore those reports and risk duplicates.
    commitLog.getLatest() match {
      case Some((latestEpochId, _)) =>
        execCtx.updateStatusMessage("Starting new streaming query " +
          s"and getting offsets from latest epoch $latestEpochId")
        val nextOffsets = offsetLog.get(latestEpochId).getOrElse {
          throw new IllegalStateException(
            s"Batch $latestEpochId was committed without end epoch offsets!")
        }
        committedOffsets = nextOffsets.toStreamProgress(sources)
        execCtx.batchId = latestEpochId + 1

        logDebug(s"Resuming at epoch ${execCtx.batchId} with start offsets ${execCtx.startOffsets}")
        nextOffsets
      case None =>
        // We are starting this stream for the first time. Offsets are all None.
        execCtx.updateStatusMessage("Starting new streaming query")
        logInfo(s"Starting new streaming query.")
        execCtx.batchId = 0
        OffsetSeq.fill(sources.map(_ => null): _*)
    }
  }

  /**
   * Do a continuous run.
   * @param sparkSessionForQuery Isolated [[SparkSession]] to run the continuous query with.
   */
  private def runContinuous(sparkSessionForQuery: SparkSession): Unit = {
    val offsets = getStartOffsets()
    val execCtx = latestExecutionContext

    if (execCtx.batchId > 0) {
      AcceptsLatestSeenOffsetHandler.setLatestSeenOffsetOnSources(Some(offsets), sources)
    }

    val withNewSources: LogicalPlan = logicalPlan transform {
      case relation: StreamingDataSourceV2ScanRelation =>
        val loggedOffset = offsets.offsets(0)
        val realOffset = loggedOffset.map(off => relation.stream.deserializeOffset(off.json))
        val startOffset = realOffset.getOrElse(relation.stream.initialOffset)
        relation.copy(startOffset = Some(startOffset))
    }

    withNewSources.transformAllExpressionsWithPruning(_.containsPattern(CURRENT_LIKE)) {
      case (_: CurrentTimestampLike | _: CurrentDate | _: LocalTimestamp) =>
        throw new IllegalStateException("CurrentTimestamp, Now, CurrentDate and LocalTimestamp" +
          " not yet supported for continuous processing")
    }

    execCtx.reportTimeTaken("queryPlanning") {
      execCtx.executionPlan = new IncrementalExecution(
        sparkSessionForQuery,
        withNewSources,
        outputMode,
        checkpointFile("state"),
        id,
        runId,
        execCtx.batchId,
        None,
        execCtx.offsetSeqMetadata,
        WatermarkPropagator.noop(),
        false)
      execCtx.executionPlan.executedPlan // Force the lazy generation of execution plan
    }

    val stream = withNewSources.collect {
      case relation: StreamingDataSourceV2ScanRelation =>
        relation.stream.asInstanceOf[ContinuousStream]
    }.head

    sparkSessionForQuery.sparkContext.setLocalProperty(
      StreamExecution.IS_CONTINUOUS_PROCESSING, true.toString)
    sparkSessionForQuery.sparkContext.setLocalProperty(
      ContinuousExecution.START_EPOCH_KEY, execCtx.batchId.toString)
    // Add another random ID on top of the run ID, to distinguish epoch coordinators across
    // reconfigurations.
    val epochCoordinatorId = s"$runId--${UUID.randomUUID}"
    currentEpochCoordinatorId = epochCoordinatorId
    sparkSessionForQuery.sparkContext.setLocalProperty(
      ContinuousExecution.EPOCH_COORDINATOR_ID_KEY, epochCoordinatorId)

    // Use the parent Spark session for the endpoint since it's where this query ID is registered.
    val epochEndpoint = EpochCoordinatorRef.create(
      logicalPlan.write,
      stream,
      this,
      epochCoordinatorId,
      execCtx.batchId,
      sparkSession,
      SparkEnv.get)
    val epochUpdateThread = new Thread(new Runnable {
      override def run: Unit = {
        try {
          triggerExecutor.execute((_) => {
            execCtx.startTrigger()

            if (stream.needsReconfiguration && state.compareAndSet(ACTIVE, RECONFIGURING)) {
              if (queryExecutionThread.isAlive) {
                queryExecutionThread.interrupt()
              }
              false
            } else if (isActive) {
              execCtx.batchId = epochEndpoint.askSync[Long](IncrementAndGetEpoch)
              logInfo(log"New epoch ${MDC(BATCH_ID, execCtx.batchId)} is starting.")
              true
            } else {
              false
            }
          })
        } catch {
          case _: InterruptedException =>
            // Cleanly stop the query.
        }
      }
    }, s"epoch update thread for $prettyIdString")

    try {
      epochUpdateThread.setDaemon(true)
      epochUpdateThread.start()

      execCtx.updateStatusMessage("Running")
      execCtx.reportTimeTaken("runContinuous") {
        SQLExecution.withNewExecutionId(lastExecution) {
          lastExecution.executedPlan.execute()
        }
      }

      val f = failure.get()
      if (f != null) {
        throw f
      }
    } catch {
      case t: Throwable if StreamExecution.isInterruptionException(t, sparkSession.sparkContext) &&
          state.get() == RECONFIGURING =>
        logInfo(log"Query ${MDC(QUERY_ID, id)} ignoring exception from reconfiguring: " +
          log"${MDC(ERROR, t)}")
        // interrupted by reconfiguration - swallow exception so we can restart the query
    } finally {
      // The above execution may finish before getting interrupted, for example, a Spark job having
      // 0 partitions will complete immediately. Then the interrupted status will sneak here.
      //
      // To handle this case, we do the two things here:
      //
      // 1. Clean up the resources in `queryExecutionThread.runUninterruptibly`. This may increase
      //    the waiting time of `stop` but should be minor because the operations here are very fast
      //    (just sending an RPC message in the same process and stopping a very simple thread).
      // 2. Clear the interrupted status at the end so that it won't impact the `runContinuous`
      //    call. We may clear the interrupted status set by `stop`, but it doesn't affect the query
      //    termination because `runActivatedStream` will check `state` and exit accordingly.
      queryExecutionThread.runUninterruptibly {
        try {
          epochEndpoint.askSync[Unit](StopContinuousExecutionWrites)
        } finally {
          SparkEnv.get.rpcEnv.stop(epochEndpoint)
          epochUpdateThread.interrupt()
          epochUpdateThread.join()
          // The following line must be the last line because it may fail if SparkContext is stopped
          sparkSession.sparkContext.cancelJobGroup(runId.toString,
            s"Continuous execution finished for query $prettyIdString")
        }
      }
      Thread.interrupted()
    }
  }

  /**
   * Report ending partition offsets for the given reader at the given epoch.
   */
  def addOffset(
      epoch: Long,
      stream: ContinuousStream,
      partitionOffsets: Seq[PartitionOffset]): Unit = {
    assert(sources.length == 1, "only one continuous source supported currently")

    val globalOffset = stream.mergeOffsets(partitionOffsets.toArray)
    val oldOffset = synchronized {
      offsetLog.add(epoch, OffsetSeq.fill(globalOffset))
      offsetLog.get(epoch - 1)
    }

    // If offset hasn't changed since last epoch, there's been no new data.
    if (oldOffset.contains(OffsetSeq.fill(globalOffset))) {
      noNewData = true
    }

    awaitProgressLock.lock()
    try {
      awaitProgressLockCondition.signalAll()
    } finally {
      awaitProgressLock.unlock()
    }
  }

  /**
   * Mark the specified epoch as committed. All readers must have reported end offsets for the epoch
   * before this is called.
   */
  def commit(epoch: Long): Unit = {
    val execCtx = latestExecutionContext.asInstanceOf[ContinuousExecutionContext]
    execCtx.updateStatusMessage(s"Committing epoch $epoch")

    assert(sources.length == 1, "only one continuous source supported currently")
    assert(offsetLog.get(epoch).isDefined, s"offset for epoch $epoch not reported before commit")

    synchronized {
      // Record offsets before updating `committedOffsets`
      execCtx.recordTriggerOffsets(
        from = execCtx.startOffsets,
        to = execCtx.endOffsets,
        latest = execCtx.latestOffsets)
      if (queryExecutionThread.isAlive) {
        commitLog.add(epoch, CommitMetadata())
        val offset =
          sources(0).deserializeOffset(offsetLog.get(epoch).get.offsets(0).get.json)
        committedOffsets ++= Seq(sources(0) -> offset)
        sources(0).commit(offset)
      } else {
        return
      }
    }

    // Since currentBatchId increases independently in cp mode, the current committed epoch may
    // be far behind currentBatchId. It is not safe to discard the metadata with thresholdBatchId
    // computed based on currentBatchId. As minLogEntriesToMaintain is used to keep the minimum
    // number of batches that must be retained and made recoverable, so we should keep the
    // specified number of metadata that have been committed.
    if (minLogEntriesToMaintain <= epoch) {
      purge(epoch + 1 - minLogEntriesToMaintain)
    }

    awaitProgressLock.lock()
    try {
      awaitProgressLockCondition.signalAll()
    } finally {
      awaitProgressLock.unlock()
    }
  }

  /**
   * Blocks the current thread until execution has committed at or after the specified epoch.
   */
  private[sql] def awaitEpoch(epoch: Long): Unit = {
    def notDone = {
      val latestCommit = commitLog.getLatest()
      latestCommit match {
        case Some((latestEpoch, _)) =>
          latestEpoch < epoch
        case None => true
      }
    }

    while (notDone) {
      awaitProgressLock.lock()
      try {
        awaitProgressLockCondition.await(100, TimeUnit.MILLISECONDS)
        if (streamDeathCause != null) {
          throw streamDeathCause
        }
      } finally {
        awaitProgressLock.unlock()
      }
    }
  }

  /**
   * Stores error and stops the query execution thread to terminate the query in new thread.
   */
  def stopInNewThread(error: Throwable): Unit = {
    if (failure.compareAndSet(null, error)) {
      logError(log"Query ${MDC(PRETTY_ID_STRING, prettyIdString)} received exception " +
        log"${MDC(ERROR, error)}")
      stopInNewThread()
    }
  }

  /**
   * Stops the query execution thread to terminate the query in new thread.
   */
  private def stopInNewThread(): Unit = {
    new Thread("stop-continuous-execution") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          ContinuousExecution.this.stop()
        } catch {
          case e: Throwable =>
            logError(e.getMessage, e)
            throw e
        }
      }
    }.start()
  }

  /**
   * Stops the query execution thread to terminate the query.
   */
  override def stop(): Unit = {
    // Set the state to TERMINATED so that the batching thread knows that it was interrupted
    // intentionally
    state.set(TERMINATED)
    if (queryExecutionThread.isAlive) {
      // The query execution thread will clean itself up in the finally clause of runContinuous.
      // We just need to interrupt the long running job.
      interruptAndAwaitExecutionThreadTermination()
    }
    logInfo(log"Query ${MDC(PRETTY_ID_STRING, prettyIdString)} was stopped")
  }
}

object ContinuousExecution {
  val START_EPOCH_KEY = "__continuous_start_epoch"
  val EPOCH_COORDINATOR_ID_KEY = "__epoch_coordinator_id"
}
