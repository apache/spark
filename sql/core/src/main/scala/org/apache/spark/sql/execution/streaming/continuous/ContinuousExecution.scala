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

import java.util.IdentityHashMap
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.function.UnaryOperator

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{CurrentDate, CurrentTimestamp}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.v2.{FakeContinuousExec, StreamingDataSourceV2Relation}
import org.apache.spark.sql.execution.streaming.{StreamingRelationV2, _}
import org.apache.spark.sql.sources.v2
import org.apache.spark.sql.sources.v2.{DataSourceOptions, StreamingWriteSupportProvider, SupportsContinuousRead}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousInputStream, InputStream, PartitionOffset}
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, Trigger}
import org.apache.spark.util.Clock

class ContinuousExecution(
    sparkSession: SparkSession,
    name: String,
    checkpointRoot: String,
    analyzedPlan: LogicalPlan,
    sink: StreamingWriteSupportProvider,
    trigger: Trigger,
    triggerClock: Clock,
    outputMode: OutputMode,
    extraOptions: Map[String, String],
    deleteCheckpointOnStop: Boolean)
  extends StreamExecution(
    sparkSession, name, checkpointRoot, analyzedPlan, sink,
    trigger, triggerClock, outputMode, deleteCheckpointOnStop) {

  @volatile protected var sources: Seq[InputStream] = Seq.empty

  // For use only in test harnesses.
  private[sql] var currentEpochCoordinatorId: String = _

  override val logicalPlan: LogicalPlan = {
    val v2ToContinuousExecutionMap = MutableMap[StreamingRelationV2, ContinuousExecutionRelation]()
    var nextSourceId = 0
    val _logicalPlan = analyzedPlan.transform {
      case s @ StreamingRelationV2(
          sourceName, ds, table: SupportsContinuousRead, options, output, _) =>
        v2ToContinuousExecutionMap.getOrElseUpdate(s, {
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          nextSourceId += 1
          ContinuousExecutionRelation(
            sourceName, ds, table, output, metadataPath, options)(sparkSession)
        })
      case r: StreamingRelationV2 =>
        throw new UnsupportedOperationException(
          s"Data source ${r.sourceName} does not support continuous processing.")
    }

    // This is a temporary query planning, to get operator pushdown result of v2 sources.
    // TODO: update the streaming engine to do query planning only once.
    val relationToStream = new IdentityHashMap[ContinuousExecutionRelation, ContinuousInputStream]
    createExecution(_logicalPlan, sparkSession).sparkPlan.foreach {
      case exec: FakeContinuousExec =>
        if (relationToStream.containsKey(exec.relation)) {
          // This is a self-union/self-join, don't apply operator pushdown, since we want to keep
          // one stream instance for the self-unioned/self-joined source.
          // TODO: we can push down shared operators to the self-unioned/self-joined sources.
          val options = new DataSourceOptions(exec.relation.options.asJava)
          val configBuilder = exec.relation.table.newScanConfigBuilder(options)
          val config = configBuilder.build()
          val stream = exec.relation.table.createContinuousInputStream(
            exec.relation.metadataPath, config, options)
          relationToStream.put(exec.relation, stream)
        } else {
          relationToStream.put(exec.relation, exec.stream)
        }

      case _ =>
    }

    val finalPlan = _logicalPlan.transform {
      case r: ContinuousExecutionRelation =>
        val stream = relationToStream.get(r)
        assert(stream != null)
        StreamingDataSourceV2Relation(r.output, r.ds, r.options, stream)
    }

    sources = finalPlan.collect {
      case r: StreamingDataSourceV2Relation => r.stream
    }
    uniqueSources = sources.distinct

    finalPlan
  }

  private def createExecution(plan: LogicalPlan, session: SparkSession): IncrementalExecution = {
    new IncrementalExecution(
      session,
      plan,
      outputMode,
      checkpointFile("state"),
      runId,
      currentBatchId,
      offsetSeqMetadata)
  }

  private val triggerExecutor = trigger match {
    case ContinuousTrigger(t) => ProcessingTimeExecutor(ProcessingTime(t), triggerClock)
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
  private def getStartOffsets(sparkSessionToRunBatches: SparkSession): OffsetSeq = {
    // Note that this will need a slight modification for exactly once. If ending offsets were
    // reported but not committed for any epochs, we must replay exactly to those offsets.
    // For at least once, we can just ignore those reports and risk duplicates.
    commitLog.getLatest() match {
      case Some((latestEpochId, _)) =>
        val nextOffsets = offsetLog.get(latestEpochId).getOrElse {
          throw new IllegalStateException(
            s"Batch $latestEpochId was committed without end epoch offsets!")
        }
        committedOffsets = nextOffsets.toStreamProgress(sources)
        currentBatchId = latestEpochId + 1

        logDebug(s"Resuming at epoch $currentBatchId with committed offsets $committedOffsets")
        nextOffsets
      case None =>
        // We are starting this stream for the first time. Offsets are all None.
        logInfo(s"Starting new streaming query.")
        currentBatchId = 0
        OffsetSeq.fill(sources.map(_ => null): _*)
    }
  }

  /**
   * Do a continuous run.
   * @param sparkSessionForQuery Isolated [[SparkSession]] to run the continuous query with.
   */
  private def runContinuous(sparkSessionForQuery: SparkSession): Unit = {
    val offsets = getStartOffsets(sparkSessionForQuery)

    val withNewSources: LogicalPlan = logicalPlan transform {
      case relation: StreamingDataSourceV2Relation =>
        val loggedOffset = offsets.offsets(0)
        val realOffset = loggedOffset.map(off => relation.stream.deserializeOffset(off.json))
        val startOffset = realOffset.getOrElse(relation.stream.initialOffset)
        relation.copy(startOffset = Some(startOffset))
    }

    withNewSources transformAllExpressions {
      case (_: CurrentTimestamp | _: CurrentDate) =>
        throw new IllegalStateException(
          "CurrentTimestamp and CurrentDate not yet supported for continuous processing")
    }

    val writer = sink.createStreamingWriteSupport(
      s"$runId",
      withNewSources.schema,
      outputMode,
      new DataSourceOptions(extraOptions.asJava))
    val planWithSink = WriteToContinuousDataSource(writer, withNewSources)

    reportTimeTaken("queryPlanning") {
      lastExecution = createExecution(planWithSink, sparkSessionForQuery)
      lastExecution.executedPlan // Force the lazy generation of execution plan
    }

    val stream = planWithSink.collect {
      case relation: StreamingDataSourceV2Relation =>
        relation.stream.asInstanceOf[ContinuousInputStream]
    }.head

    sparkSessionForQuery.sparkContext.setLocalProperty(
      StreamExecution.IS_CONTINUOUS_PROCESSING, true.toString)
    sparkSessionForQuery.sparkContext.setLocalProperty(
      ContinuousExecution.START_EPOCH_KEY, currentBatchId.toString)
    // Add another random ID on top of the run ID, to distinguish epoch coordinators across
    // reconfigurations.
    val epochCoordinatorId = s"$runId--${UUID.randomUUID}"
    currentEpochCoordinatorId = epochCoordinatorId
    sparkSessionForQuery.sparkContext.setLocalProperty(
      ContinuousExecution.EPOCH_COORDINATOR_ID_KEY, epochCoordinatorId)
    sparkSessionForQuery.sparkContext.setLocalProperty(
      ContinuousExecution.EPOCH_INTERVAL_KEY,
      trigger.asInstanceOf[ContinuousTrigger].intervalMs.toString)

    // Use the parent Spark session for the endpoint since it's where this query ID is registered.
    val epochEndpoint =
      EpochCoordinatorRef.create(
        writer, stream, this, epochCoordinatorId, currentBatchId, sparkSession, SparkEnv.get)
    val epochUpdateThread = new Thread(new Runnable {
      override def run: Unit = {
        try {
          triggerExecutor.execute(() => {
            startTrigger()

            if (stream.needsReconfiguration && state.compareAndSet(ACTIVE, RECONFIGURING)) {
              if (queryExecutionThread.isAlive) {
                queryExecutionThread.interrupt()
              }
              false
            } else if (isActive) {
              currentBatchId = epochEndpoint.askSync[Long](IncrementAndGetEpoch)
              logInfo(s"New epoch $currentBatchId is starting.")
              true
            } else {
              false
            }
          })
        } catch {
          case _: InterruptedException =>
            // Cleanly stop the query.
            return
        }
      }
    }, s"epoch update thread for $prettyIdString")

    try {
      epochUpdateThread.setDaemon(true)
      epochUpdateThread.start()

      reportTimeTaken("runContinuous") {
        SQLExecution.withNewExecutionId(
          sparkSessionForQuery, lastExecution)(lastExecution.toRdd)
      }
    } catch {
      case t: Throwable if StreamExecution.isInterruptionException(t, sparkSession.sparkContext) &&
          state.get() == RECONFIGURING =>
        logInfo(s"Query $id ignoring exception from reconfiguring: $t")
        // interrupted by reconfiguration - swallow exception so we can restart the query
    } finally {
      epochEndpoint.askSync[Unit](StopContinuousExecutionWrites)
      SparkEnv.get.rpcEnv.stop(epochEndpoint)

      epochUpdateThread.interrupt()
      epochUpdateThread.join()

      sparkSession.sparkContext.cancelJobGroup(runId.toString)
    }
  }

  /**
   * Report ending partition offsets for the given reader at the given epoch.
   */
  def addOffset(
      epoch: Long,
      stream: ContinuousInputStream,
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
    assert(sources.length == 1, "only one continuous source supported currently")
    assert(offsetLog.get(epoch).isDefined, s"offset for epoch $epoch not reported before commit")

    synchronized {
      // Record offsets before updating `committedOffsets`
      recordTriggerOffsets(from = committedOffsets, to = availableOffsets)
      if (queryExecutionThread.isAlive) {
        commitLog.add(epoch, CommitMetadata())
        val offset =
          sources(0).deserializeOffset(offsetLog.get(epoch).get.offsets(0).get.json)
        committedOffsets ++= Seq(sources(0) -> offset)
        sources(0).commit(offset.asInstanceOf[v2.reader.streaming.Offset])
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
      offsetLog.purge(epoch + 1 - minLogEntriesToMaintain)
      commitLog.purge(epoch + 1 - minLogEntriesToMaintain)
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
   * Stops the query execution thread to terminate the query.
   */
  override def stop(): Unit = {
    // Set the state to TERMINATED so that the batching thread knows that it was interrupted
    // intentionally
    state.set(TERMINATED)
    if (queryExecutionThread.isAlive) {
      // The query execution thread will clean itself up in the finally clause of runContinuous.
      // We just need to interrupt the long running job.
      queryExecutionThread.interrupt()
      queryExecutionThread.join()
    }
    logInfo(s"Query $prettyIdString was stopped")
  }
}

object ContinuousExecution {
  val START_EPOCH_KEY = "__continuous_start_epoch"
  val EPOCH_COORDINATOR_ID_KEY = "__epoch_coordinator_id"
  val EPOCH_INTERVAL_KEY = "__continuous_epoch_interval"
}
