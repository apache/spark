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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Map => MutableMap}

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, CurrentBatchTimestamp, CurrentDate, CurrentTimestamp}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, WriteToDataSourceV2}
import org.apache.spark.sql.execution.streaming.{ContinuousExecutionRelation, StreamingRelationV2, _}
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, ContinuousWriteSupport, DataSourceV2Options}
import org.apache.spark.sql.sources.v2.reader.{ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.sources.v2.writer.ContinuousWriter
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{Clock, Utils}

class ContinuousExecution(
    sparkSession: SparkSession,
    name: String,
    checkpointRoot: String,
    analyzedPlan: LogicalPlan,
    sink: ContinuousWriteSupport,
    trigger: Trigger,
    triggerClock: Clock,
    outputMode: OutputMode,
    extraOptions: Map[String, String],
    deleteCheckpointOnStop: Boolean)
  extends StreamExecution(
    sparkSession, name, checkpointRoot, analyzedPlan, sink,
    trigger, triggerClock, outputMode, deleteCheckpointOnStop) {

  @volatile protected var continuousSources: Seq[ContinuousReader] = Seq.empty
  override protected def sources: Seq[BaseStreamingSource] = continuousSources

  override lazy val logicalPlan: LogicalPlan = {
    assert(queryExecutionThread eq Thread.currentThread,
      "logicalPlan must be initialized in StreamExecutionThread " +
        s"but the current thread was ${Thread.currentThread}")
    val toExecutionRelationMap = MutableMap[StreamingRelationV2, ContinuousExecutionRelation]()
    analyzedPlan.transform {
      case r @ StreamingRelationV2(
          source: ContinuousReadSupport, _, extraReaderOptions, output, _) =>
        toExecutionRelationMap.getOrElseUpdate(r, {
          ContinuousExecutionRelation(source, extraReaderOptions, output)(sparkSession)
        })
      case StreamingRelationV2(_, sourceName, _, _, _) =>
        throw new AnalysisException(
          s"Data source $sourceName does not support continuous processing.")
    }
  }

  private val triggerExecutor = trigger match {
    case ContinuousTrigger(t) => ProcessingTimeExecutor(ProcessingTime(t), triggerClock)
    case _ => throw new IllegalStateException(s"Unsupported type of trigger: $trigger")
  }

  override protected def runActivatedStream(sparkSessionForStream: SparkSession): Unit = {
    do {
      try {
        runContinuous(sparkSessionForStream)
      } catch {
        case _: InterruptedException if state.get().equals(RECONFIGURING) =>
          // swallow exception and run again
          state.set(ACTIVE)
      }
    } while (state.get() == ACTIVE)
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

        // Forcibly align commit and offset logs by slicing off any spurious offset logs from
        // a previous run. We can't allow commits to an epoch that a previous run reached but
        // this run has not.
        offsetLog.purgeAfter(latestEpochId)

        currentBatchId = latestEpochId + 1
        logDebug(s"Resuming at epoch $currentBatchId with committed offsets $committedOffsets")
        nextOffsets
      case None =>
        // We are starting this stream for the first time. Offsets are all None.
        logInfo(s"Starting new streaming query.")
        currentBatchId = 0
        OffsetSeq.fill(continuousSources.map(_ => null): _*)
    }
  }

  /**
   * Do a continuous run.
   * @param sparkSessionForQuery Isolated [[SparkSession]] to run the continuous query with.
   */
  private def runContinuous(sparkSessionForQuery: SparkSession): Unit = {
    // A list of attributes that will need to be updated.
    val replacements = new ArrayBuffer[(Attribute, Attribute)]
    // Translate from continuous relation to the underlying data source.
    var nextSourceId = 0
    continuousSources = logicalPlan.collect {
      case ContinuousExecutionRelation(dataSource, extraReaderOptions, output) =>
        val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
        nextSourceId += 1

        dataSource.createContinuousReader(
          java.util.Optional.empty[StructType](),
          metadataPath,
          new DataSourceV2Options(extraReaderOptions.asJava))
    }
    uniqueSources = continuousSources.distinct

    val offsets = getStartOffsets(sparkSessionForQuery)

    var insertedSourceId = 0
    val withNewSources = logicalPlan transform {
      case ContinuousExecutionRelation(_, _, output) =>
        val reader = continuousSources(insertedSourceId)
        insertedSourceId += 1
        val newOutput = reader.readSchema().toAttributes

        assert(output.size == newOutput.size,
          s"Invalid reader: ${Utils.truncatedString(output, ",")} != " +
            s"${Utils.truncatedString(newOutput, ",")}")
        replacements ++= output.zip(newOutput)

        val loggedOffset = offsets.offsets(0)
        val realOffset = loggedOffset.map(off => reader.deserializeOffset(off.json))
        reader.setOffset(java.util.Optional.ofNullable(realOffset.orNull))
        DataSourceV2Relation(newOutput, reader)
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    val replacementMap = AttributeMap(replacements)
    val triggerLogicalPlan = withNewSources transformAllExpressions {
      case a: Attribute if replacementMap.contains(a) =>
        replacementMap(a).withMetadata(a.metadata)
      case (_: CurrentTimestamp | _: CurrentDate) =>
        throw new IllegalStateException(
          "CurrentTimestamp and CurrentDate not yet supported for continuous processing")
    }

    val writer = sink.createContinuousWriter(
      s"$runId",
      triggerLogicalPlan.schema,
      outputMode,
      new DataSourceV2Options(extraOptions.asJava))
    val withSink = WriteToDataSourceV2(writer.get(), triggerLogicalPlan)

    val reader = withSink.collect {
      case DataSourceV2Relation(_, r: ContinuousReader) => r
    }.head

    reportTimeTaken("queryPlanning") {
      lastExecution = new IncrementalExecution(
        sparkSessionForQuery,
        withSink,
        outputMode,
        checkpointFile("state"),
        runId,
        currentBatchId,
        offsetSeqMetadata)
      lastExecution.executedPlan // Force the lazy generation of execution plan
    }

    sparkSession.sparkContext.setLocalProperty(
      ContinuousExecution.START_EPOCH_KEY, currentBatchId.toString)
    sparkSession.sparkContext.setLocalProperty(
      ContinuousExecution.RUN_ID_KEY, runId.toString)

    // Use the parent Spark session for the endpoint since it's where this query ID is registered.
    val epochEndpoint =
      EpochCoordinatorRef.create(
        writer.get(), reader, this, currentBatchId, sparkSession, SparkEnv.get)
    val epochUpdateThread = new Thread(new Runnable {
      override def run: Unit = {
        try {
          triggerExecutor.execute(() => {
            startTrigger()

            if (reader.needsReconfiguration()) {
              state.set(RECONFIGURING)
              stopSources()
              if (queryExecutionThread.isAlive) {
                sparkSession.sparkContext.cancelJobGroup(runId.toString)
                queryExecutionThread.interrupt()
                // No need to join - this thread is about to end anyway.
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
    } finally {
      SparkEnv.get.rpcEnv.stop(epochEndpoint)

      epochUpdateThread.interrupt()
      epochUpdateThread.join()
    }
  }

  /**
   * Report ending partition offsets for the given reader at the given epoch.
   */
  def addOffset(
      epoch: Long, reader: ContinuousReader, partitionOffsets: Seq[PartitionOffset]): Unit = {
    assert(continuousSources.length == 1, "only one continuous source supported currently")

    if (partitionOffsets.contains(null)) {
      // If any offset is null, that means the corresponding partition hasn't seen any data yet, so
      // there's nothing meaningful to add to the offset log.
    }
    val globalOffset = reader.mergeOffsets(partitionOffsets.toArray)
    synchronized {
      if (queryExecutionThread.isAlive) {
        offsetLog.add(epoch, OffsetSeq.fill(globalOffset))
      } else {
        return
      }
    }
  }

  /**
   * Mark the specified epoch as committed. All readers must have reported end offsets for the epoch
   * before this is called.
   */
  def commit(epoch: Long): Unit = {
    assert(continuousSources.length == 1, "only one continuous source supported currently")
    assert(offsetLog.get(epoch).isDefined, s"offset for epoch $epoch not reported before commit")
    synchronized {
      if (queryExecutionThread.isAlive) {
        commitLog.add(epoch)
        val offset = offsetLog.get(epoch).get.offsets(0).get
        committedOffsets ++= Seq(continuousSources(0) -> offset)
      } else {
        return
      }
    }

    if (minLogEntriesToMaintain < currentBatchId) {
      offsetLog.purge(currentBatchId - minLogEntriesToMaintain)
      commitLog.purge(currentBatchId - minLogEntriesToMaintain)
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
}

object ContinuousExecution {
  val START_EPOCH_KEY = "__continuous_start_epoch"
  val RUN_ID_KEY = "__run_id"
}
