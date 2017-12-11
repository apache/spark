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
    var nextSourceId = 0L
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
        runFromOffsets(sparkSessionForStream)
      } catch {
        case _: Throwable if state.get().equals(RECONFIGURING) =>
          // swallow exception and run again
          state.set(ACTIVE)
      }
    } while (true)
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
  private def getStartOffsets(sparkSessionToRunBatches: SparkSession): OffsetSeq = {
    batchCommitLog.getLatest() match {
      case Some((latestEpochId, _)) =>
        currentBatchId = latestEpochId + 1
        val nextOffsets = offsetLog.get(currentBatchId).getOrElse {
          throw new IllegalStateException(
            s"Batch $latestEpochId was committed without next epoch offsets!")
        }
        committedOffsets = nextOffsets.toStreamProgress(sources)

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
   * Processes any data available between `availableOffsets` and `committedOffsets`.
   * @param sparkSessionToRunBatch Isolated [[SparkSession]] to run this batch with.
   */
  private def runFromOffsets(sparkSessionToRunBatch: SparkSession): Unit = {
    import scala.collection.JavaConverters._
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

    val offsets = getStartOffsets(sparkSessionToRunBatch)

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
        
        reader.setOffset(java.util.Optional.ofNullable(offsets.offsets(0).orNull))
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
        sparkSessionToRunBatch,
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
        writer.get(), reader, currentBatchId,
        id.toString, runId.toString, sparkSession, SparkEnv.get)
    val epochUpdateThread = new Thread(new Runnable {
      override def run: Unit = {
        try {
          triggerExecutor.execute(() => {
            startTrigger()

            if (reader.needsReconfiguration()) {
              stopSources()
              state.set(RECONFIGURING)
              if (queryExecutionThread.isAlive) {
                sparkSession.sparkContext.cancelJobGroup(runId.toString)
                queryExecutionThread.interrupt()
                // No need to join - this thread is about to end anyway.
              }
              false
            } else if (isActive) {
              currentBatchId = epochEndpoint.askSync[Long](IncrementAndGetEpoch())
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
    })

    try {
      epochUpdateThread.setDaemon(true)
      epochUpdateThread.start()

      reportTimeTaken("runContinuous") {
        SQLExecution.withNewExecutionId(
          sparkSessionToRunBatch, lastExecution)(lastExecution.toRdd)
      }
    } finally {
      SparkEnv.get.rpcEnv.stop(epochEndpoint)

      epochUpdateThread.interrupt()
      epochUpdateThread.join()
    }
  }

  def addOffset(
      epoch: Long, reader: ContinuousReader, partitionOffsets: Seq[PartitionOffset]): Unit = {
    assert(continuousSources.length == 1, "only one continuous source supported currently")

    if (partitionOffsets.contains(null)) {
      // If any offset is null, that means the corresponding partition hasn't seen any data yet, so
      // there's nothing meaningful to add to the offset log.
    }
    val globalOffset = reader.mergeOffsets(partitionOffsets.toArray)
    synchronized {
      offsetLog.add(epoch, OffsetSeq.fill(globalOffset))
    }
  }

  def commit(epoch: Long): Unit = {
    assert(continuousSources.length == 1, "only one continuous source supported currently")
    synchronized {
      batchCommitLog.add(epoch)
      val offset = offsetLog.get(epoch + 1).get.offsets(0).get
      committedOffsets ++= Seq(continuousSources(0) -> offset)
    }

    awaitProgressLock.lock()
    try {
      awaitProgressLockCondition.signalAll()
    } finally {
      awaitProgressLock.unlock()
    }
  }

  /**
   * Blocks the current thread until execution has committed past the specified epoch.
   */
  private[sql] def awaitEpoch(epoch: Long): Unit = {
    def notDone = {
      val latestCommit = batchCommitLog.getLatest()
      latestCommit match {
        case Some((latestEpoch, _)) => latestEpoch < epoch
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
