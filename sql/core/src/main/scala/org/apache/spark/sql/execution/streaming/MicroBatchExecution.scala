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

import scala.collection.mutable.{Map => MutableMap}

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CurrentBatchTimestamp, CurrentDate, CurrentTimestamp, LocalTimestamp}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.streaming.{StreamingRelationV2, WriteToStream}
import org.apache.spark.sql.catalyst.trees.TreePattern.CURRENT_LIKE
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset => OffsetV2, ReadLimit, SparkDataStream, SupportsAdmissionControl, SupportsTriggerAvailableNow}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, StreamingDataSourceV2Relation, StreamWriterCommitProgress, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.util.{Clock, Utils}

class MicroBatchExecution(
    sparkSession: SparkSession,
    trigger: Trigger,
    triggerClock: Clock,
    extraOptions: Map[String, String],
    plan: WriteToStream)
  extends StreamExecution(
    sparkSession, plan.name, plan.resolvedCheckpointLocation, plan.inputQuery, plan.sink, trigger,
    triggerClock, plan.outputMode, plan.deleteCheckpointOnStop) {

  @volatile protected var sources: Seq[SparkDataStream] = Seq.empty

  private val triggerExecutor = trigger match {
    case t: ProcessingTimeTrigger => ProcessingTimeExecutor(t, triggerClock)
    case OneTimeTrigger => SingleBatchExecutor()
    case AvailableNowTrigger => MultiBatchExecutor()
    case _ => throw new IllegalStateException(s"Unknown type of trigger: $trigger")
  }

  private var watermarkTracker: WatermarkTracker = _

  override lazy val logicalPlan: LogicalPlan = {
    assert(queryExecutionThread eq Thread.currentThread,
      "logicalPlan must be initialized in QueryExecutionThread " +
        s"but the current thread was ${Thread.currentThread}")
    var nextSourceId = 0L
    val toExecutionRelationMap = MutableMap[StreamingRelation, StreamingExecutionRelation]()
    val v2ToExecutionRelationMap = MutableMap[StreamingRelationV2, StreamingExecutionRelation]()
    val v2ToRelationMap = MutableMap[StreamingRelationV2, StreamingDataSourceV2Relation]()
    // We transform each distinct streaming relation into a StreamingExecutionRelation, keeping a
    // map as we go to ensure each identical relation gets the same StreamingExecutionRelation
    // object. For each microbatch, the StreamingExecutionRelation will be replaced with a logical
    // plan for the data within that batch.
    // Note that we have to use the previous `output` as attributes in StreamingExecutionRelation,
    // since the existing logical plan has already used those attributes. The per-microbatch
    // transformation is responsible for replacing attributes with their final values.

    val disabledSources =
      Utils.stringToSeq(sparkSession.sqlContext.conf.disabledV2StreamingMicroBatchReaders)

    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
    val _logicalPlan = analyzedPlan.transform {
      case streamingRelation @ StreamingRelation(dataSourceV1, sourceName, output) =>
        toExecutionRelationMap.getOrElseUpdate(streamingRelation, {
          // Materialize source to avoid creating it in every batch
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          val source = dataSourceV1.createSource(metadataPath)
          nextSourceId += 1
          logInfo(s"Using Source [$source] from DataSourceV1 named '$sourceName' [$dataSourceV1]")
          StreamingExecutionRelation(source, output)(sparkSession)
        })

      case s @ StreamingRelationV2(src, srcName, table: SupportsRead, options, output, _, _, v1) =>
        val dsStr = if (src.nonEmpty) s"[${src.get}]" else ""
        val v2Disabled = disabledSources.contains(src.getOrElse(None).getClass.getCanonicalName)
        if (!v2Disabled && table.supports(TableCapability.MICRO_BATCH_READ)) {
          v2ToRelationMap.getOrElseUpdate(s, {
            // Materialize source to avoid creating it in every batch
            val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
            nextSourceId += 1
            logInfo(s"Reading table [$table] from DataSourceV2 named '$srcName' $dsStr")
            // TODO: operator pushdown.
            val scan = table.newScanBuilder(options).build()
            val stream = scan.toMicroBatchStream(metadataPath)
            StreamingDataSourceV2Relation(output, scan, stream)
          })
        } else if (v1.isEmpty) {
          throw QueryExecutionErrors.microBatchUnsupportedByDataSourceError(srcName)
        } else {
          v2ToExecutionRelationMap.getOrElseUpdate(s, {
            // Materialize source to avoid creating it in every batch
            val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
            val source =
              v1.get.asInstanceOf[StreamingRelation].dataSource.createSource(metadataPath)
            nextSourceId += 1
            logInfo(s"Using Source [$source] from DataSourceV2 named '$srcName' $dsStr")
            StreamingExecutionRelation(source, output)(sparkSession)
          })
        }
    }
    sources = _logicalPlan.collect {
      // v1 source
      case s: StreamingExecutionRelation => s.source
      // v2 source
      case r: StreamingDataSourceV2Relation => r.stream
    }
    uniqueSources = triggerExecutor match {
      case _: SingleBatchExecutor =>
        sources.distinct.map {
          case s: SupportsAdmissionControl =>
            val limit = s.getDefaultReadLimit
            if (limit != ReadLimit.allAvailable()) {
              logWarning(
                s"The read limit $limit for $s is ignored when Trigger.Once is used.")
            }
            s -> ReadLimit.allAvailable()
          case s =>
            s -> ReadLimit.allAvailable()
        }.toMap

      case _: MultiBatchExecutor =>
        sources.distinct.map {
          case s: SupportsTriggerAvailableNow => s
          case s: Source => new AvailableNowSourceWrapper(s)
          case s: MicroBatchStream => new AvailableNowMicroBatchStreamWrapper(s)
        }.map { s =>
          s.prepareForTriggerAvailableNow()
          s -> s.getDefaultReadLimit
        }.toMap

      case _ =>
        sources.distinct.map {
          case s: SupportsAdmissionControl => s -> s.getDefaultReadLimit
          case s => s -> ReadLimit.allAvailable()
        }.toMap
    }

    // TODO (SPARK-27484): we should add the writing node before the plan is analyzed.
    sink match {
      case s: SupportsWrite =>
        val (streamingWrite, customMetrics) = createStreamingWrite(s, extraOptions, _logicalPlan)
        val relationOpt = plan.catalogAndIdent.map {
          case (catalog, ident) => DataSourceV2Relation.create(s, Some(catalog), Some(ident))
        }
        WriteToMicroBatchDataSource(relationOpt, streamingWrite, _logicalPlan, customMetrics)

      case _ => _logicalPlan
    }
  }

  /**
   * Signifies whether current batch (i.e. for the batch `currentBatchId`) has been constructed
   * (i.e. written to the offsetLog) and is ready for execution.
   */
  private var isCurrentBatchConstructed = false

  /**
   * Signals to the thread executing micro-batches that it should stop running after the next
   * batch. This method blocks until the thread stops running.
   */
  override def stop(): Unit = {
    // Set the state to TERMINATED so that the batching thread knows that it was interrupted
    // intentionally
    state.set(TERMINATED)
    if (queryExecutionThread.isAlive) {
      sparkSession.sparkContext.cancelJobGroup(runId.toString)
      interruptAndAwaitExecutionThreadTermination()
      // microBatchThread may spawn new jobs, so we need to cancel again to prevent a leak
      sparkSession.sparkContext.cancelJobGroup(runId.toString)
    }
    logInfo(s"Query $prettyIdString was stopped")
  }

  /** Begins recording statistics about query progress for a given trigger. */
  override protected def startTrigger(): Unit = {
    super.startTrigger()
    currentStatus = currentStatus.copy(isTriggerActive = true)
  }

  /**
   * Repeatedly attempts to run batches as data arrives.
   */
  protected def runActivatedStream(sparkSessionForStream: SparkSession): Unit = {

    val noDataBatchesEnabled =
      sparkSessionForStream.sessionState.conf.streamingNoDataMicroBatchesEnabled

    triggerExecutor.execute(() => {
      if (isActive) {
        var currentBatchHasNewData = false // Whether the current batch had new data

        startTrigger()

        reportTimeTaken("triggerExecution") {
          // We'll do this initialization only once every start / restart
          if (currentBatchId < 0) {
            populateStartOffsets(sparkSessionForStream)
            logInfo(s"Stream started from $committedOffsets")
          }

          // Set this before calling constructNextBatch() so any Spark jobs executed by sources
          // while getting new data have the correct description
          sparkSession.sparkContext.setJobDescription(getBatchDescriptionString)

          // Try to construct the next batch. This will return true only if the next batch is
          // ready and runnable. Note that the current batch may be runnable even without
          // new data to process as `constructNextBatch` may decide to run a batch for
          // state cleanup, etc. `isNewDataAvailable` will be updated to reflect whether new data
          // is available or not.
          if (!isCurrentBatchConstructed) {
            isCurrentBatchConstructed = constructNextBatch(noDataBatchesEnabled)
          }

          // Record the trigger offset range for progress reporting *before* processing the batch
          recordTriggerOffsets(
            from = committedOffsets,
            to = availableOffsets,
            latest = latestOffsets)

          // Remember whether the current batch has data or not. This will be required later
          // for bookkeeping after running the batch, when `isNewDataAvailable` will have changed
          // to false as the batch would have already processed the available data.
          currentBatchHasNewData = isNewDataAvailable

          currentStatus = currentStatus.copy(isDataAvailable = isNewDataAvailable)
          if (isCurrentBatchConstructed) {
            if (currentBatchHasNewData) updateStatusMessage("Processing new data")
            else updateStatusMessage("No new data but cleaning up state")
            runBatch(sparkSessionForStream)
          } else {
            updateStatusMessage("Waiting for data to arrive")
          }
        }

        // Must be outside reportTimeTaken so it is recorded
        finishTrigger(currentBatchHasNewData, isCurrentBatchConstructed)

        // Signal waiting threads. Note this must be after finishTrigger() to ensure all
        // activities (progress generation, etc.) have completed before signaling.
        withProgressLocked { awaitProgressLockCondition.signalAll() }

        // If the current batch has been executed, then increment the batch id and reset flag.
        // Otherwise, there was no data to execute the batch and sleep for some time
        if (isCurrentBatchConstructed) {
          currentBatchId += 1
          isCurrentBatchConstructed = false
        } else if (triggerExecutor.isInstanceOf[MultiBatchExecutor]) {
          logInfo("Finished processing all available data for the trigger, terminating this " +
            "Trigger.AvailableNow query")
          state.set(TERMINATED)
        } else Thread.sleep(pollingDelayMs)
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
    sinkCommitProgress = None
    offsetLog.getLatest() match {
      case Some((latestBatchId, nextOffsets)) =>
        /* First assume that we are re-executing the latest known batch
         * in the offset log */
        currentBatchId = latestBatchId
        isCurrentBatchConstructed = true
        availableOffsets = nextOffsets.toStreamProgress(sources)
        /* Initialize committed offsets to a committed batch, which at this
         * is the second latest batch id in the offset log. */
        if (latestBatchId != 0) {
          val secondLatestOffsets = offsetLog.get(latestBatchId - 1).getOrElse {
            throw new IllegalStateException(s"batch ${latestBatchId - 1} doesn't exist")
          }
          committedOffsets = secondLatestOffsets.toStreamProgress(sources)
        }

        // update offset metadata
        nextOffsets.metadata.foreach { metadata =>
          OffsetSeqMetadata.setSessionConf(metadata, sparkSessionToRunBatches.conf)
          offsetSeqMetadata = OffsetSeqMetadata(
            metadata.batchWatermarkMs, metadata.batchTimestampMs, sparkSessionToRunBatches.conf)
          watermarkTracker = WatermarkTracker(sparkSessionToRunBatches.conf)
          watermarkTracker.setWatermark(metadata.batchWatermarkMs)
        }

        /* identify the current batch id: if commit log indicates we successfully processed the
         * latest batch id in the offset log, then we can safely move to the next batch
         * i.e., committedBatchId + 1 */
        commitLog.getLatest() match {
          case Some((latestCommittedBatchId, commitMetadata)) =>
            if (latestBatchId == latestCommittedBatchId) {
              /* The last batch was successfully committed, so we can safely process a
               * new next batch but first:
               * Make a call to getBatch using the offsets from previous batch.
               * because certain sources (e.g., KafkaSource) assume on restart the last
               * batch will be executed before getOffset is called again. */
              availableOffsets.foreach {
                case (source: Source, end: Offset) =>
                  val start = committedOffsets.get(source).map(_.asInstanceOf[Offset])
                  source.getBatch(start, end)
                case nonV1Tuple =>
                  // The V2 API does not have the same edge case requiring getBatch to be called
                  // here, so we do nothing here.
              }
              currentBatchId = latestCommittedBatchId + 1
              isCurrentBatchConstructed = false
              committedOffsets ++= availableOffsets
              watermarkTracker.setWatermark(
                math.max(watermarkTracker.currentWatermark, commitMetadata.nextBatchWatermarkMs))
            } else if (latestCommittedBatchId == latestBatchId - 1) {
              availableOffsets.foreach {
                case (source: Source, end: Offset) =>
                  val start = committedOffsets.get(source).map(_.asInstanceOf[Offset])
                  if (start.map(_ == end).getOrElse(true)) {
                    source.getBatch(start, end)
                  }
                case nonV1Tuple =>
                  // The V2 API does not have the same edge case requiring getBatch to be called
                  // here, so we do nothing here.
              }
            } else if (latestCommittedBatchId < latestBatchId - 1) {
              logWarning(s"Batch completion log latest batch id is " +
                s"${latestCommittedBatchId}, which is not trailing " +
                s"batchid $latestBatchId by one")
            }
          case None => logInfo("no commit log present")
        }
        logInfo(s"Resuming at batch $currentBatchId with committed offsets " +
          s"$committedOffsets and available offsets $availableOffsets")
      case None => // We are starting this stream for the first time.
        logInfo(s"Starting new streaming query.")
        currentBatchId = 0
        watermarkTracker = WatermarkTracker(sparkSessionToRunBatches.conf)
    }
  }

  /**
   * Returns true if there is any new data available to be processed.
   */
  private def isNewDataAvailable: Boolean = {
    availableOffsets.exists {
      case (source, available) =>
        committedOffsets
          .get(source)
          .map(committed => committed != available)
          .getOrElse(true)
    }
  }

  /**
   * Get the startOffset from availableOffsets. This is to be used in
   * latestOffset(startOffset, readLimit)
   */
  private def getStartOffset(dataStream: SparkDataStream): OffsetV2 = {
    val startOffsetOpt = availableOffsets.get(dataStream)
    dataStream match {
      case _: Source =>
        startOffsetOpt.orNull
      case v2: MicroBatchStream =>
        startOffsetOpt.map(offset => v2.deserializeOffset(offset.json))
          .getOrElse(v2.initialOffset())
    }
  }

  /**
   * Attempts to construct a batch according to:
   *  - Availability of new data
   *  - Need for timeouts and state cleanups in stateful operators
   *
   * Returns true only if the next batch should be executed.
   *
   * Here is the high-level logic on how this constructs the next batch.
   * - Check each source whether new data is available
   * - Updated the query's metadata and check using the last execution whether there is any need
   *   to run another batch (for state clean up, etc.)
   * - If either of the above is true, then construct the next batch by committing to the offset
   *   log that range of offsets that the next batch will process.
   */
  private def constructNextBatch(noDataBatchesEnabled: Boolean): Boolean = withProgressLocked {
    if (isCurrentBatchConstructed) return true

    // Generate a map from each unique source to the next available offset.
    val (nextOffsets, recentOffsets) = uniqueSources.toSeq.map {
      case (s: AvailableNowDataStreamWrapper, limit) =>
        updateStatusMessage(s"Getting offsets from $s")
        val originalSource = s.delegate
        reportTimeTaken("latestOffset") {
          val next = s.latestOffset(getStartOffset(originalSource), limit)
          val latest = s.reportLatestOffset()
          ((originalSource, Option(next)), (originalSource, Option(latest)))
        }
      case (s: SupportsAdmissionControl, limit) =>
        updateStatusMessage(s"Getting offsets from $s")
        reportTimeTaken("latestOffset") {
          val next = s.latestOffset(getStartOffset(s), limit)
          val latest = s.reportLatestOffset()
          ((s, Option(next)), (s, Option(latest)))
        }
      case (s: Source, _) =>
        updateStatusMessage(s"Getting offsets from $s")
        reportTimeTaken("getOffset") {
          val offset = s.getOffset
          ((s, offset), (s, offset))
        }
      case (s: MicroBatchStream, _) =>
        updateStatusMessage(s"Getting offsets from $s")
        reportTimeTaken("latestOffset") {
          val latest = s.latestOffset()
          ((s, Option(latest)), (s, Option(latest)))
        }
      case (s, _) =>
        // for some reason, the compiler is unhappy and thinks the match is not exhaustive
        throw new IllegalStateException(s"Unexpected source: $s")
    }.unzip

    availableOffsets ++= nextOffsets.filter { case (_, o) => o.nonEmpty }
      .map(p => p._1 -> p._2.get).toMap
    latestOffsets ++= recentOffsets.filter { case (_, o) => o.nonEmpty }
      .map(p => p._1 -> p._2.get).toMap

    // Update the query metadata
    offsetSeqMetadata = offsetSeqMetadata.copy(
      batchWatermarkMs = watermarkTracker.currentWatermark,
      batchTimestampMs = triggerClock.getTimeMillis())

    // Check whether next batch should be constructed
    val lastExecutionRequiresAnotherBatch = noDataBatchesEnabled &&
      Option(lastExecution).exists(_.shouldRunAnotherBatch(offsetSeqMetadata))
    val shouldConstructNextBatch = isNewDataAvailable || lastExecutionRequiresAnotherBatch
    logTrace(
      s"noDataBatchesEnabled = $noDataBatchesEnabled, " +
      s"lastExecutionRequiresAnotherBatch = $lastExecutionRequiresAnotherBatch, " +
      s"isNewDataAvailable = $isNewDataAvailable, " +
      s"shouldConstructNextBatch = $shouldConstructNextBatch")

    if (shouldConstructNextBatch) {
      // Commit the next batch offset range to the offset log
      updateStatusMessage("Writing offsets to log")
      reportTimeTaken("walCommit") {
        assert(offsetLog.add(currentBatchId,
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
              case (src: Source, off: Offset) => src.commit(off)
              case (stream: MicroBatchStream, off) =>
                stream.commit(stream.deserializeOffset(off.json))
              case (src, _) =>
                throw new IllegalArgumentException(
                  s"Unknown source is found at constructNextBatch: $src")
            }
          } else {
            throw new IllegalStateException(s"batch ${currentBatchId - 1} doesn't exist")
          }
        }

        // It is now safe to discard the metadata beyond the minimum number to retain.
        // Note that purge is exclusive, i.e. it purges everything before the target ID.
        if (minLogEntriesToMaintain < currentBatchId) {
          purge(currentBatchId - minLogEntriesToMaintain)
        }
      }
      noNewData = false
    } else {
      noNewData = true
      awaitProgressLockCondition.signalAll()
    }
    shouldConstructNextBatch
  }

  /**
   * Processes any data available between `availableOffsets` and `committedOffsets`.
   * @param sparkSessionToRunBatch Isolated [[SparkSession]] to run this batch with.
   */
  private def runBatch(sparkSessionToRunBatch: SparkSession): Unit = {
    logDebug(s"Running batch $currentBatchId")

    // Request unprocessed data from all sources.
    newData = reportTimeTaken("getBatch") {
      availableOffsets.flatMap {
        case (source: Source, available: Offset)
          if committedOffsets.get(source).map(_ != available).getOrElse(true) =>
          val current = committedOffsets.get(source).map(_.asInstanceOf[Offset])
          val batch = source.getBatch(current, available)
          assert(batch.isStreaming,
            s"DataFrame returned by getBatch from $source did not have isStreaming=true\n" +
              s"${batch.queryExecution.logical}")
          logDebug(s"Retrieving data from $source: $current -> $available")
          Some(source -> batch.logicalPlan)

        case (stream: MicroBatchStream, available)
          if committedOffsets.get(stream).map(_ != available).getOrElse(true) =>
          val current = committedOffsets.get(stream).map {
            off => stream.deserializeOffset(off.json)
          }
          val endOffset: OffsetV2 = available match {
            case v1: SerializedOffset => stream.deserializeOffset(v1.json)
            case v2: OffsetV2 => v2
          }
          val startOffset = current.getOrElse(stream.initialOffset)
          logDebug(s"Retrieving data from $stream: $current -> $endOffset")

          // To be compatible with the v1 source, the `newData` is represented as a logical plan,
          // while the `newData` of v2 source is just the start and end offsets. Here we return a
          // fake logical plan to carry the offsets.
          Some(stream -> OffsetHolder(startOffset, endOffset))

        case _ => None
      }
    }

    // Replace sources in the logical plan with data that has arrived since the last batch.
    val newBatchesPlan = logicalPlan transform {
      // For v1 sources.
      case StreamingExecutionRelation(source, output) =>
        newData.get(source).map { dataPlan =>
          val maxFields = SQLConf.get.maxToStringFields
          assert(output.size == dataPlan.output.size,
            s"Invalid batch: ${truncatedString(output, ",", maxFields)} != " +
              s"${truncatedString(dataPlan.output, ",", maxFields)}")

          val aliases = output.zip(dataPlan.output).map { case (to, from) =>
            Alias(from, to.name)(exprId = to.exprId, explicitMetadata = Some(from.metadata))
          }
          Project(aliases, dataPlan)
        }.getOrElse {
          LocalRelation(output, isStreaming = true)
        }

      // For v2 sources.
      case r: StreamingDataSourceV2Relation =>
        newData.get(r.stream).map {
          case OffsetHolder(start, end) =>
            r.copy(startOffset = Some(start), endOffset = Some(end))
        }.getOrElse {
          LocalRelation(r.output, isStreaming = true)
        }
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    val newAttributePlan = newBatchesPlan.transformAllExpressionsWithPruning(
      _.containsPattern(CURRENT_LIKE)) {
      case ct: CurrentTimestamp =>
        // CurrentTimestamp is not TimeZoneAwareExpression while CurrentBatchTimestamp is.
        // Without TimeZoneId, CurrentBatchTimestamp is unresolved. Here, we use an explicit
        // dummy string to prevent UnresolvedException and to prevent to be used in the future.
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          ct.dataType, Some("Dummy TimeZoneId"))
      case lt: LocalTimestamp =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          lt.dataType, lt.timeZoneId)
      case cd: CurrentDate =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          cd.dataType, cd.timeZoneId)
    }

    val triggerLogicalPlan = sink match {
      case _: Sink => newAttributePlan
      case _: SupportsWrite =>
        newAttributePlan.asInstanceOf[WriteToMicroBatchDataSource].createPlan(currentBatchId)
      case _ => throw new IllegalArgumentException(s"unknown sink type for $sink")
    }

    sparkSessionToRunBatch.sparkContext.setLocalProperty(
      MicroBatchExecution.BATCH_ID_KEY, currentBatchId.toString)
    sparkSessionToRunBatch.sparkContext.setLocalProperty(
      StreamExecution.IS_CONTINUOUS_PROCESSING, false.toString)

    reportTimeTaken("queryPlanning") {
      lastExecution = new IncrementalExecution(
        sparkSessionToRunBatch,
        triggerLogicalPlan,
        outputMode,
        checkpointFile("state"),
        id,
        runId,
        currentBatchId,
        offsetSeqMetadata)
      lastExecution.executedPlan // Force the lazy generation of execution plan
    }

    val nextBatch =
      new Dataset(lastExecution, RowEncoder(lastExecution.analyzed.schema))

    val batchSinkProgress: Option[StreamWriterCommitProgress] = reportTimeTaken("addBatch") {
      SQLExecution.withNewExecutionId(lastExecution) {
        sink match {
          case s: Sink => s.addBatch(currentBatchId, nextBatch)
          case _: SupportsWrite =>
            // This doesn't accumulate any data - it just forces execution of the microbatch writer.
            nextBatch.collect()
        }
        lastExecution.executedPlan match {
          case w: WriteToDataSourceV2Exec => w.commitProgress
          case _ => None
        }
      }
    }

    withProgressLocked {
      sinkCommitProgress = batchSinkProgress
      watermarkTracker.updateWatermark(lastExecution.executedPlan)
      assert(commitLog.add(currentBatchId, CommitMetadata(watermarkTracker.currentWatermark)),
        "Concurrent update to the commit log. Multiple streaming jobs detected for " +
          s"$currentBatchId")
      committedOffsets ++= availableOffsets
    }
    logDebug(s"Completed batch ${currentBatchId}")
  }

  /** Execute a function while locking the stream from making an progress */
  private[sql] def withProgressLocked[T](f: => T): T = {
    awaitProgressLock.lock()
    try {
      f
    } finally {
      awaitProgressLock.unlock()
    }
  }
}

object MicroBatchExecution {
  val BATCH_ID_KEY = "streaming.sql.batchId"
}

case class OffsetHolder(start: OffsetV2, end: OffsetV2) extends LeafNode {
  override def output: Seq[Attribute] = Nil
}
