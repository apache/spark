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

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, CurrentBatchTimestamp, CurrentDate, CurrentTimestamp}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.StreamingExplainCommand
import org.apache.spark.sql.streaming._
import org.apache.spark.util.{Clock, UninterruptibleThread, Utils}

/**
 * Manages the execution of a streaming Spark SQL query that is occurring in a separate thread.
 * Unlike a standard query, a streaming query executes repeatedly each time new data arrives at any
 * [[Source]] present in the query plan. Whenever new data arrives, a [[QueryExecution]] is created
 * and the results are committed transactionally to the given [[Sink]].
 *
 * @param deleteCheckpointOnStop whether to delete the checkpoint if the query is stopped without
 *                               errors
 */
class StreamExecution(
    override val sparkSession: SparkSession,
    override val name: String,
    val checkpointRoot: String,
    analyzedPlan: LogicalPlan,
    val sink: Sink,
    val trigger: Trigger,
    val triggerClock: Clock,
    val outputMode: OutputMode,
    deleteCheckpointOnStop: Boolean)
  extends StreamingQuery with ProgressReporter with Logging {

  import org.apache.spark.sql.streaming.StreamingQueryListener._

  private val pollingDelayMs = sparkSession.sessionState.conf.streamingPollingDelay

  private val minBatchesToRetain = sparkSession.sessionState.conf.minBatchesToRetain
  require(minBatchesToRetain > 0, "minBatchesToRetain has to be positive")

  /**
   * A lock used to wait/notify when batches complete. Use a fair lock to avoid thread starvation.
   */
  private val awaitBatchLock = new ReentrantLock(true)
  private val awaitBatchLockCondition = awaitBatchLock.newCondition()

  private val initializationLatch = new CountDownLatch(1)
  private val startLatch = new CountDownLatch(1)
  private val terminationLatch = new CountDownLatch(1)

  /**
   * Tracks how much data we have processed and committed to the sink or state store from each
   * input source.
   * Only the scheduler thread should modify this field, and only in atomic steps.
   * Other threads should make a shallow copy if they are going to access this field more than
   * once, since the field's value may change at any time.
   */
  @volatile
  var committedOffsets = new StreamProgress

  /**
   * Tracks the offsets that are available to be processed, but have not yet be committed to the
   * sink.
   * Only the scheduler thread should modify this field, and only in atomic steps.
   * Other threads should make a shallow copy if they are going to access this field more than
   * once, since the field's value may change at any time.
   */
  @volatile
  var availableOffsets = new StreamProgress

  /** The current batchId or -1 if execution has not yet been initialized. */
  protected var currentBatchId: Long = -1

  /** Metadata associated with the whole query */
  protected val streamMetadata: StreamMetadata = {
    val metadataPath = new Path(checkpointFile("metadata"))
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    StreamMetadata.read(metadataPath, hadoopConf).getOrElse {
      val newMetadata = new StreamMetadata(UUID.randomUUID.toString)
      StreamMetadata.write(newMetadata, metadataPath, hadoopConf)
      newMetadata
    }
  }

  /** Metadata associated with the offset seq of a batch in the query. */
  protected var offsetSeqMetadata = OffsetSeqMetadata()

  override val id: UUID = UUID.fromString(streamMetadata.id)

  override val runId: UUID = UUID.randomUUID

  /**
   * Pretty identified string of printing in logs. Format is
   * If name is set "queryName [id = xyz, runId = abc]" else "[id = xyz, runId = abc]"
   */
  private val prettyIdString =
    Option(name).map(_ + " ").getOrElse("") + s"[id = $id, runId = $runId]"

  /**
   * All stream sources present in the query plan. This will be set when generating logical plan.
   */
  @volatile protected var sources: Seq[Source] = Seq.empty

  /**
   * A list of unique sources in the query plan. This will be set when generating logical plan.
   */
  @volatile private var uniqueSources: Seq[Source] = Seq.empty

  override lazy val logicalPlan: LogicalPlan = {
    assert(microBatchThread eq Thread.currentThread,
      "logicalPlan must be initialized in StreamExecutionThread " +
        s"but the current thread was ${Thread.currentThread}")
    var nextSourceId = 0L
    val _logicalPlan = analyzedPlan.transform {
      case StreamingRelation(dataSource, _, output) =>
        // Materialize source to avoid creating it in every batch
        val metadataPath = s"$checkpointRoot/sources/$nextSourceId"
        val source = dataSource.createSource(metadataPath)
        nextSourceId += 1
        // We still need to use the previous `output` instead of `source.schema` as attributes in
        // "df.logicalPlan" has already used attributes of the previous `output`.
        StreamingExecutionRelation(source, output)
    }
    sources = _logicalPlan.collect { case s: StreamingExecutionRelation => s.source }
    uniqueSources = sources.distinct
    _logicalPlan
  }

  private val triggerExecutor = trigger match {
    case t: ProcessingTime => ProcessingTimeExecutor(t, triggerClock)
  }

  /** Defines the internal state of execution */
  private val state = new AtomicReference[State](INITIALIZING)

  @volatile
  var lastExecution: IncrementalExecution = _

  /** Holds the most recent input data for each source. */
  protected var newData: Map[Source, DataFrame] = _

  @volatile
  private var streamDeathCause: StreamingQueryException = null

  /* Get the call site in the caller thread; will pass this into the micro batch thread */
  private val callSite = Utils.getCallSite()

  /** Used to report metrics to coda-hale. This uses id for easier tracking across restarts. */
  lazy val streamMetrics = new MetricsReporter(
    this, s"spark.streaming.${Option(name).getOrElse(id)}")

  /**
   * The thread that runs the micro-batches of this stream. Note that this thread must be
   * [[org.apache.spark.util.UninterruptibleThread]] to workaround KAFKA-1894: interrupting a
   * running `KafkaConsumer` may cause endless loop, and HADOOP-10622: interrupting
   * `Shell.runCommand` causes deadlock. (SPARK-14131)
   */
  val microBatchThread =
    new StreamExecutionThread(s"stream execution thread for $prettyIdString") {
      override def run(): Unit = {
        // To fix call site like "run at <unknown>:0", we bridge the call site from the caller
        // thread to this micro batch thread
        sparkSession.sparkContext.setCallSite(callSite)
        runBatches()
      }
    }

  /**
   * A write-ahead-log that records the offsets that are present in each batch. In order to ensure
   * that a given batch will always consist of the same data, we write to this log *before* any
   * processing is done.  Thus, the Nth record in this log indicated data that is currently being
   * processed and the N-1th entry indicates which offsets have been durably committed to the sink.
   */
  val offsetLog = new OffsetSeqLog(sparkSession, checkpointFile("offsets"))

  /** Whether all fields of the query have been initialized */
  private def isInitialized: Boolean = state.get != INITIALIZING

  /** Whether the query is currently active or not */
  override def isActive: Boolean = state.get != TERMINATED

  /** Returns the [[StreamingQueryException]] if the query was terminated by an exception. */
  override def exception: Option[StreamingQueryException] = Option(streamDeathCause)

  /** Returns the path of a file with `name` in the checkpoint directory. */
  private def checkpointFile(name: String): String =
    new Path(new Path(checkpointRoot), name).toUri.toString

  /**
   * Starts the execution. This returns only after the thread has started and [[QueryStartedEvent]]
   * has been posted to all the listeners.
   */
  def start(): Unit = {
    logInfo(s"Starting $prettyIdString. Use $checkpointRoot to store the query checkpoint.")
    microBatchThread.setDaemon(true)
    microBatchThread.start()
    startLatch.await()  // Wait until thread started and QueryStart event has been posted
  }

  /**
   * Repeatedly attempts to run batches as data arrives.
   *
   * Note that this method ensures that [[QueryStartedEvent]] and [[QueryTerminatedEvent]] are
   * posted such that listeners are guaranteed to get a start event before a termination.
   * Furthermore, this method also ensures that [[QueryStartedEvent]] event is posted before the
   * `start()` method returns.
   */
  private def runBatches(): Unit = {
    try {
      if (sparkSession.sessionState.conf.streamingMetricsEnabled) {
        sparkSession.sparkContext.env.metricsSystem.registerSource(streamMetrics)
      }

      // `postEvent` does not throw non fatal exception.
      postEvent(new QueryStartedEvent(id, runId, name))

      // Unblock starting thread
      startLatch.countDown()

      // While active, repeatedly attempt to run batches.
      SparkSession.setActiveSession(sparkSession)

      updateStatusMessage("Initializing sources")
      // force initialization of the logical plan so that the sources can be created
      logicalPlan
      if (state.compareAndSet(INITIALIZING, ACTIVE)) {
        // Unblock `awaitInitialization`
        initializationLatch.countDown()

        triggerExecutor.execute(() => {
          startTrigger()

          val continueToRun =
            if (isActive) {
              reportTimeTaken("triggerExecution") {
                if (currentBatchId < 0) {
                  // We'll do this initialization only once
                  populateStartOffsets()
                  logDebug(s"Stream running from $committedOffsets to $availableOffsets")
                } else {
                  constructNextBatch()
                }
                if (dataAvailable) {
                  currentStatus = currentStatus.copy(isDataAvailable = true)
                  updateStatusMessage("Processing new data")
                  runBatch()
                }
              }

              // Report trigger as finished and construct progress object.
              finishTrigger(dataAvailable)
              if (dataAvailable) {
                // We'll increase currentBatchId after we complete processing current batch's data
                currentBatchId += 1
              } else {
                currentStatus = currentStatus.copy(isDataAvailable = false)
                updateStatusMessage("Waiting for data to arrive")
                Thread.sleep(pollingDelayMs)
              }
              true
            } else {
              false
            }

          // Update committed offsets.
          committedOffsets ++= availableOffsets
          updateStatusMessage("Waiting for next trigger")
          continueToRun
        })
        updateStatusMessage("Stopped")
      } else {
        // `stop()` is already called. Let `finally` finish the cleanup.
      }
    } catch {
      case _: InterruptedException if state.get == TERMINATED => // interrupted by stop()
        updateStatusMessage("Stopped")
      case e: Throwable =>
        streamDeathCause = new StreamingQueryException(
          toDebugString(includeLogicalPlan = isInitialized),
          s"Query $prettyIdString terminated with exception: ${e.getMessage}",
          e,
          committedOffsets.toOffsetSeq(sources, offsetSeqMetadata).toString,
          availableOffsets.toOffsetSeq(sources, offsetSeqMetadata).toString)
        logError(s"Query $prettyIdString terminated with error", e)
        updateStatusMessage(s"Terminated with exception: ${e.getMessage}")
        // Rethrow the fatal errors to allow the user using `Thread.UncaughtExceptionHandler` to
        // handle them
        if (!NonFatal(e)) {
          throw e
        }
    } finally {
      // Release latches to unblock the user codes since exception can happen in any place and we
      // may not get a chance to release them
      startLatch.countDown()
      initializationLatch.countDown()

      try {
        stopSources()
        state.set(TERMINATED)
        currentStatus = status.copy(isTriggerActive = false, isDataAvailable = false)

        // Update metrics and status
        sparkSession.sparkContext.env.metricsSystem.removeSource(streamMetrics)

        // Notify others
        sparkSession.streams.notifyQueryTermination(StreamExecution.this)
        postEvent(
          new QueryTerminatedEvent(id, runId, exception.map(_.cause).map(Utils.exceptionString)))

        // Delete the temp checkpoint only when the query didn't fail
        if (deleteCheckpointOnStop && exception.isEmpty) {
          val checkpointPath = new Path(checkpointRoot)
          try {
            val fs = checkpointPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
            fs.delete(checkpointPath, true)
          } catch {
            case NonFatal(e) =>
              // Deleting temp checkpoint folder is best effort, don't throw non fatal exceptions
              // when we cannot delete them.
              logWarning(s"Cannot delete $checkpointPath", e)
          }
        }
      } finally {
        awaitBatchLock.lock()
        try {
          // Wake up any threads that are waiting for the stream to progress.
          awaitBatchLockCondition.signalAll()
        } finally {
          awaitBatchLock.unlock()
        }
        terminationLatch.countDown()
      }
    }
  }

  /**
   * Populate the start offsets to start the execution at the current offsets stored in the sink
   * (i.e. avoid reprocessing data that we have already processed). This function must be called
   * before any processing occurs and will populate the following fields:
   *  - currentBatchId
   *  - committedOffsets
   *  - availableOffsets
   */
  private def populateStartOffsets(): Unit = {
    offsetLog.getLatest() match {
      case Some((batchId, nextOffsets)) =>
        logInfo(s"Resuming streaming query, starting with batch $batchId")
        currentBatchId = batchId
        availableOffsets = nextOffsets.toStreamProgress(sources)
        offsetSeqMetadata = nextOffsets.metadata.getOrElse(OffsetSeqMetadata())
        logDebug(s"Found possibly unprocessed offsets $availableOffsets " +
          s"at batch timestamp ${offsetSeqMetadata.batchTimestampMs}")

        offsetLog.get(batchId - 1).foreach {
          case lastOffsets =>
            committedOffsets = lastOffsets.toStreamProgress(sources)
            logDebug(s"Resuming with committed offsets: $committedOffsets")
        }
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
      awaitBatchLock.lock()
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
        awaitBatchLock.unlock()
      }
    }
    if (hasNewData) {
      // Current batch timestamp in milliseconds
      offsetSeqMetadata.batchTimestampMs = triggerClock.getTimeMillis()
      // Update the eventTime watermark if we find one in the plan.
      if (lastExecution != null) {
        lastExecution.executedPlan.collect {
          case e: EventTimeWatermarkExec if e.eventTimeStats.value.count > 0 =>
            logDebug(s"Observed event time stats: ${e.eventTimeStats.value}")
            e.eventTimeStats.value.max - e.delayMs
        }.headOption.foreach { newWatermarkMs =>
          if (newWatermarkMs > offsetSeqMetadata.batchWatermarkMs) {
            logInfo(s"Updating eventTime watermark to: $newWatermarkMs ms")
            offsetSeqMetadata.batchWatermarkMs = newWatermarkMs
          } else {
            logDebug(
              s"Event time didn't move: $newWatermarkMs < " +
                s"${offsetSeqMetadata.batchWatermarkMs}")
          }
        }
      }

      updateStatusMessage("Writing offsets to log")
      reportTimeTaken("walCommit") {
        assert(offsetLog.add(
          currentBatchId,
          availableOffsets.toOffsetSeq(sources, offsetSeqMetadata)),
          s"Concurrent update to the log. Multiple streaming jobs detected for $currentBatchId")
        logInfo(s"Committed offsets for batch $currentBatchId. " +
          s"Metadata ${offsetSeqMetadata.toString}")

        // NOTE: The following code is correct because runBatches() processes exactly one
        // batch at a time. If we add pipeline parallelism (multiple batches in flight at
        // the same time), this cleanup logic will need to change.

        // Now that we've updated the scheduler's persistent checkpoint, it is safe for the
        // sources to discard data from the previous batch.
        val prevBatchOff = offsetLog.get(currentBatchId - 1)
        if (prevBatchOff.isDefined) {
          prevBatchOff.get.toStreamProgress(sources).foreach {
            case (src, off) => src.commit(off)
          }
        }

        // It is now safe to discard the metadata beyond the minimum number to retain.
        // Note that purge is exclusive, i.e. it purges everything before the target ID.
        if (minBatchesToRetain < currentBatchId) {
          offsetLog.purge(currentBatchId - minBatchesToRetain)
        }
      }
    } else {
      awaitBatchLock.lock()
      try {
        // Wake up any threads that are waiting for the stream to progress.
        awaitBatchLockCondition.signalAll()
      } finally {
        awaitBatchLock.unlock()
      }
    }
  }

  /**
   * Processes any data available between `availableOffsets` and `committedOffsets`.
   */
  private def runBatch(): Unit = {
    // Request unprocessed data from all sources.
    newData = reportTimeTaken("getBatch") {
      availableOffsets.flatMap {
        case (source, available)
          if committedOffsets.get(source).map(_ != available).getOrElse(true) =>
          val current = committedOffsets.get(source)
          val batch = source.getBatch(current, available)
          logDebug(s"Retrieving data from $source: $current -> $available")
          Some(source -> batch)
        case _ => None
      }
    }

    // A list of attributes that will need to be updated.
    var replacements = new ArrayBuffer[(Attribute, Attribute)]
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
          LocalRelation(output)
        }
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    val replacementMap = AttributeMap(replacements)
    val triggerLogicalPlan = withNewSources transformAllExpressions {
      case a: Attribute if replacementMap.contains(a) => replacementMap(a)
      case ct: CurrentTimestamp =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          ct.dataType)
      case cd: CurrentDate =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          cd.dataType)
    }

    reportTimeTaken("queryPlanning") {
      lastExecution = new IncrementalExecution(
        sparkSession,
        triggerLogicalPlan,
        outputMode,
        checkpointFile("state"),
        currentBatchId,
        offsetSeqMetadata.batchWatermarkMs)
      lastExecution.executedPlan // Force the lazy generation of execution plan
    }

    val nextBatch =
      new Dataset(sparkSession, lastExecution, RowEncoder(lastExecution.analyzed.schema))

    reportTimeTaken("addBatch") {
      sink.addBatch(currentBatchId, nextBatch)
    }

    awaitBatchLock.lock()
    try {
      // Wake up any threads that are waiting for the stream to progress.
      awaitBatchLockCondition.signalAll()
    } finally {
      awaitBatchLock.unlock()
    }
  }

  override protected def postEvent(event: StreamingQueryListener.Event): Unit = {
    sparkSession.streams.postListenerEvent(event)
  }

  /** Stops all streaming sources safely. */
  private def stopSources(): Unit = {
    uniqueSources.foreach { source =>
      try {
        source.stop()
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to stop streaming source: $source. Resources may have leaked.", e)
      }
    }
  }

  /**
   * Signals to the thread executing micro-batches that it should stop running after the next
   * batch. This method blocks until the thread stops running.
   */
  override def stop(): Unit = {
    // Set the state to TERMINATED so that the batching thread knows that it was interrupted
    // intentionally
    state.set(TERMINATED)
    if (microBatchThread.isAlive) {
      microBatchThread.interrupt()
      microBatchThread.join()
    }
    logInfo(s"Query $prettyIdString was stopped")
  }

  /**
   * Blocks the current thread until processing for data from the given `source` has reached at
   * least the given `Offset`. This method is intended for use primarily when writing tests.
   */
  private[sql] def awaitOffset(source: Source, newOffset: Offset): Unit = {
    assertAwaitThread()
    def notDone = {
      val localCommittedOffsets = committedOffsets
      !localCommittedOffsets.contains(source) || localCommittedOffsets(source) != newOffset
    }

    while (notDone) {
      awaitBatchLock.lock()
      try {
        awaitBatchLockCondition.await(100, TimeUnit.MILLISECONDS)
        if (streamDeathCause != null) {
          throw streamDeathCause
        }
      } finally {
        awaitBatchLock.unlock()
      }
    }
    logDebug(s"Unblocked at $newOffset for $source")
  }

  /** A flag to indicate that a batch has completed with no new data available. */
  @volatile private var noNewData = false

  /**
   * Assert that the await APIs should not be called in the stream thread. Otherwise, it may cause
   * dead-lock, e.g., calling any await APIs in `StreamingQueryListener.onQueryStarted` will block
   * the stream thread forever.
   */
  private def assertAwaitThread(): Unit = {
    if (microBatchThread eq Thread.currentThread) {
      throw new IllegalStateException(
        "Cannot wait for a query state from the same thread that is running the query")
    }
  }

  /**
   * Await until all fields of the query have been initialized.
   */
  def awaitInitialization(timeoutMs: Long): Unit = {
    assertAwaitThread()
    require(timeoutMs > 0, "Timeout has to be positive")
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
    initializationLatch.await(timeoutMs, TimeUnit.MILLISECONDS)
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
  }

  override def processAllAvailable(): Unit = {
    assertAwaitThread()
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
    awaitBatchLock.lock()
    try {
      noNewData = false
      while (true) {
        awaitBatchLockCondition.await(10000, TimeUnit.MILLISECONDS)
        if (streamDeathCause != null) {
          throw streamDeathCause
        }
        if (noNewData) {
          return
        }
      }
    } finally {
      awaitBatchLock.unlock()
    }
  }

  override def awaitTermination(): Unit = {
    assertAwaitThread()
    terminationLatch.await()
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
  }

  override def awaitTermination(timeoutMs: Long): Boolean = {
    assertAwaitThread()
    require(timeoutMs > 0, "Timeout has to be positive")
    terminationLatch.await(timeoutMs, TimeUnit.MILLISECONDS)
    if (streamDeathCause != null) {
      throw streamDeathCause
    } else {
      !isActive
    }
  }

  /** Expose for tests */
  def explainInternal(extended: Boolean): String = {
    if (lastExecution == null) {
      "No physical plan. Waiting for data."
    } else {
      val explain = StreamingExplainCommand(lastExecution, extended = extended)
      sparkSession.sessionState.executePlan(explain).executedPlan.executeCollect()
        .map(_.getString(0)).mkString("\n")
    }
  }

  override def explain(extended: Boolean): Unit = {
    // scalastyle:off println
    println(explainInternal(extended))
    // scalastyle:on println
  }

  override def explain(): Unit = explain(extended = false)

  override def toString: String = {
    s"Streaming Query $prettyIdString [state = $state]"
  }

  private def toDebugString(includeLogicalPlan: Boolean): String = {
    val debugString =
      s"""|=== Streaming Query ===
          |Identifier: $prettyIdString
          |Current Committed Offsets: $committedOffsets
          |Current Available Offsets: $availableOffsets
          |
          |Current State: $state
          |Thread State: ${microBatchThread.getState}""".stripMargin
    if (includeLogicalPlan) {
      debugString + s"\n\nLogical Plan:\n$logicalPlan"
    } else {
      debugString
    }
  }

  trait State
  case object INITIALIZING extends State
  case object ACTIVE extends State
  case object TERMINATED extends State
}


/**
 * A special thread to run the stream query. Some codes require to run in the StreamExecutionThread
 * and will use `classOf[StreamExecutionThread]` to check.
 */
abstract class StreamExecutionThread(name: String) extends UninterruptibleThread(name)
