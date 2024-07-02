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

import java.io.{InterruptedIOException, UncheckedIOException}
import java.nio.channels.ClosedByInterruptException
import java.util.UUID
import java.util.concurrent.{CountDownLatch, ExecutionException, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.{Map => MutableMap}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.common.util.concurrent.UncheckedExecutionException
import org.apache.hadoop.fs.Path

import org.apache.spark.{JobArtifactSet, SparkContext, SparkException, SparkThrowable}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{CHECKPOINT_PATH, CHECKPOINT_ROOT, LOGICAL_PLAN, PATH, PRETTY_ID_STRING, SPARK_DATA_STREAM}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table}
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2, ReadLimit, SparkDataStream}
import org.apache.spark.sql.connector.write.{LogicalWriteInfoImpl, SupportsTruncate, Write}
import org.apache.spark.sql.execution.command.StreamingExplainCommand
import org.apache.spark.sql.execution.streaming.sources.ForeachBatchUserFuncException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsStreamingUpdateAsAppend
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.{Clock, UninterruptibleThread, Utils}

/** States for [[StreamExecution]]'s lifecycle. */
trait State
case object INITIALIZING extends State
case object ACTIVE extends State
case object TERMINATED extends State
case object RECONFIGURING extends State

/**
 * Manages the execution of a streaming Spark SQL query that is occurring in a separate thread.
 * Unlike a standard query, a streaming query executes repeatedly each time new data arrives at any
 * [[Source]] present in the query plan. Whenever new data arrives, a [[QueryExecution]] is created
 * and the results are committed transactionally to the given [[Sink]].
 *
 * @param deleteCheckpointOnStop whether to delete the checkpoint if the query is stopped without
 *                               errors. Checkpoint deletion can be forced with the appropriate
 *                               Spark configuration.
 */
abstract class StreamExecution(
    override val sparkSession: SparkSession,
    override val name: String,
    val resolvedCheckpointRoot: String,
    val analyzedPlan: LogicalPlan,
    val sink: Table,
    val trigger: Trigger,
    val triggerClock: Clock,
    val outputMode: OutputMode,
    deleteCheckpointOnStop: Boolean)
  extends StreamingQuery with Logging {

  import org.apache.spark.sql.streaming.StreamingQueryListener._

  protected val pollingDelayMs: Long = sparkSession.sessionState.conf.streamingPollingDelay

  protected val minLogEntriesToMaintain: Int = sparkSession.sessionState.conf.minBatchesToRetain
  require(minLogEntriesToMaintain > 0, "minBatchesToRetain has to be positive")

  /**
   * A lock used to wait/notify when batches complete. Use a fair lock to avoid thread starvation.
   */
  protected val awaitProgressLock = new ReentrantLock(true)
  protected val awaitProgressLockCondition = awaitProgressLock.newCondition()

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
  @volatile var committedOffsets = new StreamProgress

  /**
   * Get the latest execution context .
   */
  def getLatestExecutionContext(): StreamExecutionContext

  /**
   * Get the start offsets of the latest batch that has been planned
   */
  def getStartOffsetsOfLatestBatch: StreamProgress = {
    getLatestExecutionContext().startOffsets
  }

  /**
   * Get the end or formerly know as "available" offsets of the latest batch that has been planned
   */
  def availableOffsets: StreamProgress = {
    getLatestExecutionContext().endOffsets
  }

  def latestOffsets: StreamProgress = {
    getLatestExecutionContext().latestOffsets
  }

  override def status: StreamingQueryStatus = {
    getLatestExecutionContext().currentStatus
  }

  override def recentProgress: Array[StreamingQueryProgress] = progressReporter.recentProgress

  override def lastProgress: StreamingQueryProgress = progressReporter.lastProgress

  /**
   * The base logical plan which will be used across batch runs. Once the value is set, it should
   * not be modified.
   */
  def logicalPlan: LogicalPlan

  /**
   * The list of stream instances which will be used across batch runs. Once the value is set,
   * it should not be modified.
   */
  protected def sources: Seq[SparkDataStream]

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

  /**
   * A map of current watermarks, keyed by the position of the watermark operator in the
   * physical plan.
   *
   * This state is 'soft state', which does not affect the correctness and semantics of watermarks
   * and is not persisted across query restarts.
   * The fault-tolerant watermark state is in offsetSeqMetadata.
   */
  protected val watermarkMsMap: MutableMap[Int, Long] = MutableMap()

  override val id: UUID = UUID.fromString(streamMetadata.id)

  override val runId: UUID = UUID.randomUUID

  protected val progressReporter = new ProgressReporter(sparkSession, triggerClock,
    () => logicalPlan)

  /**
   * Pretty identified string of printing in logs. Format is
   * If name is set "queryName [id = xyz, runId = abc]" else "[id = xyz, runId = abc]"
   */
  protected val prettyIdString =
    Option(name).map(_ + " ").getOrElse("") + s"[id = $id, runId = $runId]"

  /**
   * A list of unique sources in the query plan. This will be set when generating logical plan.
   */
  @volatile protected var uniqueSources: Map[SparkDataStream, ReadLimit] = Map.empty

  /** Defines the internal state of execution */
  protected val state = new AtomicReference[State](INITIALIZING)

  def lastExecution: IncrementalExecution = getLatestExecutionContext().executionPlan

  @volatile
  protected var streamDeathCause: StreamingQueryException = null

  /* Get the call site in the caller thread; will pass this into the micro batch thread */
  private val callSite = Utils.getCallSite()

  /* Make sure we propagate the ArtifactSet to the micro batch thread. */
  private val jobArtifactState = JobArtifactSet.getCurrentJobArtifactState.orNull

  /** Used to report metrics to coda-hale. This uses id for easier tracking across restarts. */
  lazy val streamMetrics = new MetricsReporter(
    this, s"spark.streaming.${Option(name).getOrElse(id)}")

  /** Isolated spark session to run the batches with. */
  private val sparkSessionForStream = sparkSession.cloneSession()

  /**
   * The thread that runs the micro-batches of this stream. Note that this thread must be
   * [[org.apache.spark.util.UninterruptibleThread]] to workaround KAFKA-1894: interrupting a
   * running `KafkaConsumer` may cause endless loop.
   */
  val queryExecutionThread: QueryExecutionThread =
    new QueryExecutionThread(s"stream execution thread for $prettyIdString") {
      override def run(): Unit = {
        // To fix call site like "run at <unknown>:0", we bridge the call site from the caller
        // thread to this micro batch thread
        sparkSession.sparkContext.setCallSite(callSite)
        JobArtifactSet.withActiveJobArtifactState(jobArtifactState) {
          runStream()
        }
      }
    }

  /**
   * A write-ahead-log that records the offsets that are present in each batch. In order to ensure
   * that a given batch will always consist of the same data, we write to this log *before* any
   * processing is done.  Thus, the Nth record in this log indicated data that is currently being
   * processed and the N-1th entry indicates which offsets have been durably committed to the sink.
   */
  val offsetLog = new OffsetSeqLog(sparkSession, checkpointFile("offsets"))

  /**
   * A log that records the batch ids that have completed. This is used to check if a batch was
   * fully processed, and its output was committed to the sink, hence no need to process it again.
   * This is used (for instance) during restart, to help identify which batch to run next.
   */
  val commitLog = new CommitLog(sparkSession, checkpointFile("commits"))

  /** Whether all fields of the query have been initialized */
  private def isInitialized: Boolean = state.get != INITIALIZING

  /** Whether the query is currently active or not */
  override def isActive: Boolean = state.get != TERMINATED

  /** Returns the [[StreamingQueryException]] if the query was terminated by an exception. */
  override def exception: Option[StreamingQueryException] = Option(streamDeathCause)

  /** Returns the path of a file with `name` in the checkpoint directory. */
  protected def checkpointFile(name: String): String =
    new Path(new Path(resolvedCheckpointRoot), name).toString

  /** All checkpoint file operations should be performed through `CheckpointFileManager`. */
  private val fileManager = CheckpointFileManager.create(new Path(resolvedCheckpointRoot),
      sparkSession.sessionState.newHadoopConf())

  /**
   * Starts the execution. This returns only after the thread has started and [[QueryStartedEvent]]
   * has been posted to all the listeners.
   */
  def start(): Unit = {
    logInfo(log"Starting ${MDC(PRETTY_ID_STRING, prettyIdString)}. " +
      log"Use ${MDC(CHECKPOINT_ROOT, resolvedCheckpointRoot)} to store the query checkpoint.")
    queryExecutionThread.setDaemon(true)
    queryExecutionThread.start()
    startLatch.await()  // Wait until thread started and QueryStart event has been posted
  }

  /**
   * Run the activated stream until stopped.
   */
  protected def runActivatedStream(sparkSessionForStream: SparkSession): Unit

  /**
   * Activate the stream and then wrap a callout to runActivatedStream, handling start and stop.
   *
   * Note that this method ensures that [[QueryStartedEvent]] and [[QueryTerminatedEvent]] are
   * posted such that listeners are guaranteed to get a start event before a termination.
   * Furthermore, this method also ensures that [[QueryStartedEvent]] event is posted before the
   * `start()` method returns.
   */
  private def runStream(): Unit = {
    var errorClassOpt: Option[String] = None
    try {
      sparkSession.sparkContext.setJobGroup(runId.toString, getBatchDescriptionString,
        interruptOnCancel = true)
      sparkSession.sparkContext.setLocalProperty(StreamExecution.QUERY_ID_KEY, id.toString)
      if (sparkSession.sessionState.conf.streamingMetricsEnabled) {
        sparkSession.sparkContext.env.metricsSystem.registerSource(streamMetrics)
      }

      // `postEvent` does not throw non fatal exception.
      val startTimestamp = triggerClock.getTimeMillis()
      postEvent(
        new QueryStartedEvent(id, runId, name, progressReporter.formatTimestamp(startTimestamp)))

      // Unblock starting thread
      startLatch.countDown()

      // While active, repeatedly attempt to run batches.
      sparkSessionForStream.withActive {
        // Adaptive execution can change num shuffle partitions, disallow
        sparkSessionForStream.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
        // Disable cost-based join optimization as we do not want stateful operations
        // to be rearranged
        sparkSessionForStream.conf.set(SQLConf.CBO_ENABLED.key, "false")
        // Disable any config affecting the required child distribution of stateful operators.
        // Please read through the NOTE on the classdoc of StatefulOpClusteredDistribution for
        // details.
        sparkSessionForStream.conf.set(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key,
          "false")

        getLatestExecutionContext().updateStatusMessage("Initializing sources")
        // force initialization of the logical plan so that the sources can be created
        logicalPlan

        getLatestExecutionContext().offsetSeqMetadata = OffsetSeqMetadata(
          batchWatermarkMs = 0, batchTimestampMs = 0, sparkSessionForStream.conf)

        if (state.compareAndSet(INITIALIZING, ACTIVE)) {
          // Log logical plan at the start of the query to help debug issues related to
          // plan changes.
          logInfo(log"Finish initializing with logical plan:\n${MDC(LOGICAL_PLAN, logicalPlan)}")

          // Unblock `awaitInitialization`
          initializationLatch.countDown()
          runActivatedStream(sparkSessionForStream)
          getLatestExecutionContext().updateStatusMessage("Stopped")
        } else {
          // `stop()` is already called. Let `finally` finish the cleanup.
        }
      }
    } catch {
      case e if isInterruptedByStop(e, sparkSession.sparkContext) =>
        // interrupted by stop()
        getLatestExecutionContext().updateStatusMessage("Stopped")
      case e: Throwable =>
        val message = if (e.getMessage == null) "" else e.getMessage
        val cause = if (e.isInstanceOf[ForeachBatchUserFuncException]) {
          // We want to maintain the current way users get the causing exception
          // from the StreamingQueryException. Hence the ForeachBatch exception is unwrapped here.
          e.getCause
        } else {
          e
        }

        streamDeathCause = new StreamingQueryException(
          toDebugString(includeLogicalPlan = isInitialized),
          cause = cause,
          getLatestExecutionContext().startOffsets
            .toOffsetSeq(sources.toSeq, getLatestExecutionContext().offsetSeqMetadata)
            .toString,
          getLatestExecutionContext().endOffsets
            .toOffsetSeq(sources.toSeq, getLatestExecutionContext().offsetSeqMetadata)
            .toString,
          errorClass = "STREAM_FAILED",
          messageParameters = Map(
            "id" -> id.toString,
            "runId" -> runId.toString,
            "message" -> message,
            "queryDebugString" -> toDebugString(includeLogicalPlan = isInitialized),
            "startOffset" -> getLatestExecutionContext().startOffsets.toOffsetSeq(
              sources.toSeq, getLatestExecutionContext().offsetSeqMetadata).toString,
            "endOffset" -> getLatestExecutionContext().endOffsets.toOffsetSeq(
              sources.toSeq, getLatestExecutionContext().offsetSeqMetadata).toString
          ))

        errorClassOpt = e match {
          case t: SparkThrowable => Option(t.getErrorClass)
          case _ => None
        }

        logError(log"Query ${MDC(PRETTY_ID_STRING, prettyIdString)} terminated with error", e)
        getLatestExecutionContext().updateStatusMessage(s"Terminated with exception: $message")
        // Rethrow the fatal errors to allow the user using `Thread.UncaughtExceptionHandler` to
        // handle them
        if (!NonFatal(cause)) {
          throw cause
        }
    } finally queryExecutionThread.runUninterruptibly {
      // The whole `finally` block must run inside `runUninterruptibly` to avoid being interrupted
      // when a query is stopped by the user. We need to make sure the following codes finish
      // otherwise it may throw `InterruptedException` to `UncaughtExceptionHandler` (SPARK-21248).

      // Release latches to unblock the user codes since exception can happen in any place and we
      // may not get a chance to release them
      startLatch.countDown()
      initializationLatch.countDown()

      try {
        stopSources()
        cleanup()
        state.set(TERMINATED)
        getLatestExecutionContext().currentStatus =
          status.copy(isTriggerActive = false, isDataAvailable = false)

        // Update metrics and status
        sparkSession.sparkContext.env.metricsSystem.removeSource(streamMetrics)

        // Notify others
        sparkSession.streams.notifyQueryTermination(StreamExecution.this)
        postEvent(
          new QueryTerminatedEvent(id, runId, exception.map(_.cause).map(Utils.exceptionString),
            errorClassOpt))

        // Delete the temp checkpoint when either force delete enabled or the query didn't fail
        if (deleteCheckpointOnStop &&
            (sparkSession.sessionState.conf
              .getConf(SQLConf.FORCE_DELETE_TEMP_CHECKPOINT_LOCATION) || exception.isEmpty)) {
          val checkpointPath = new Path(resolvedCheckpointRoot)
          try {
            logInfo(log"Deleting checkpoint ${MDC(CHECKPOINT_PATH, checkpointPath)}.")
            fileManager.delete(checkpointPath)
          } catch {
            case NonFatal(e) =>
              // Deleting temp checkpoint folder is best effort, don't throw non fatal exceptions
              // when we cannot delete them.
              logWarning(log"Cannot delete ${MDC(PATH, checkpointPath)}", e)
          }
        }
      } finally {
        awaitProgressLock.lock()
        try {
          // Wake up any threads that are waiting for the stream to progress.
          awaitProgressLockCondition.signalAll()
        } finally {
          awaitProgressLock.unlock()
        }
        terminationLatch.countDown()
      }
    }
  }

  private def isInterruptedByStop(e: Throwable, sc: SparkContext): Boolean = {
    if (state.get == TERMINATED) {
      StreamExecution.isInterruptionException(e, sc)
    } else {
      false
    }
  }

  protected def postEvent(event: StreamingQueryListener.Event): Unit = {
    sparkSession.streams.postListenerEvent(event)
  }

  /** Stops all streaming sources safely. */
  protected def stopSources(): Unit = {
    uniqueSources.foreach { case (source, _) =>
      try {
        source.stop()
      } catch {
        case NonFatal(e) =>
          logWarning(log"Failed to stop streaming source: ${MDC(SPARK_DATA_STREAM, source)}. " +
            log"Resources may have leaked.", e)
      }
    }
  }


  /**
   * Any clean up that needs to happen when the query is stopped or exits
   */
  protected def cleanup(): Unit = {}

  /**
   * Interrupts the query execution thread and awaits its termination until until it exceeds the
   * timeout. The timeout can be set on "spark.sql.streaming.stopTimeout".
   *
   * @throws TimeoutException If the thread cannot be stopped within the timeout
   */
  @throws[TimeoutException]
  protected def interruptAndAwaitExecutionThreadTermination(): Unit = {
    val timeout = math.max(
      sparkSession.conf.get(SQLConf.STREAMING_STOP_TIMEOUT), 0)
    queryExecutionThread.interrupt()
    queryExecutionThread.join(timeout)
    if (queryExecutionThread.isAlive) {
      val stackTraceException = new SparkException("The stream thread was last executing:")
      stackTraceException.setStackTrace(queryExecutionThread.getStackTrace)
      val timeoutException = new TimeoutException(
        s"Stream Execution thread for stream $prettyIdString failed to stop within $timeout " +
        s"milliseconds (specified by ${SQLConf.STREAMING_STOP_TIMEOUT.key}). See the cause on " +
        s"what was being executed in the streaming query thread.")
      timeoutException.initCause(stackTraceException)
      throw timeoutException
    }
  }

  /**
   * Blocks the current thread until processing for data from the given `source` has reached at
   * least the given `Offset`. This method is intended for use primarily when writing tests.
   */
  private[sql] def awaitOffset(sourceIndex: Int, newOffset: OffsetV2, timeoutMs: Long): Unit = {
    assertAwaitThread()
    def notDone = {
      val localCommittedOffsets = committedOffsets
      if (sources == null) {
        // sources might not be initialized yet
        false
      } else {
        val source = sources(sourceIndex)
        // SPARK-39242 For numeric increasing offsets, we could have called awaitOffset
        // after the stream has moved past the expected newOffset or if committedOffsets
        // changed after notify. In this case, its safe to exit, since at-least the given
        // Offset has been reached and the equality condition might never be met.
        (localCommittedOffsets.get(source), newOffset) match {
          case (Some(LongOffset(localOffVal)), LongOffset(newOffVal)) => localOffVal < newOffVal
          case (Some(localOff), newOff) => localOff != newOff
          case (None, newOff) => true
        }
      }
    }

    while (notDone) {
      awaitProgressLock.lock()
      try {
        awaitProgressLockCondition.await(timeoutMs, TimeUnit.MILLISECONDS)
        if (streamDeathCause != null) {
          throw streamDeathCause
        }
      } finally {
        awaitProgressLock.unlock()
      }
    }
    logDebug(s"Unblocked at $newOffset for ${sources(sourceIndex)}")
  }

  /** A flag to indicate that a batch has completed with no new data available. */
  @volatile protected var noNewData = false

  /**
   * Assert that the await APIs should not be called in the stream thread. Otherwise, it may cause
   * dead-lock, e.g., calling any await APIs in `StreamingQueryListener.onQueryStarted` will block
   * the stream thread forever.
   */
  private def assertAwaitThread(): Unit = {
    if (queryExecutionThread eq Thread.currentThread) {
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
    if (!isActive) return
    awaitProgressLock.lock()
    try {
      noNewData = false
      while (true) {
        awaitProgressLockCondition.await(10000, TimeUnit.MILLISECONDS)
        if (streamDeathCause != null) {
          throw streamDeathCause
        }
        if (noNewData || !isActive) {
          return
        }
      }
    } finally {
      awaitProgressLock.unlock()
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
          |Current Committed Offsets: ${getLatestExecutionContext().startOffsets}
          |Current Available Offsets: ${getLatestExecutionContext().endOffsets}
          |
          |Current State: $state
          |Thread State: ${queryExecutionThread.getState}""".stripMargin
    if (includeLogicalPlan) {
      debugString + s"\n\nLogical Plan:\n$logicalPlan"
    } else {
      debugString
    }
  }

  protected def getBatchDescriptionString: String = {
    val batchDescription = if (getLatestExecutionContext().batchId < 0) "init"
    else getLatestExecutionContext().batchId.toString
    s"""|${Option(name).getOrElse("")}
        |id = $id
        |runId = $runId
        |batch = $batchDescription""".stripMargin
  }

  protected def createWrite(
      table: SupportsWrite,
      options: Map[String, String],
      inputPlan: LogicalPlan): Write = {
    val info = LogicalWriteInfoImpl(
      queryId = id.toString,
      inputPlan.schema,
      new CaseInsensitiveStringMap(options.asJava))
    val writeBuilder = table.newWriteBuilder(info)
    outputMode match {
      case Append =>
        writeBuilder.build()

      case Complete =>
        // TODO: we should do this check earlier when we have capability API.
        require(writeBuilder.isInstanceOf[SupportsTruncate],
          table.name + " does not support Complete mode.")
        writeBuilder.asInstanceOf[SupportsTruncate].truncate().build()

      case Update =>
        require(writeBuilder.isInstanceOf[SupportsStreamingUpdateAsAppend],
          table.name + " does not support Update mode.")
        writeBuilder.asInstanceOf[SupportsStreamingUpdateAsAppend].build()
    }
  }

  protected def purge(threshold: Long): Unit = {
    logDebug(s"Purging metadata at threshold=$threshold")
    offsetLog.purge(threshold)
    commitLog.purge(threshold)
  }
}

object StreamExecution {
  val QUERY_ID_KEY = "sql.streaming.queryId"
  val IS_CONTINUOUS_PROCESSING = "__is_continuous_processing"
  val IO_EXCEPTION_NAMES = Seq(
    classOf[InterruptedException].getName,
    classOf[InterruptedIOException].getName,
    classOf[ClosedByInterruptException].getName)
  val PROXY_ERROR = (
    "py4j.protocol.Py4JJavaError: An error occurred while calling" +
      s"((.|\\r\\n|\\r|\\n)*)(${IO_EXCEPTION_NAMES.mkString("|")})").r

  @scala.annotation.tailrec
  def isInterruptionException(e: Throwable, sc: SparkContext): Boolean = e match {
    // InterruptedIOException - thrown when an I/O operation is interrupted
    // ClosedByInterruptException - thrown when an I/O operation upon a channel is interrupted
    case _: InterruptedException | _: InterruptedIOException | _: ClosedByInterruptException =>
      true
    // The cause of the following exceptions may be one of the above exceptions:
    //
    // UncheckedIOException - thrown by codes that cannot throw a checked IOException, such as
    //                        BiFunction.apply
    // ExecutionException - thrown by codes running in a thread pool and these codes throw an
    //                      exception
    // UncheckedExecutionException - thrown by codes that cannot throw a checked
    //                               ExecutionException, such as BiFunction.apply
    case e2 @ (_: UncheckedIOException | _: ExecutionException | _: UncheckedExecutionException)
        if e2.getCause != null =>
      isInterruptionException(e2.getCause, sc)
    case fe: ForeachBatchUserFuncException => isInterruptionException(fe.getCause, sc)
    case se: SparkException =>
      val jobGroup = sc.getLocalProperty("spark.jobGroup.id")
      if (jobGroup == null) return false
      val errorMsg = se.getMessage
      if (errorMsg.contains("cancelled") && errorMsg.contains(jobGroup) && se.getCause == null) {
        true
      } else if (se.getCause != null) {
        isInterruptionException(se.getCause, sc)
      } else {
        false
      }
    // py4j.Py4JException - with pinned thread mode on, the exception can be interrupted by Py4J
    //                      access, for example, in `DataFrameWriter.foreachBatch`. See also
    //                      SPARK-39218.
    case e: py4j.Py4JException => PROXY_ERROR.findFirstIn(e.getMessage).isDefined
    case _ =>
      false
  }

  /** Whether the path contains special chars that will be escaped when converting to a `URI`. */
  def containsSpecialCharsInPath(path: Path): Boolean = {
    path.toUri.getPath != new Path(path.toUri.toString).toUri.getPath
  }
}

/**
 * A special thread to run the stream query. Some codes require to run in the QueryExecutionThread
 * and will use `classOf[QueryExecutionThread]` to check.
 */
abstract class QueryExecutionThread(name: String) extends UninterruptibleThread(name)
