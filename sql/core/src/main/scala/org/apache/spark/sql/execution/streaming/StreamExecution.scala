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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.ContinuousQueryListener
import org.apache.spark.sql.util.ContinuousQueryListener._

/**
 * Manages the execution of a streaming Spark SQL query that is occurring in a separate thread.
 * Unlike a standard query, a streaming query executes repeatedly each time new data arrives at any
 * [[Source]] present in the query plan. Whenever new data arrives, a [[QueryExecution]] is created
 * and the results are committed transactionally to the given [[Sink]].
 */
class StreamExecution(
    val sqlContext: SQLContext,
    override val name: String,
    val metadataRoot: String,
    private[sql] val logicalPlan: LogicalPlan,
    val sink: Sink) extends ContinuousQuery with Logging {

  /** An monitor used to wait/notify when batches complete. */
  private val awaitBatchLock = new Object
  private val startLatch = new CountDownLatch(1)
  private val terminationLatch = new CountDownLatch(1)

  /** Minimum amount of time in between the start of each batch. */
  private val minBatchTime = 10

  /**
   * Tracks how much data we have processed and committed to the sink or state store from each
   * input source.
   */
  private[sql] val committedOffsets = new StreamProgress

  private[sql] val availableOffsets = new StreamProgress

  private[sql] var currentBatchId: Long = -1

  /** All stream sources present the query plan. */
  private val sources =
    logicalPlan.collect { case s: StreamingRelation => s.source }

  private val uniqueSources = sources.distinct

  /** Defines the internal state of execution */
  @volatile
  private var state: State = INITIALIZED

  @volatile
  private[sql] var lastExecution: QueryExecution = null

  @volatile
  private[sql] var streamDeathCause: ContinuousQueryException = null

  /** The thread that runs the micro-batches of this stream. */
  private[sql] val microBatchThread = new Thread(s"stream execution thread for $name") {
    override def run(): Unit = { runBatches() }
  }

  val offsetLog = new HDFSMetadataLog[Offset](sqlContext, metadataDirectory("offsets"))

  /** Whether the query is currently active or not */
  override def isActive: Boolean = state == ACTIVE

  /** Returns current status of all the sources. */
  override def sourceStatuses: Array[SourceStatus] = {
    sources.map(s => new SourceStatus(s.toString, availableOffsets.get(s))).toArray
  }

  /** Returns current status of the sink. */
  override def sinkStatus: SinkStatus =
    new SinkStatus(sink.toString, committedOffsets.toCompositeOffset(sources))

  /** Returns the [[ContinuousQueryException]] if the query was terminated by an exception. */
  override def exception: Option[ContinuousQueryException] = Option(streamDeathCause)

  private def metadataDirectory(name: String): String =
    new Path(new Path(metadataRoot), name).toUri.toString

  /**
   * Starts the execution. This returns only after the thread has started and [[QueryStarted]] event
   * has been posted to all the listeners.
   */
  private[sql] def start(): Unit = {
    microBatchThread.setDaemon(true)
    microBatchThread.start()
    startLatch.await()  // Wait until thread started and QueryStart event has been posted
  }

  /**
   * Repeatedly attempts to run batches as data arrives.
   *
   * Note that this method ensures that [[QueryStarted]] and [[QueryTerminated]] events are posted
   * such that listeners are guaranteed to get a start event before a termination. Furthermore, this
   * method also ensures that [[QueryStarted]] event is posted before the `start()` method returns.
   */
  private def runBatches(): Unit = {
    try {
      // Mark ACTIVE and then post the event. QueryStarted event is synchronously sent to listeners,
      // so must mark this as ACTIVE first.
      state = ACTIVE
      postEvent(new QueryStarted(this)) // Assumption: Does not throw exception.

      // Unblock starting thread
      startLatch.countDown()

      // While active, repeatedly attempt to run batches.
      SQLContext.setActive(sqlContext)
      populateStartOffsets()
      logError(s"Stream running from $committedOffsets to $availableOffsets")
      while (isActive) {
        if (dataAvailable) attemptBatch()
        commitAndConstructNextBatch()
        Thread.sleep(minBatchTime) // TODO: Could be tighter
      }
    } catch {
      case _: InterruptedException if state == TERMINATED => // interrupted by stop()
      case NonFatal(e) =>
        streamDeathCause = new ContinuousQueryException(
          this,
          s"Query $name terminated with exception: ${e.getMessage}",
          e,
          Some(committedOffsets.toCompositeOffset(sources)))
        logError(s"Query $name terminated with error", e)
    } finally {
      state = TERMINATED
      sqlContext.streams.notifyQueryTermination(StreamExecution.this)
      postEvent(new QueryTerminated(this))
      terminationLatch.countDown()
    }
  }

  /**
   * Populate the start offsets to start the execution at the current offsets stored in the sink
   * (i.e. avoid reprocessing data that we have already processed).
   */
  private def populateStartOffsets(): Unit = {
    offsetLog.getLatest() match {
      case Some((batchId, nextOffsets: CompositeOffset)) =>
        logError(s"Resuming continuous query, starting with batch $batchId")
        currentBatchId = batchId + 1
        nextOffsets.toStreamProgress(sources, availableOffsets)
        logError(s"Found possibly uncommitted offsets $availableOffsets")

        logError(s"Attempting to restore ${batchId - 1}")
        offsetLog.get(batchId - 1).foreach {
          case lastOffsets: CompositeOffset =>
            lastOffsets.toStreamProgress(sources, committedOffsets)
            logError(s"Resuming with committed offsets: $committedOffsets")
        }

      case None => // We are starting this stream for the first time.
        logError(s"Starting new continuous query.")
        currentBatchId = 0
        commitAndConstructNextBatch()

      case Some((_, offset)) =>
        sys.error(s"Invalid offset $offset")
    }
  }

  def dataAvailable: Boolean = {
    availableOffsets.exists {
      case (source, available) =>
        committedOffsets
            .get(source)
            .map(committed => committed < available)
            .getOrElse(true)
    }
  }

  /**
   *
   */
  def commitAndConstructNextBatch(): Boolean = committedOffsets.synchronized {
    // Update committed offsets.
    availableOffsets.foreach(committedOffsets.update)

    // Check to see what new data is available.
    uniqueSources.foreach { source =>
      source.getOffset.foreach(availableOffsets.update(source, _))
    }

    if (dataAvailable) {
      logError(s"Commiting offsets for batch $currentBatchId.")
      assert(
        offsetLog.add(currentBatchId, availableOffsets.toCompositeOffset(sources)),
        "Concurrent update to the log.  Multiple streaming jobs detected.")
      currentBatchId += 1
      true
    } else {
      false
    }
  }

  /**
   * Checks to see if any new data is present in any of the sources. When new data is available,
   * a batch is executed and passed to the sink, updating the currentOffsets.
   */
  private def attemptBatch(): Unit = {
    val startTime = System.nanoTime()

    val newData = availableOffsets.flatMap {
      case (source, available) if committedOffsets.get(source).map(_ < available).getOrElse(true) =>
        val current = committedOffsets.get(source)
        val batch = source.getBatch(current, available)
        logError(s"Retrieving data from $source: $current -> $available")
        Some(source -> batch)
      case _ => None
    }.toMap

    // A list of attributes that will need to be updated.
    var replacements = new ArrayBuffer[(Attribute, Attribute)]
    // Replace sources in the logical plan with data that has arrived since the last batch.
    val withNewSources = logicalPlan transform {
      case StreamingRelation(source, output) =>
        newData.get(source).map { data =>
          val newPlan = data.logicalPlan
          assert(output.size == newPlan.output.size,
            s"Invalid batch: ${output.mkString(",")} != ${newPlan.output.mkString(",")}")
          replacements ++= output.zip(newPlan.output)
          newPlan
        }.getOrElse {
          LocalRelation(output)
        }
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    val replacementMap = AttributeMap(replacements)
    val newPlan = withNewSources transformAllExpressions {
      case a: Attribute if replacementMap.contains(a) => replacementMap(a)
    }

    val optimizerStart = System.nanoTime()

    lastExecution = new QueryExecution(sqlContext, newPlan)
    val executedPlan = lastExecution.executedPlan
    val optimizerTime = (System.nanoTime() - optimizerStart).toDouble / 1000000
    logDebug(s"Optimized batch in ${optimizerTime}ms")

    val nextBatch = Dataset.newDataFrame(sqlContext, newPlan)
    sink.addBatch(currentBatchId - 1, nextBatch)

    awaitBatchLock.synchronized {
      // Wake up any threads that are waiting for the stream to progress.
      awaitBatchLock.notifyAll()
    }

    val batchTime = (System.nanoTime() - startTime).toDouble / 1000000
    logInfo(s"Completed up to $availableOffsets in ${batchTime}ms")
    postEvent(new QueryProgress(this))
  }

  private def postEvent(event: ContinuousQueryListener.Event) {
    sqlContext.streams.postListenerEvent(event)
  }

  /**
   * Signals to the thread executing micro-batches that it should stop running after the next
   * batch. This method blocks until the thread stops running.
   */
  override def stop(): Unit = {
    // Set the state to TERMINATED so that the batching thread knows that it was interrupted
    // intentionally
    state = TERMINATED
    if (microBatchThread.isAlive) {
      microBatchThread.interrupt()
      microBatchThread.join()
    }
    logInfo(s"Query $name was stopped")
  }

  /**
   * Blocks the current thread until processing for data from the given `source` has reached at
   * least the given `Offset`. This method is indented for use primarily when writing tests.
   */
  def awaitOffset(source: Source, newOffset: Offset): Unit = {
    def notDone = committedOffsets.synchronized {
      !committedOffsets.contains(source) || committedOffsets(source) < newOffset
    }

    while (notDone) {
      logInfo(s"Waiting until $newOffset at $source")
      awaitBatchLock.synchronized { awaitBatchLock.wait(100) }
    }
    logDebug(s"Unblocked at $newOffset for $source")
  }

  override def awaitTermination(): Unit = {
    if (state == INITIALIZED) {
      throw new IllegalStateException("Cannot wait for termination on a query that has not started")
    }
    terminationLatch.await()
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
  }

  override def awaitTermination(timeoutMs: Long): Boolean = {
    if (state == INITIALIZED) {
      throw new IllegalStateException("Cannot wait for termination on a query that has not started")
    }
    require(timeoutMs > 0, "Timeout has to be positive")
    terminationLatch.await(timeoutMs, TimeUnit.MILLISECONDS)
    if (streamDeathCause != null) {
      throw streamDeathCause
    } else {
      !isActive
    }
  }

  override def toString: String = {
    s"Continuous Query - $name [state = $state]"
  }

  def toDebugString: String = {
    val deathCauseStr = if (streamDeathCause != null) {
      "Error:\n" + stackTraceToString(streamDeathCause.cause)
    } else ""
    s"""
       |=== Continuous Query ===
       |Name: $name
       |Current Offsets: $committedOffsets
       |
       |Current State: $state
       |Thread State: ${microBatchThread.getState}
       |
       |Logical Plan:
       |$logicalPlan
       |
       |$deathCauseStr
     """.stripMargin
  }

  trait State
  case object INITIALIZED extends State
  case object ACTIVE extends State
  case object TERMINATED extends State
}

private[sql] object StreamExecution {
  private val nextId = new AtomicInteger()

  def nextName: String = s"query-${nextId.getAndIncrement}"
}
