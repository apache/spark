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

package org.apache.spark.sql.pipelines.graph

import java.util.concurrent.{ConcurrentHashMap, Semaphore}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.internal.{LogKeys, MDC}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.graph.TriggeredGraphExecution._
import org.apache.spark.sql.pipelines.util.ExponentialBackoffStrategy
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * Executes all of the flows in the given graph in topological order. Each flow processes
 * all available data before downstream flows are triggered.
 *
 * @param graphForExecution the graph to execute.
 * @param env the context in which the graph is executed.
 * @param onCompletion a callback to execute after all streams are done. The boolean
 *                     argument is true if the execution was successful.
 * @param clock a clock used to determine the time of execution.
 */
class TriggeredGraphExecution(
    graphForExecution: DataflowGraph,
    env: PipelineUpdateContext,
    onCompletion: RunTerminationReason => Unit = _ => (),
    clock: Clock = new SystemClock()
) extends GraphExecution(graphForExecution, env) {

  /**
   * [Visible for testing] A map to store stream state of all flows which should be materialized.
   * This includes flows whose streams have not yet been started, ie they are queued or have been
   * marked as skipped.
   */
  private[pipelines] val pipelineState = {
    new ConcurrentHashMap[TableIdentifier, StreamState]().asScala
  }

  /**
   * Keeps track of flow failure information required for retry logic.
   * This only contains values for flows that either failed previously or are currently in the
   * failed state.
   */
  private val failureTracker = {
    new ConcurrentHashMap[TableIdentifier, TriggeredFailureInfo]().asScala
  }

  /** Back-off strategy used to determine duration between retries. */
  private val backoffStrategy = ExponentialBackoffStrategy(
    maxTime = (env.spark.sessionState.conf.watchdogMaxRetryTimeInSeconds * 1000).millis,
    stepSize = (env.spark.sessionState.conf.watchdogMinRetryTimeInSeconds * 1000).millis
  )

  override def streamTrigger(flow: Flow): Trigger = {
    Trigger.AvailableNow()
  }

  /** The control thread responsible for topologically executing flows. */
  private var topologicalExecutionThread: Option[Thread] = None

  private def buildTopologicalExecutionThread(): Thread = {
    new Thread("Topological Execution") {
      override def run(): Unit = {
        try {
          topologicalExecution()
        } finally {
          TriggeredGraphExecution.super.stop()
        }
      }
    }
  }

  override def start(): Unit = {
    super.start()
    // If tablesToUpdate is empty, queue all flows; Otherwise, queue flows for which the
    // destination tables are specified in tablesToUpdate.
    env.refreshFlows
      .filter(graphForExecution.materializedFlows)
      .foreach { f =>
        env.flowProgressEventLogger.recordQueued(f)
        pipelineState.put(f.identifier, StreamState.QUEUED)
      }
    env.refreshFlows
      .filterNot(graphForExecution.materializedFlows)
      .foreach { f =>
        env.flowProgressEventLogger.recordExcluded(f)
        pipelineState.put(f.identifier, StreamState.EXCLUDED)
      }
    val thread = buildTopologicalExecutionThread()
    UncaughtExceptionHandler.addHandler(
      thread, {
        case _: InterruptedException =>
        case _ =>
          try {
            stopInternal(stopTopologicalExecutionThread = false)
          } catch {
            case ex: Throwable =>
              logError(s"Exception thrown while stopping the update...", ex)
          } finally {
            onCompletion(UnexpectedRunFailure())
          }
      }
    )
    thread.start()
    topologicalExecutionThread = Option(thread)
  }

  /** Used to control how many flows are executing at once. */
  private val concurrencyLimit: Semaphore = new Semaphore(
    env.spark.sessionState.conf.maxConcurrentFlows
  )

  /**
   * Runs the pipeline in a topological order.
   *
   * Non-accepting states: Queued, Running
   * Accepting states: Successful, TerminatedWithError, Skipped, Cancelled, Excluded
   * All [[Flow]]s which can write to a stream begin in a queued state. The following state
   * transitions describe the topological execution of a [[DataflowGraph]].
   *
   * Queued -> Running if Flow has no parents or the parent tables of the queued [[Flow]]
   *   have run successfully.
   * Running -> Successful if the stream associated with the [[Flow]] succeeds.
   * Running -> TerminatedWithError if the stream associated with the [[Flow]] stops with an
   *   exception.
   *
   * Non-fatally failed flows are retried with exponential back-off a bounded no. of times.
   * If a flow cannot be retried, all downstream flows of the failed flow are moved to Skipped
   * state.
   * Running -> Cancelled if the stream associated with the [[Flow]] is stopped mid-run by
   * calling `stop`. All remaining [[Flow]]s in queue are moved to state Skipped.
   *
   * The execution is over once there are no [[Flow]]s left running or in the queue.
   */
  private def topologicalExecution(): Unit = {
    // Done executing once no flows remain running or in queue
    def allFlowsDone = {
      flowsWithState(StreamState.QUEUED).isEmpty && flowsWithState(StreamState.RUNNING).isEmpty &&
      flowsQueuedForRetry().isEmpty
    }

    // LinkedHashSet returns elements in the order inserted. This ensures that flows queued but
    // unable to run because we are at max concurrent execution will get priority on the next round.
    val runnableFlows: mutable.LinkedHashSet[TableIdentifier] = new mutable.LinkedHashSet()

    while (!Thread.interrupted() && !allFlowsDone) {
      // Since queries are managed by FlowExecutions, so update state based on [[FlowExecution]]s.
      flowsWithState(StreamState.RUNNING).foreach { flowIdentifier =>
        flowExecutions(flowIdentifier) match {
          case f if !f.isCompleted => // Nothing to be done; let this stream continue.
          case f if f.isCompleted && f.exception.isEmpty =>
            recordSuccess(flowIdentifier)
          case f =>
            recordFailed(flowIdentifier = flowIdentifier, e = f.exception.get)
        }
      }

      // Log info on if we're leaking Semaphore permits. Synchronize here so we don't double-count
      // or mis-count because a batch flow is finishing asynchronously.
      val (runningFlows, availablePermits) = concurrencyLimit.synchronized {
        (flowsWithState(StreamState.RUNNING).size, concurrencyLimit.availablePermits)
      }
      if ((runningFlows + availablePermits) < env.spark.sessionState.conf.maxConcurrentFlows) {
        val errorStr =
          s"The max concurrency is ${env.spark.sessionState.conf.maxConcurrentFlows}, but " +
          s"there are only $availablePermits permits available with $runningFlows flows running. " +
          s"If this happens consistently, it's possible we're leaking permits."
        logError(errorStr)
        if (Utils.isTesting) {
          throw new IllegalStateException(errorStr)
        }
      }

      // All flows which can potentially be run now if their parent tables have successfully
      // completed or have been excluded.
      val queuedForRetry =
        flowsQueuedForRetry().filter(nextRetryTime(_) <= clock.getTimeMillis())
      // Take flows that have terminated but have retry attempts left and flows that are queued, and
      // filter the ones whose parents have all successfully completed, excluded, or idled because
      // they are ONCE flows which already ran.
      runnableFlows ++= (queuedForRetry ++ flowsWithState(StreamState.QUEUED)).filter { id =>
        graphForExecution
          .upstreamFlows(id)
          .intersect(graphForExecution.materializedFlowIdentifiers)
          .forall { id =>
            pipelineState(id) == StreamState.SUCCESSFUL ||
            pipelineState(id) == StreamState.EXCLUDED ||
            pipelineState(id) == StreamState.IDLE
          }
      }

      // collect flow that are ready to start
      val flowsToStart = mutable.ArrayBuffer[ResolvedFlow]()
      while (runnableFlows.nonEmpty && concurrencyLimit.tryAcquire()) {
        val flowIdentifier = runnableFlows.head
        runnableFlows.remove(flowIdentifier)
        flowsToStart.append(graphForExecution.resolvedFlow(flowIdentifier))
      }

      def startFlow(flow: ResolvedFlow): Unit = {
        val flowIdentifier = flow.identifier
        logInfo(log"Starting flow ${MDC(LogKeys.FLOW_NAME, flow.identifier)}")
        env.flowProgressEventLogger.recordPlanningForBatchFlow(flow)
        try {
          val flowStarted = planAndStartFlow(flow)
          if (flowStarted.nonEmpty) {
            pipelineState.put(flowIdentifier, StreamState.RUNNING)
            logInfo(log"Flow ${MDC(LogKeys.FLOW_NAME, flowIdentifier)} started.")
          } else {
            if (flow.once) {
              // ONCE flows are marked as IDLE in the event buffer for consistency with continuous
              // execution where all unstarted flows are IDLE.
              env.flowProgressEventLogger.recordIdle(flow)
              pipelineState.put(flowIdentifier, StreamState.IDLE)
              concurrencyLimit.release()
            } else {
              env.flowProgressEventLogger.recordSkipped(flow)
              concurrencyLimit.release()
              pipelineState.put(flowIdentifier, StreamState.SKIPPED)
            }
          }
        } catch {
          case NonFatal(ex) => recordFailed(flowIdentifier, ex)
        }
      }

      // start each flow serially
      flowsToStart.foreach(startFlow)

      try {
        // Put thread to sleep for the configured polling interval to avoid busy-waiting
        // and holding one CPU core.
        Thread.sleep(env.spark.sessionState.conf.streamStatePollingInterval * 1000)
      } catch {
        case _: InterruptedException => return
      }
    }
    if (allFlowsDone) {
      onCompletion(getRunTerminationReason)
    }
  }

  /** Record the specified flow as successful. */
  private def recordSuccess(flowIdentifier: TableIdentifier): Unit = {
    concurrencyLimit.synchronized {
      concurrencyLimit.release()
      pipelineState.put(flowIdentifier, StreamState.SUCCESSFUL)
    }
    logInfo(
      log"Flow ${MDC(LogKeys.FLOW_NAME, flowIdentifier)} has COMPLETED " +
      log"in TriggeredFlowExecution."
    )
  }

  /**
   * Record the specified flow as failed and any downstream flows as failed.
   *
   * @param e The error that caused the query to fail.
   */
  private def recordFailed(
      flowIdentifier: TableIdentifier,
      e: Throwable
  ): Unit = {
    logError(log"Flow ${MDC(LogKeys.FLOW_NAME, flowIdentifier)} failed", e)
    concurrencyLimit.synchronized {
      concurrencyLimit.release()
      pipelineState.put(flowIdentifier, StreamState.TERMINATED_WITH_ERROR)
    }
    val prevFailureCount = failureTracker.get(flowIdentifier).map(_.numFailures).getOrElse(0)
    val flow = graphForExecution.resolvedFlow(flowIdentifier)

    failureTracker.put(
      flowIdentifier,
      TriggeredFailureInfo(
        lastFailTimestamp = clock.getTimeMillis(),
        numFailures = prevFailureCount + 1,
        lastException = e,
        lastExceptionAction = GraphExecution.determineFlowExecutionActionFromError(
          ex = e,
          flowDisplayName = flow.displayName,
          currentNumTries = prevFailureCount + 1,
          maxAllowedRetries = maxRetryAttemptsForFlow(flowIdentifier)
        )
      )
    )
    if (graphForExecution.resolvedFlow(flow.identifier).df.isStreaming) {
      // Batch query failure log comes from the batch execution thread.
      PipelinesErrors.checkStreamingErrorsAndRetry(
        ex = e,
        env = env,
        graphExecution = this,
        flow = flow,
        shouldRethrow = false,
        prevFailureCount = prevFailureCount,
        maxRetries = maxRetryAttemptsForFlow(flowIdentifier),
        onRetry = {
          env.flowProgressEventLogger.recordFailed(
            flow = flow,
            exception = e,
            logAsWarn = true
          )
        }
      )
    }

    // Don't skip downstream outputs yet if this flow still has retries left and didn't fail
    // fatally.
    if (!flowsQueuedForRetry().contains(flowIdentifier)) {
      graphForExecution
        .downstreamFlows(flowIdentifier)
        .intersect(graphForExecution.materializedFlowIdentifiers)
        .foreach(recordSkippedIfSelected)
    }
  }

  /**
   * Record the specified flow as skipped. This is no-op if the flow is already excluded in the
   * refresh selection.
   */
  private def recordSkippedIfSelected(flowIdentifier: TableIdentifier): Unit = {
    if (pipelineState(flowIdentifier) != StreamState.EXCLUDED) {
      val flow = graphForExecution.resolvedFlow(flowIdentifier)
      pipelineState.put(flowIdentifier, StreamState.SKIPPED)
      logWarning(
        log"Flow ${MDC(LogKeys.FLOW_NAME, flowIdentifier)} SKIPPED due " +
        log"to upstream failure(s)."
      )
      env.flowProgressEventLogger.recordSkippedOnUpStreamFailure(flow)
    }
  }

  private def flowsWithState(state: StreamState): Set[TableIdentifier] = {
    pipelineState
      .filter {
        case (_, flowState) => flowState == state
      }
      .keySet
      .toSet
  }

  /** Set of flows which have failed, but can be queued again for a retry. */
  private def flowsQueuedForRetry(): Set[TableIdentifier] = {
    flowsWithState(StreamState.TERMINATED_WITH_ERROR).filter(!failureTracker(_).nonRetryable)
  }

  /** Earliest time at which flow can be run next. */
  private def nextRetryTime(flowIdentifier: TableIdentifier): Long = {
    failureTracker
      .get(flowIdentifier)
      .map { failureInfo =>
        failureInfo.lastFailTimestamp +
        backoffStrategy.waitDuration(failureInfo.numFailures).toMillis
      }
      .getOrElse(-1)
  }

  private def stopInternal(stopTopologicalExecutionThread: Boolean): Unit = {
    super.stop()
    if (stopTopologicalExecutionThread) {
      topologicalExecutionThread.filter(_.isAlive).foreach { t =>
        t.interrupt()
        stopThread(t)
      }
    }
    flowsWithState(StreamState.QUEUED).foreach(recordSkippedIfSelected)

    val flowsFailedToStop = ThreadUtils
      .parmap(flowsWithState(StreamState.RUNNING).toSeq, "stop-flow", maxThreads = 10) { flowName =>
        pipelineState.put(flowName, StreamState.CANCELED)
        flowExecutions.get(flowName).map { f =>
          (
            f.identifier,
            Try(stopFlow(f))
          )
        }
      }
      .filter(_.nonEmpty)
      .filter(_.get._2.isFailure)
      .map(_.get._1)

    if (flowsFailedToStop.nonEmpty) {
      throw RunTerminationException(FailureStoppingFlow(flowsFailedToStop))
    }
  }

  override def awaitCompletion(): Unit = {
    topologicalExecutionThread.foreach(_.join)
  }

  override def stop(): Unit = { stopInternal(stopTopologicalExecutionThread = true) }

  override def getRunTerminationReason: RunTerminationReason = {
    val success =
      pipelineState.valuesIterator.forall(TERMINAL_NON_FAILURE_STREAM_STATES.contains)
    if (success) {
      return RunCompletion()
    }

    val executionFailureOpt = failureTracker.iterator
      .map {
        case (flowIdentifier, failureInfo) =>
          (
            graphForExecution.flow(flowIdentifier),
            failureInfo.lastException,
            failureInfo.lastExceptionAction
          )
      }
      .collectFirst {
        case (_, _, GraphExecution.StopFlowExecution(reason)) =>
          reason.runTerminationReason
      }

    executionFailureOpt.getOrElse(UnexpectedRunFailure())
  }
}

case class TriggeredFailureInfo(
    lastFailTimestamp: Long,
    numFailures: Int,
    lastException: Throwable,
    lastExceptionAction: GraphExecution.FlowExecutionAction) {

  // Whether this failure can be retried or not with flow execution.
  lazy val nonRetryable: Boolean = {
    lastExceptionAction.isInstanceOf[GraphExecution.StopFlowExecution]
  }
}

object TriggeredGraphExecution {

  // All possible states of a data stream for a flow
  sealed trait StreamState
  object StreamState {
    // Stream is waiting on its parent tables to successfully finish processing
    // data to start running, in triggered execution
    case object QUEUED extends StreamState

    // Stream is processing data
    case object RUNNING extends StreamState

    // Stream excluded if it's not selected in the partial graph update API call.
    case object EXCLUDED extends StreamState

    // Stream will not be rerun because it is a ONCE flow.
    case object IDLE extends StreamState

    // Stream will not be run due to parent tables not finishing successfully in triggered execution
    case object SKIPPED extends StreamState

    // Stream has been stopped with a fatal error
    case object TERMINATED_WITH_ERROR extends StreamState

    // Stream stopped before completion in triggered execution
    case object CANCELED extends StreamState

    // Stream successfully processed all available data in triggered execution
    case object SUCCESSFUL extends StreamState
  }

  /**
   * List of terminal states which we don't consider as failures.
   *
   * An update was successful if all rows either updated successfully or were skipped (if they
   * didn't have any data to process) or excluded (if they were not selected in a refresh
   * selection.)
   */
  private val TERMINAL_NON_FAILURE_STREAM_STATES: Set[StreamState] = Set(
    StreamState.SUCCESSFUL,
    StreamState.SKIPPED,
    StreamState.EXCLUDED,
    StreamState.IDLE
  )
}
