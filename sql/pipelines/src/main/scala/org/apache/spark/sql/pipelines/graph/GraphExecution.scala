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
import java.util.concurrent.{ConcurrentHashMap, TimeoutException}

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.logging.StreamListener
import org.apache.spark.sql.streaming.Trigger

abstract class GraphExecution(
    val graphForExecution: DataflowGraph,
    env: PipelineUpdateContext
) extends Logging {

  /** The `Trigger` configuration for a streaming flow. */
  def streamTrigger(flow: Flow): Trigger

  /** Maps flow identifier to count of consecutive failures. Used to manage flow retries */
  private val flowToNumConsecutiveFailure = new ConcurrentHashMap[TableIdentifier, Int].asScala

  /** Maps flow identifier to count of successful runs. Used to populate batch id. */
  private val flowToNumSuccess = new ConcurrentHashMap[TableIdentifier, Long].asScala

  /**
   * `FlowExecution`s currently being executed and tracked by the graph execution.
   */
  val flowExecutions = new collection.concurrent.TrieMap[TableIdentifier, FlowExecution]

  /** Increments flow execution retry count for `flow`. */
  private def incrementFlowToNumConsecutiveFailure(flowIdentifier: TableIdentifier): Unit = {
    flowToNumConsecutiveFailure.put(flowIdentifier, flowToNumConsecutiveFailure(flowIdentifier) + 1)
  }

  /**
   * Planner use to convert each logical dataflow (i.e., `Flow`) defined in the
   * `DataflowGraph` into a concrete execution plan `FlowExecution` used by the
   * pipeline execution.
   */
  private val flowPlanner = new FlowPlanner(
    graph = graphForExecution,
    updateContext = env,
    triggerFor = streamTrigger
  )

  /** Listener to process streaming events and metrics. */
  private val streamListener = new StreamListener(env, graphForExecution)

  /**
   * Plans the logical `ResolvedFlow` into a `FlowExecution` and then starts executing it.
   * Implementation note: Thread safe
   *
   * @return None if the flow planner decided that there is no actual update required here.
   *         Otherwise returns the corresponding physical flow.
   */
  def planAndStartFlow(flow: ResolvedFlow): Option[FlowExecution] = {
    try {
      val flowExecution = flowPlanner.plan(
        flow = graphForExecution.resolvedFlow(flow.identifier)
      )

      env.flowProgressEventLogger.recordStart(flowExecution)

      flowExecution.executeAsync()
      flowExecutions.put(flow.identifier, flowExecution)
      implicit val ec: ExecutionContext = flowExecution.executionContext

      // Note: The asynchronous handling here means that completed events might be recorded after
      // initializing events for the next retry of this flow.
      flowExecution.getFuture.onComplete {
        case Failure(ex) if !flowExecution.isStreaming =>
          incrementFlowToNumConsecutiveFailure(flow.identifier)
          env.flowProgressEventLogger.recordFailed(
            flow = flow,
            exception = ex,
            // Log as warn if flow has retries left
            logAsWarn = {
              flowToNumConsecutiveFailure(flow.identifier) <
              1 + maxRetryAttemptsForFlow(flow.identifier)
            }
          )
        case Success(ExecutionResult.STOPPED) =>
        // We already recorded a STOPPED event in [[FlowExecution.stopFlow()]].
        // We don't need to log another one here.
        case Success(ExecutionResult.FINISHED) if !flowExecution.isStreaming =>
          // Reset consecutive failure count on success
          flowToNumConsecutiveFailure.put(flow.identifier, 0)
          flowToNumSuccess.put(
            flow.identifier,
            flowToNumSuccess.getOrElse(flow.identifier, 0L) + 1L
          )
          env.flowProgressEventLogger.recordCompletion(flow)
        case _ => // Handled by StreamListener
      }
      Option(flowExecution)
    } catch {
      // This is if the flow fails to even start.
      case ex: Throwable =>
        logError(
          log"Unhandled exception while starting flow:${MDC(LogKeys.FLOW_NAME, flow.displayName)}",
          ex
        )
        // InterruptedException is thrown when the thread executing `startFlow` is interrupted.
        if (ex.isInstanceOf[InterruptedException]) {
          env.flowProgressEventLogger.recordStop(flow)
        } else {
          env.flowProgressEventLogger.recordFailed(
            flow = flow,
            exception = ex,
            logAsWarn = false
          )
        }
        throw ex
    }
  }

  /**
   * Starts the execution of flows in `graphForExecution`. Does not block.
   */
  def start(): Unit = {
    env.spark.listenerManager.clear()
    env.spark.streams.addListener(streamListener)
  }

  /**
   * Stops this execution by stopping all streams and terminating any other resources.
   *
   * This method may be called multiple times due to race conditions and must be idempotent.
   */
  def stop(): Unit = {
    env.spark.streams.removeListener(streamListener)
  }

  /** Stops execution of a `FlowExecution`. */
  def stopFlow(pf: FlowExecution): Unit = {
    if (!pf.isCompleted) {
      val flow = graphForExecution.resolvedFlow(pf.identifier)
      try {
        logInfo(log"Stopping ${MDC(LogKeys.FLOW_NAME, pf.identifier)}")
        pf.stop()
      } catch {
        case e: Throwable =>
          val message = s"Error stopping flow ${pf.identifier}"
          logError(message, e)
          env.flowProgressEventLogger.recordFailed(
            flow = flow,
            exception = e,
            logAsWarn = false,
            messageOpt = Option(s"Flow '${pf.displayName}' has failed to stop.")
          )
          throw e
      }
      env.flowProgressEventLogger.recordStop(flow)
      logInfo(log"Stopped ${MDC(LogKeys.FLOW_NAME, pf.identifier)}")
    } else {
      logWarning(
        log"Flow ${MDC(LogKeys.FLOW_NAME, pf.identifier)} was not stopped because it " +
        log"was already completed. Exception: ${MDC(LogKeys.EXCEPTION, pf.exception)}"
      )
    }
  }

  /**
   * Blocks the current thread while any flows are queued or running. Returns when all flows that
   * could be run have completed. When this returns, all flows are either SUCCESSFUL,
   * TERMINATED_WITH_ERROR, SKIPPED, CANCELED, or EXCLUDED.
   */
  def awaitCompletion(): Unit

  /**
   * Returns the reason why this flow execution has terminated.
   * If the function is called before the flow has not terminated yet, the behavior is undefined,
   * and may return `UnexpectedRunFailure`.
   */
  def getRunTerminationReason: RunTerminationReason

  def maxRetryAttemptsForFlow(flowName: TableIdentifier): Int = {
    val flow = graphForExecution.flow(flowName)
    flow.sqlConf
      .get(SQLConf.PIPELINES_MAX_FLOW_RETRY_ATTEMPTS.key)
      .map(_.toInt) // Flow-level conf
      // Pipeline-level conf, else default flow retry limit
      .getOrElse(env.spark.sessionState.conf.maxFlowRetryAttempts)
  }

  /**
   * Stop a thread timeout.
   */
  def stopThread(thread: Thread): Unit = {
    // Don't wait to join if current thread is the thread to stop
    if (thread.getId != Thread.currentThread().getId) {
      thread.join(env.spark.sessionState.conf.timeoutMsForTerminationJoinAndLock)
      // thread is alive after we join.
      if (thread.isAlive) {
        throw new TimeoutException("Failed to stop the update due to a hanging control thread.")
      }
    }
  }
}

object GraphExecution extends Logging {

  // Set of states after checking the exception for flow execution retryability analysis.
  sealed trait FlowExecutionAction

  /** Indicates that the flow execution should be retried. */
  case object RetryFlowExecution extends FlowExecutionAction

  /** Indicates that the flow execution should be stopped with a specific reason. */
  case class StopFlowExecution(reason: FlowExecutionStopReason) extends FlowExecutionAction

  /** Represents the reason why a flow execution should be stopped. */
  sealed trait FlowExecutionStopReason {
    def cause: Throwable
    def flowDisplayName: String
    def runTerminationReason: RunTerminationReason
    def failureMessage: String
    // If true, we record this flow execution as STOPPED with a WARNING instead a FAILED with ERROR.
    def warnInsteadOfError: Boolean = false
  }

  /**
   * Represents the `FlowExecution` should be stopped due to it failed with some retryable errors
   * and has exhausted all the retry attempts.
   */
  private case class MaxRetryExceeded(
      cause: Throwable,
      flowDisplayName: String,
      maxAllowedRetries: Int
  ) extends FlowExecutionStopReason {
    override lazy val runTerminationReason: RunTerminationReason = {
      QueryExecutionFailure(flowDisplayName, maxAllowedRetries, Option(cause))
    }
    override lazy val failureMessage: String = {
      s"Flow '$flowDisplayName' has FAILED more than $maxAllowedRetries times and will not be " +
      s"restarted."
    }
  }

  /**
   * Analyze the exception thrown by flow execution and figure out if we should retry the execution,
   * or we need to reanalyze the flow entirely to resolve issues like schema changes.
   * This should be the narrow waist for all exception analysis in flow execution.
   * TODO: currently it only handles schema change and max retries, we should aim to extend this to
   *  include other non-retryable exception as well so we can have a single SoT for all these error
   *  matching logic.
   * @param ex Exception to analyze.
   * @param flowDisplayName The user facing flow name with the error.
   * @param currentNumTries Number of times the flow has been tried.
   * @param maxAllowedRetries Maximum number of retries allowed for the flow.
   */
  def determineFlowExecutionActionFromError(
      ex: => Throwable,
      flowDisplayName: => String,
      currentNumTries: => Int,
      maxAllowedRetries: => Int
  ): FlowExecutionAction = {
    val flowExecutionNonRetryableReasonOpt = if (currentNumTries > maxAllowedRetries) {
      Some(MaxRetryExceeded(ex, flowDisplayName, maxAllowedRetries))
    } else {
      None
    }

    if (flowExecutionNonRetryableReasonOpt.isDefined) {
      StopFlowExecution(flowExecutionNonRetryableReasonOpt.get)
    } else {
      RetryFlowExecution
    }
  }
}
