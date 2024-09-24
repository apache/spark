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

package org.apache.spark.sql.connect.execution

import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.protobuf.Message
import org.apache.commons.lang3.StringUtils

import org.apache.spark.SparkSQLException
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.common.ProtoUtils
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.{ExecuteHolder, ExecuteSessionTag, SparkConnectService}
import org.apache.spark.sql.connect.utils.ErrorUtils
import org.apache.spark.util.Utils

/**
 * This class launches the actual execution in an execution thread. The execution pushes the
 * responses to a ExecuteResponseObserver in executeHolder.
 */
private[connect] class ExecuteThreadRunner(executeHolder: ExecuteHolder) extends Logging {

  /** The thread state. */
  private val state: AtomicInteger = new AtomicInteger(ThreadState.notStarted)

  // The newly created thread will inherit all InheritableThreadLocals used by Spark,
  // e.g. SparkContext.localProperties. If considering implementing a thread-pool,
  // forwarding of thread locals needs to be taken into account.
  private val executionThread: ExecutionThread = new ExecutionThread()

  /**
   * Launches the execution in a background thread, returns immediately. This method is expected
   * to be invoked only once for an ExecuteHolder.
   */
  private[connect] def start(): Unit = {
    val currentState = state.getAcquire()
    if (currentState == ThreadState.notStarted) {
      executionThread.start()
    } else {
      // This assertion does not hold if it is called more than once.
      assert(currentState == ThreadState.interrupted)
    }
  }

  /**
   * Interrupts the execution thread if the execution has been interrupted by this method call.
   *
   * @return
   *   true if the thread is running and interrupted.
   */
  private[connect] def interrupt(): Boolean = {
    var currentState = state.getAcquire()
    while (currentState == ThreadState.notStarted || currentState == ThreadState.started) {
      val newState = if (currentState == ThreadState.notStarted) {
        ThreadState.interrupted
      } else {
        ThreadState.startedInterrupted
      }

      val prevState = state.compareAndExchangeRelease(currentState, newState)
      if (prevState == currentState) {
        if (prevState == ThreadState.notStarted) {
          // The execution thread has not been started, or will immediately return because the state
          // transition happens at the beginning of executeInternal.
          try {
            ErrorUtils.handleError(
              "execute",
              executeHolder.responseObserver,
              executeHolder.sessionHolder.userId,
              executeHolder.sessionHolder.sessionId,
              Some(executeHolder.eventsManager),
              true)(new SparkSQLException("OPERATION_CANCELED", Map.empty))
          } finally {
            executeHolder.cleanup()
          }
        } else {
          // Interrupt execution.
          executionThread.interrupt()
        }
        return true
      }
      currentState = prevState
    }

    // Already interrupted, completed, or not started.
    false
  }

  private def execute(): Unit = {
    // Outer execute handles errors.
    // Separate it from executeInternal to save on indent and improve readability.
    try {
      try {
        executeInternal()
      } catch {
        // Need to catch throwable instead of NonFatal, because e.g. InterruptedException is fatal.
        case e: Throwable =>
          logDebug(s"Exception in execute: $e")
          // Always cancel all remaining execution after error.
          executeHolder.sessionHolder.session.sparkContext.cancelJobsWithTag(
            executeHolder.jobTag,
            s"A job with the same tag ${executeHolder.jobTag} has failed.")
          // Rethrow the original error.
          throw e
      } finally {
        executeHolder.sessionHolder.session.sparkContext.removeJobTag(executeHolder.jobTag)
        SparkConnectService.executionListener.foreach(_.removeJobTag(executeHolder.jobTag))
        executeHolder.sparkSessionTags.foreach { tag =>
          executeHolder.sessionHolder.session.sparkContext.removeJobTag(
            ExecuteSessionTag(
              executeHolder.sessionHolder.userId,
              executeHolder.sessionHolder.sessionId,
              tag))
        }
      }
    } catch {
      case e: Throwable if state.getAcquire() != ThreadState.startedInterrupted =>
        ErrorUtils.handleError(
          "execute",
          executeHolder.responseObserver,
          executeHolder.sessionHolder.userId,
          executeHolder.sessionHolder.sessionId,
          Some(executeHolder.eventsManager),
          false)(e)
    } finally {
      // Make sure to transition to completed in order to prevent the thread from being interrupted
      // afterwards.
      var currentState = state.getAcquire()
      while (currentState == ThreadState.started ||
        currentState == ThreadState.startedInterrupted) {
        val interrupted = currentState == ThreadState.startedInterrupted
        val prevState = state.compareAndExchangeRelease(currentState, ThreadState.completed)
        if (prevState == currentState) {
          if (interrupted) {
            try {
              ErrorUtils.handleError(
                "execute",
                executeHolder.responseObserver,
                executeHolder.sessionHolder.userId,
                executeHolder.sessionHolder.sessionId,
                Some(executeHolder.eventsManager),
                true)(new SparkSQLException("OPERATION_CANCELED", Map.empty))
            } finally {
              executeHolder.cleanup()
            }
          }
          return
        }
        currentState = prevState
      }
    }
  }

  // Inner executeInternal is wrapped by execute() for error handling.
  private def executeInternal(): Unit = {
    val prevState = state.compareAndExchangeRelease(ThreadState.notStarted, ThreadState.started)
    if (prevState != ThreadState.notStarted) {
      // Silently return, expecting that the caller would handle the interruption.
      assert(prevState == ThreadState.interrupted)
      return
    }

    // `withSession` ensures that session-specific artifacts (such as JARs and class files) are
    // available during processing.
    executeHolder.sessionHolder.withSession { session =>
      val debugString = requestString(executeHolder.request)

      // Set tag for query cancellation
      session.sparkContext.addJobTag(executeHolder.jobTag)
      // Register the job for progress reports.
      SparkConnectService.executionListener.foreach(_.registerJobTag(executeHolder.jobTag))
      // Also set all user defined tags as Spark Job tags.
      executeHolder.sparkSessionTags.foreach { tag =>
        session.sparkContext.addJobTag(
          ExecuteSessionTag(
            executeHolder.sessionHolder.userId,
            executeHolder.sessionHolder.sessionId,
            tag))
      }
      session.sparkContext.setJobDescription(
        s"Spark Connect - ${StringUtils.abbreviate(debugString, 128)}")
      session.sparkContext.setInterruptOnCancel(true)

      // Add debug information to the query execution so that the jobs are traceable.
      session.sparkContext.setLocalProperty(
        "callSite.short",
        s"Spark Connect - ${StringUtils.abbreviate(debugString, 128)}")
      session.sparkContext.setLocalProperty(
        "callSite.long",
        StringUtils.abbreviate(debugString, 2048))

      executeHolder.request.getPlan.getOpTypeCase match {
        case proto.Plan.OpTypeCase.COMMAND => handleCommand(executeHolder.request)
        case proto.Plan.OpTypeCase.ROOT => handlePlan(executeHolder.request)
        case _ =>
          throw new UnsupportedOperationException(
            s"${executeHolder.request.getPlan.getOpTypeCase} not supported.")
      }

      val observedMetrics: Map[String, Seq[(Option[String], Any)]] = {
        executeHolder.observations.map { case (name, observation) =>
          val values = observation.getOrEmpty.map { case (key, value) =>
            (Some(key), value)
          }.toSeq
          name -> values
        }.toMap
      }
      val accumulatedInPython: Map[String, Seq[(Option[String], Any)]] = {
        executeHolder.sessionHolder.pythonAccumulator.flatMap { accumulator =>
          accumulator.synchronized {
            val value = accumulator.value.asScala.toSeq
            if (value.nonEmpty) {
              accumulator.reset()
              Some("__python_accumulator__" -> value.map(value => (None, value)))
            } else {
              None
            }
          }
        }.toMap
      }
      if (observedMetrics.nonEmpty || accumulatedInPython.nonEmpty) {
        executeHolder.responseObserver.onNext(
          SparkConnectPlanExecution
            .createObservedMetricsResponse(
              executeHolder.sessionHolder.sessionId,
              executeHolder.sessionHolder.serverSessionId,
              executeHolder.request.getPlan.getRoot.getCommon.getPlanId,
              observedMetrics ++ accumulatedInPython))
      }

      // State transition should be atomic to prevent a situation in which a client of reattachable
      // execution receives ResultComplete, and proceeds to send ReleaseExecute, and that triggers
      // an interrupt before it finishes. Failing to transition to completed means that the thread
      // was interrupted, and that will be checked at the end of the execution.
      if (state.compareAndExchangeRelease(
          ThreadState.started,
          ThreadState.completed) == ThreadState.started) {
        // Now, the execution cannot be interrupted.

        // If the request starts a long running iterator (e.g. StreamingQueryListener needs
        // a long-running iterator to continuously stream back events, it runs in a separate
        // thread, and holds the responseObserver to send back the listener events.)
        // In such cases, even after the ExecuteThread returns, we still want to keep the
        // client side iterator open, i.e. don't send the ResultComplete to the client.
        // So delegate the sending of the final ResultComplete to the listener thread itself.
        if (!shouldDelegateCompleteResponse(executeHolder.request)) {
          if (executeHolder.reattachable) {
            // Reattachable execution sends a ResultComplete at the end of the stream
            // to signal that there isn't more coming.
            executeHolder.responseObserver.onNextComplete(createResultComplete())
          } else {
            executeHolder.responseObserver.onCompleted()
          }
        }
      }
    }
  }

  /**
   * Perform a check to see if we should delegate sending ResultCompelete. Currently, the
   * ADD_LISTENER_BUS_LISTENER command creates a new thread and continuously streams back listener
   * events to the client side StreamingQueryListenerBus. In this case, we would like to delegate
   * the sending of the final ResultComplete to the handler thread itself.
   * @param request
   *   The request to check
   * @return
   *   True if we should delegate sending the final ResultComplete to the handler thread, i.e.
   *   don't send a ResultComplete when the ExecuteThread returns.
   */
  private def shouldDelegateCompleteResponse(request: proto.ExecutePlanRequest): Boolean = {
    request.getPlan.getOpTypeCase == proto.Plan.OpTypeCase.COMMAND &&
    request.getPlan.getCommand.getCommandTypeCase ==
      proto.Command.CommandTypeCase.STREAMING_QUERY_LISTENER_BUS_COMMAND &&
      request.getPlan.getCommand.getStreamingQueryListenerBusCommand.getCommandCase ==
      proto.StreamingQueryListenerBusCommand.CommandCase.ADD_LISTENER_BUS_LISTENER
  }

  private def handlePlan(request: proto.ExecutePlanRequest): Unit = {
    val responseObserver = executeHolder.responseObserver

    val execution = new SparkConnectPlanExecution(executeHolder)
    execution.handlePlan(responseObserver)
  }

  private def handleCommand(request: proto.ExecutePlanRequest): Unit = {
    val responseObserver = executeHolder.responseObserver

    val command = request.getPlan.getCommand
    val planner = new SparkConnectPlanner(executeHolder)
    planner.process(command = command, responseObserver = responseObserver)
  }

  private def requestString(request: Message) = {
    try {
      Utils.redact(
        executeHolder.sessionHolder.session.sessionState.conf.stringRedactionPattern,
        ProtoUtils.abbreviate(request, maxLevel = 8).toString)
    } catch {
      case NonFatal(e) =>
        logWarning("Fail to extract debug information", e)
        "UNKNOWN"
    }
  }

  private def createResultComplete(): proto.ExecutePlanResponse = {
    // Send the Spark data type
    proto.ExecutePlanResponse
      .newBuilder()
      .setResultComplete(proto.ExecutePlanResponse.ResultComplete.newBuilder().build())
      .build()
  }

  private class ExecutionThread()
      extends Thread(s"SparkConnectExecuteThread_opId=${executeHolder.operationId}") {
    override def run(): Unit = execute()
  }
}

/**
 * Defines possible execution thread states.
 *
 * The state transitions as follows.
 *   - notStarted -> interrupted.
 *   - notStarted -> started -> startedInterrupted -> completed.
 *   - notStarted -> started -> completed.
 *
 * The thread can only be interrupted if the thread is in the startedInterrupted state.
 */
private object ThreadState {

  /** The thread has not started: transition to interrupted or started. */
  val notStarted: Int = 0

  /** Execution was interrupted: terminal state. */
  val interrupted: Int = 1

  /** The thread has started: transition to startedInterrupted or completed. */
  val started: Int = 2

  /** The thread has started and execution was interrupted: transition to completed. */
  val startedInterrupted: Int = 3

  /** Execution was completed: terminal state. */
  val completed: Int = 4
}
