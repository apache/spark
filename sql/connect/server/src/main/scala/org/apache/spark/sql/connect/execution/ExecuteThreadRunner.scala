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

import scala.concurrent.{ExecutionContext, Promise}
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

import com.google.protobuf.Message
import org.apache.commons.lang3.StringUtils

import org.apache.spark.SparkSQLException
import org.apache.spark.connect.proto
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.connect.common.ProtoUtils
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.{ExecuteHolder, ExecuteSessionTag, SparkConnectService}
import org.apache.spark.sql.connect.utils.ErrorUtils
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * This class launches the actual execution in an execution thread. The execution pushes the
 * responses to a ExecuteResponseObserver in executeHolder.
 */
private[connect] class ExecuteThreadRunner(executeHolder: ExecuteHolder) extends Logging {

  private val promise: Promise[Unit] = Promise[Unit]()

  // The newly created thread will inherit all InheritableThreadLocals used by Spark,
  // e.g. SparkContext.localProperties. If considering implementing a thread-pool,
  // forwarding of thread locals needs to be taken into account.
  private val executionThread: ExecutionThread = new ExecutionThread(promise)

  private var started: Boolean = false

  private var interrupted: Boolean = false

  private var completed: Boolean = false

  private val lock = new Object

  /** Launches the execution in a background thread, returns immediately. */
  private[connect] def start(): Unit = {
    lock.synchronized {
      assert(!started)
      // Do not start if already interrupted.
      if (!interrupted) {
        executionThread.start()
        started = true
      }
    }
  }

  /**
   * Register a callback that gets executed after completion/interruption of the execution thread.
   */
  private[connect] def processOnCompletion(callback: Try[Unit] => Unit): Unit = {
    promise.future.onComplete(callback)(ExecuteThreadRunner.namedExecutionContext)
  }

  /**
   * Interrupt the executing thread.
   * @return
   *   true if it was not interrupted before, false if it was already interrupted or completed.
   */
  private[connect] def interrupt(): Boolean = {
    lock.synchronized {
      if (!started && !interrupted) {
        // execution thread hasn't started yet, and will not be started.
        // handle the interrupted error here directly.
        interrupted = true
        ErrorUtils.handleError(
          "execute",
          executeHolder.responseObserver,
          executeHolder.sessionHolder.userId,
          executeHolder.sessionHolder.sessionId,
          Some(executeHolder.eventsManager),
          interrupted)(new SparkSQLException("OPERATION_CANCELED", Map.empty))
        true
      } else if (!interrupted && !completed) {
        // checking completed prevents sending interrupt onError after onCompleted
        interrupted = true
        executionThread.interrupt()
        true
      } else {
        false
      }
    }
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
          logDebug(log"Exception in execute: ${MDC(LogKeys.EXCEPTION, e)}")
          // Always cancel all remaining execution after error.
          executeHolder.sessionHolder.session.sparkContext.cancelJobsWithTag(
            executeHolder.jobTag,
            s"A job with the same tag ${executeHolder.jobTag} has failed.")
          // Rely on an internal interrupted flag, because Thread.interrupted() could be cleared,
          // and different exceptions like InterruptedException, ClosedByInterruptException etc.
          // could be thrown.
          if (interrupted) {
            throw new SparkSQLException("OPERATION_CANCELED", Map.empty)
          } else {
            // Rethrown the original error.
            throw e
          }
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
      ErrorUtils.handleError(
        "execute",
        executeHolder.responseObserver,
        executeHolder.sessionHolder.userId,
        executeHolder.sessionHolder.sessionId,
        Some(executeHolder.eventsManager),
        interrupted)
    }
  }

  // Inner executeInternal is wrapped by execute() for error handling.
  private def executeInternal() = {
    // synchronized - check if already got interrupted while starting.
    lock.synchronized {
      if (interrupted) {
        throw new InterruptedException()
      }
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

      lock.synchronized {
        // Synchronized before sending ResultComplete, and up until completing the result stream
        // to prevent a situation in which a client of reattachable execution receives
        // ResultComplete, and proceeds to send ReleaseExecute, and that triggers an interrupt
        // before it finishes.

        if (interrupted) {
          // check if it got interrupted at the very last moment
          throw new InterruptedException()
        }
        completed = true // no longer interruptible

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
        logWarning(log"Fail to extract debug information: ${MDC(LogKeys.EXCEPTION, e)}")
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

  private class ExecutionThread(onCompletionPromise: Promise[Unit])
      extends Thread(s"SparkConnectExecuteThread_opId=${executeHolder.operationId}") {
    override def run(): Unit = {
      try {
        execute()
        onCompletionPromise.success(())
      } catch {
        case NonFatal(e) =>
          onCompletionPromise.failure(e)
      }
    }
  }
}

private[connect] object ExecuteThreadRunner {
  private implicit val namedExecutionContext: ExecutionContext = ExecutionContext
    .fromExecutor(ThreadUtils.newDaemonSingleThreadExecutor("SparkConnectExecuteThreadCallback"))
}
