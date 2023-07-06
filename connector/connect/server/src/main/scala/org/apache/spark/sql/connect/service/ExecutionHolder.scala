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

package org.apache.spark.sql.connect.service

import scala.util.control.NonFatal

import com.google.protobuf.Message
import io.grpc.stub.StreamObserver
import org.apache.commons.lang3.StringUtils

import org.apache.spark.SparkSQLException
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{ExecutePlanRequest, ExecutePlanResponse}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.common.ProtoUtils
import org.apache.spark.sql.connect.execution.{ExecutePlanResponseObserver, SparkConnectPlanExecution}
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.util.Utils

/**
 * Object used to hold the Spark Connect execution state, and perform
 */
case class ExecutionHolder(operationId: String, sessionHolder: SessionHolder) extends Logging {

  val jobTag =
    s"User_${sessionHolder.userId}_Session_${sessionHolder.sessionId}_Request_${operationId}"

  val session = sessionHolder.session

  var executePlanRequest: Option[proto.ExecutePlanRequest] = None

  var executePlanResponseObserver: Option[ExecutePlanResponseObserver] = None

  private var executionThread: Thread = null

  private var executionError: Option[Throwable] = None

  private var interrupted: Boolean = false

  def run(
      request: proto.ExecutePlanRequest,
      responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
    // Set the state of what needs to be run.
    this.executePlanRequest = Some(request)
    this.executePlanResponseObserver = Some(new ExecutePlanResponseObserver(responseObserver))
    // And start the execution.
    startExecute()
  }

  protected def startExecute(): Unit = {
    // synchronized in case of interrupt while starting.
    synchronized {
      // The newly created thread will inherit all InheritableThreadLocals used by Spark,
      // e.g. SparkContext.localProperties./ If considering implementing a threadpool,
      // forwarding of thread locals needs to be taken into account.
      this.executionThread = new Thread() {
        override def run(): Unit = {
          execute()
        }
      }
    }

    try {
      // Start execution thread..
      this.executionThread.start()
      // ... and wait for execution thread to finish.
      // TODO: Detach execution from RPC request. Then this can return early, and results
      // are served to the client via additional RPCs from ExecutePlanResponseObserver.
      this.executionThread.join()

      executionError.foreach { error =>
        logDebug(s"executionError: ${error}")
        throw error
      }
    } catch {
      case NonFatal(e) =>
        // In case of exception happening on the handler thread, interrupt the underlying execution.
        this.interrupt()
        throw e
    }
  }

  protected def execute() = {
    try {
      // synchronized - check if already got interrupted while starting.
      synchronized {
        if (interrupted) {
          throw new InterruptedException()
        }
      }

      // `withSession` ensures that session-specific artifacts (such as JARs and class files) are
      // available during processing.
      sessionHolder.withSession { session =>
        val debugString = requestString(executePlanRequest.get)

        // Set tag for query cancellation
        session.sparkContext.addJobTag(jobTag)
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

        executePlanRequest.foreach { request =>
          request.getPlan.getOpTypeCase match {
            case proto.Plan.OpTypeCase.COMMAND => handleCommand(request)
            case proto.Plan.OpTypeCase.ROOT => handlePlan(request)
            case _ =>
              throw new UnsupportedOperationException(
                s"${request.getPlan.getOpTypeCase} not supported.")
          }
        }
      }
    } catch {
      // Actually do need to catch Throwable as some failures don't inherit from Exception and
      // HiveServer will silently swallow them.
      case e: Throwable =>
        // scalastyle:off
        logDebug(s"Exception in execute: $e")
        // Always cancel all remaining execution after error.
        sessionHolder.session.sparkContext.cancelJobsWithTag(jobTag)
        executionError = if (interrupted) {
          // Turn the interrupt into OPERATION_CANCELLED error.
          Some(new SparkSQLException("OPERATION_CANCELLED", Map.empty))
        } else {
          // Return the originally thrown error.
          Some(e)
        }
    } finally {
      session.sparkContext.removeJobTag(jobTag)
    }
  }

  def interrupt(): Unit = {
    synchronized {
      interrupted = true
      if (executionThread != null) {
        executionThread.interrupt()
      }
    }
  }

  private def handlePlan(request: ExecutePlanRequest): Unit = {
    val request = executePlanRequest.get
    val responseObserver = executePlanResponseObserver.get

    val execution = new SparkConnectPlanExecution(this)
    execution.handlePlan(responseObserver)
  }

  private def handleCommand(request: ExecutePlanRequest): Unit = {
    val request = executePlanRequest.get
    val responseObserver = executePlanResponseObserver.get

    val command = request.getPlan.getCommand
    val planner = new SparkConnectPlanner(sessionHolder)
    planner.process(
      command = command,
      userId = request.getUserContext.getUserId,
      sessionId = request.getSessionId,
      responseObserver = responseObserver)
    responseObserver.onCompleted()
  }

  private def requestString(request: Message) = {
    try {
      Utils.redact(
        sessionHolder.session.sessionState.conf.stringRedactionPattern,
        ProtoUtils.abbreviate(request).toString)
    } catch {
      case NonFatal(e) =>
        logWarning("Fail to extract debug information", e)
        "UNKNOWN"
    }
  }
}
