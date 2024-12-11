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

import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.common.ProtoUtils
import org.apache.spark.sql.connect.config.Connect.CONNECT_EXECUTE_REATTACHABLE_ENABLED
import org.apache.spark.sql.connect.execution.{ExecuteGrpcResponseSender, ExecuteResponseObserver, ExecuteThreadRunner}
import org.apache.spark.util.SystemClock

/**
 * Object used to hold the Spark Connect execution state.
 */
private[connect] class ExecuteHolder(
    val request: proto.ExecutePlanRequest,
    val sessionHolder: SessionHolder)
    extends Logging {

  val session = sessionHolder.session

  val operationId = if (request.hasOperationId) {
    try {
      UUID.fromString(request.getOperationId).toString
    } catch {
      case _: IllegalArgumentException =>
        throw new SparkSQLException(
          errorClass = "INVALID_HANDLE.FORMAT",
          messageParameters = Map("handle" -> request.getOperationId))
    }
  } else {
    UUID.randomUUID().toString
  }

  /**
   * Tag that is set for this execution on SparkContext, via SparkContext.addJobTag. Used
   * (internally) for cancellation of the Spark Jobs ran by this execution.
   */
  val jobTag = ExecuteJobTag(sessionHolder.userId, sessionHolder.sessionId, operationId)

  /**
   * Tags set by Spark Connect client users via SparkSession.addTag. Used to identify and group
   * executions, and for user cancellation using SparkSession.interruptTag.
   */
  val sparkSessionTags: Set[String] = request
    .getTagsList()
    .asScala
    .toSeq
    .map { tag =>
      ProtoUtils.throwIfInvalidTag(tag)
      tag
    }
    .toSet

  /**
   * If execution is reattachable, it's life cycle is not limited to a single ExecutePlanRequest,
   * but can be reattached with ReattachExecute, and released with ReleaseExecute
   */
  val reattachable: Boolean = {
    SparkEnv.get.conf.get(CONNECT_EXECUTE_REATTACHABLE_ENABLED) &&
    request.getRequestOptionsList.asScala.exists { option =>
      option.hasReattachOptions && option.getReattachOptions.getReattachable == true
    }
  }

  val responseObserver: ExecuteResponseObserver[proto.ExecutePlanResponse] =
    new ExecuteResponseObserver[proto.ExecutePlanResponse](this)

  val eventsManager: ExecuteEventsManager = ExecuteEventsManager(this, new SystemClock())

  private val runner: ExecuteThreadRunner = new ExecuteThreadRunner(this)

  /** System.currentTimeMillis when this ExecuteHolder was created. */
  val creationTime = System.currentTimeMillis()

  /**
   * None if there is currently an attached RPC (grpcResponseSenders not empty or during initial
   * ExecutePlan handler). Otherwise, the System.currentTimeMillis when the last RPC detached
   * (grpcResponseSenders became empty).
   */
  @volatile var lastAttachedRpcTime: Option[Long] = None

  /** System.currentTimeMillis when this ExecuteHolder was closed. */
  private var closedTime: Option[Long] = None

  /**
   * Attached ExecuteGrpcResponseSenders that send the GRPC responses.
   *
   * In most situations at most one, except network hang issues where temporarily there would be a
   * stale one, before being interrupted by a new one in ReattachExecute.
   */
  private val grpcResponseSenders
      : mutable.ArrayBuffer[ExecuteGrpcResponseSender[proto.ExecutePlanResponse]] =
    new mutable.ArrayBuffer[ExecuteGrpcResponseSender[proto.ExecutePlanResponse]]()

  /** For testing. Whether the async completion callback is called. */
  @volatile private[connect] var completionCallbackCalled: Boolean = false

  /**
   * Start the execution. The execution is started in a background thread in ExecuteThreadRunner.
   * Responses are produced and cached in ExecuteResponseObserver. A GRPC thread consumes the
   * responses by attaching an ExecuteGrpcResponseSender,
   * @see
   *   runGrpcResponseSender.
   */
  def start(): Unit = {
    runner.start()
  }

  /**
   * Attach an ExecuteGrpcResponseSender that will consume responses from the query and send them
   * out on the Grpc response stream. The sender will start from the start of the response stream.
   * @param responseSender
   *   the ExecuteGrpcResponseSender
   */
  def runGrpcResponseSender(
      responseSender: ExecuteGrpcResponseSender[proto.ExecutePlanResponse]): Unit = {
    addGrpcResponseSender(responseSender)
    responseSender.run(0)
  }

  /**
   * Attach an ExecuteGrpcResponseSender that will consume responses from the query and send them
   * out on the Grpc response stream.
   * @param responseSender
   *   the ExecuteGrpcResponseSender
   * @param lastConsumedResponseId
   *   the last response that was already consumed. The sender will start from response after
   *   that.
   */
  def runGrpcResponseSender(
      responseSender: ExecuteGrpcResponseSender[proto.ExecutePlanResponse],
      lastConsumedResponseId: String): Unit = {
    val lastConsumedIndex = responseObserver.getResponseIndexById(lastConsumedResponseId)
    addGrpcResponseSender(responseSender)
    responseSender.run(lastConsumedIndex)
  }

  private def addGrpcResponseSender(
      sender: ExecuteGrpcResponseSender[proto.ExecutePlanResponse]) = synchronized {
    if (closedTime.isEmpty) {
      grpcResponseSenders += sender
      lastAttachedRpcTime = None
    } else {
      // execution is closing... interrupt it already.
      sender.interrupt()
    }
  }

  def removeGrpcResponseSender[_](sender: ExecuteGrpcResponseSender[_]): Unit = synchronized {
    // if closed, we are shutting down and interrupting all senders already
    if (closedTime.isEmpty) {
      grpcResponseSenders -=
        sender.asInstanceOf[ExecuteGrpcResponseSender[proto.ExecutePlanResponse]]
      if (grpcResponseSenders.isEmpty) {
        lastAttachedRpcTime = Some(System.currentTimeMillis())
      }
    }
  }

  // For testing.
  private[connect] def setGrpcResponseSendersDeadline(deadlineMs: Long) = synchronized {
    grpcResponseSenders.foreach(_.setDeadline(deadlineMs))
  }

  // For testing
  private[connect] def interruptGrpcResponseSenders() = synchronized {
    grpcResponseSenders.foreach(_.interrupt())
  }

  /**
   * For a short period in ExecutePlan after creation and until runGrpcResponseSender is called,
   * there is no attached response sender, but yet we start with lastAttachedRpcTime = None, so we
   * don't get garbage collected. End this grace period when the initial ExecutePlan ends.
   */
  def afterInitialRPC(): Unit = synchronized {
    if (closedTime.isEmpty) {
      if (grpcResponseSenders.isEmpty) {
        lastAttachedRpcTime = Some(System.currentTimeMillis())
      }
    }
  }

  /**
   * Remove cached responses from the response observer until and including the response with
   * given responseId.
   */
  def releaseUntilResponseId(responseId: String): Unit = {
    responseObserver.removeResponsesUntilId(responseId)
  }

  /**
   * Interrupt the execution. Interrupts the running thread, which cancels all running Spark Jobs
   * and makes the execution throw an OPERATION_CANCELED error.
   * @return
   *   true if it was not interrupted before, false if it was already interrupted.
   */
  def interrupt(): Boolean = {
    runner.interrupt()
  }

  /**
   * Interrupt (if still running) and close the execution.
   *
   * Called only by SparkConnectExecutionManager.removeExecuteHolder, which then also removes the
   * execution from global tracking and from its session.
   */
  def close(): Unit = synchronized {
    if (closedTime.isEmpty) {
      // interrupt execution, if still running.
      runner.interrupt()
      // Do not wait for the execution to finish, clean up resources immediately.
      runner.processOnCompletion { _ =>
        completionCallbackCalled = true
        // The execution may not immediately get interrupted, clean up any remaining resources when
        // it does.
        responseObserver.removeAll()
        // post closed to UI
        try {
          eventsManager.postClosed()
        } catch {
          // Catching the exception to prevent the wrong error code from being returned to the
          // user: SPARK-49688. The issue was fixed by completely refactoring the code in Spark 4.0.
          case e: Throwable if NonFatal.apply(e) =>
            logError(s"Error posting closed event to UI: ${e.getMessage()}")
        }
      }
      // interrupt any attached grpcResponseSenders
      grpcResponseSenders.foreach(_.interrupt())
      // if there were still any grpcResponseSenders, register detach time
      if (grpcResponseSenders.nonEmpty) {
        lastAttachedRpcTime = Some(System.currentTimeMillis())
        grpcResponseSenders.clear()
      }
      // remove all cached responses from observer
      responseObserver.removeAll()
      closedTime = Some(System.currentTimeMillis())
    }
  }

  /**
   * Spark Connect tags are also added as SparkContext job tags, but to make the tag unique, they
   * need to be combined with userId and sessionId.
   */
  def tagToSparkJobTag(tag: String): String = {
    "SparkConnect_Execute_" +
      s"User_${sessionHolder.userId}_Session_${sessionHolder.sessionId}_Tag_${tag}"
  }

  /** Get ExecuteInfo with information about this ExecuteHolder. */
  def getExecuteInfo: ExecuteInfo = synchronized {
    ExecuteInfo(
      request = request,
      userId = sessionHolder.userId,
      sessionId = sessionHolder.sessionId,
      operationId = operationId,
      jobTag = jobTag,
      sparkSessionTags = sparkSessionTags,
      reattachable = reattachable,
      status = eventsManager.status,
      creationTime = creationTime,
      lastAttachedRpcTime = lastAttachedRpcTime,
      closedTime = closedTime)
  }

  /** Get key used by SparkConnectExecutionManager global tracker. */
  def key: ExecuteKey = ExecuteKey(sessionHolder.userId, sessionHolder.sessionId, operationId)
}

/** Used to identify ExecuteHolder jobTag among SparkContext.SPARK_JOB_TAGS. */
object ExecuteJobTag {
  private val prefix = "SparkConnect_OperationTag"

  def apply(userId: String, sessionId: String, operationId: String): String = {
    s"${prefix}_" +
      s"User_${userId}_" +
      s"Session_${sessionId}_" +
      s"Operation_${operationId}"
  }

  def unapply(jobTag: String): Option[String] = {
    if (jobTag.startsWith(prefix)) Some(jobTag) else None
  }
}

/** Used to identify ExecuteHolder sessionTag among SparkContext.SPARK_JOB_TAGS. */
object ExecuteSessionTag {
  private val prefix = "SparkConnect_SessionTag"

  def apply(userId: String, sessionId: String, tag: String): String = {
    ProtoUtils.throwIfInvalidTag(tag)
    s"${prefix}_" +
      s"User_${userId}_" +
      s"Session_${sessionId}_" +
      s"Tag_${tag}"
  }

  def unapply(sessionTag: String): Option[String] = {
    if (sessionTag.startsWith(prefix)) Some(sessionTag) else None
  }
}

/** Information about an ExecuteHolder. */
case class ExecuteInfo(
    request: proto.ExecutePlanRequest,
    userId: String,
    sessionId: String,
    operationId: String,
    jobTag: String,
    sparkSessionTags: Set[String],
    reattachable: Boolean,
    status: ExecuteStatus,
    creationTime: Long,
    lastAttachedRpcTime: Option[Long],
    closedTime: Option[Long])
