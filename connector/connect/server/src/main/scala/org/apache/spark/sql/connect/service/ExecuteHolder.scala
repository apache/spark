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

import org.apache.spark.SparkSQLException
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.common.ProtoUtils
import org.apache.spark.sql.connect.execution.{ExecuteGrpcResponseSender, ExecuteResponseObserver, ExecuteThreadRunner}
import org.apache.spark.util.SystemClock

/**
 * Object used to hold the Spark Connect execution state.
 */
private[connect] class ExecuteHolder(
    val request: proto.ExecutePlanRequest,
    val sessionHolder: SessionHolder)
    extends Logging {

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
   * (internally) for cancallation of the Spark Jobs ran by this execution.
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
  val reattachable: Boolean = request.getRequestOptionsList.asScala.exists { option =>
    option.hasReattachOptions && option.getReattachOptions.getReattachable == true
  }

  /**
   * True if there is currently an RPC (ExecutePlanRequest, ReattachExecute) attached to this
   * execution.
   */
  var attached: Boolean = true

  val session = sessionHolder.session

  val responseObserver: ExecuteResponseObserver[proto.ExecutePlanResponse] =
    new ExecuteResponseObserver[proto.ExecutePlanResponse](this)

  val eventsManager: ExecuteEventsManager = ExecuteEventsManager(this, new SystemClock())

  private val runner: ExecuteThreadRunner = new ExecuteThreadRunner(this)

  /**
   * Start the execution. The execution is started in a background thread in ExecuteThreadRunner.
   * Responses are produced and cached in ExecuteResponseObserver. A GRPC thread consumes the
   * responses by attaching an ExecuteGrpcResponseSender,
   * @see
   *   attachAndRunGrpcResponseSender.
   */
  def start(): Unit = {
    runner.start()
  }

  /**
   * Wait for the execution thread to finish and join it.
   */
  def join(): Unit = {
    runner.join()
  }

  /**
   * Attach an ExecuteGrpcResponseSender that will consume responses from the query and send them
   * out on the Grpc response stream. The sender will start from the start of the response stream.
   * @param responseSender
   *   the ExecuteGrpcResponseSender
   */
  def attachAndRunGrpcResponseSender(
      responseSender: ExecuteGrpcResponseSender[proto.ExecutePlanResponse]): Unit = {
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
  def attachAndRunGrpcResponseSender(
      responseSender: ExecuteGrpcResponseSender[proto.ExecutePlanResponse],
      lastConsumedResponseId: String): Unit = {
    val lastConsumedIndex = responseObserver.getResponseIndexById(lastConsumedResponseId)
    responseSender.run(lastConsumedIndex)
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
   * Close the execution and remove it from the session. Note: It blocks joining the
   * ExecuteThreadRunner thread, so it assumes that it's called when the execution is ending or
   * ended. If it is desired to kill the execution, interrupt() should be called first.
   */
  def close(): Unit = {
    runner.join()
    eventsManager.postClosed()
    sessionHolder.removeExecuteHolder(operationId)
  }

  /**
   * Spark Connect tags are also added as SparkContext job tags, but to make the tag unique, they
   * need to be combined with userId and sessionId.
   */
  def tagToSparkJobTag(tag: String): String = {
    "SparkConnect_Execute_" +
      s"User_${sessionHolder.userId}_Session_${sessionHolder.sessionId}_Tag_${tag}"
  }
}

/** Used to identify ExecuteHolder jobTag among SparkContext.SPARK_JOB_TAGS. */
object ExecuteJobTag {
  private val prefix = "SparkConnect_OperationTag"

  def apply(sessionId: String, userId: String, operationId: String): String = {
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
