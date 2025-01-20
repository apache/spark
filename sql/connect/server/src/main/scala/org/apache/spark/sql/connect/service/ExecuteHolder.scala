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

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.google.protobuf.GeneratedMessage

import org.apache.spark.SparkEnv
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Observation
import org.apache.spark.sql.connect.common.ProtoUtils
import org.apache.spark.sql.connect.config.Connect.CONNECT_EXECUTE_REATTACHABLE_ENABLED
import org.apache.spark.sql.connect.execution.{ExecuteGrpcResponseSender, ExecuteResponseObserver, ExecuteThreadRunner}
import org.apache.spark.util.SystemClock

/**
 * Object used to hold the Spark Connect execution state.
 */
private[connect] class ExecuteHolder(
    val executeKey: ExecuteKey,
    val request: proto.ExecutePlanRequest,
    val sessionHolder: SessionHolder)
    extends Logging {

  val session = sessionHolder.session

  /**
   * Tag that is set for this execution on SparkContext, via SparkContext.addJobTag. Used
   * (internally) for cancellation of the Spark Jobs ran by this execution.
   */
  val jobTag =
    ExecuteJobTag(sessionHolder.userId, sessionHolder.sessionId, executeKey.operationId)

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

  val observations: mutable.Map[String, Observation] = mutable.Map.empty

  lazy val allObservationAndPlanIds: Map[String, Long] = {
    ExecuteHolder.collectAllObservationAndPlanIds(request.getPlan).toMap
  }

  private val runner: ExecuteThreadRunner = new ExecuteThreadRunner(this)

  /** System.nanoTime when this ExecuteHolder was created. */
  val creationTimeNs = System.nanoTime()

  /**
   * None if there is currently an attached RPC (grpcResponseSenders not empty or during initial
   * ExecutePlan handler). Otherwise, the System.nanoTime when the last RPC detached
   * (grpcResponseSenders became empty).
   */
  @volatile var lastAttachedRpcTimeNs: Option[Long] = None

  /** System.nanoTime when this ExecuteHolder was closed. */
  private var closedTimeNs: Option[Long] = None

  /**
   * Attached ExecuteGrpcResponseSenders that send the GRPC responses.
   *
   * In most situations at most one, except network hang issues where temporarily there would be a
   * stale one, before being interrupted by a new one in ReattachExecute.
   */
  private val grpcResponseSenders
      : mutable.ArrayBuffer[ExecuteGrpcResponseSender[proto.ExecutePlanResponse]] =
    new mutable.ArrayBuffer[ExecuteGrpcResponseSender[proto.ExecutePlanResponse]]()

  /** Indicates whether the cleanup method was called. */
  private[connect] val completionCallbackCalled: AtomicBoolean = new AtomicBoolean(false)

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
   * Check if the execution was ended without finalizing the outcome and no further progress will
   * be made. If the execution was delegated, this method always returns false.
   */
  def isOrphan(): Boolean = {
    !runner.isAlive() &&
    !runner.shouldDelegateCompleteResponse(request) &&
    !responseObserver.completed()
  }

  def addObservation(name: String, observation: Observation): Unit = synchronized {
    observations += (name -> observation)
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
    if (closedTimeNs.isEmpty) {
      // Interrupt all other senders - there can be only one active sender.
      // Interrupted senders will remove themselves with removeGrpcResponseSender when they exit.
      grpcResponseSenders.foreach(_.interrupt())
      // And add this one.
      grpcResponseSenders += sender
      lastAttachedRpcTimeNs = None
    } else {
      // execution is closing... interrupt it already.
      sender.interrupt()
    }
  }

  def removeGrpcResponseSender(sender: ExecuteGrpcResponseSender[_]): Unit = synchronized {
    // if closed, we are shutting down and interrupting all senders already
    if (closedTimeNs.isEmpty) {
      grpcResponseSenders -=
        sender.asInstanceOf[ExecuteGrpcResponseSender[proto.ExecutePlanResponse]]
      if (grpcResponseSenders.isEmpty) {
        lastAttachedRpcTimeNs = Some(System.nanoTime())
      }
    }
  }

  // For testing.
  private[connect] def setGrpcResponseSendersDeadline(deadlineNs: Long) = synchronized {
    grpcResponseSenders.foreach(_.setDeadline(deadlineNs))
  }

  // For testing
  private[connect] def interruptGrpcResponseSenders() = synchronized {
    grpcResponseSenders.foreach(_.interrupt())
  }

  // For testing
  private[connect] def undoResponseObserverCompletion() = synchronized {
    responseObserver.undoCompletion()
  }

  // For testing
  private[connect] def isExecuteThreadRunnerAlive() = {
    runner.isAlive()
  }

  /**
   * For a short period in ExecutePlan after creation and until runGrpcResponseSender is called,
   * there is no attached response sender, but yet we start with lastAttachedRpcTime = None, so we
   * don't get garbage collected. End this grace period when the initial ExecutePlan ends.
   */
  def afterInitialRPC(): Unit = synchronized {
    if (closedTimeNs.isEmpty) {
      if (grpcResponseSenders.isEmpty) {
        lastAttachedRpcTimeNs = Some(System.nanoTime())
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
    if (closedTimeNs.isEmpty) {
      // interrupt execution, if still running.
      val interrupted = runner.interrupt()
      // interrupt any attached grpcResponseSenders
      grpcResponseSenders.foreach(_.interrupt())
      // if there were still any grpcResponseSenders, register detach time
      if (grpcResponseSenders.nonEmpty) {
        lastAttachedRpcTimeNs = Some(System.nanoTime())
        grpcResponseSenders.clear()
      }
      if (!interrupted) {
        cleanup()
      }
      closedTimeNs = Some(System.nanoTime())
    }
  }

  /**
   * A piece of code that is called only once when this execute holder is closed or the
   * interrupted execution thread is terminated.
   */
  private[connect] def cleanup(): Unit = {
    if (completionCallbackCalled.compareAndSet(false, true)) {
      // Remove all cached responses from the observer.
      responseObserver.removeAll()
      // Post "closed" to UI.
      eventsManager.postClosed()
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
      operationId = executeKey.operationId,
      jobTag = jobTag,
      sparkSessionTags = sparkSessionTags,
      reattachable = reattachable,
      status = eventsManager.status,
      creationTimeNs = creationTimeNs,
      lastAttachedRpcTimeNs = lastAttachedRpcTimeNs,
      closedTimeNs = closedTimeNs)
  }

  /** Get key used by SparkConnectExecutionManager global tracker. */
  def key: ExecuteKey = executeKey

  /** Get the operation ID. */
  def operationId: String = key.operationId
}

private object ExecuteHolder {
  private def collectAllObservationAndPlanIds(
      planOrMessage: GeneratedMessage,
      collected: mutable.Map[String, Long] = mutable.Map.empty): mutable.Map[String, Long] = {
    planOrMessage match {
      case relation: proto.Relation if relation.hasCollectMetrics =>
        collected += relation.getCollectMetrics.getName -> relation.getCommon.getPlanId
        collectAllObservationAndPlanIds(relation.getCollectMetrics.getInput, collected)
      case _ =>
        planOrMessage.getAllFields.values().asScala.foreach {
          case message: GeneratedMessage =>
            collectAllObservationAndPlanIds(message, collected)
          case _ =>
          // not a message (probably a primitive type), do nothing
        }
    }
    collected
  }
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
    creationTimeNs: Long,
    lastAttachedRpcTimeNs: Option[Long],
    closedTimeNs: Option[Long]) {

  def key: ExecuteKey = ExecuteKey(userId, sessionId, operationId)
}
