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

import scala.collection.JavaConverters._

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
    val operationId: String,
    val sessionHolder: SessionHolder)
    extends Logging {

  /**
   * Tag that is set for this execution on SparkContext, via SparkContext.addJobTag. Used
   * (internally) for cancallation of the Spark Jobs ran by this execution.
   */
  val jobTag =
    s"SparkConnect_Execute_" +
      s"User_${sessionHolder.userId}_" +
      s"Session_${sessionHolder.sessionId}_" +
      s"Operation_${operationId}"

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
   * out on the Grpc response stream.
   * @param responseSender
   *   the ExecuteGrpcResponseSender
   * @param lastConsumedStreamIndex
   *   the last index that was already consumed. The consumer will start from index after that. 0
   *   means start from beginning (since first response has index 1)
   * @return
   *   true if the sender got detached without completing the stream. false if the executing
   *   stream was completely sent out.
   */
  def attachAndRunGrpcResponseSender(
      responseSender: ExecuteGrpcResponseSender[proto.ExecutePlanResponse],
      lastConsumedStreamIndex: Long): Boolean = {
    responseSender.run(responseObserver, lastConsumedStreamIndex)
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
   * Spark Connect tags are also added as SparkContext job tags, but to make the tag unique, they
   * need to be combined with userId and sessionId.
   */
  def tagToSparkJobTag(tag: String): String = {
    "SparkConnect_Execute_" +
      s"User_${sessionHolder.userId}_Session_${sessionHolder.sessionId}_Tag_${tag}"
  }
}
