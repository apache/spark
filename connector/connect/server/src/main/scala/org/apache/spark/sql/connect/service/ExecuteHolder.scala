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

import org.apache.spark.SparkContext
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
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

  val jobTag =
    s"SparkConnect_Execute_" +
      s"User_${sessionHolder.userId}_" +
      s"Session_${sessionHolder.sessionId}_" +
      s"Request_${operationId}"

  val userDefinedTags: Seq[String] = request.getTagsList().asScala.toSeq.map { tag =>
    throwIfInvalidTag(tag)
    tag
  }

  val session = sessionHolder.session

  val responseObserver: ExecuteResponseObserver[proto.ExecutePlanResponse] =
    new ExecuteResponseObserver[proto.ExecutePlanResponse]()

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
   */
  def interrupt(): Unit = {
    runner.interrupt()
  }

  def tagToSparkJobTag(tag: String): String = {
    "SparkConnectUserDefinedTag_" +
      s"User_${sessionHolder.userId}_Session_${sessionHolder.sessionId}"
  }

  private def throwIfInvalidTag(tag: String) = {
    // Same format rules apply to Spark Connect execution tags as to SparkContext job tags.
    // see SparkContext.throwIfInvalidTag.
    if (tag == null) {
      throw new IllegalArgumentException("Spark Connect execution tag cannot be null.")
    }
    if (tag.contains(SparkContext.SPARK_JOB_TAGS_SEP)) {
      throw new IllegalArgumentException(
        s"Spark Connect execution tag cannot contain '${SparkContext.SPARK_JOB_TAGS_SEP}'.")
    }
    if (tag.isEmpty) {
      throw new IllegalArgumentException("Spark Connect execution tag cannot be an empty string.")
    }
  }
}
