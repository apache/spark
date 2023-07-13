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

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.execution.{ExecuteGrpcResponseSender, ExecuteResponseObserver, ExecuteThreadRunner}

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

  val session = sessionHolder.session

  var responseObserver: ExecuteResponseObserver[proto.ExecutePlanResponse] =
    new ExecuteResponseObserver[proto.ExecutePlanResponse]()

  var runner: ExecuteThreadRunner = new ExecuteThreadRunner(this)

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
}
