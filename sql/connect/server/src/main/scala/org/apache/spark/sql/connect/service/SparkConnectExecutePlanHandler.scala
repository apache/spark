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

import io.grpc.stub.StreamObserver

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.execution.ExecuteGrpcResponseSender

class SparkConnectExecutePlanHandler(responseObserver: StreamObserver[proto.ExecutePlanResponse])
    extends Logging {

  def handle(v: proto.ExecutePlanRequest): Unit = {
    val executeHolder = SparkConnectService.executionManager.createExecuteHolder(v)
    try {
      executeHolder.eventsManager.postStarted()
      executeHolder.start()
    } catch {
      // Errors raised before the execution holder has finished spawning a thread are considered
      // plan execution failure, and the client should not try reattaching it afterwards.
      case s: SparkThrowable =>
        SparkConnectService.executionManager.removeExecuteHolder(executeHolder.key)
        throw s
      case t: Throwable =>
        SparkConnectService.executionManager.removeExecuteHolder(executeHolder.key)
        throw SparkException.internalError(t.getMessage(), t)
    }

    try {
      val responseSender =
        new ExecuteGrpcResponseSender[proto.ExecutePlanResponse](executeHolder, responseObserver)
      executeHolder.runGrpcResponseSender(responseSender)
    } finally {
      executeHolder.afterInitialRPC()
    }
  }
}
