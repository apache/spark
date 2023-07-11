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

import org.apache.spark.connect.proto.{ExecutePlanRequest, ExecutePlanResponse}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.execution.ExecutePlanResponseSender

class SparkConnectExecutePlanHandler(responseObserver: StreamObserver[ExecutePlanResponse])
    extends Logging {

  def handle(v: ExecutePlanRequest): Unit = {
    val sessionHolder = SparkConnectService
      .getOrCreateIsolatedSession(v.getUserContext.getUserId, v.getSessionId)
    val executeHolder = sessionHolder.createExecuteHolder(v)

    try {
      executeHolder.start()
      val responseSender = new ExecutePlanResponseSender(responseObserver)
      val detached = executeHolder.attachRpc(responseSender, 0)
      if (detached) {
        // Detached before execution finished.
        // TODO this doesn't happen yet without reattachable execution.
        responseObserver.onCompleted()
      }
    } finally {
      executeHolder.runner.join()
      sessionHolder.removeExecutePlanHolder(executeHolder.operationId)
    }
  }
}
