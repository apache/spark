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

import org.apache.spark.SparkSQLException
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.service.ExecuteKey

class SparkConnectReleaseExecuteHandler(
    responseObserver: StreamObserver[proto.ReleaseExecuteResponse])
    extends Logging {

  def handle(v: proto.ReleaseExecuteRequest): Unit = {
    // We do not validate the spark session for ReleaseExecute on the server,
    // leaving the validation only to happen on the client side.
    val sessionHolder = SparkConnectService.sessionManager
      .getIsolatedSession(SessionKey(v.getUserContext.getUserId, v.getSessionId), None)

    val responseBuilder = proto.ReleaseExecuteResponse
      .newBuilder()
      .setSessionId(v.getSessionId)
      .setServerSideSessionId(sessionHolder.serverSessionId)

    // ExecuteHolder may be concurrently released by SparkConnectExecutionManager if the
    // ReleaseExecute arrived after it was abandoned and timed out.
    // An asynchronous ReleastUntil operation may also arrive after ReleaseAll.
    // Because of that, make it noop and not fail if the ExecuteHolder is no longer there.
    val executeKey = ExecuteKey(sessionHolder.userId, sessionHolder.sessionId, v.getOperationId)
    val executeHolderOption =
      SparkConnectService.executionManager.getExecuteHolder(executeKey).foreach { executeHolder =>
        if (!executeHolder.reattachable) {
          throw new SparkSQLException(
            errorClass = "INVALID_CURSOR.NOT_REATTACHABLE",
            messageParameters = Map.empty)
        }

        responseBuilder.setOperationId(executeHolder.operationId)

        if (v.hasReleaseAll) {
          SparkConnectService.executionManager.removeExecuteHolder(executeHolder.key)
        } else if (v.hasReleaseUntil) {
          val responseId = v.getReleaseUntil.getResponseId
          executeHolder.releaseUntilResponseId(responseId)
        } else {
          throw new UnsupportedOperationException(s"Unknown ReleaseExecute type!")
        }
      }

    responseObserver.onNext(responseBuilder.build())
    responseObserver.onCompleted()
  }
}
