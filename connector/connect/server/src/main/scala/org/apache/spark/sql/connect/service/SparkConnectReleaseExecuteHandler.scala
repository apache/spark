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

class SparkConnectReleaseExecuteHandler(
    responseObserver: StreamObserver[proto.ReleaseExecuteResponse])
    extends Logging {

  def handle(v: proto.ReleaseExecuteRequest): Unit = {
    val sessionHolder = SparkConnectService
      .getIsolatedSession(v.getUserContext.getUserId, v.getSessionId)
    val executeHolder = sessionHolder.executeHolder(v.getOperationId).getOrElse {
      throw new SparkSQLException(
        errorClass = "INVALID_HANDLE.OPERATION_NOT_FOUND",
        messageParameters = Map("handle" -> v.getOperationId))
    }
    if (!executeHolder.reattachable) {
      throw new SparkSQLException(
        errorClass = "INVALID_CURSOR.NOT_REATTACHABLE",
        messageParameters = Map.empty)
    }

    if (v.hasReleaseAll) {
      executeHolder.close()
    } else if (v.hasReleaseUntil) {
      val responseId = v.getReleaseUntil.getResponseId
      executeHolder.releaseUntilResponseId(responseId)
    } else {
      throw new UnsupportedOperationException(s"Unknown ReleaseExecute type!")
    }

    val response = proto.ReleaseExecuteResponse
      .newBuilder()
      .setSessionId(v.getSessionId)
      .setOperationId(v.getOperationId)
      .build()

    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}
