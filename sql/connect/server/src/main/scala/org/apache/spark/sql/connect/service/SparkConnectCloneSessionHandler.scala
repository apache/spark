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

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging

class SparkConnectCloneSessionHandler(
    responseObserver: StreamObserver[proto.CloneSessionResponse])
    extends Logging {

  def handle(request: proto.CloneSessionRequest): Unit = {
    val sourceKey = SessionKey(request.getUserContext.getUserId, request.getSessionId)
    val newSessionId = if (request.hasNewSessionId && request.getNewSessionId.nonEmpty) {
      request.getNewSessionId
    } else {
      UUID.randomUUID().toString
    }
    val previouslyObservedSessionId =
      if (request.hasClientObservedServerSideSessionId &&
        request.getClientObservedServerSideSessionId.nonEmpty) {
        Some(request.getClientObservedServerSideSessionId)
      } else {
        None
      }
    // Get the original session to retrieve its server session ID for validation
    val originalSessionHolder = SparkConnectService.sessionManager
      .getIsolatedSession(sourceKey, previouslyObservedSessionId)
    val clonedSessionHolder = SparkConnectService.sessionManager.cloneSession(
      sourceKey,
      newSessionId,
      previouslyObservedSessionId)
    val response = proto.CloneSessionResponse
      .newBuilder()
      .setSessionId(request.getSessionId)
      .setNewSessionId(clonedSessionHolder.sessionId)
      .setServerSideSessionId(originalSessionHolder.serverSessionId)
      .setNewServerSideSessionId(clonedSessionHolder.serverSessionId)
      .build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}
