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

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging

class SparkConnectReleaseSessionHandler(
    responseObserver: StreamObserver[proto.ReleaseSessionResponse])
    extends Logging {

  def handle(v: proto.ReleaseSessionRequest): Unit = {
    val responseBuilder = proto.ReleaseSessionResponse.newBuilder()
    responseBuilder.setSessionId(v.getSessionId)

    // If the session doesn't exist, this will just be a noop.
    val key = SessionKey(v.getUserContext.getUserId, v.getSessionId)
    // if the session is present, update the server-side session ID.
    // Note we do not validate the previously seen server-side session id.
    val maybeSession = SparkConnectService.sessionManager.getIsolatedSessionIfPresent(key)
    maybeSession.foreach(f => responseBuilder.setServerSideSessionId(f.serverSessionId))

    val allowReconnect = v.getAllowReconnect
    SparkConnectService.sessionManager.closeSession(key, allowReconnect)

    responseObserver.onNext(responseBuilder.build())
    responseObserver.onCompleted()
  }
}
