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

import scala.jdk.CollectionConverters._

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.storage.CacheId

class SparkConnectArtifactStatusesHandler(
    val responseObserver: StreamObserver[proto.ArtifactStatusesResponse])
    extends Logging {

  protected def cacheExists(
      userId: String,
      sessionId: String,
      previouslySeenSessionId: Option[String],
      hash: String): Boolean = {
    val session = SparkConnectService
      .getOrCreateIsolatedSession(userId, sessionId, previouslySeenSessionId)
      .session
    val blockManager = session.sparkContext.env.blockManager
    blockManager.getStatus(CacheId(session.sessionUUID, hash)).isDefined
  }

  def handle(request: proto.ArtifactStatusesRequest): Unit = {
    val previousSessionId = request.hasClientObservedServerSideSessionId match {
      case true => Some(request.getClientObservedServerSideSessionId)
      case false => None
    }
    val holder = SparkConnectService
      .getOrCreateIsolatedSession(
        request.getUserContext.getUserId,
        request.getSessionId,
        previousSessionId)

    val builder = proto.ArtifactStatusesResponse.newBuilder()
    builder.setSessionId(holder.sessionId)
    builder.setServerSideSessionId(holder.serverSessionId)
    request.getNamesList().iterator().asScala.foreach { name =>
      val status = proto.ArtifactStatusesResponse.ArtifactStatus.newBuilder()
      val exists = if (name.startsWith("cache/")) {
        cacheExists(
          userId = request.getUserContext.getUserId,
          sessionId = request.getSessionId,
          previouslySeenSessionId = previousSessionId,
          hash = name.stripPrefix("cache/"))
      } else false
      builder.putStatuses(name, status.setExists(exists).build())
    }
    responseObserver.onNext(builder.build())
    responseObserver.onCompleted()
  }
}
