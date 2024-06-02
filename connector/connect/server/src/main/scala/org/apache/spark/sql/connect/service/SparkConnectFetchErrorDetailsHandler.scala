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
import org.apache.spark.connect.proto.FetchErrorDetailsResponse
import org.apache.spark.sql.connect.utils.ErrorUtils

/**
 * Handles [[proto.FetchErrorDetailsRequest]]s for the [[SparkConnectService]]. The handler
 * retrieves the matched error with details from the cache based on a provided error id.
 *
 * @param responseObserver
 */
class SparkConnectFetchErrorDetailsHandler(
    responseObserver: StreamObserver[proto.FetchErrorDetailsResponse]) {

  def handle(v: proto.FetchErrorDetailsRequest): Unit = {
    val previousSessionId = v.hasClientObservedServerSideSessionId match {
      case true => Some(v.getClientObservedServerSideSessionId)
      case false => None
    }
    val sessionHolder =
      SparkConnectService
        .getOrCreateIsolatedSession(v.getUserContext.getUserId, v.getSessionId, previousSessionId)

    val response = Option(sessionHolder.errorIdToError.getIfPresent(v.getErrorId))
      .map { error =>
        // This error can only be fetched once,
        // if a connection dies in the middle you cannot repeat.
        sessionHolder.errorIdToError.invalidate(v.getErrorId)

        ErrorUtils.throwableToFetchErrorDetailsResponse(
          st = error,
          serverStackTraceEnabled = true)
      }
      .getOrElse(FetchErrorDetailsResponse.newBuilder().build())

    responseObserver.onNext(response)

    responseObserver.onCompleted()
  }
}
