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

class SparkConnectInterruptHandler(responseObserver: StreamObserver[proto.InterruptResponse])
    extends Logging {

  def handle(v: proto.InterruptRequest): Unit = {
    val previousSessionId = v.hasClientObservedServerSideSessionId match {
      case true => Some(v.getClientObservedServerSideSessionId)
      case false => None
    }
    val sessionHolder =
      SparkConnectService
        .getOrCreateIsolatedSession(v.getUserContext.getUserId, v.getSessionId, previousSessionId)

    val interruptedIds = v.getInterruptType match {
      case proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL =>
        sessionHolder.interruptAll()
      case proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_TAG =>
        if (!v.hasOperationTag) {
          throw new IllegalArgumentException(
            s"INTERRUPT_TYPE_TAG requested, but no operation_tag provided.")
        }
        sessionHolder.interruptTag(v.getOperationTag)
      case proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_OPERATION_ID =>
        if (!v.hasOperationId) {
          throw new IllegalArgumentException(
            s"INTERRUPT_TYPE_OPERATION_ID requested, but no operation_id provided.")
        }
        sessionHolder.interruptOperation(v.getOperationId)
      case other =>
        throw new UnsupportedOperationException(s"Unknown InterruptType $other!")
    }

    val response = proto.InterruptResponse
      .newBuilder()
      .setSessionId(v.getSessionId)
      .setServerSideSessionId(sessionHolder.serverSessionId)
      .addAllInterruptedIds(interruptedIds.asJava)
      .build()

    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}
